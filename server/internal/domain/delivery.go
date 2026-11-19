package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// DeliveryAuth carries the caller's passthrough credentials into a
// delivery. Agents use this to act on behalf of the caller (e.g.,
// bootstrapping RBAC for the user who created a cluster).
//
// Cryptographic provenance is stored on [Deployment.Provenance], not
// here. When provenance is present the orchestration assembles a
// self-contained [Attestation] and passes it to the delivery agent
// separately.
type DeliveryAuth struct {
	Caller   *SubjectClaims // identity of the user who initiated the delivery
	Audience []Audience     // token audience; used to derive target OIDC client ID
	// TODO: Never store this; should eventually refer to in memory or pause if unavailable
	Token RawToken // verified JWT; agents use for passthrough to target APIs
}

// DeliveryState indicates where a delivery is in its lifecycle.
type DeliveryState string

const (
	DeliveryStatePending     DeliveryState = "pending"
	DeliveryStateAccepted    DeliveryState = "accepted"
	DeliveryStateProgressing DeliveryState = "progressing"
	DeliveryStateDelivered   DeliveryState = "delivered"
	DeliveryStateFailed      DeliveryState = "failed"
	DeliveryStatePartial     DeliveryState = "partial"
	DeliveryStateAuthFailed  DeliveryState = "auth_failed"
)

// DeliveryOperation indicates the type of delivery operation.
type DeliveryOperation string

const (
	DeliveryOperationDeliver DeliveryOperation = "deliver"
	DeliveryOperationRemove  DeliveryOperation = "remove"
)

// IsTerminal reports whether the state represents a completed delivery
// that should not transition further.
func (s DeliveryState) IsTerminal() bool {
	switch s {
	case DeliveryStateDelivered, DeliveryStateFailed,
		DeliveryStatePartial, DeliveryStateAuthFailed:
		return true
	default:
		return false
	}
}

// deliveryStateOrder defines the forward-only ordering of non-terminal
// delivery states. Terminal states are not ordered relative to each
// other; they are reachable from any non-terminal state.
var deliveryStateOrder = map[DeliveryState]int{
	DeliveryStatePending:     0,
	DeliveryStateAccepted:    1,
	DeliveryStateProgressing: 2,
}

// knownDeliveryStates is the complete set of valid delivery states.
// Used by [Delivery.TransitionTo] to reject unknown values.
var knownDeliveryStates = map[DeliveryState]struct{}{
	DeliveryStatePending:     {},
	DeliveryStateAccepted:    {},
	DeliveryStateProgressing: {},
	DeliveryStateDelivered:   {},
	DeliveryStateFailed:      {},
	DeliveryStatePartial:     {},
	DeliveryStateAuthFailed:  {},
}

// Delivery is a first-class entity capturing a single
// fulfillment-to-target delivery and its lifecycle.
//
// Construct new instances with [NewDelivery]; reconstitute from
// persistence with [DeliveryFromSnapshot]. Mutations go through domain
// methods; reads go through accessor methods.
type Delivery struct {
	id            DeliveryID
	fulfillmentID FulfillmentID
	targetID      TargetID
	manifests     []Manifest
	generation    Generation // fulfillment generation at dispatch; used for stale-delivery fencing
	state         DeliveryState
	operation     DeliveryOperation
	message       string // outcome message of the last failed attempt; empty otherwise
	attempts      int    // failed attempts in the current cycle; reset when a new one starts
	createdAt     time.Time
	updatedAt     time.Time
}

// NewDelivery creates a brand-new [Delivery] in the
// [DeliveryStatePending] lifecycle state. Use this on creation paths;
// use [DeliveryFromSnapshot] only for reconstituting from persistence.
func NewDelivery(id DeliveryID, fulfillmentID FulfillmentID, targetID TargetID, manifests []Manifest, generation Generation, now time.Time) Delivery {
	return Delivery{
		id:            id,
		fulfillmentID: fulfillmentID,
		targetID:      targetID,
		manifests:     manifests,
		generation:    generation,
		state:         DeliveryStatePending,
		operation:     DeliveryOperationDeliver,
		createdAt:     now,
		updatedAt:     now,
	}
}

// TransitionTo moves the delivery to the given state if the transition
// is legal. It returns [ErrIllegalStateTransition] if the current state
// does not permit the requested transition (e.g. from a terminal
// state, or backward along the lifecycle). A same-state transition is
// a no-op (no error, no timestamp update).
func (d *Delivery) TransitionTo(state DeliveryState, now time.Time) error {
	if _, ok := knownDeliveryStates[state]; !ok {
		return fmt.Errorf("%w: unknown delivery state %q", ErrIllegalStateTransition, state)
	}
	if d.state == state {
		return nil
	}
	if d.state.IsTerminal() {
		return fmt.Errorf(
			"%w: delivery %s is in terminal state %q, cannot transition to %q",
			ErrIllegalStateTransition, d.id, d.state, state)
	}
	if !state.IsTerminal() {
		fromOrd, fromOk := deliveryStateOrder[d.state]
		toOrd, toOk := deliveryStateOrder[state]
		if fromOk && toOk && toOrd <= fromOrd {
			return fmt.Errorf(
				"%w: delivery %s cannot move backward from %q to %q",
				ErrIllegalStateTransition, d.id, d.state, state)
		}
	}
	d.state = state
	d.updatedAt = now
	return nil
}

// ApplyResult records the outcome of a delivery attempt reported by the
// addon. It transitions to the result's state (rejecting illegal
// transitions) and, when that transition is accepted, records the
// failure details the API surfaces to the user: the message of a
// failed, partial, or auth-failed attempt is kept and the attempt is
// counted; a delivered attempt clears the message. A report repeating
// the current state is a no-op, so duplicate reports do not inflate the
// attempt count.
func (d *Delivery) ApplyResult(result DeliveryResult, now time.Time) error {
	previous := d.state
	if err := d.TransitionTo(result.State, now); err != nil {
		return err
	}
	if d.state == previous {
		return nil
	}
	switch d.state {
	case DeliveryStateFailed, DeliveryStatePartial, DeliveryStateAuthFailed:
		d.message = result.Message
		d.attempts++
	case DeliveryStateDelivered:
		d.message = ""
	}
	return nil
}

// Redispatch resets the delivery for a new put cycle. The manifests are
// replaced, the lifecycle restarts from [DeliveryStatePending], and the
// failure details of the previous cycle are cleared.
//
// The cycle identity is the generation and the operation. The new put
// cycle either advances the generation, or — at the same generation —
// turns a record that was removing the target back into a delivery
// because the target re-entered placement. A record that is already a
// put cycle at the same generation is not redispatched this way; its
// in-flight attempt is handled by [Delivery.Retry] or
// [Delivery.ResetForRetry].
//
// Returns [ErrIllegalStateTransition] if generation moves backwards or
// the delivery is already a put cycle at that generation.
func (d *Delivery) Redispatch(manifests []Manifest, generation Generation, now time.Time) error {
	if generation < d.generation {
		return fmt.Errorf("%w: cannot redispatch at generation %d (current %d)",
			ErrIllegalStateTransition, generation, d.generation)
	}
	if generation == d.generation && d.operation == DeliveryOperationDeliver {
		return fmt.Errorf("%w: delivery at generation %d is already a put cycle",
			ErrIllegalStateTransition, generation)
	}
	d.manifests = manifests
	d.generation = generation
	d.state = DeliveryStatePending
	d.operation = DeliveryOperationDeliver
	d.message = ""
	d.attempts = 0
	d.updatedAt = now
	return nil
}

// Withdraw prepares the delivery for a removal cycle. The manifests
// are preserved — the withdrawal targets whatever was actually
// delivered to the target. The generation is advanced to the given
// value so that the staleness fence used by [DeliveryReporter] and
// orchestration signals correctly identifies this removal cycle. A
// target is withdrawn when it leaves placement (e.g. label change,
// pool mutation, fulfillment deletion).
//
// A removal attempt that fails is retried at the same generation; the
// failure reason and attempt count are preserved then, so the API keeps
// reporting why the removal is not done and the retry waits longer. Any
// other record — a new generation, a put cycle, or a put cycle that
// only now becomes a removal — starts a fresh removal cycle with no
// failure history to carry.
//
// Returns (true, nil) when the delivery was modified (reset to
// [DeliveryStatePending] with [DeliveryOperationRemove]). Returns
// (false, nil) when no modification is needed: the delivery is already
// in a removal cycle at the same generation and did not fail. That is a
// pending removal (a retry, handled by [Delivery.Retry]), one already
// in progress (acked, completion signal queued), or a terminal removal
// (completed, partial, or auth-failed), which is never re-dispatched at
// the same generation.
//
// Returns an error if the given generation would move backwards.
//
// TODO: we advance generation but don't update manifests; might want a specific state for this
// In general the identity of Delivery seems possibly fragile; we might want something
// to differentiate between all of the fulfillment, target, and generation.
func (d *Delivery) Withdraw(generation Generation, now time.Time) (bool, error) {
	if generation < d.generation {
		return false, fmt.Errorf("%w: withdraw generation %d is older than current %d",
			ErrIllegalStateTransition, generation, d.generation)
	}

	// Already removing at this generation without a failed attempt: pending
	// (retry case handled by Retry), in progress (acked, signal queued), or
	// terminal. A terminal removal completed its work and is not resurrected
	// while the rest of the delete pipeline retries — that would dispatch a
	// duplicate removal and report a finished target as outstanding. Only a
	// failed removal falls through to the same-generation retry below.
	if generation == d.generation && d.operation == DeliveryOperationRemove && d.state != DeliveryStateFailed {
		return false, nil
	}

	// A same-generation reset of a removal is the removal path's retry
	// after a failed attempt: like [Delivery.ResetForRetry] for the put
	// path, the reason and attempt count stay so the API keeps explaining
	// the failure and the retry backoff keeps growing. Anything else is a
	// fresh cycle — a new generation, or a put-cycle record turning into a
	// removal because the target left placement mid-retry — and starts
	// with no failure history to carry.
	retry := generation == d.generation && d.operation == DeliveryOperationRemove
	d.state = DeliveryStatePending
	d.generation = generation
	d.operation = DeliveryOperationRemove
	if !retry {
		d.message = ""
		d.attempts = 0
	}
	d.updatedAt = now
	return true, nil
}

// ResetForRetry resets a failed delivery back to
// [DeliveryStatePending] for at-least-once re-dispatch after a workflow
// ContinueAsNew restart. Only a failed attempt is retryable: delivered,
// partial, and auth_failed are terminal observations of the cycle and
// are never re-dispatched at the same generation — a new generation
// starts a fresh cycle instead, and auth_failed pauses the fulfillment
// for fresh credentials. The failure message and attempt count are
// preserved: the API keeps reporting why the last attempt failed while
// the next one runs, and the retry backoff keeps growing.
//
// Returns [ErrIllegalStateTransition] if the delivery is not in
// [DeliveryStateFailed].
func (d *Delivery) ResetForRetry(now time.Time) error {
	if d.state != DeliveryStateFailed {
		return fmt.Errorf(
			"%w: delivery %s is in state %q, only a failed delivery can be reset for retry",
			ErrIllegalStateTransition, d.id, d.state)
	}
	d.state = DeliveryStatePending
	d.updatedAt = now
	return nil
}

// Retry prepares the delivery for a same-generation re-dispatch. It
// returns true if a re-dispatch is needed (i.e. the delivery is still
// [DeliveryStatePending] at the expected generation because the
// previous dispatch failed before the addon received it). Returns
// false if the delivery has already progressed past Pending, or the
// generation doesn't match (stale activity invocation).
func (d *Delivery) Retry(generation Generation, now time.Time) bool {
	if d.generation != generation {
		return false
	}
	if d.state != DeliveryStatePending {
		return false
	}
	d.updatedAt = now
	return true
}

// Accessor methods -- read-only getters for private fields.

// ID returns the delivery's unique identifier.
func (d *Delivery) ID() DeliveryID { return d.id }

// FulfillmentID returns the associated fulfillment's identifier.
func (d *Delivery) FulfillmentID() FulfillmentID { return d.fulfillmentID }

// TargetID returns the delivery target's identifier.
func (d *Delivery) TargetID() TargetID { return d.targetID }

// Manifests returns the delivered manifest payloads.
func (d *Delivery) Manifests() []Manifest { return d.manifests }

// Generation returns the fulfillment generation at dispatch.
func (d *Delivery) Generation() Generation { return d.generation }

// State returns the current delivery lifecycle state.
func (d *Delivery) State() DeliveryState { return d.state }

// CreatedAt returns the creation timestamp.
func (d *Delivery) CreatedAt() time.Time { return d.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (d *Delivery) UpdatedAt() time.Time { return d.updatedAt }

// Operation returns the current delivery operation (deliver or remove).
func (d *Delivery) Operation() DeliveryOperation { return d.operation }

// NeedsWithdrawal reports whether a deletion must dispatch a removal
// for the delivery's target. A completed removal — Delivered, or
// Partial for one that finished with objects left in place — needs
// nothing more; every other record describes work that may still stand
// on the target — a put that may have applied, or a removal that did not
// finish — and the target must be withdrawn before the record is
// discarded.
func (d *Delivery) NeedsWithdrawal() bool {
	return d.operation != DeliveryOperationRemove ||
		(d.state != DeliveryStateDelivered && d.state != DeliveryStatePartial)
}

// Message returns the outcome message of the last failed attempt. It is
// empty while no attempt has failed, and is cleared once an attempt
// delivers.
func (d *Delivery) Message() string { return d.message }

// Attempts returns the number of failed attempts recorded at the
// delivery's current generation and operation cycle. It drives the retry
// backoff and is reset when a new cycle starts: a re-dispatch at an
// advanced generation, a withdrawal at a new generation, or a withdrawal
// that turns a put cycle into a removal at the same generation.
func (d *Delivery) Attempts() int { return d.attempts }

// ActiveDelivery is the enriched view of a [Delivery] returned by
// [DeliveryReporter.ListActiveDeliveries]. It bundles the delivery
// record with the full context an addon needs to resume work after a
// restart: target connection info, caller auth, and (when the
// fulfillment was signed) the re-assembled attestation.
//
// Stale deliveries — where the fulfillment's generation has advanced
// past the delivery's — are excluded because their auth and
// attestation cannot be correctly reconstructed from the current
// fulfillment state.
//
// TODO: A Pending delivery returned here may also arrive via
// [DeliveryAgent.Deliver] if the addon starts up while a
// DeliverToTarget activity is in flight. Addons must deduplicate
// by DeliveryID across both paths. See OME-77.
type ActiveDelivery struct {
	Delivery    Delivery
	Target      TargetInfo
	Auth        DeliveryAuth
	Attestation *Attestation // nil for unsigned (token-passthrough) fulfillments
}

// DeliveryResult is the outcome of a single delivery attempt.
// Agents may include structured outputs (provisioned targets, produced
// secrets) that the platform processes with the report itself.
//
// Operation identifies the cycle the report belongs to. The cycle
// identity of a delivery is (generation, operation): the same delivery
// ID carries both put and remove work over its lifetime, and a target
// can leave and re-enter placement at a single generation. A report
// whose operation does not match the delivery's current operation is
// stale work from the other operation and is discarded, so an in-flight
// put cannot report a removal delivered (or the reverse).
type DeliveryResult struct {
	State              DeliveryState
	Message            string
	Operation          DeliveryOperation
	ProvisionedTargets []ProvisionedTarget
	ProducedSecrets    []ProducedSecret
}

// ProvisionedTarget declares a target that a delivery created and that
// the platform should register. Properties should include vault refs
// for any associated secrets (e.g. "kubeconfig_ref").
type ProvisionedTarget struct {
	ID                    TargetID
	Type                  TargetType
	Name                  string
	Labels                map[string]string
	Properties            map[string]string
	AcceptedManifestTypes []ManifestType
}

// ProducedSecret declares a secret that a delivery produced and that
// the platform should store in the [Vault].
type ProducedSecret struct {
	Ref   SecretRef
	Value []byte
}

// DeliveryEventKind classifies a [DeliveryEvent].
type DeliveryEventKind string

const (
	DeliveryEventProgress DeliveryEventKind = "progress"
	DeliveryEventWarning  DeliveryEventKind = "warning"
	DeliveryEventError    DeliveryEventKind = "error"
)

// DeliveryEvent is a single entry in a delivery's event log.
type DeliveryEvent struct {
	Timestamp time.Time
	Kind      DeliveryEventKind
	Message   string
	Detail    json.RawMessage
}
