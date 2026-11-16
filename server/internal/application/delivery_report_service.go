package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// DeliveryReportService implements [domain.DeliveryReporter] as an
// application-layer service. It handles addon-to-platform delivery
// lifecycle updates: transitioning delivery state, signaling
// fulfillment workflows, and calling [domain.DeliveryObserver].
//
// This service stays long term. Even with gRPC transport, the
// transport handler delegates to this service.
//
// TODO: Naming may change
type DeliveryReportService struct {
	store       domain.Store
	signaler    domain.FulfillmentSignaler
	attestation domain.AttestationAssembler
	observer    domain.DeliveryObserver
	vault       domain.Vault
	now         func() time.Time
}

// DeliveryReportServiceOption configures a [DeliveryReportService].
type DeliveryReportServiceOption func(*DeliveryReportService)

// WithDeliveryObserver sets the observer for delivery lifecycle events.
func WithDeliveryObserver(o domain.DeliveryObserver) DeliveryReportServiceOption {
	return func(s *DeliveryReportService) { s.observer = o }
}

// WithAttestationAssembler sets the assembler used to reconstruct
// attestation bundles in [DeliveryReportService.ListActiveDeliveries].
func WithAttestationAssembler(a domain.AttestationAssembler) DeliveryReportServiceOption {
	return func(s *DeliveryReportService) { s.attestation = a }
}

// WithDeliveryReportClock overrides the wall-clock used for delivery
// state transition timestamps. Defaults to [time.Now]. A nil fn is
// treated as a no-op to prevent nil-dereference panics at runtime.
func WithDeliveryReportClock(fn func() time.Time) DeliveryReportServiceOption {
	return func(s *DeliveryReportService) {
		if fn != nil {
			s.now = fn
		}
	}
}

// WithDeliveryReportVault sets the vault that stores secrets produced
// by delivery results. Without one, produced secrets are dropped and
// only the provisioned targets are registered.
func WithDeliveryReportVault(v domain.Vault) DeliveryReportServiceOption {
	return func(s *DeliveryReportService) { s.vault = v }
}

// NewDeliveryReportService creates a service with the given
// dependencies. The signaler is called to notify the fulfillment
// workflow when a delivery reaches a terminal state; any
// [domain.Registry] satisfies [domain.FulfillmentSignaler].
func NewDeliveryReportService(
	store domain.Store,
	signaler domain.FulfillmentSignaler,
	opts ...DeliveryReportServiceOption,
) *DeliveryReportService {
	s := &DeliveryReportService{
		store:    store,
		signaler: signaler,
		observer: domain.NoOpDeliveryObserver{},
		now:      time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ReportEvent records a non-terminal delivery event. If the delivery
// is not yet in [domain.DeliveryStateProgressing], it transitions it.
// When this is the first transition out of [domain.DeliveryStatePending],
// a [domain.DeliveryAckedEvent] signal is sent to the fulfillment
// workflow so it knows the addon received the work.
// Events for deliveries already in a terminal state are silently
// ignored (the state machine rejects the transition).
// Reports whose generation does not match the delivery's current
// generation are silently discarded (stale work). Progress events do
// not carry an operation: they cannot change the delivery's operation,
// and the acknowledgement signal is stamped with the delivery's current
// one, so a stale event of the other operation at most acknowledges the
// current cycle early.
//
// The acknowledgement signal is sent after the state change commits and
// is not recorded durably: a lost acknowledgement is recovered by the
// orchestration, which re-dispatches deliveries that stay unacknowledged
// past its ack timeout. Completion signals are different -- see
// [DeliveryReportService.ReportResult].
func (s *DeliveryReportService) ReportEvent(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, event domain.DeliveryEvent) error {
	ctx, probe := s.observer.ReportEventStarted(ctx, deliveryID, generation, event)
	defer probe.End()

	tx, err := s.store.Begin(ctx)
	if err != nil {
		probe.Error(err)
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()

	// Read under the row's write fence: a report that reads a progress
	// state and commits after a terminal report would otherwise replace
	// the terminal state with progressing.
	d, err := tx.Deliveries().GetForUpdate(ctx, deliveryID)
	if err != nil {
		probe.Error(err)
		return fmt.Errorf("get delivery: %w", err)
	}

	if d.Generation() != generation {
		probe.Stale(generation, d.Generation())
		return nil
	}

	wasPending := d.State() == domain.DeliveryStatePending

	if err := d.TransitionTo(domain.DeliveryStateProgressing, s.now()); err != nil {
		if errors.Is(err, domain.ErrIllegalStateTransition) {
			return nil
		}
		probe.Error(err)
		return fmt.Errorf("transition delivery state: %w", err)
	}
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		probe.Error(err)
		return fmt.Errorf("update delivery state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		probe.Error(err)
		return fmt.Errorf("commit: %w", err)
	}

	if wasPending && s.signaler != nil {
		if err := s.signaler.SignalFulfillmentEvent(ctx, d.FulfillmentID(), domain.FulfillmentEvent{
			DeliveryAcked: &domain.DeliveryAckedEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  d.Operation(),
			},
		}); err != nil {
			return fmt.Errorf("signal ack: %w", err)
		}
	}
	return nil
}

// ReportResult records a delivery state transition and signals the
// fulfillment workflow on two occasions:
//   - When the delivery first transitions out of [domain.DeliveryStatePending]
//     (ack signal), telling the workflow the addon received the work.
//   - When a terminal state is reached (completion signal), telling
//     the workflow the work is done.
//
// Reports whose generation or operation does not match the delivery's
// current cycle are silently discarded (stale work). The cycle identity
// is (generation, operation): a put and a removal of the same target
// share a delivery ID, and a target can leave and re-enter placement at
// one generation, so generation alone cannot tell the two apart.
//
// Structured outputs (provisioned targets and produced secrets) are
// committed with the report: secrets are written to the vault first,
// then the inventory and target registrations join the delivery state
// transition in a single transaction. A reported terminal state
// therefore implies its outputs are durable, including across a
// workflow restart, and replaying the same report is an idempotent
// upsert. The vault write cannot happen inside the store transaction:
// the vault may be backed by the same database as the store, and a
// second write while the transaction is open would contend for the
// single writer (or deadlock on a shared-cache in-memory database). It
// therefore runs before the transaction opens, after a read-only check
// that the report still matches the delivery's cycle. The write
// transaction re-checks both fences under the delivery row's write
// fence, so a report that read a live cycle cannot commit after a
// terminal one and undo it. If the cycle
// changes, the transaction fails, or the state machine rejects the
// report after that check, the secrets stay in the vault: nothing
// records their refs, so the delivery-owned cleanup, which finds refs
// only in committed inventory, cannot remove them. Replaying the same
// report overwrites them; a report that is never replayed leaves them
// behind.
//
// Returns nil without persisting if the state machine rejects the
// transition (e.g. the delivery is already terminal).
//
// A terminal report records a completion obligation in the same
// transaction as the terminal state (see
// [domain.DeliveryCompletionRepository]): the completion signal is then
// sent promptly, and the obligation is removed once the send succeeds.
// If the send fails or the process stops first, the obligation survives
// and a relay delivers it later, so a workflow can never miss a
// completion that was durably reported. Replaying the signal is safe:
// the workflow ignores completions for deliveries it is no longer
// waiting for.
func (s *DeliveryReportService) ReportResult(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, result domain.DeliveryResult) error {
	ctx, probe := s.observer.ReportResultStarted(ctx, deliveryID, generation, result)
	defer probe.End()

	if result.State.IsTerminal() && len(result.ProducedSecrets) > 0 {
		current, err := s.reportMatchesCycle(ctx, deliveryID, generation, result.Operation)
		if err != nil {
			probe.Error(err)
			return err
		}
		if current {
			if err := s.storeProducedSecrets(ctx, result.ProducedSecrets); err != nil {
				probe.Error(err)
				return err
			}
		}
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		probe.Error(err)
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()

	// Read under the row's write fence: the cycle check and the state
	// machine decision below must be made on the state that is still
	// there at commit, or a stale report can undo a terminal one.
	d, err := tx.Deliveries().GetForUpdate(ctx, deliveryID)
	if err != nil {
		probe.Error(err)
		return fmt.Errorf("get delivery: %w", err)
	}

	if d.Generation() != generation {
		probe.Stale(generation, d.Generation())
		return nil
	}
	if result.Operation != d.Operation() {
		probe.StaleOperation(result.Operation, d.Operation())
		return nil
	}

	wasPending := d.State() == domain.DeliveryStatePending

	if err := d.ApplyResult(result, s.now()); err != nil {
		if errors.Is(err, domain.ErrIllegalStateTransition) {
			return nil
		}
		probe.Error(err)
		return fmt.Errorf("transition delivery state: %w", err)
	}
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		probe.Error(err)
		return fmt.Errorf("update delivery state: %w", err)
	}
	if err := s.processDeliveryTargets(ctx, tx, deliveryID, result); err != nil {
		probe.Error(err)
		return err
	}
	if result.State.IsTerminal() && s.signaler != nil {
		if err := tx.DeliveryCompletions().Put(ctx, domain.DeliveryCompletion{
			DeliveryID:    deliveryID,
			FulfillmentID: d.FulfillmentID(),
			Generation:    generation,
			Operation:     d.Operation(),
			State:         result.State,
			Message:       result.Message,
			CreatedAt:     s.now(),
		}); err != nil {
			probe.Error(err)
			return fmt.Errorf("record delivery completion: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		probe.Error(err)
		return fmt.Errorf("commit: %w", err)
	}

	operation := d.Operation()
	if s.signaler != nil {
		// TODO: maybe result should not accept non terminal states? not sure it makes sense otherwise
		if wasPending && !result.State.IsTerminal() {
			if err := s.signaler.SignalFulfillmentEvent(ctx, d.FulfillmentID(), domain.FulfillmentEvent{
				DeliveryAcked: &domain.DeliveryAckedEvent{
					DeliveryID: deliveryID,
					Generation: generation,
					Operation:  operation,
				},
			}); err != nil {
				return fmt.Errorf("signal ack: %w", err)
			}
		}
		if result.State.IsTerminal() {
			if err := s.signaler.SignalFulfillmentEvent(ctx, d.FulfillmentID(), domain.FulfillmentEvent{
				DeliveryCompleted: &domain.DeliveryCompletionEvent{
					DeliveryID: deliveryID,
					Generation: generation,
					Operation:  operation,
					Result:     result,
				},
			}); err != nil {
				// The obligation survives in the outbox and the relay
				// delivers it; returning the error lets the caller retry
				// promptly too, and replays are idempotent.
				return fmt.Errorf("signal workflow: %w", err)
			}
			s.clearCompletion(ctx, deliveryID, generation, operation)
		}
	}
	return nil
}

// clearCompletion removes a completion obligation whose signal has
// already been sent. A failure only means the relay will signal it
// again, which the workflow tolerates, so it is not reported to the
// caller.
func (s *DeliveryReportService) clearCompletion(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, operation domain.DeliveryOperation) {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.RollbackOnExit()
	if err := tx.DeliveryCompletions().Delete(ctx, deliveryID, generation, operation); err != nil {
		return
	}
	_ = tx.Commit()
}

// reportMatchesCycle reports whether the report still matches the
// delivery's current cycle. It reads without a write transaction so
// callers can decide to touch the vault before the store transaction
// opens. The write transaction re-checks both fences, so a cycle change
// after this check is still discarded there.
func (s *DeliveryReportService) reportMatchesCycle(
	ctx context.Context,
	deliveryID domain.DeliveryID,
	generation domain.Generation,
	operation domain.DeliveryOperation,
) (bool, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()

	d, err := tx.Deliveries().Get(ctx, deliveryID)
	if err != nil {
		return false, fmt.Errorf("get delivery: %w", err)
	}
	return d.Generation() == generation && d.Operation() == operation, nil
}

// storeProducedSecrets writes the secrets a delivery result produced.
// The vault is a separate store which may share the store's database,
// so this must not run while a store transaction is open.
func (s *DeliveryReportService) storeProducedSecrets(ctx context.Context, secrets []domain.ProducedSecret) error {
	if s.vault == nil {
		return nil
	}
	for _, secret := range secrets {
		if err := s.vault.Put(ctx, secret.Ref, secret.Value); err != nil {
			return fmt.Errorf("store secret %q: %w", secret.Ref, err)
		}
	}
	return nil
}

// processDeliveryTargets registers the targets a terminal delivery
// result provisioned. The inventory and target writes join the report
// transaction so the delivery's recorded state and its outputs become
// visible together. Identifiers are derived from the delivery and
// target IDs, so a replayed report upserts the same rows.
func (s *DeliveryReportService) processDeliveryTargets(
	ctx context.Context,
	tx domain.Tx,
	deliveryID domain.DeliveryID,
	result domain.DeliveryResult,
) error {
	if !result.State.IsTerminal() || len(result.ProvisionedTargets) == 0 {
		return nil
	}

	now := s.now()
	for _, pt := range result.ProvisionedTargets {
		props, err := json.Marshal(pt.Properties)
		if err != nil {
			return fmt.Errorf("marshal properties for target %q: %w", pt.ID, err)
		}
		invID := domain.InventoryItemID("target:" + string(pt.ID))
		if err := tx.Inventory().CreateOrUpdate(ctx, domain.NewInventoryItem(
			invID,
			domain.InventoryItemType(pt.Type),
			pt.Name,
			props,
			pt.Labels,
			&deliveryID,
			now,
		)); err != nil {
			return fmt.Errorf("upsert inventory item for target %q: %w", pt.ID, err)
		}
		if err := tx.Targets().CreateOrUpdate(ctx, domain.NewTargetInfo(
			pt.ID,
			pt.Type,
			pt.Name,
			"",
			pt.Labels,
			pt.Properties,
			pt.AcceptedManifestTypes,
		)); err != nil {
			return fmt.Errorf("upsert target %q: %w", pt.ID, err)
		}
	}
	return nil
}

// ListActiveDeliveries returns non-terminal deliveries enriched with
// target info, caller auth, and (when signed) re-assembled
// attestation. Deliveries whose fulfillment has advanced past the
// delivery's generation are filtered out because their auth and
// attestation cannot be correctly reconstructed.
//
// A Pending delivery returned here may also arrive via
// DeliveryAgent.Deliver if the addon starts up while a
// DeliverToTarget activity is in flight. Addons must deduplicate by
// DeliveryID across both paths; the kubernetes agent keeps one dispatch
// per delivery in flight.
func (s *DeliveryReportService) ListActiveDeliveries(ctx context.Context, targetIDs []domain.TargetID) ([]domain.ActiveDelivery, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()

	deliveries, err := tx.Deliveries().ListActive(ctx, targetIDs)
	if err != nil {
		return nil, fmt.Errorf("list active deliveries: %w", err)
	}
	if len(deliveries) == 0 {
		return nil, nil
	}

	fulfillments := make(map[domain.FulfillmentID]*domain.Fulfillment, len(deliveries))
	targets := make(map[domain.TargetID]domain.TargetInfo, len(deliveries))
	for _, d := range deliveries {
		fulfillments[d.FulfillmentID()] = nil
		targets[d.TargetID()] = domain.TargetInfo{}
	}

	for fID := range fulfillments {
		f, err := tx.Fulfillments().Get(ctx, fID)
		if errors.Is(err, domain.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get fulfillment %s: %w", fID, err)
		}
		fulfillments[fID] = f
	}

	for tID := range targets {
		t, err := tx.Targets().Get(ctx, tID)
		if errors.Is(err, domain.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get target %s: %w", tID, err)
		}
		targets[tID] = t
	}

	// Pre-resolve attestation evidence per fulfillment. Fulfillments
	// without provenance (unsigned / token-passthrough) get nil.
	evidence := make(map[domain.FulfillmentID]*domain.ResolvedEvidence, len(fulfillments))
	for fID, f := range fulfillments {
		if f == nil || f.Provenance() == nil {
			continue
		}
		ev, err := s.attestation.Resolve(ctx, tx, f)
		if err != nil {
			// TODO: revisit this / need to add observers
			continue // best-effort: skip attestation if evidence resolution fails
		}
		evidence[fID] = ev
	}

	var result []domain.ActiveDelivery
	for _, d := range deliveries {
		f := fulfillments[d.FulfillmentID()]
		if f == nil {
			continue // fulfillment deleted
		}
		if d.Generation() < f.Generation() {
			continue // stale: fulfillment has advanced
		}

		t, ok := targets[d.TargetID()]
		if !ok || t.ID() == "" {
			continue // target deleted
		}

		ad := domain.ActiveDelivery{
			Delivery: d,
			Target:   t,
			Auth:     f.Auth(),
		}
		if ev := evidence[d.FulfillmentID()]; ev != nil {
			ad.Attestation = domain.AssembleDeliverAttestation(*f, d.Manifests(), ev)
		}
		result = append(result, ad)
	}
	return result, nil
}
