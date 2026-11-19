package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"

	"github.com/stolostron/ocm-fleet/server/internal/attestation"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// TargetType is the [domain.TargetType] for Kubernetes clusters
// managed by the direct delivery agent (token-passthrough, no fleetlet).
const TargetType domain.TargetType = "kubernetes"

// ManifestManifestType is the [domain.ManifestType] for generic
// Kubernetes manifests applied via server-side apply.
const ManifestManifestType domain.ManifestType = "kubernetes"

// attestationVerifyTimeout bounds one attestation verification. The
// verification fetches trust material (a JWKS document, a registry
// key), and the activity that runs it carries no deadline of its own,
// so the bound lives here.
const attestationVerifyTimeout = 30 * time.Second

// DeliveryAgent implements [domain.DeliveryAgent] for Kubernetes clusters.
// When a target has a trust_bundle property and an attestation is
// present, verification is done per-target. Verifiers are cached by
// trust bundle content so repeated deliveries to the same target
// don't re-initialize JWKS fetching. Falls back to token passthrough
// when no attestation is present.
//
// Delivery supports two modes:
//
//   - Token passthrough: authenticates using the caller's JWT (legacy).
//   - Attested delivery: verifies the attestation bundle, then applies
//     using platform credentials from target properties (kubeconfig or
//     service account token). This is the "run-as-platform" model.
//
// A target may also declare, in its delivery_credential property, that
// its platform credential applies the intent of any authenticated
// caller (see [platformCredentialGranted]). That is an explicit grant
// recorded on the target, never a fallback.
type DeliveryAgent struct {
	reporter    domain.DeliveryReporter
	keyResolver *domain.KeyResolver
	httpClient  *http.Client
	vault       domain.Vault

	mu        sync.RWMutex
	verifiers map[string]*attestation.Verifier
	// guards keep deliveries and removals out of what is not a workload
	// on a cluster; see [DeliveryGuard].
	guards []DeliveryGuard

	// inflight tracks delivery IDs with work currently in progress.
	// The platform provides at-least-once delivery: a startup recovery
	// and a replayed workflow activity can dispatch the same delivery,
	// and a retry for one already being processed is safely skipped.
	inflight sync.Map // map[domain.DeliveryID]struct{}

	// workMu gates a dispatch against Close, and wg joins the background
	// deliveries and removals Close waits for.
	workMu  sync.Mutex
	wg      sync.WaitGroup
	closing bool

	probe AgentProbe
}

// AgentProbe observes the delivery agent's shutdown. A nil probe is valid
// and observes nothing.
type AgentProbe interface {
	// Closing is called once Close has refused further dispatches and
	// before it starts waiting for the ones already running. After it
	// returns, Close returns nil only once that work has finished.
	Closing()
}

// DeliveryAgentOption configures a [DeliveryAgent].
type DeliveryAgentOption func(*DeliveryAgent)

// WithKeyResolver sets the key resolver used for attestation
// verification (resolving signing keys from external registries).
func WithKeyResolver(r *domain.KeyResolver) DeliveryAgentOption {
	return func(a *DeliveryAgent) { a.keyResolver = r }
}

// WithHTTPClient sets the HTTP client used by per-target JWKS
// fetchers. Defaults to [http.DefaultClient].
func WithHTTPClient(c *http.Client) DeliveryAgentOption {
	return func(a *DeliveryAgent) { a.httpClient = c }
}

// WithVault configures the vault used to resolve secret references in
// target properties (e.g. service_account_token_ref). Required for
// attested delivery when platform credentials are vault-backed.
func WithVault(v domain.Vault) DeliveryAgentOption {
	return func(a *DeliveryAgent) { a.vault = v }
}

// WithAgentProbe observes the agent's shutdown; see [AgentProbe].
func WithAgentProbe(p AgentProbe) DeliveryAgentOption {
	return func(a *DeliveryAgent) { a.probe = p }
}

// NewDeliveryAgent returns a DeliveryAgent. The reporter is the addon's
// client interface for communicating delivery updates back to the platform.
func NewDeliveryAgent(reporter domain.DeliveryReporter, opts ...DeliveryAgentOption) *DeliveryAgent {
	a := &DeliveryAgent{
		reporter:  reporter,
		verifiers: make(map[string]*attestation.Verifier),
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// startWork reserves a place for one background delivery. Close takes the
// same lock to mark the agent closing, so it cannot see a zero WaitGroup
// while a dispatch is about to start.
func (a *DeliveryAgent) startWork() bool {
	a.workMu.Lock()
	defer a.workMu.Unlock()
	if a.closing {
		return false
	}
	a.wg.Add(1)
	return true
}

// Close stops new dispatches and waits for the deliveries already running
// to report their outcome, or until ctx is done. A delivery still running
// at the deadline is left to the platform's active-delivery recovery after
// the restart: the applies and removals are safe to repeat. It returns a
// wrapped ctx.Err when the deadline is reached first.
func (a *DeliveryAgent) Close(ctx context.Context) error {
	a.workMu.Lock()
	a.closing = true
	a.workMu.Unlock()
	if a.probe != nil {
		a.probe.Closing()
	}

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("kubernetes: close: %w", ctx.Err())
	}
}

// Deliver validates the target and auth synchronously then dispatches
// the actual SSA apply in a background goroutine. All delivery
// outcomes are reported through the [domain.DeliveryReporter].
//
// When an attestation is provided and the agent has a verification
// config, the attestation is verified before apply. Verification
// failure is reported as [domain.DeliveryStateAuthFailed].
func (a *DeliveryAgent) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	if target.Properties()[PropAPIServer] == "" {
		return fmt.Errorf("%w: target %q missing %s property", domain.ErrInvalidArgument, target.ID(), PropAPIServer)
	}

	// One dispatch per delivery at a time. A duplicate would apply the
	// same manifests, and its reports could land after the first run's
	// terminal report. The run in progress reports the outcome.
	if _, loaded := a.inflight.LoadOrStore(deliveryID, struct{}{}); loaded {
		return nil
	}

	if att != nil {
		v, err := a.verifierForTarget(target)
		if err != nil {
			a.inflight.Delete(deliveryID)
			a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				Operation: domain.DeliveryOperationDeliver,
				State:     domain.DeliveryStateAuthFailed,
				Message:   fmt.Sprintf("build verifier for target %q: %v", target.ID(), err),
			})
			return nil
		}
		verifyCtx, cancel := context.WithTimeout(ctx, attestationVerifyTimeout)
		err = v.Verify(verifyCtx, att, generation)
		cancel()
		if err != nil {
			a.inflight.Delete(deliveryID)
			a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				Operation: domain.DeliveryOperationDeliver,
				State:     domain.DeliveryStateAuthFailed,
				Message:   fmt.Sprintf("attestation verification failed: %v", err),
			})
			return nil
		}
		if !a.startWork() {
			a.inflight.Delete(deliveryID)
			return fmt.Errorf("kubernetes: agent is closing")
		}
		go func() {
			defer a.wg.Done()
			defer a.inflight.Delete(deliveryID)
			a.deliverAsyncPlatform(context.WithoutCancel(ctx), target, deliveryID, generation, manifests)
		}()
		return nil
	}

	if platformCredentialGranted(target, auth) {
		if !a.startWork() {
			a.inflight.Delete(deliveryID)
			return fmt.Errorf("kubernetes: agent is closing")
		}
		go func() {
			defer a.wg.Done()
			defer a.inflight.Delete(deliveryID)
			a.deliverAsyncPlatform(context.WithoutCancel(ctx), target, deliveryID, generation, manifests)
		}()
		return nil
	}

	if auth.Token == "" {
		a.inflight.Delete(deliveryID)
		return fmt.Errorf("%w: delivery to target %q requires an authenticated caller token", domain.ErrInvalidArgument, target.ID())
	}
	if !a.startWork() {
		a.inflight.Delete(deliveryID)
		return fmt.Errorf("kubernetes: agent is closing")
	}
	go func() {
		defer a.wg.Done()
		defer a.inflight.Delete(deliveryID)
		a.deliverAsync(context.WithoutCancel(ctx), target, deliveryID, generation, manifests, auth)
	}()
	return nil
}

// platformCredentialGranted reports whether the target's declared rule
// lets the stored platform credential apply intent that carries no
// attestation: the target records an explicit grant
// ([DeliveryCredentialPlatform]) and the caller is authenticated. With
// any other rule, or none, unsigned intent uses the caller's own token.
func platformCredentialGranted(target domain.TargetInfo, auth domain.DeliveryAuth) bool {
	return target.Properties()[PropDeliveryCredential] == DeliveryCredentialPlatform && auth.Caller != nil
}

// verifierForTarget builds or retrieves a cached [attestation.Verifier]
// from the target's trust_bundle property. Returns an error if the
// target has no trust_bundle (you cannot deliver attested content to a
// target without trust config).
func (a *DeliveryAgent) verifierForTarget(target domain.TargetInfo) (*attestation.Verifier, error) {
	trustJSON := target.Properties()["trust_bundle"]
	if trustJSON == "" {
		return nil, fmt.Errorf("target %q has no trust_bundle property", target.ID())
	}

	a.mu.RLock()
	if v, ok := a.verifiers[trustJSON]; ok {
		a.mu.RUnlock()
		return v, nil
	}
	a.mu.RUnlock()

	var entries []domain.TrustBundleEntry
	if err := json.Unmarshal([]byte(trustJSON), &entries); err != nil {
		return nil, fmt.Errorf("parse trust_bundle: %w", err)
	}

	issuers := make(map[domain.IssuerURL]attestation.TrustedIssuer, len(entries))
	for _, e := range entries {
		issuers[e.IssuerURL] = attestation.TrustedIssuer{
			JWKSURI:                  e.JWKSURI,
			Audience:                 e.EnrollmentAudience,
			PublicKeyClaimExpression: e.PublicKeyClaimExpression,
			RegistrySubjectMapping:   e.RegistrySubjectMapping,
		}
	}

	var opts []attestation.VerifierOption
	if a.httpClient != nil {
		opts = append(opts, attestation.WithHTTPClient(a.httpClient))
	}
	if a.keyResolver != nil {
		opts = append(opts, attestation.WithKeyResolver(a.keyResolver))
	}
	v := attestation.NewVerifier(issuers, opts...)

	a.mu.Lock()
	a.verifiers[trustJSON] = v
	a.mu.Unlock()

	return v, nil
}

// deliverAsyncPlatform applies manifests using platform credentials
// from target properties. Called after attestation verification passes.
// The platform token is resolved from the target's properties: first
// a direct service_account_token, then a service_account_token_ref
// that is looked up in the agent's [domain.Vault].
func (a *DeliveryAgent) deliverAsyncPlatform(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, generation domain.Generation, manifests []domain.Manifest) {
	cfg, err := BuildPlatformRESTConfig(ctx, a.vault, target)
	if err != nil {
		a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			Operation: domain.DeliveryOperationDeliver,
			State:     domain.DeliveryStateFailed,
			Message:   fmt.Sprintf("build platform kubernetes client for target %q: %v", target.ID(), err),
		})
		return
	}
	a.applyManifests(ctx, target, deliveryID, generation, cfg, manifests, true)
}

func (a *DeliveryAgent) deliverAsync(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, generation domain.Generation, manifests []domain.Manifest, auth domain.DeliveryAuth) {
	cfg, err := buildCallerRESTConfig(target, auth.Token)
	if err != nil {
		a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			Operation: domain.DeliveryOperationDeliver,
			State:     domain.DeliveryStateFailed,
			Message:   fmt.Sprintf("build kubernetes client for target %q: %v", target.ID(), err),
		})
		return
	}
	a.applyManifests(ctx, target, deliveryID, generation, cfg, manifests, false)
}

// applyManifests applies the manifests with the credential in cfg and
// reports the outcome. stored says that the credential is the
// platform's stored one, which decides how a refusal is classified (see
// [failureForError]).
//
// Every manifest is shown to the guards first (see [DeliveryGuard]). A
// delivery that names an object out of reach fails before anything is
// applied: a delivery is not applied in part around what it may not
// touch.
func (a *DeliveryAgent) applyManifests(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, generation domain.Generation, cfg *rest.Config, manifests []domain.Manifest, stored bool) {
	if i, obj, why, refused := refusedByGuard(a.guardFor(ctx, target), manifests); refused {
		a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			Operation: domain.DeliveryOperationDeliver,
			State:     domain.DeliveryStateFailed,
			Message:   fmt.Sprintf("manifest %d (%s) is out of reach of deliveries on this cluster: %s; nothing of this delivery was applied", i+1, obj, why),
		})
		return
	}

	ap, err := newApplierFromConfig(cfg)
	if err != nil {
		state, reason := failureForError(err, stored)
		a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			Operation: domain.DeliveryOperationDeliver,
			State:     state,
			Message:   fmt.Sprintf("build kubernetes client for target %q: %s", target.ID(), reason),
		})
		return
	}

	for i, m := range manifests {
		_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Applying manifest %d/%d", i+1, len(manifests)),
		})

		if err := ap.apply(ctx, m.Raw); err != nil {
			state, reason := failureForError(err, stored)
			a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				Operation: domain.DeliveryOperationDeliver,
				State:     state,
				Message:   fmt.Sprintf("apply manifest %d: %s", i+1, reason),
			})
			return
		}
	}

	a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
		Operation: domain.DeliveryOperationDeliver,
		State:     domain.DeliveryStateDelivered,
	})
}

// deleteManifests deletes Kubernetes resources described by manifests.
// Resources that are already gone (404) are silently skipped, and an
// object another manager wrote is left in place and named (see
// [applier.delete]). So is an object guard keeps out of reach; a nil
// guard keeps nothing.
func (a *DeliveryAgent) deleteManifests(ctx context.Context, cfg *rest.Config, manifests []domain.Manifest, guard ObjectGuard) (left []string, err error) {
	ap, err := newApplierFromConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	ap.guard = guard

	for i, m := range manifests {
		one, err := ap.delete(ctx, m.Raw)
		if err != nil {
			return left, fmt.Errorf("remove manifest %d: %w", i+1, err)
		}
		if one != "" {
			left = append(left, one)
		}
	}
	return left, nil
}

// deleteManifestsWithStoredCredential is deleteManifests for a removal
// that runs with the platform's stored credential (see
// [applier.deleteWithStoredCredential]). It names the objects it left
// in place, with the reason for each.
func (a *DeliveryAgent) deleteManifestsWithStoredCredential(ctx context.Context, cfg *rest.Config, manifests []domain.Manifest, guard ObjectGuard) (left []string, err error) {
	ap, err := newApplierFromConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	ap.guard = guard
	return deleteWithStoredCredential(ctx, ap, manifests)
}

func deleteWithStoredCredential(ctx context.Context, ap *applier, manifests []domain.Manifest) (left []string, err error) {
	for i, m := range manifests {
		one, err := ap.deleteWithStoredCredential(ctx, m.Raw)
		if err != nil {
			return left, fmt.Errorf("remove manifest %d: %w", i+1, err)
		}
		if one != "" {
			left = append(left, one)
		}
	}
	return left, nil
}

// removalLeftMessage says what a removal left in place, with the reason
// recorded for each object.
func removalLeftMessage(left []string) string {
	return "left as they are: " + strings.Join(left, "; ")
}

// removalResult is the outcome of a removal whose deletions finished.
// Objects left in place because another manager wrote them, because
// the stored credential may not touch them, or because a guard keeps
// them out of reach make the result Partial rather than Delivered: the
// removal is over, but something the delivery may have applied still
// stands.
func removalResult(left []string) domain.DeliveryResult {
	result := domain.DeliveryResult{Operation: domain.DeliveryOperationRemove, State: domain.DeliveryStateDelivered}
	if len(left) > 0 {
		result.State = domain.DeliveryStatePartial
		result.Message = removalLeftMessage(left)
	}
	return result
}

// reportResult reports a terminal outcome, retrying briefly: a delivery
// result lost to a transient platform error would leave the delivery
// record in progress until a re-dispatch. The retry envelope bounds the
// work; nothing waits on a report that keeps failing.
func (a *DeliveryAgent) reportResult(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, result domain.DeliveryResult) {
	_ = RetryLocalEnvelope(ctx, ReportResultRetryDeadline, func(attemptCtx context.Context) error {
		return a.reporter.ReportResult(attemptCtx, deliveryID, generation, result)
	})
}

// deliveryStateForError returns [domain.DeliveryStateAuthFailed] for
// Kubernetes API authentication/authorization errors (401/403), and
// [domain.DeliveryStateFailed] for everything else.
//
// One "forbidden" is not about authorization: a cluster refuses new
// content in a namespace that is still being deleted. The same call
// succeeds once the namespace is gone, so it is a failed delivery.
func deliveryStateForError(err error) domain.DeliveryState {
	if namespaceIsBeingDeleted(err) {
		return domain.DeliveryStateFailed
	}
	if apierrors.IsUnauthorized(err) || apierrors.IsForbidden(err) {
		return domain.DeliveryStateAuthFailed
	}
	return domain.DeliveryStateFailed
}

// namespaceIsBeingDeleted reports whether the cluster refused the call
// because the namespace it writes into is terminating.
func namespaceIsBeingDeleted(err error) bool {
	return apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause)
}

// namespaceBeingDeleted is added to the reason of a delivery that the
// cluster refused because the namespace it writes into is terminating.
const namespaceBeingDeleted = "the namespace is still being deleted; the delivery is retried, and succeeds once the namespace is gone"

// storedCredentialNotRecognized is added to the reason of a delivery
// that failed because the cluster did not recognize the platform's
// stored credential.
const storedCredentialNotRecognized = "the cluster does not recognize the stored credential; the delivery is retried, and succeeds once the credential is renewed or the cluster's side presents a new one"

// unsignedWithoutGrant is added to the reason of a call the cluster
// refused while it used the caller's own token: such intent is unsigned,
// and the target's enrollment does not let the stored credential deliver
// unsigned deployments, which is why the caller's token was used.
const unsignedWithoutGrant = "the deployment is not signed and its target's enrollment does not let the stored credential deliver unsigned deployments, so the caller's own token was used and the cluster refused it: sign the deployment, or enroll the target with that grant"

// failureForError classifies a failed call to the cluster and words its
// reason. stored says that the call used the platform's stored
// credential instead of the caller's own.
//
// An authentication failure pauses the fulfillment until a caller
// resumes it with fresh credentials. That fits the caller's token. It
// does not fit a stored credential the cluster does not recognize
// (401): no caller can replace it, and it comes back by itself when the
// platform renews it or the cluster's side presents a new one. Such a
// call is a failed delivery, which the platform retries. A cluster that
// recognizes the stored credential and forbids the call (403) is an
// authorization failure like any other: someone has to widen what the
// credential may do.
//
// A refusal of the caller's own token says why that token was used: the
// deployment is unsigned and the target has no grant for unsigned
// intent, so the reason names both ways out.
//
// A cluster that refuses new content in a namespace that is still being
// deleted answers 403 as well, whatever the credential. Nobody has to
// widen anything for it: it is a failed delivery, retried like one.
func failureForError(err error, stored bool) (domain.DeliveryState, string) {
	if namespaceIsBeingDeleted(err) {
		return domain.DeliveryStateFailed, err.Error() + ": " + namespaceBeingDeleted
	}
	if stored && apierrors.IsUnauthorized(err) {
		return domain.DeliveryStateFailed, err.Error() + ": " + storedCredentialNotRecognized
	}
	if !stored && (apierrors.IsUnauthorized(err) || apierrors.IsForbidden(err)) {
		return deliveryStateForError(err), err.Error() + ": " + unsignedWithoutGrant
	}
	return deliveryStateForError(err), err.Error()
}

// Remove deletes the manifested resources this resource manager wrote on
// the target cluster. An object whose managed-fields record does not
// name this addon's field manager is left in place and named in the
// result's message: a removal can be dispatched for a delivery that
// failed before it applied anything, and its manifest then names another
// manager's object. A removal that finishes with objects left in place
// reports Partial rather than Delivered, so the platform can tell that
// something may still stand.
// When an attestation is provided the agent verifies it against the
// target's trust bundle (same dynamic, per-target verification as
// Deliver) and uses platform credentials. It also uses them when the
// target grants that to authenticated callers (same rule as Deliver).
// Otherwise falls back to token passthrough (auth.Token).
// Resources that are already gone (404) are silently skipped. With the
// platform's stored credential, a read the cluster forbids is not an
// error: the object is left and named (see
// [applier.deleteWithStoredCredential]). With the caller's own
// credential a refusal always stands: the stored credential is not
// tried in its place, not even to look.
//
// Like Deliver, the work runs asynchronously. The method validates
// inputs synchronously and returns nil, then reports the outcome via
// [domain.DeliveryReporter.ReportResult].
func (a *DeliveryAgent) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	if target.Properties()[PropAPIServer] == "" {
		return fmt.Errorf("%w: target %q missing %s property", domain.ErrInvalidArgument, target.ID(), PropAPIServer)
	}

	// One dispatch per delivery at a time, as in Deliver: a duplicate
	// removal would delete the same objects concurrently and could report
	// after the run in progress.
	if _, loaded := a.inflight.LoadOrStore(deliveryID, struct{}{}); loaded {
		return nil
	}

	asyncCtx := context.WithoutCancel(ctx)
	guard := a.guardFor(ctx, target)
	usePlatform := false
	if att != nil {
		v, err := a.verifierForTarget(target)
		if err != nil {
			a.inflight.Delete(deliveryID)
			a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				Operation: domain.DeliveryOperationRemove,
				State:     domain.DeliveryStateAuthFailed,
				Message:   fmt.Sprintf("build verifier for target %q: %v", target.ID(), err),
			})
			return nil
		}
		verifyCtx, cancel := context.WithTimeout(ctx, attestationVerifyTimeout)
		err = v.Verify(verifyCtx, att, generation)
		cancel()
		if err != nil {
			a.inflight.Delete(deliveryID)
			a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				Operation: domain.DeliveryOperationRemove,
				State:     domain.DeliveryStateAuthFailed,
				Message:   fmt.Sprintf("attestation verification failed: %v", err),
			})
			return nil
		}
		usePlatform = true
	} else if platformCredentialGranted(target, auth) {
		usePlatform = true
	}
	if usePlatform {
		cfg, err := BuildPlatformRESTConfig(ctx, a.vault, target)
		if err != nil {
			a.inflight.Delete(deliveryID)
			a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				Operation: domain.DeliveryOperationRemove,
				State:     domain.DeliveryStateFailed,
				Message:   fmt.Sprintf("build platform REST config: %v", err),
			})
			return nil
		}
		if !a.startWork() {
			a.inflight.Delete(deliveryID)
			return fmt.Errorf("kubernetes: agent is closing")
		}
		go func() {
			defer a.wg.Done()
			defer a.inflight.Delete(deliveryID)
			left, err := a.deleteManifestsWithStoredCredential(asyncCtx, cfg, manifests, guard)
			if err != nil {
				state, reason := failureForError(err, true)
				a.reportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
					Operation: domain.DeliveryOperationRemove,
					State:     state, Message: reason,
				})
				return
			}
			a.reportResult(asyncCtx, deliveryID, generation, removalResult(left))
		}()
		return nil
	}

	if auth.Token == "" {
		a.inflight.Delete(deliveryID)
		return fmt.Errorf("%w: removal from target %q requires an authenticated caller token", domain.ErrInvalidArgument, target.ID())
	}

	cfg, err := buildCallerRESTConfig(target, auth.Token)
	if err != nil {
		a.inflight.Delete(deliveryID)
		a.reportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			Operation: domain.DeliveryOperationRemove,
			State:     domain.DeliveryStateFailed,
			Message:   fmt.Sprintf("build REST config: %v", err),
		})
		return nil
	}
	if !a.startWork() {
		a.inflight.Delete(deliveryID)
		return fmt.Errorf("kubernetes: agent is closing")
	}
	go func() {
		defer a.wg.Done()
		defer a.inflight.Delete(deliveryID)
		left, err := a.deleteManifests(asyncCtx, cfg, manifests, guard)
		if err != nil {
			state, reason := failureForError(err, false)
			a.reportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{
				Operation: domain.DeliveryOperationRemove,
				State:     state, Message: reason,
			})
			return
		}
		a.reportResult(asyncCtx, deliveryID, generation, removalResult(left))
	}()
	return nil
}
