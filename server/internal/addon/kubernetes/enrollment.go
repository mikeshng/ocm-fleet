package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// indexerStartTimeout bounds how long an enrollment waits for the
// cluster's indexer to start. A slow start does not fail the
// enrollment; the worker starts the indexer later.
const indexerStartTimeout = 20 * time.Second

// PresentationVerifier vouches for clusters presented under a
// verifier-scoped bundle. An integration that already knows a set of
// clusters (for example from a registry it watches) registers one, so
// that an enrollment is checked against that knowledge and not only
// against what the cluster says about itself.
type PresentationVerifier interface {
	// VerifyPresentation checks that clusterName belongs to scope and
	// that presentedCA is that cluster's CA, and returns the cluster's
	// id, its API address, and its CA as the verifier itself knows
	// them. It returns an [EnrollmentError] of kind
	// [ErrEnrollmentRefused] or [ErrEnrollmentUnverified] when it does
	// not vouch for the cluster.
	VerifyPresentation(ctx context.Context, scope, clusterName string, presentedCA []byte) (VerifiedCluster, error)
}

// VerifiedCluster is what a [PresentationVerifier] states about a
// cluster it vouches for.
type VerifiedCluster struct {
	// ClusterID is the id to enroll the cluster under.
	ClusterID string
	// APIServer and CACert are the verifier's own record of the
	// cluster. The resource manager probes only this address and
	// trusts only this CA.
	APIServer string
	CACert    []byte
	// Labels are facts the verifier knows, copied onto the target.
	Labels map[string]string
}

// TrustBundleSource returns the trust settings enrolled targets get,
// so that signed intent can be verified for them.
type TrustBundleSource func(ctx context.Context) ([]domain.TrustBundleEntry, error)

// EnrollmentDeps are the collaborators of an [EnrollmentService].
type EnrollmentDeps struct {
	Store   domain.Store
	Vault   domain.Vault
	Records InventoryRecordStore
	// Indexing may be nil: enrolled clusters are then not indexed.
	Indexing IndexingRuntime
	Prober   ClusterProber
	// Trust may be nil: enrolled targets then accept no signed intent.
	Trust  TrustBundleSource
	Logger *slog.Logger
	// Now and Random default to the wall clock and crypto/rand.
	Now    func() time.Time
	Random io.Reader
}

// EnrollmentService takes existing clusters in and lets them go.
type EnrollmentService struct {
	store    domain.Store
	vault    domain.Vault
	records  InventoryRecordStore
	indexing IndexingRuntime
	prober   ClusterProber
	trust    TrustBundleSource
	logger   *slog.Logger
	now      func() time.Time
	random   io.Reader

	mu        sync.Mutex
	verifiers map[string]PresentationVerifier
	locks     map[string]*sync.Mutex
	probes    map[string]*probeHistory
	observers []func(clusterID string)
}

// NewEnrollmentService builds the service. Store, Vault, Records,
// Prober, and Logger are required.
func NewEnrollmentService(deps EnrollmentDeps) *EnrollmentService {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	random := deps.Random
	if random == nil {
		random = defaultRandom
	}
	return &EnrollmentService{
		store:     deps.Store,
		vault:     deps.Vault,
		records:   deps.Records,
		indexing:  deps.Indexing,
		prober:    deps.Prober,
		trust:     deps.Trust,
		logger:    deps.Logger.With("component", "enrollment"),
		now:       now,
		random:    random,
		verifiers: map[string]PresentationVerifier{},
		locks:     map[string]*sync.Mutex{},
		probes:    map[string]*probeHistory{},
	}
}

// RegisterVerifier makes a verifier available to bundles under name.
func (s *EnrollmentService) RegisterVerifier(name string, v PresentationVerifier) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verifiers[name] = v
}

func (s *EnrollmentService) verifier(name string) PresentationVerifier {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.verifiers[name]
}

// OnChange registers fn to be called when a cluster's target comes to
// exist or ceases to (first enrollment, withdrawal by either side) and
// when its record is forgotten. fn runs while the cluster is locked:
// it must return quickly and must not call back into the service.
func (s *EnrollmentService) OnChange(fn func(clusterID string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observers = append(s.observers, fn)
}

func (s *EnrollmentService) notify(clusterID string) {
	s.mu.Lock()
	observers := append([]func(string){}, s.observers...)
	s.mu.Unlock()
	for _, fn := range observers {
		fn(clusterID)
	}
}

// lockCluster serializes every change to one cluster id.
func (s *EnrollmentService) lockCluster(clusterID string) *sync.Mutex {
	s.mu.Lock()
	m, ok := s.locks[clusterID]
	if !ok {
		m = &sync.Mutex{}
		s.locks[clusterID] = m
	}
	s.mu.Unlock()
	m.Lock()
	return m
}

// EnrollRequest is a hand enrollment: an authenticated user states
// where the cluster is and shows a credential the cluster issued.
type EnrollRequest struct {
	ClusterID string
	APIServer string
	CACert    []byte
	Token     string
	// ServiceAccount is optional; when set, the cluster must name it.
	ServiceAccount ServiceAccountRef
	Labels         map[string]string
	// GrantPlatformDelivery records the explicit choice to let the
	// stored credential apply unsigned intent of authenticated callers.
	GrantPlatformDelivery bool
	// Profile is the access profile the user says the cluster's side
	// granted. What the cluster reports is recorded next to it.
	Profile string
	// Actor is the authenticated subject.
	Actor string
}

// PresentRequest is what a cluster's side presents under a bundle.
type PresentRequest struct {
	// ClusterName is the cluster's name within a verifier's scope. It
	// is ignored for a bundle that names one cluster.
	ClusterName    string
	CACert         []byte
	Token          string
	ServiceAccount ServiceAccountRef
}

// PresentResult answers a presentation.
type PresentResult struct {
	Cluster ClusterRecord
	// RenewAfter is when the cluster's side should present again.
	RenewAfter time.Duration
}

// admission is a verified cluster ready to be recorded.
type admission struct {
	clusterID string
	conn      ClusterConnection
	probe     ClusterProbeResult
	actor     string
	bundle    *BundleObservation
	source    *ClusterSource
	// labels is the enrollment's own label set; nil keeps what a
	// refreshed target already has.
	labels map[string]string
	// externalLabels are a verifier's facts; nil keeps what exists.
	externalLabels map[string]string
	// rule and profile are applied on first enrollment and whenever
	// applyChoices is set; a plain credential refresh keeps them.
	deliveryRule string
	grantBy      string
	profile      string
	applyChoices bool
}

// Enroll takes a cluster in by hand.
func (s *EnrollmentService) Enroll(ctx context.Context, req EnrollRequest) (ClusterRecord, error) {
	if err := ValidateClusterID(req.ClusterID); err != nil {
		return ClusterRecord{}, err
	}
	apiServer := NormalizeAPIServerURL(req.APIServer)
	if err := ValidateAPIServerURL(apiServer); err != nil {
		return ClusterRecord{}, err
	}
	if _, err := CAFingerprints(req.CACert); err != nil {
		return ClusterRecord{}, err
	}
	if req.Token == "" {
		return ClusterRecord{}, fmt.Errorf("%w: a credential issued by the cluster is required", domain.ErrInvalidArgument)
	}
	if err := validateLabels(req.Labels); err != nil {
		return ClusterRecord{}, err
	}
	switch req.Profile {
	case "", AccessProfileClusterAdmin, AccessProfileWorkloads, AccessProfileReadOnly, AccessProfileCustom:
	default:
		return ClusterRecord{}, fmt.Errorf("%w: unknown access profile %q", domain.ErrInvalidArgument, req.Profile)
	}

	conn := ClusterConnection{APIServer: apiServer, CACert: req.CACert, Token: req.Token}
	probe, err := s.probe(ctx, conn)
	if err != nil {
		return ClusterRecord{}, err
	}
	if probe.ServiceAccount.Name == "" {
		return ClusterRecord{}, enrollmentErr(ErrEnrollmentUnverified, "not-a-service-account",
			"the credential belongs to %q; a service account token is required so that it can be renewed", probe.Username)
	}
	if req.ServiceAccount.Name != "" && req.ServiceAccount != probe.ServiceAccount {
		return ClusterRecord{}, enrollmentErr(ErrEnrollmentUnverified, "unexpected-identity",
			"the cluster says the credential belongs to service account %s, not %s", probe.ServiceAccount, req.ServiceAccount)
	}

	rule := DeliveryCredentialSigned
	if req.GrantPlatformDelivery {
		rule = DeliveryCredentialPlatform
	}
	labels := req.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return s.admit(ctx, admission{
		clusterID:    req.ClusterID,
		conn:         conn,
		probe:        probe,
		actor:        req.Actor,
		labels:       labels,
		deliveryRule: rule,
		grantBy:      req.Actor,
		profile:      req.Profile,
		applyChoices: true,
	})
}

// Present takes a cluster in, or refreshes its credential, on the
// strength of an enrollment bundle. bearer is "<bundle id>.<secret>".
func (s *EnrollmentService) Present(ctx context.Context, bearer string, req PresentRequest) (PresentResult, error) {
	bundle, err := s.authenticateBundle(ctx, bearer)
	if err != nil {
		return PresentResult{}, err
	}
	if req.Token == "" {
		return PresentResult{}, fmt.Errorf("%w: a credential issued by the cluster is required", domain.ErrInvalidArgument)
	}
	if _, err := CAFingerprints(req.CACert); err != nil {
		return PresentResult{}, err
	}

	a := admission{
		actor:          "bundle:" + bundle.ID,
		bundle:         &bundle,
		labels:         bundle.Labels,
		deliveryRule:   DeliveryCredentialSigned,
		profile:        bundle.Profile,
		externalLabels: nil,
	}
	if a.labels == nil {
		a.labels = map[string]string{}
	}
	if bundle.GrantPlatformDelivery {
		a.deliveryRule = DeliveryCredentialPlatform
		a.grantBy = bundle.CreatedBy
	}

	trustedCA := req.CACert
	if bundle.Scope.Verifier != "" {
		v := s.verifier(bundle.Scope.Verifier)
		if v == nil {
			return PresentResult{}, enrollmentErr(ErrEnrollmentRefused, "verifier-unavailable",
				"the bundle needs verifier %q, which is not available", bundle.Scope.Verifier)
		}
		if req.ClusterName == "" {
			return PresentResult{}, fmt.Errorf("%w: cluster name is required", domain.ErrInvalidArgument)
		}
		verified, err := v.VerifyPresentation(ctx, bundle.Scope.VerifierScope, req.ClusterName, req.CACert)
		if err != nil {
			return PresentResult{}, err
		}
		if err := ValidateClusterID(verified.ClusterID); err != nil {
			return PresentResult{}, fmt.Errorf("verifier %q: %w", bundle.Scope.Verifier, err)
		}
		a.clusterID = verified.ClusterID
		a.conn.APIServer = NormalizeAPIServerURL(verified.APIServer)
		if len(verified.CACert) > 0 {
			trustedCA = verified.CACert
		}
		a.externalLabels = verified.Labels
		a.source = &ClusterSource{Verifier: bundle.Scope.Verifier, Scope: bundle.Scope.VerifierScope, Name: req.ClusterName}
	} else {
		a.clusterID = bundle.Scope.ClusterID
		a.conn.APIServer = bundle.Scope.APIServer
	}
	if err := ValidateAPIServerURL(a.conn.APIServer); err != nil {
		return PresentResult{}, err
	}
	a.conn.CACert = trustedCA
	a.conn.Token = req.Token

	// A cluster that was withdrawn is not taken in again by a bundle
	// that existed before the withdrawal.
	if rec, err := s.GetCluster(ctx, a.clusterID); err == nil {
		if rec.Observation.State == ClusterStateWithdrawn && rec.Observation.WithdrawnAt != nil &&
			!bundle.CreatedAt.After(*rec.Observation.WithdrawnAt) {
			return PresentResult{}, enrollmentErr(ErrEnrollmentRefused, "cluster-withdrawn",
				"cluster %q was withdrawn after this bundle was created; a new bundle, or forgetting the cluster, is needed to enroll it again", a.clusterID)
		}
	} else if !errors.Is(err, domain.ErrNotFound) {
		return PresentResult{}, err
	}

	probe, err := s.probe(ctx, a.conn)
	if err != nil {
		return PresentResult{}, err
	}
	if probe.ServiceAccount.Name != AccessServiceAccount {
		return PresentResult{}, enrollmentErr(ErrEnrollmentUnverified, "unexpected-identity",
			"the cluster says the credential belongs to %q; the access account %q is expected", probe.Username, AccessServiceAccount)
	}
	if req.ServiceAccount.Name != "" && req.ServiceAccount != probe.ServiceAccount {
		return PresentResult{}, enrollmentErr(ErrEnrollmentUnverified, "unexpected-identity",
			"the cluster says the credential belongs to service account %s, not %s", probe.ServiceAccount, req.ServiceAccount)
	}
	a.probe = probe

	rec, err := s.admit(ctx, a)
	if err != nil {
		return PresentResult{}, err
	}
	s.noteBundleUse(ctx, bundle)

	lifetime := bundle.TokenLifetime()
	if !probe.TokenIssuedAt.IsZero() && probe.TokenExpiresAt.After(probe.TokenIssuedAt) {
		lifetime = probe.TokenExpiresAt.Sub(probe.TokenIssuedAt)
	}
	return PresentResult{Cluster: rec, RenewAfter: lifetime / 3}, nil
}

// probe asks the cluster about the credential and turns the two
// answers that mean "this is not that cluster's credential" into
// verification failures.
func (s *EnrollmentService) probe(ctx context.Context, conn ClusterConnection) (ClusterProbeResult, error) {
	probe, err := s.prober.Probe(ctx, conn)
	switch {
	case err == nil:
		return probe, nil
	case errors.Is(err, ErrClusterRejectedCredential):
		return ClusterProbeResult{}, enrollmentErr(ErrEnrollmentUnverified, "credential-rejected",
			"the cluster at %s does not recognize the presented credential", conn.APIServer)
	case errors.Is(err, domain.ErrInvalidArgument):
		return ClusterProbeResult{}, err
	default:
		return ClusterProbeResult{}, enrollmentErr(ErrEnrollmentUnverified, "cluster-unreachable",
			"the cluster at %s could not be verified with the presented CA: %v", conn.APIServer, err)
	}
}

// checkClusterIDFree fails when the id's target exists and was not
// created by enrollment.
func (s *EnrollmentService) checkClusterIDFree(ctx context.Context, clusterID string) error {
	target, found, err := s.getTarget(ctx, TargetIDForCluster(clusterID))
	if err != nil {
		return err
	}
	if found && !IsEnrolledTarget(target) {
		return clusterIDInUse(clusterID, target)
	}
	return nil
}

func clusterIDInUse(clusterID string, target domain.TargetInfo) *EnrollmentError {
	e := enrollmentErr(ErrEnrollmentConflict, "cluster-id-in-use",
		"cluster id %q is in use by target %q, which the resource manager did not take in by enrollment", clusterID, target.ID())
	e.Details = map[string]any{"target": string(target.ID())}
	return e
}

// admit records a verified cluster: credential in the vault, target,
// cluster record, indexer. It is also the refresh path for a cluster
// that is already enrolled.
func (s *EnrollmentService) admit(ctx context.Context, a admission) (ClusterRecord, error) {
	lock := s.lockCluster(a.clusterID)
	defer lock.Unlock()

	targetID := TargetIDForCluster(a.clusterID)
	targets, err := s.listTargets(ctx)
	if err != nil {
		return ClusterRecord{}, err
	}
	var existing *domain.TargetInfo
	for i := range targets {
		t := targets[i]
		if t.ID() == targetID {
			if !IsEnrolledTarget(t) {
				return ClusterRecord{}, clusterIDInUse(a.clusterID, t)
			}
			existing = &targets[i]
			continue
		}
		// One cluster, one target: the same API address with the same
		// CA is the same cluster, whatever it is called here.
		if t.Type() == TargetType &&
			NormalizeAPIServerURL(t.Properties()[PropAPIServer]) == a.conn.APIServer &&
			SameCA([]byte(t.Properties()[PropCACert]), a.conn.CACert) {
			e := enrollmentErr(ErrEnrollmentConflict, "cluster-already-known",
				"the cluster at %s is already known as target %q", a.conn.APIServer, t.ID())
			e.Details = map[string]any{"target": string(t.ID())}
			if id, ok := ClusterIDFromTarget(t); ok {
				e.Details["cluster"] = id
			}
			return ClusterRecord{}, e
		}
	}

	now := s.now().UTC()
	props := map[string]string{}
	firstEnrollment := existing == nil
	if existing != nil {
		for k, v := range existing.Properties() {
			props[k] = v
		}
	}
	// A different bundle than the one on record is a new enrollment
	// with that bundle's choices, not a credential refresh.
	applyChoices := firstEnrollment || a.applyChoices ||
		(a.bundle != nil && props[PropEnrollmentBundle] != a.bundle.ID)

	tokenRef := domain.SecretRef(fmt.Sprintf("targets/%s/sa-token", targetID))
	props[PropAPIServer] = a.conn.APIServer
	props[PropCACert] = string(a.conn.CACert)
	props[PropServiceAccountTokenRef] = string(tokenRef)
	props[PropClusterResourceName] = string(ClusterRecordName(a.clusterID))
	props[PropOrigin] = OriginEnrolled
	props[PropAccessForm] = AccessFormDirect
	props[PropServiceAccount] = a.probe.ServiceAccount.String()
	if firstEnrollment || props[PropEnrolledAt] == "" {
		props[PropEnrolledBy] = a.actor
		props[PropEnrolledAt] = now.Format(time.RFC3339)
	}
	if a.bundle != nil {
		props[PropEnrollmentBundle] = a.bundle.ID
	} else if applyChoices {
		delete(props, PropEnrollmentBundle)
	}
	if a.source != nil {
		raw, err := json.Marshal(a.source)
		if err != nil {
			return ClusterRecord{}, fmt.Errorf("encode enrollment source: %w", err)
		}
		props[PropEnrollmentSource] = string(raw)
	} else if applyChoices {
		delete(props, PropEnrollmentSource)
	}
	if applyChoices {
		props[PropDeliveryCredential] = a.deliveryRule
		delete(props, PropDeliveryGrantBy)
		delete(props, PropDeliveryGrantAt)
		if a.deliveryRule == DeliveryCredentialPlatform {
			props[PropDeliveryGrantBy] = a.grantBy
			props[PropDeliveryGrantAt] = now.Format(time.RFC3339)
		}
		if a.profile != "" {
			props[PropAccessProfile] = a.profile
		} else {
			delete(props, PropAccessProfile)
		}
		setLabelProperty(props, PropEnrollmentLabels, a.labels)
	}
	if a.externalLabels != nil {
		setLabelProperty(props, PropExternalLabels, a.externalLabels)
	}
	if props[PropDeliveryCredential] == "" {
		props[PropDeliveryCredential] = DeliveryCredentialSigned
	}

	if s.trust != nil {
		entries, err := s.trust(ctx)
		if err != nil {
			return ClusterRecord{}, fmt.Errorf("read trust settings: %w", err)
		}
		if len(entries) > 0 {
			raw, err := json.Marshal(entries)
			if err != nil {
				return ClusterRecord{}, fmt.Errorf("encode trust settings: %w", err)
			}
			props[propTrustBundle] = string(raw)
		} else {
			delete(props, propTrustBundle)
		}
	}

	labels := mergeLabels(labelsFromProperty(props[PropExternalLabels]), labelsFromProperty(props[PropEnrollmentLabels]))

	if err := s.vault.Put(ctx, tokenRef, []byte(a.conn.Token)); err != nil {
		return ClusterRecord{}, fmt.Errorf("store cluster credential: %w", err)
	}
	target := domain.NewTargetInfo(targetID, TargetType, a.clusterID, domain.TargetStateReady, labels, props,
		[]domain.ManifestType{ManifestManifestType})
	if err := s.putTarget(ctx, target, now); err != nil {
		if firstEnrollment {
			_ = s.vault.Delete(ctx, tokenRef)
		}
		return ClusterRecord{}, err
	}

	state := s.loadClusterState(ctx, a.clusterID)
	rights := a.probe.Rights
	state.obs = observationFromTarget(target, state.obs)
	state.obs.State = ClusterStateEnrolled
	state.obs.KubernetesVersion = a.probe.KubernetesVersion
	state.obs.Rights = &rights
	state.obs.CredentialExpiresAt = timePtr(a.probe.TokenExpiresAt)
	state.obs.CredentialRenewedAt = &now
	state.obs.WithdrawnAt = nil
	state.obs.WithdrawnBy = ""
	state.obs.WithdrawnReason = ""
	state.set(ClusterConditionEnrolled, domain.ConditionTrue, "Enrolled", "the cluster's side presented a credential that the cluster confirms", now)
	state.set(ClusterConditionReachable, domain.ConditionTrue, "Reachable", "the cluster's API answered", now)
	state.set(ClusterConditionCredentialValid, domain.ConditionTrue, "Valid", "the cluster recognizes the stored credential", now)

	indexErr := s.ensureIndexer(ctx, target, []byte(a.conn.Token))
	state.setIndexing(s.indexing != nil, indexErr, now)
	state.setReady(now)
	if err := s.writeClusterState(ctx, a.clusterID, labels, state, now); err != nil {
		return ClusterRecord{}, err
	}
	s.forgetProbeHistory(a.clusterID)

	if firstEnrollment {
		s.logger.Info("cluster enrolled", "cluster", a.clusterID, "target", string(targetID), "by", a.actor,
			"delivery_credential", props[PropDeliveryCredential])
		s.notify(a.clusterID)
	} else {
		s.logger.Info("cluster credential refreshed", "cluster", a.clusterID, "by", a.actor)
	}
	return ClusterRecord{ID: a.clusterID, Name: string(ClusterRecordName(a.clusterID)), Labels: labels, Observation: state.obs}, nil
}

func setLabelProperty(props map[string]string, key string, labels map[string]string) {
	if v := labelsToProperty(labels); v != "" {
		props[key] = v
	} else {
		delete(props, key)
	}
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	t = t.UTC()
	return &t
}

// WithdrawRequest is the input of [EnrollmentService.Withdraw].
type WithdrawRequest struct {
	Actor  string
	Reason string
	// LeaveDeployments lets the withdrawal proceed although deployments
	// still resolve to the cluster's target.
	LeaveDeployments bool
}

// Withdraw lets an enrolled cluster go from the management side: the
// indexer stops, the cluster's indexed objects, its credential, and
// its target are removed, and the record says withdrawn. Nothing on
// the cluster is touched.
//
// It is refused while deployments still resolve to the cluster's
// target, so that nobody loses the means to remove what they delivered
// without knowing. With LeaveDeployments it proceeds all the same: for
// a cluster that no longer exists, for a deployment whose removal the
// cluster refuses, or for a deployment placed by label that also runs
// elsewhere. What those deployments delivered then stays on the
// cluster, and the record names them.
func (s *EnrollmentService) Withdraw(ctx context.Context, clusterID string, req WithdrawRequest) (ClusterRecord, error) {
	if err := ValidateClusterID(clusterID); err != nil {
		return ClusterRecord{}, err
	}
	lock := s.lockCluster(clusterID)
	defer lock.Unlock()

	target, found, err := s.getTarget(ctx, TargetIDForCluster(clusterID))
	if err != nil {
		return ClusterRecord{}, err
	}
	if !found {
		return ClusterRecord{}, fmt.Errorf("%w: cluster %q is not enrolled", domain.ErrNotFound, clusterID)
	}
	if !IsEnrolledTarget(target) {
		return ClusterRecord{}, enrollmentErr(ErrEnrollmentConflict, "not-enrolled",
			"cluster %q was not taken in by enrollment and cannot be withdrawn", clusterID)
	}
	inUse, err := s.deploymentsUsingTarget(ctx, target.ID())
	if err != nil {
		return ClusterRecord{}, err
	}
	if len(inUse) > 0 && !req.LeaveDeployments {
		e := enrollmentErr(ErrEnrollmentConflict, "deployments-resolve-to-target",
			"%d deployment(s) still resolve to target %q; delete them first, or withdraw with leaveDeployments to leave on the cluster what they delivered", len(inUse), target.ID())
		e.Details = map[string]any{"deployments": inUse, "target": string(target.ID())}
		return ClusterRecord{}, e
	}
	reason := req.Reason
	if reason == "" {
		reason = "withdrawn from the management side"
	}
	if len(inUse) > 0 {
		reason += "; deployments left as they were, with what they delivered still on the cluster: " + strings.Join(inUse, ", ")
	}
	return s.release(ctx, clusterID, target, req.Actor, reason)
}

// release removes everything the resource manager holds for an
// enrolled cluster except its record, which it marks withdrawn.
// The caller holds the cluster's lock.
func (s *EnrollmentService) release(ctx context.Context, clusterID string, target domain.TargetInfo, actor, reason string) (ClusterRecord, error) {
	now := s.now().UTC()
	if s.indexing != nil {
		if err := s.indexing.StopIndexer(ctx, target.ID()); err != nil {
			s.logger.Warn("could not stop indexer on withdrawal", "cluster", clusterID, "error", err)
		}
	}
	purged, err := s.records.PurgeCollectionPrefix(ctx, ObjectResourceType, clusterObjectPrefix(clusterID))
	if err != nil {
		return ClusterRecord{}, fmt.Errorf("remove indexed objects of cluster %q: %w", clusterID, err)
	}
	if ref := target.Properties()[PropServiceAccountTokenRef]; ref != "" {
		if err := s.vault.Delete(ctx, domain.SecretRef(ref)); err != nil && !errors.Is(err, domain.ErrNotFound) {
			return ClusterRecord{}, fmt.Errorf("remove credential of cluster %q: %w", clusterID, err)
		}
	}
	if err := s.deleteTarget(ctx, target.ID()); err != nil {
		return ClusterRecord{}, err
	}

	state := s.loadClusterState(ctx, clusterID)
	state.obs = observationFromTarget(target, state.obs)
	state.obs.State = ClusterStateWithdrawn
	state.obs.Target = ""
	state.obs.WithdrawnAt = &now
	state.obs.WithdrawnBy = actor
	state.obs.WithdrawnReason = reason
	state.obs.CredentialExpiresAt = nil
	state.set(ClusterConditionEnrolled, domain.ConditionFalse, "Withdrawn", reason, now)
	state.set(ClusterConditionCredentialValid, domain.ConditionFalse, "Withdrawn", "the credential was removed", now)
	state.set(ClusterConditionIndexing, domain.ConditionFalse, "Withdrawn", "the cluster is not indexed", now)
	state.setReady(now)
	if err := s.writeClusterState(ctx, clusterID, target.Labels(), state, now); err != nil {
		return ClusterRecord{}, err
	}
	s.forgetProbeHistory(clusterID)
	s.logger.Info("cluster withdrawn", "cluster", clusterID, "by", actor, "reason", reason, "objects_removed", purged)
	s.notify(clusterID)
	return ClusterRecord{ID: clusterID, Name: string(ClusterRecordName(clusterID)), Labels: target.Labels(), Observation: state.obs}, nil
}

// Forget removes the record of a cluster that is not enrolled. It is
// the explicit act that lets a withdrawn cluster be enrolled again by
// a bundle that existed before the withdrawal.
func (s *EnrollmentService) Forget(ctx context.Context, clusterID string) error {
	if err := ValidateClusterID(clusterID); err != nil {
		return err
	}
	lock := s.lockCluster(clusterID)
	defer lock.Unlock()

	if _, err := s.readRecord(ctx, ClusterResourceType, ClusterRecordName(clusterID)); err != nil {
		return err
	}
	if _, found, err := s.getTarget(ctx, TargetIDForCluster(clusterID)); err != nil {
		return err
	} else if found {
		return enrollmentErr(ErrEnrollmentConflict, "still-enrolled",
			"cluster %q is enrolled; withdraw it before forgetting it", clusterID)
	}
	if err := s.records.ReplaceBatch(ctx, ClusterResourceType, []InventoryObjectReport{{
		Name: ClusterRecordName(clusterID), IsDelete: true,
	}}); err != nil {
		return fmt.Errorf("delete record of cluster %q: %w", clusterID, err)
	}
	s.notify(clusterID)
	return nil
}

// RenewCredential asks the cluster for a new credential now, with the
// one the resource manager holds, and stores it in its place.
func (s *EnrollmentService) RenewCredential(ctx context.Context, clusterID string) (ClusterRecord, error) {
	if err := ValidateClusterID(clusterID); err != nil {
		return ClusterRecord{}, err
	}
	lock := s.lockCluster(clusterID)
	defer lock.Unlock()

	target, found, err := s.getTarget(ctx, TargetIDForCluster(clusterID))
	if err != nil {
		return ClusterRecord{}, err
	}
	if !found || !IsEnrolledTarget(target) {
		return ClusterRecord{}, fmt.Errorf("%w: cluster %q is not enrolled", domain.ErrNotFound, clusterID)
	}
	token, err := s.storedToken(ctx, target)
	if err != nil {
		return ClusterRecord{}, err
	}
	if _, err := s.renew(ctx, clusterID, target, token); err != nil {
		if errors.Is(err, ErrClusterRejectedCredential) {
			return ClusterRecord{}, enrollmentErr(ErrEnrollmentUnverified, "credential-rejected",
				"the cluster does not recognize the stored credential; its side has to present a new one")
		}
		if errors.Is(err, ErrClusterDeniedRequest) {
			return ClusterRecord{}, enrollmentErr(ErrEnrollmentUnverified, "renewal-denied",
				"the cluster does not let the access account request its own token")
		}
		return ClusterRecord{}, err
	}
	return s.GetCluster(ctx, clusterID)
}

// renew requests a new token and stores it. The caller holds the
// cluster's lock. It returns the new token.
func (s *EnrollmentService) renew(ctx context.Context, clusterID string, target domain.TargetInfo, token []byte) ([]byte, error) {
	props := target.Properties()
	sa, ok := ParseServiceAccountRef(props[PropServiceAccount])
	if !ok {
		return nil, fmt.Errorf("%w: target %q records no service account to renew for", domain.ErrInvalidArgument, target.ID())
	}
	lifetime := DefaultTokenLifetime
	if iat, exp := TokenLifetime(string(token)); !iat.IsZero() && exp.After(iat) {
		lifetime = exp.Sub(iat)
	}
	if lifetime < MinTokenLifetime {
		lifetime = MinTokenLifetime
	}
	conn := ClusterConnection{APIServer: props[PropAPIServer], CACert: []byte(props[PropCACert]), Token: string(token)}
	newToken, expiresAt, err := s.prober.RenewToken(ctx, conn, sa, lifetime)
	if err != nil {
		return nil, err
	}
	ref := domain.SecretRef(props[PropServiceAccountTokenRef])
	if err := s.vault.Put(ctx, ref, []byte(newToken)); err != nil {
		return nil, fmt.Errorf("store renewed credential: %w", err)
	}

	now := s.now().UTC()
	state := s.loadClusterState(ctx, clusterID)
	state.obs = observationFromTarget(target, state.obs)
	state.obs.State = ClusterStateEnrolled
	if _, exp := TokenLifetime(newToken); !exp.IsZero() {
		expiresAt = exp
	}
	state.obs.CredentialExpiresAt = timePtr(expiresAt)
	state.obs.CredentialRenewedAt = &now
	state.set(ClusterConditionCredentialValid, domain.ConditionTrue, "Renewed", "the cluster issued a new credential", now)
	state.set(ClusterConditionReachable, domain.ConditionTrue, "Reachable", "the cluster's API answered", now)
	indexErr := s.ensureIndexer(ctx, target, []byte(newToken))
	state.setIndexing(s.indexing != nil, indexErr, now)
	state.setReady(now)
	if err := s.writeClusterState(ctx, clusterID, target.Labels(), state, now); err != nil {
		return nil, err
	}
	s.logger.Info("cluster credential renewed", "cluster", clusterID, "expires_at", expiresAt.UTC().Format(time.RFC3339))
	return []byte(newToken), nil
}

// SetExternalLabels replaces the labels an integration contributes to
// an enrolled cluster's target. The enrollment's own labels are kept
// and win where both name a key. It reports whether anything changed.
func (s *EnrollmentService) SetExternalLabels(ctx context.Context, clusterID string, labels map[string]string) (bool, error) {
	if err := validateLabels(labels); err != nil {
		return false, err
	}
	lock := s.lockCluster(clusterID)
	defer lock.Unlock()

	target, found, err := s.getTarget(ctx, TargetIDForCluster(clusterID))
	if err != nil {
		return false, err
	}
	if !found || !IsEnrolledTarget(target) {
		return false, fmt.Errorf("%w: cluster %q is not enrolled", domain.ErrNotFound, clusterID)
	}
	props := map[string]string{}
	for k, v := range target.Properties() {
		props[k] = v
	}
	setLabelProperty(props, PropExternalLabels, labels)
	merged := mergeLabels(labelsFromProperty(props[PropExternalLabels]), labelsFromProperty(props[PropEnrollmentLabels]))
	if props[PropExternalLabels] == target.Properties()[PropExternalLabels] && labelsEqual(merged, target.Labels()) {
		return false, nil
	}

	now := s.now().UTC()
	updated := domain.NewTargetInfo(target.ID(), target.Type(), target.Name(), target.State(), merged, props, target.AcceptedManifestTypes())
	if err := s.putTarget(ctx, updated, now); err != nil {
		return false, err
	}
	state := s.loadClusterState(ctx, clusterID)
	state.obs = observationFromTarget(updated, state.obs)
	if err := s.writeClusterState(ctx, clusterID, merged, state, now); err != nil {
		return false, err
	}
	return true, nil
}

func labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// GetCluster returns the record of a cluster.
func (s *EnrollmentService) GetCluster(ctx context.Context, clusterID string) (ClusterRecord, error) {
	res, err := s.readRecord(ctx, ClusterResourceType, ClusterRecordName(clusterID))
	if err != nil {
		return ClusterRecord{}, err
	}
	return clusterRecordFromResource(res)
}

// ListClusters returns every cluster record, enrolled or withdrawn.
func (s *EnrollmentService) ListClusters(ctx context.Context) ([]ClusterRecord, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	resources, err := tx.ExtensionResources().ListByResourceType(ctx, ClusterResourceType)
	if err != nil {
		return nil, fmt.Errorf("list cluster records: %w", err)
	}
	out := make([]ClusterRecord, 0, len(resources))
	for _, res := range resources {
		rec, err := clusterRecordFromResource(res)
		if err != nil {
			s.logger.Warn("skipping unreadable cluster record", "name", string(res.Name()), "error", err)
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// EnrolledTargets returns the targets taken in by enrollment.
func (s *EnrollmentService) EnrolledTargets(ctx context.Context) ([]domain.TargetInfo, error) {
	targets, err := s.listTargets(ctx)
	if err != nil {
		return nil, err
	}
	out := targets[:0]
	for _, t := range targets {
		if IsEnrolledTarget(t) {
			out = append(out, t)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------
// Store access
// ---------------------------------------------------------------------

func (s *EnrollmentService) listTargets(ctx context.Context) ([]domain.TargetInfo, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	targets, err := tx.Targets().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list targets: %w", err)
	}
	return targets, nil
}

func (s *EnrollmentService) getTarget(ctx context.Context, id domain.TargetID) (domain.TargetInfo, bool, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return domain.TargetInfo{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	target, err := tx.Targets().Get(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		return domain.TargetInfo{}, false, nil
	}
	if err != nil {
		return domain.TargetInfo{}, false, fmt.Errorf("get target %q: %w", id, err)
	}
	return target, true, nil
}

// putTarget upserts a target and its inventory item in one
// transaction, the same pair a provisioner's delivery output produces.
func (s *EnrollmentService) putTarget(ctx context.Context, target domain.TargetInfo, now time.Time) error {
	props, err := json.Marshal(publicTargetProperties(target.Properties()))
	if err != nil {
		return fmt.Errorf("encode target properties: %w", err)
	}
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Inventory().CreateOrUpdate(ctx, domain.NewInventoryItem(
		target.InventoryItemID(), domain.InventoryItemType(target.Type()), target.Name(), props, target.Labels(), nil, now,
	)); err != nil {
		return fmt.Errorf("upsert inventory item for target %q: %w", target.ID(), err)
	}
	if err := tx.Targets().CreateOrUpdate(ctx, target); err != nil {
		return fmt.Errorf("upsert target %q: %w", target.ID(), err)
	}
	return tx.Commit()
}

// publicTargetProperties drops the properties that are only useful to
// the delivery path (CA, trust settings) from the copy kept on the
// inventory item.
func publicTargetProperties(props map[string]string) map[string]string {
	out := make(map[string]string, len(props))
	for k, v := range props {
		if k == PropCACert || k == propTrustBundle {
			continue
		}
		out[k] = v
	}
	return out
}

func (s *EnrollmentService) deleteTarget(ctx context.Context, id domain.TargetID) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Targets().Delete(ctx, id); err != nil && !errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("delete target %q: %w", id, err)
	}
	invID := domain.InventoryItemID("target:" + string(id))
	if err := tx.Inventory().Delete(ctx, invID); err != nil && !errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("delete inventory item %q: %w", invID, err)
	}
	return tx.Commit()
}

// deploymentsUsingTarget names the deployments whose placement
// resolved to the target or statically names it.
func (s *EnrollmentService) deploymentsUsingTarget(ctx context.Context, id domain.TargetID) ([]string, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	views, err := tx.Deployments().ListView(ctx)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	var out []string
	for _, v := range views {
		placement := v.Fulfillment.PlacementStrategy()
		if slices.Contains(v.Fulfillment.ResolvedTargets(), id) ||
			(placement.Type == domain.PlacementStrategyStatic && slices.Contains(placement.Targets, id)) {
			out = append(out, string(v.Deployment.Name()))
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *EnrollmentService) storedToken(ctx context.Context, target domain.TargetInfo) ([]byte, error) {
	ref := target.Properties()[PropServiceAccountTokenRef]
	if ref == "" {
		return nil, fmt.Errorf("%w: target %q has no stored credential", domain.ErrNotFound, target.ID())
	}
	token, err := s.vault.Get(ctx, domain.SecretRef(ref))
	if err != nil {
		return nil, fmt.Errorf("read credential of target %q: %w", target.ID(), err)
	}
	if len(token) == 0 {
		return nil, fmt.Errorf("%w: target %q has an empty stored credential", domain.ErrNotFound, target.ID())
	}
	return token, nil
}

func (s *EnrollmentService) readRecord(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (*domain.ExtensionResource, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	res, err := tx.ExtensionResources().Get(ctx, rt.FullName(name))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func decodeObservation(res *domain.ExtensionResource, into any) error {
	inv := res.Inventory()
	if inv == nil || inv.Observation() == nil {
		return fmt.Errorf("record %q has no observation", res.Name())
	}
	if err := json.Unmarshal(*inv.Observation(), into); err != nil {
		return fmt.Errorf("decode observation of %q: %w", res.Name(), err)
	}
	return nil
}

func clusterRecordFromResource(res *domain.ExtensionResource) (ClusterRecord, error) {
	var obs ClusterObservation
	if err := decodeObservation(res, &obs); err != nil {
		return ClusterRecord{}, err
	}
	labels := map[string]string{}
	if inv := res.Inventory(); inv != nil {
		for k, v := range inv.Labels() {
			labels[k] = v
		}
	}
	return ClusterRecord{ID: string(res.Name().ID()), Name: string(res.Name()), Labels: labels, Observation: obs}, nil
}

// ensureIndexer starts (or replaces) the indexer of an enrolled
// target with the given credential. It returns nil when indexing is
// not configured.
func (s *EnrollmentService) ensureIndexer(ctx context.Context, target domain.TargetInfo, token []byte) error {
	if s.indexing == nil {
		return nil
	}
	props := target.Properties()
	clusterName, err := ParseClusterResourceName(props[PropClusterResourceName])
	if err != nil {
		return err
	}
	input, err := NewIndexRuntimeInput(
		target.ID(), clusterName, props[PropAPIServer], props[PropCACert], token,
		domain.SecretRef(props[PropServiceAccountTokenRef]), StartupReplayGeneration, DefaultIndexConfig(),
	)
	if err != nil {
		return err
	}
	return RetryLocalEnvelope(ctx, indexerStartTimeout, func(attemptCtx context.Context) error {
		return s.indexing.EnsureIndexer(attemptCtx, input)
	})
}
