package ocm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// Deps are the collaborators of a [Service].
type Deps struct {
	Store   domain.Store
	Vault   domain.Vault
	Records kubernetesaddon.InventoryRecordStore
	// Enrollment is the resource manager's enrollment capability. It
	// may be nil: hubs are then observed and nothing more.
	Enrollment *kubernetesaddon.EnrollmentService
	// Clients defaults to [ClientGoHubClients].
	Clients HubClients
	Logger  *slog.Logger
	// Now defaults to the wall clock.
	Now func() time.Time
}

// Service keeps hub connections and what is observed through them.
type Service struct {
	store      domain.Store
	vault      domain.Vault
	records    kubernetesaddon.InventoryRecordStore
	enrollment *kubernetesaddon.EnrollmentService
	clients    HubClients
	logger     *slog.Logger
	now        func() time.Time

	mu     sync.Mutex
	hubs   map[string]*hubWatch
	appCtx context.Context
	closed bool
}

// NewService builds the service. With an enrollment capability it
// registers itself as the verifier for clusters presented under a
// hub-scoped bundle.
func NewService(deps Deps) *Service {
	s := &Service{
		store:      deps.Store,
		vault:      deps.Vault,
		records:    deps.Records,
		enrollment: deps.Enrollment,
		clients:    deps.Clients,
		logger:     deps.Logger.With("component", "ocm-integration"),
		now:        deps.Now,
		hubs:       map[string]*hubWatch{},
	}
	if s.clients == nil {
		s.clients = ClientGoHubClients{}
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.enrollment != nil {
		s.enrollment.RegisterVerifier(VerifierName, s)
		s.enrollment.OnChange(func(string) { s.markLinksDirty() })
	}
	return s
}

// hubErr builds a failure with a stable reason word, in the form the
// enrollment routes already answer with.
func hubErr(kind error, reason, format string, args ...any) *kubernetesaddon.EnrollmentError {
	return &kubernetesaddon.EnrollmentError{Kind: kind, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// Start reconnects every hub that was connected before. ctx is the
// application's context: watches run until it ends or Close is called.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	s.appCtx = ctx
	s.mu.Unlock()

	records, err := s.listRecords(ctx, HubResourceType)
	if err != nil {
		return fmt.Errorf("list hub connections: %w", err)
	}
	for _, res := range records {
		hubID := string(res.Name().ID())
		var obs HubObservation
		if err := decodeObservation(res, &obs); err != nil {
			s.logger.Warn("skipping unreadable hub record", "hub", hubID, "error", err)
			continue
		}
		conn, err := s.loadConnection(ctx, hubID)
		if err != nil {
			s.logger.Warn("hub connection has no usable stored credential; connect the hub again", "hub", hubID, "error", err)
			continue
		}
		if err := s.startWatch(hubID, conn, obs); err != nil {
			s.logger.Warn("could not start watching hub", "hub", hubID, "error", err)
		}
	}
	return nil
}

// Close stops every watch and waits for them.
func (s *Service) Close(context.Context) error {
	s.mu.Lock()
	s.closed = true
	hubs := make([]*hubWatch, 0, len(s.hubs))
	for _, hw := range s.hubs {
		hubs = append(hubs, hw)
	}
	s.hubs = map[string]*hubWatch{}
	s.mu.Unlock()
	for _, hw := range hubs {
		hw.stop()
	}
	return nil
}

func (s *Service) hub(hubID string) *hubWatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hubs[hubID]
}

func (s *Service) hubList() []*hubWatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*hubWatch, 0, len(s.hubs))
	for _, hw := range s.hubs {
		out = append(out, hw)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func (s *Service) markLinksDirty() {
	for _, hw := range s.hubList() {
		hw.markLinksDirty()
	}
}

// startWatch creates and starts the watch of one hub.
func (s *Service) startWatch(hubID string, conn HubConnection, obs HubObservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.appCtx == nil {
		return fmt.Errorf("the integration is not running")
	}
	hw := newHubWatch(s, hubID, conn, obs)
	if err := hw.start(s.appCtx); err != nil {
		return err
	}
	s.hubs[hubID] = hw
	return nil
}

// ConnectRequest is the input of [Service.Connect].
type ConnectRequest struct {
	HubID       string
	DisplayName string
	Connection  HubConnection
	// Actor is the authenticated subject.
	Actor string
}

// Connect starts observing a hub. The credential is checked against
// the hub first: it must be able to list and watch ManagedCluster
// objects. Connecting an id that is already connected to the same hub
// replaces its credential.
func (s *Service) Connect(ctx context.Context, req ConnectRequest) (HubRecord, error) {
	if err := ValidateHubID(req.HubID); err != nil {
		return HubRecord{}, err
	}
	conn := req.Connection
	conn.APIServer = kubernetesaddon.NormalizeAPIServerURL(conn.APIServer)
	if err := kubernetesaddon.ValidateAPIServerURL(conn.APIServer); err != nil {
		return HubRecord{}, err
	}
	fingerprints, err := kubernetesaddon.CAFingerprints([]byte(conn.CACert))
	if err != nil {
		return HubRecord{}, err
	}
	if conn.Token == "" {
		return HubRecord{}, fmt.Errorf("%w: a credential issued by the hub is required", domain.ErrInvalidArgument)
	}
	if len(req.DisplayName) > 120 || strings.ContainsAny(req.DisplayName, "\n\r") {
		return HubRecord{}, fmt.Errorf("%w: display name must be a single line of at most 120 characters", domain.ErrInvalidArgument)
	}

	var replaced *hubWatch
	for _, other := range s.hubList() {
		otherConn := other.connection()
		same := otherConn.APIServer == conn.APIServer && kubernetesaddon.SameCA([]byte(otherConn.CACert), []byte(conn.CACert))
		switch {
		case other.id == req.HubID && !same:
			e := hubErr(kubernetesaddon.ErrEnrollmentConflict, "hub-id-in-use",
				"hub id %q is connected to another hub (%s); disconnect it first or choose another id", req.HubID, otherConn.APIServer)
			return HubRecord{}, e
		case other.id != req.HubID && same:
			e := hubErr(kubernetesaddon.ErrEnrollmentConflict, "hub-already-connected",
				"the hub at %s is already connected as %q", conn.APIServer, other.id)
			e.Details = map[string]any{"hub": other.id}
			return HubRecord{}, e
		case other.id == req.HubID:
			replaced = other
		}
	}

	probe, err := s.clients.Probe(ctx, conn)
	switch {
	case err == nil:
	case errors.Is(err, kubernetesaddon.ErrClusterRejectedCredential):
		return HubRecord{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "credential-rejected",
			"the hub at %s does not recognize the credential", conn.APIServer)
	case errors.Is(err, errNotAHub):
		return HubRecord{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "not-a-hub",
			"the cluster at %s does not serve the ManagedCluster API: it is not an Open Cluster Management hub", conn.APIServer)
	case errors.Is(err, domain.ErrInvalidArgument):
		return HubRecord{}, err
	default:
		return HubRecord{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "hub-unreachable",
			"the hub at %s could not be verified with the given CA: %v", conn.APIServer, err)
	}
	if !probe.CanRead {
		return HubRecord{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "access-denied",
			"the credential (%s) may not list and watch ManagedCluster objects on the hub", probe.Username)
	}

	now := s.now().UTC()
	obs := HubObservation{
		ID:               req.HubID,
		DisplayName:      req.DisplayName,
		APIServer:        conn.APIServer,
		CAFingerprint:    fingerprints[0],
		ServiceAccount:   probe.ServiceAccount.String(),
		ConnectedBy:      req.Actor,
		ConnectedAt:      &now,
		WriteAccessFound: probe.CanWrite,
	}
	if !probe.TokenExpiresAt.IsZero() {
		exp := probe.TokenExpiresAt.UTC()
		obs.CredentialExpiresAt = &exp
	}
	if replaced != nil {
		prev := replaced.observation()
		obs.ConnectedBy, obs.ConnectedAt = prev.ConnectedBy, prev.ConnectedAt
		if obs.DisplayName == "" {
			obs.DisplayName = prev.DisplayName
		}
	}

	if err := s.storeConnection(ctx, req.HubID, conn); err != nil {
		return HubRecord{}, err
	}
	if replaced != nil {
		s.mu.Lock()
		delete(s.hubs, req.HubID)
		s.mu.Unlock()
		replaced.stop()
	}
	if err := s.startWatch(req.HubID, conn, obs); err != nil {
		return HubRecord{}, err
	}
	hw := s.hub(req.HubID)
	if err := hw.writeHubRecord(ctx, now, true); err != nil {
		return HubRecord{}, err
	}
	s.logger.Info("hub connected", "hub", req.HubID, "api_server", conn.APIServer, "by", req.Actor,
		"identity", probe.Username, "write_access_found", probe.CanWrite)
	return hw.record(), nil
}

// Disconnect stops observing a hub and removes what was observed
// through it: the hub's record, the records of its clusters, the
// stored credential, and the enrollment bundles scoped to the hub.
// Clusters enrolled through the hub stay enrolled; the labels the hub
// contributed to their targets stay as they were and are no longer
// kept up to date.
func (s *Service) Disconnect(ctx context.Context, hubID, actor string) error {
	if err := ValidateHubID(hubID); err != nil {
		return err
	}
	s.mu.Lock()
	hw := s.hubs[hubID]
	delete(s.hubs, hubID)
	s.mu.Unlock()
	if hw != nil {
		hw.stop()
	} else if _, err := s.readRecord(ctx, HubResourceType, HubRecordName(hubID)); err != nil {
		return err
	}

	removed, err := s.records.PurgeCollectionPrefix(ctx, ManagedClusterResourceType, managedClusterPrefix(hubID))
	if err != nil {
		return fmt.Errorf("remove observed clusters of hub %q: %w", hubID, err)
	}
	if err := s.records.ReplaceBatch(ctx, HubResourceType, []kubernetesaddon.InventoryObjectReport{{
		Name: HubRecordName(hubID), IsDelete: true,
	}}); err != nil {
		return fmt.Errorf("remove record of hub %q: %w", hubID, err)
	}
	if err := s.vault.Delete(ctx, hubConnectionRef(hubID)); err != nil && !errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("remove credential of hub %q: %w", hubID, err)
	}
	revoked := 0
	if s.enrollment != nil {
		bundles, err := s.enrollment.ListBundles(ctx)
		if err != nil {
			return err
		}
		for _, b := range bundles {
			if b.Scope.Verifier != VerifierName || b.Scope.VerifierScope != hubID {
				continue
			}
			if err := s.enrollment.RevokeBundle(ctx, b.ID, actor); err != nil && !errors.Is(err, domain.ErrNotFound) {
				return err
			}
			revoked++
		}
	}
	s.logger.Info("hub disconnected", "hub", hubID, "by", actor, "observed_clusters_removed", removed, "bundles_revoked", revoked)
	return nil
}

// GetHub returns the record of a connected hub.
func (s *Service) GetHub(_ context.Context, hubID string) (HubRecord, error) {
	hw := s.hub(hubID)
	if hw == nil {
		return HubRecord{}, fmt.Errorf("%w: hub %q is not connected", domain.ErrNotFound, hubID)
	}
	return hw.record(), nil
}

// ---------------------------------------------------------------------
// Vouching for presented clusters
// ---------------------------------------------------------------------

// VerifyPresentation implements [kubernetesaddon.PresentationVerifier]
// for bundles scoped to a hub. It vouches for a cluster only when the
// hub knows it, has accepted it, and records an API address and a CA
// for it that agree with the CA the cluster's side presented. The
// address and CA returned are the hub's record, never the presenter's.
func (s *Service) VerifyPresentation(_ context.Context, scope, clusterName string, presentedCA []byte) (kubernetesaddon.VerifiedCluster, error) {
	hw := s.hub(scope)
	if hw == nil {
		return kubernetesaddon.VerifiedCluster{}, hubErr(kubernetesaddon.ErrEnrollmentRefused, "hub-not-connected",
			"hub %q is not connected to this resource manager", scope)
	}
	if !hw.isSynced() {
		return kubernetesaddon.VerifiedCluster{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "hub-not-synced",
			"the clusters of hub %q have not been read yet; try again shortly", scope)
	}
	c, ok := hw.cluster(clusterName)
	if !ok {
		return kubernetesaddon.VerifiedCluster{}, hubErr(kubernetesaddon.ErrEnrollmentRefused, "cluster-not-on-hub",
			"hub %q does not know a cluster named %q", scope, clusterName)
	}
	if !c.accepted {
		return kubernetesaddon.VerifiedCluster{}, hubErr(kubernetesaddon.ErrEnrollmentRefused, "cluster-not-accepted",
			"hub %q has not accepted cluster %q", scope, clusterName)
	}
	if len(c.configs) == 0 {
		return kubernetesaddon.VerifiedCluster{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "no-recorded-address",
			"hub %q records no API address for cluster %q, so what the cluster presents cannot be checked against the hub; "+
				"give the cluster's registration agent the cluster's external API address so that the hub records it, or enroll the cluster with a bundle of its own",
			scope, clusterName)
	}
	for _, cfg := range c.configs {
		if kubernetesaddon.SameCA(cfg.ca, presentedCA) {
			return kubernetesaddon.VerifiedCluster{
				ClusterID: ClusterIDFor(scope, clusterName),
				APIServer: cfg.url,
				CACert:    cfg.ca,
				Labels:    c.targetLabels(scope),
			}, nil
		}
	}
	return kubernetesaddon.VerifiedCluster{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "ca-mismatch",
		"the CA presented for cluster %q is not the one hub %q records for it", clusterName, scope)
}

var _ kubernetesaddon.PresentationVerifier = (*Service)(nil)

// ExplainUnknownTarget tells a caller who named an observed cluster as
// a deployment target why that is not a target. It returns "" for a
// name the integration does not know.
func (s *Service) ExplainUnknownTarget(_ context.Context, id domain.TargetID) string {
	name := strings.TrimPrefix(string(id), "k8s-")
	for _, hw := range s.hubList() {
		candidate := name
		if rest, ok := strings.CutPrefix(name, hw.id+"."); ok {
			candidate = rest
		}
		if _, ok := hw.cluster(candidate); !ok {
			continue
		}
		link := hw.linkOf(candidate)
		switch link.link.State {
		case LinkEnrolled, LinkCreated:
			return fmt.Sprintf("cluster %q of hub %q is known to the resource manager as target %q", candidate, hw.id, link.link.Target)
		default:
			return fmt.Sprintf("cluster %q of hub %q is observed, not enrolled: the resource manager cannot deliver to it until it is enrolled (its target will then be %q)",
				candidate, hw.id, kubernetesaddon.TargetIDForCluster(ClusterIDFor(hw.id, candidate)))
		}
	}
	return ""
}

// ---------------------------------------------------------------------
// Store and vault access
// ---------------------------------------------------------------------

func (s *Service) storeConnection(ctx context.Context, hubID string, conn HubConnection) error {
	raw, err := json.Marshal(conn)
	if err != nil {
		return fmt.Errorf("encode hub connection: %w", err)
	}
	if err := s.vault.Put(ctx, hubConnectionRef(hubID), raw); err != nil {
		return fmt.Errorf("store hub credential: %w", err)
	}
	return nil
}

func (s *Service) loadConnection(ctx context.Context, hubID string) (HubConnection, error) {
	raw, err := s.vault.Get(ctx, hubConnectionRef(hubID))
	if err != nil {
		return HubConnection{}, err
	}
	var conn HubConnection
	if err := json.Unmarshal(raw, &conn); err != nil {
		return HubConnection{}, fmt.Errorf("decode stored hub connection: %w", err)
	}
	if conn.APIServer == "" || conn.CACert == "" || conn.Token == "" {
		return HubConnection{}, fmt.Errorf("stored hub connection is incomplete")
	}
	return conn, nil
}

func (s *Service) listRecords(ctx context.Context, rt domain.ResourceType) ([]*domain.ExtensionResource, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	return tx.ExtensionResources().ListByResourceType(ctx, rt)
}

func (s *Service) readRecord(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (*domain.ExtensionResource, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	return tx.ExtensionResources().Get(ctx, rt.FullName(name))
}

// recordedClusterNames returns the names of the clusters of a hub that
// have a record.
func (s *Service) recordedClusterNames(ctx context.Context, hubID string) ([]string, error) {
	records, err := s.listRecords(ctx, ManagedClusterResourceType)
	if err != nil {
		return nil, fmt.Errorf("list observed clusters: %w", err)
	}
	prefix := string(ManagedClusterRecordName(hubID, ""))
	var out []string
	for _, res := range records {
		if name, ok := strings.CutPrefix(string(res.Name()), prefix); ok && name != "" {
			out = append(out, name)
		}
	}
	return out, nil
}

func (s *Service) listTargets(ctx context.Context) ([]domain.TargetInfo, error) {
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

// withdrawnClusters returns the ids of clusters whose record says
// they were withdrawn.
func (s *Service) withdrawnClusters(ctx context.Context) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	if s.enrollment == nil {
		return out, nil
	}
	records, err := s.enrollment.ListClusters(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		if rec.Observation.State == kubernetesaddon.ClusterStateWithdrawn {
			out[rec.ID] = struct{}{}
		}
	}
	return out, nil
}

func decodeObservation(res *domain.ExtensionResource, into any) error {
	inv := res.Inventory()
	if inv == nil || inv.Observation() == nil {
		return fmt.Errorf("record %q has no observation", res.Name())
	}
	return json.Unmarshal(*inv.Observation(), into)
}
