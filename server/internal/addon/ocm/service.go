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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

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
	// LifecycleProbe observes the hub lifecycle lock. Nil is valid and
	// observes nothing.
	LifecycleProbe LifecycleProbe
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

	// opMu serializes hub lifecycle changes. Connect and Disconnect hold
	// it from their first read of the connected hubs to their last write,
	// probes included. They are rare administrative operations, and
	// without it a Connect whose probe is still running can finish after
	// a Disconnect and restore the credential and record it removed, or
	// two Connects can leave a watch that is no longer in the map and
	// still writes.
	opMu sync.Mutex

	lifecycleProbe LifecycleProbe
}

// LifecycleOperation names a hub lifecycle operation that takes the hub
// lifecycle lock.
type LifecycleOperation string

const (
	LifecycleConnect         LifecycleOperation = "connect"
	LifecycleDisconnect      LifecycleOperation = "disconnect"
	LifecycleEnrollmentAddon LifecycleOperation = "enrollment-addon"
	LifecycleAdoptCluster    LifecycleOperation = "adopt-cluster"
)

// LifecycleProbe observes the hub lifecycle lock. Connect, Disconnect,
// and EnrollmentAddon call Attempt just before they queue for the lock,
// so a test can tell that an operation has reached the lock without
// waiting for a scheduling window: once Attempt returns, the operation
// can only proceed after the lock is released.
type LifecycleProbe interface {
	Attempt(LifecycleOperation)
}

// lockLifecycle takes the hub lifecycle lock, reporting the attempt to
// the probe first.
func (s *Service) lockLifecycle(op LifecycleOperation) {
	if s.lifecycleProbe != nil {
		s.lifecycleProbe.Attempt(op)
	}
	s.opMu.Lock()
}

// NewService builds the service. With an enrollment capability it
// registers itself as the verifier for clusters presented under a
// hub-scoped bundle.
func NewService(deps Deps) *Service {
	s := &Service{
		store:          deps.Store,
		vault:          deps.Vault,
		records:        deps.Records,
		enrollment:     deps.Enrollment,
		clients:        deps.Clients,
		logger:         deps.Logger.With("component", "ocm-integration"),
		now:            deps.Now,
		hubs:           map[string]*hubWatch{},
		lifecycleProbe: deps.LifecycleProbe,
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
	s.opMu.Lock()
	defer s.opMu.Unlock()

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
	s.opMu.Lock()
	defer s.opMu.Unlock()

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

// hubsWithUnaddressed returns the connected hubs other than except
// that have a cluster of the name with no API address on record.
func (s *Service) hubsWithUnaddressed(name, except string) []string {
	var out []string
	for _, hw := range s.hubList() {
		if hw.id == except {
			continue
		}
		if c, ok := hw.cluster(name); ok && len(c.configs) == 0 {
			out = append(out, hw.id)
		}
	}
	return out
}

// startWatch creates and starts the watch of one hub. A watch already
// registered for the id is replaced and stopped: two watches for one hub
// would both write its record and renew its credential.
func (s *Service) startWatch(hubID string, conn HubConnection, obs HubObservation) error {
	return s.startPreparedWatch(newHubWatch(s, hubID, conn, obs))
}

// startPreparedWatch starts and registers a watch that the caller has
// prepared, and whose record the caller has written.
func (s *Service) startPreparedWatch(hw *hubWatch) error {
	s.mu.Lock()
	if s.closed || s.appCtx == nil {
		s.mu.Unlock()
		return fmt.Errorf("the integration is not running")
	}
	appCtx := s.appCtx
	s.mu.Unlock()

	if err := hw.start(appCtx); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed || s.appCtx != appCtx {
		s.mu.Unlock()
		hw.stop()
		return fmt.Errorf("the integration is not running")
	}
	previous := s.hubs[hw.id]
	s.hubs[hw.id] = hw
	s.mu.Unlock()

	// Stop the replaced watch outside s.mu: its loop takes s.mu while
	// writing and must be able to finish.
	if previous != nil {
		previous.stop()
	}
	return nil
}

// stopWatch removes the watch registered for hubID, if any, and waits for
// its loop to exit. Gone from the registry, the watch is no longer current:
// a renewal it was finishing declines to store its token, and one that had
// already stored it has finished by the time this returns. It returns the
// connection, observation and record conditions the watch held, so that a
// caller that cannot replace it can put them back.
func (s *Service) stopWatch(hubID string) (HubConnection, HubObservation, []domain.Condition, bool) {
	s.mu.Lock()
	hw := s.hubs[hubID]
	delete(s.hubs, hubID)
	s.mu.Unlock()
	if hw == nil {
		return HubConnection{}, HubObservation{}, nil, false
	}
	conn, obs := hw.connection(), hw.observation()
	hw.stop()
	_, conds := hw.snapshot(s.now().UTC())
	return conn, obs, conds, true
}

// isCurrentWatch reports whether hw is the watch registered for its hub.
// A replaced or disconnected watch has no business writing the hub's
// record or storing a renewed credential: its replacement, or the
// disconnect, owns that state now.
func (s *Service) isCurrentWatch(hw *hubWatch) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hubs[hw.id] == hw
}

// storeHubRecord writes a hub's record as given.
func (s *Service) storeHubRecord(ctx context.Context, hubID string, raw json.RawMessage, conds []domain.Condition, now time.Time) error {
	msg := json.RawMessage(raw)
	if err := s.records.ReplaceBatch(ctx, HubResourceType, []kubernetesaddon.InventoryObjectReport{{
		Name:        HubRecordName(hubID),
		Labels:      map[string]string{},
		Observation: &msg,
		Conditions:  conds,
		ObservedAt:  now,
	}}); err != nil {
		return fmt.Errorf("write record of hub %q: %w", hubID, err)
	}
	return nil
}

// deleteHubRecord removes a hub's record.
func (s *Service) deleteHubRecord(ctx context.Context, hubID string) error {
	if err := s.records.ReplaceBatch(ctx, HubResourceType, []kubernetesaddon.InventoryObjectReport{{
		Name: HubRecordName(hubID), IsDelete: true,
	}}); err != nil {
		return fmt.Errorf("remove record of hub %q: %w", hubID, err)
	}
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
// replaces its credential, and its address when the hub has moved.
//
// A hub is the cluster it runs on. Where the credential may read the
// hub's identifier (see [HubProbe.ClusterUID]), that identifier decides
// whether two connections are one hub: the same hub is refused under a
// second id at whatever address, and a connected hub that is presented
// at a new address under its own id is the same connection, moved.
// Without an identifier the API address and CA decide, as far as they
// can: the same address and CA are the same hub, and another address
// under an id in use is refused, because nothing shows it is that hub.
func (s *Service) Connect(ctx context.Context, req ConnectRequest) (HubRecord, error) {
	if err := ValidateHubID(req.HubID); err != nil {
		return HubRecord{}, err
	}
	s.lockLifecycle(LifecycleConnect)
	defer s.opMu.Unlock()

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
		return HubRecord{}, fmt.Errorf("%w: display name must be a single line of at most 120 bytes (UTF-8)", domain.ErrInvalidArgument)
	}

	var replaced, moved *hubWatch
	others := s.hubList()
	for _, other := range others {
		otherConn := other.connection()
		same := otherConn.APIServer == conn.APIServer && kubernetesaddon.SameCA([]byte(otherConn.CACert), []byte(conn.CACert))
		switch {
		case other.id == req.HubID && !same:
			// The id is in use at another address. Whether this is that
			// hub, moved, is for the hub's own identifier to say.
			moved = other
		case other.id != req.HubID && same:
			return HubRecord{}, hubAlreadyConnected(conn.APIServer, other)
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
	if probe.ServiceAccount.Name == "" {
		return HubRecord{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "not-a-service-account",
			"the credential belongs to %q; a service account token is required so that it can be renewed", probe.Username)
	}
	if !probe.CanRead {
		return HubRecord{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "access-denied",
			"the credential (%s) may not list and watch ManagedCluster objects on the hub", probe.Username)
	}

	// The hub's own identifier decides what the address and CA cannot:
	// the same hub reached at another address is still that hub.
	if probe.ClusterUID != "" {
		for _, other := range others {
			if other.id != req.HubID && other.observation().ClusterUID == probe.ClusterUID {
				return HubRecord{}, hubAlreadyConnected(conn.APIServer, other)
			}
		}
	}
	if moved != nil {
		known := moved.observation().ClusterUID
		switch {
		case probe.ClusterUID != "" && known == probe.ClusterUID:
			replaced = moved
		case probe.ClusterUID == "" || known == "":
			return HubRecord{}, hubErr(kubernetesaddon.ErrEnrollmentConflict, "hub-id-in-use",
				"hub id %q is connected to the hub at %s, and nothing shows that the hub at %s is the same one: "+
					"its identifier could not be compared (the account on a hub may read it once the current hub access manifest is applied); "+
					"disconnect the connection first or choose another id", req.HubID, moved.connection().APIServer, conn.APIServer)
		default:
			return HubRecord{}, hubErr(kubernetesaddon.ErrEnrollmentConflict, "hub-id-in-use",
				"hub id %q is connected to another hub (%s); disconnect it first or choose another id", req.HubID, moved.connection().APIServer)
		}
	}

	now := s.now().UTC()
	obs := HubObservation{
		ID:               req.HubID,
		DisplayName:      req.DisplayName,
		APIServer:        conn.APIServer,
		CAFingerprint:    fingerprints[0],
		ClusterUID:       probe.ClusterUID,
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
		obs.EnrollmentRollout, obs.EnrollmentSecrets = prev.EnrollmentRollout, prev.EnrollmentSecrets
		if obs.DisplayName == "" {
			obs.DisplayName = prev.DisplayName
		}
		if obs.ClusterUID == "" {
			// The same address and CA are the same hub: a credential
			// that may read less does not make it another one.
			obs.ClusterUID = prev.ClusterUID
		}
	}

	// Connect succeeds or leaves no trace. The hub's record is written
	// before the credential and before the watch: a process that stops
	// between the steps then leaves a record without a credential, which
	// a later Connect, Disconnect or Start finds, whereas a credential
	// without a record is unreachable and could never be removed.
	//
	// Stopping the registered watch before storing the new state keeps a
	// renewal it was finishing from landing after the store and replacing
	// the credential the operator just supplied: stop waits for its loop.
	previousConn, previousObs, previousConds, hadPrevious := s.stopWatch(req.HubID)
	hw := newHubWatch(s, req.HubID, conn, obs)
	if err := hw.writeInitialHubRecord(ctx, now); err != nil {
		if hadPrevious {
			if rerr := s.startWatch(req.HubID, previousConn, previousObs); rerr != nil {
				s.logger.Warn("could not resume the hub's watch after a failed record write", "hub", req.HubID, "error", rerr)
			}
		}
		return HubRecord{}, err
	}
	if err := s.storeConnection(ctx, req.HubID, conn); err != nil {
		s.undoConnect(ctx, req.HubID, now, previousConn, previousObs, previousConds, hadPrevious)
		return HubRecord{}, err
	}
	if err := s.startPreparedWatch(hw); err != nil {
		s.undoConnect(ctx, req.HubID, now, previousConn, previousObs, previousConds, hadPrevious)
		return HubRecord{}, err
	}
	s.logger.Info("hub connected", "hub", req.HubID, "api_server", conn.APIServer, "by", req.Actor,
		"identity", probe.Username, "write_access_found", probe.CanWrite, "moved", moved != nil)
	return hw.record(), nil
}

// hubAlreadyConnected is the refusal of a second connection to a hub
// that is connected under another id.
func hubAlreadyConnected(apiServer string, other *hubWatch) *kubernetesaddon.EnrollmentError {
	e := hubErr(kubernetesaddon.ErrEnrollmentConflict, "hub-already-connected",
		"the hub at %s is already connected as %q", apiServer, other.id)
	if at := other.connection().APIServer; at != apiServer {
		e.Message += fmt.Sprintf(" (at %s)", at)
	}
	e.Details = map[string]any{"hub": other.id}
	return e
}

// undoConnect undoes a Connect that reported failure after it began
// changing state. It runs to completion even when the request was
// cancelled: a client that goes away must not leave the half-made
// connection behind.
//
// A new connection that could not be completed loses its credential
// first and its record second. If the credential cannot be removed the
// record is kept, so that the connection stays discoverable and can be
// disconnected; deleting the record first would strand the credential.
// A replacement puts back the record, the credential and the watch of
// the connection it replaced, in that order, so that the record reads as
// the resumed watch's own state.
func (s *Service) undoConnect(ctx context.Context, hubID string, now time.Time, previousConn HubConnection, previousObs HubObservation, previousConds []domain.Condition, hadPrevious bool) {
	ctx = context.WithoutCancel(ctx)
	if !hadPrevious {
		if err := s.vault.Delete(ctx, hubConnectionRef(hubID)); err != nil && !errors.Is(err, domain.ErrNotFound) {
			s.logger.Warn("could not remove the credential of a hub whose connect failed; its record is kept so the connection stays discoverable", "hub", hubID, "error", err)
			return
		}
		if err := s.deleteHubRecord(ctx, hubID); err != nil {
			s.logger.Warn("could not remove the record of a hub whose connect failed", "hub", hubID, "error", err)
		}
		return
	}
	if raw, _, err := hubRecordPayload(previousObs, previousConds); err != nil {
		s.logger.Warn("could not encode the previous record of a hub whose reconnect failed", "hub", hubID, "error", err)
	} else if err := s.storeHubRecord(ctx, hubID, raw, previousConds, now); err != nil {
		s.logger.Warn("could not restore the previous record of a hub whose reconnect failed", "hub", hubID, "error", err)
	}
	if err := s.storeConnection(ctx, hubID, previousConn); err != nil {
		s.logger.Warn("could not restore the previous credential of a hub whose reconnect failed", "hub", hubID, "error", err)
	}
	if err := s.startWatch(hubID, previousConn, previousObs); err != nil {
		s.logger.Warn("could not resume the watch of a hub whose reconnect failed", "hub", hubID, "error", err)
	}
}

// Disconnect stops observing a hub and removes what was observed
// through it: the records of its clusters, the stored credential, the
// enrollment bundles scoped to the hub, and finally the hub's record.
// Clusters enrolled through the hub stay enrolled; the labels the hub
// contributed to their targets stay as they were and are no longer
// kept up to date, and each such cluster's record says so (see
// [ClusterConditionHubFacts]).
//
// The hub's record goes last: it is what lets a disconnect that stopped
// midway be retried. A disconnect asked of a hub whose record is
// already gone finishes what remains and reports success when it found
// anything to remove; with nothing left at all the hub is not found.
func (s *Service) Disconnect(ctx context.Context, hubID, actor string) error {
	if err := ValidateHubID(hubID); err != nil {
		return err
	}
	s.lockLifecycle(LifecycleDisconnect)
	defer s.opMu.Unlock()

	s.mu.Lock()
	hw := s.hubs[hubID]
	delete(s.hubs, hubID)
	s.mu.Unlock()
	if hw != nil {
		hw.stop()
	}
	recordGone := false
	if hw == nil {
		if _, err := s.readRecord(ctx, HubResourceType, HubRecordName(hubID)); err != nil {
			if !errors.Is(err, domain.ErrNotFound) {
				return err
			}
			recordGone = true
		}
	}

	removed, err := s.records.PurgeCollectionPrefix(ctx, ManagedClusterResourceType, managedClusterPrefix(hubID))
	if err != nil {
		return fmt.Errorf("remove observed clusters of hub %q: %w", hubID, err)
	}
	credentialRemoved := false
	if err := s.vault.Delete(ctx, hubConnectionRef(hubID)); err != nil {
		if !errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("remove credential of hub %q: %w", hubID, err)
		}
	} else {
		credentialRemoved = true
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
	s.markFactsDisconnected(ctx, hubID)
	if recordGone {
		if removed == 0 && !credentialRemoved && revoked == 0 {
			return fmt.Errorf("%w: hub %q is not connected", domain.ErrNotFound, hubID)
		}
		s.logger.Info("hub disconnect finished after its record was gone", "hub", hubID, "by", actor,
			"observed_clusters_removed", removed, "bundles_revoked", revoked)
		return nil
	}
	if err := s.deleteHubRecord(ctx, hubID); err != nil {
		return err
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

// GetHubObservation returns one connected hub's observation as its watch
// answers now. The hub's record is written at most every few minutes
// while only its last-heard time changes, so a view that shows that time
// reads the observation instead of the record.
func (s *Service) GetHubObservation(_ context.Context, hubID string) (HubObservation, error) {
	hw := s.hub(hubID)
	if hw == nil {
		return HubObservation{}, fmt.Errorf("%w: hub %q is not connected", domain.ErrNotFound, hubID)
	}
	return hw.record().Observation, nil
}

// markFactsDisconnected says, on the record of every enrolled cluster
// that carries the facts of a hub, that the connection which kept them
// is gone. A cluster whose record cannot be written keeps its earlier
// answer; the disconnect goes on.
func (s *Service) markFactsDisconnected(ctx context.Context, hubID string) {
	if s.enrollment == nil {
		return
	}
	targets, err := s.listTargets(ctx)
	if err != nil {
		s.logger.Warn("could not mark the facts of a disconnected hub's clusters", "hub", hubID, "error", err)
		return
	}
	message := fmt.Sprintf("the connection to hub %q was removed; the labels it supplied are the last ones seen and are no longer updated", hubID)
	for _, t := range targets {
		if !kubernetesaddon.IsEnrolledTarget(t) || externalLabelsOf(t)[LabelHub] != hubID {
			continue
		}
		clusterID, ok := kubernetesaddon.ClusterIDFromTarget(t)
		if !ok {
			continue
		}
		if _, err := s.enrollment.SetExternalCondition(ctx, clusterID, ClusterConditionHubFacts, domain.ConditionFalse, HubFactsHubDisconnected, message); err != nil && !errors.Is(err, domain.ErrNotFound) {
			s.logger.Warn("could not mark the facts of a disconnected hub's cluster", "hub", hubID, "cluster", clusterID, "error", err)
		}
	}
}

// AdoptCluster makes a connected hub the one whose facts an enrolled
// cluster carries: its labels for the cluster, its cluster set, its
// claims. It is the explicit way to move those facts, for a cluster
// that joined another hub, or whose hub was connected again under
// another id: by itself the integration never takes a cluster's facts
// away from the connection that supplied them, not even when that
// connection is gone.
//
// The hub must know the cluster on more than its name: it enrolled the
// cluster, or publishes the cluster's identifier, or records its API
// address and CA. It returns the cluster's name on the hub.
func (s *Service) AdoptCluster(ctx context.Context, hubID, clusterID, actor string) (string, error) {
	if s.enrollment == nil {
		return "", fmt.Errorf("%w: this resource manager has no enrollment capability (the kubernetes add-on is not enabled)", domain.ErrInvalidArgument)
	}
	if err := ValidateHubID(hubID); err != nil {
		return "", err
	}
	if err := kubernetesaddon.ValidateClusterID(clusterID); err != nil {
		return "", err
	}
	s.lockLifecycle(LifecycleAdoptCluster)
	defer s.opMu.Unlock()
	hw := s.hub(hubID)
	if hw == nil {
		return "", fmt.Errorf("%w: hub %q is not connected", domain.ErrNotFound, hubID)
	}
	if !hw.isSynced() {
		return "", hubErr(kubernetesaddon.ErrEnrollmentUnverified, "hub-not-synced",
			"the clusters of hub %q have not been read yet; try again shortly", hubID)
	}
	targets, err := s.listTargets(ctx)
	if err != nil {
		return "", err
	}
	targetID := kubernetesaddon.TargetIDForCluster(clusterID)
	enrolled := false
	for _, t := range targets {
		if t.ID() == targetID && kubernetesaddon.IsEnrolledTarget(t) {
			enrolled = true
		}
	}
	if !enrolled {
		return "", fmt.Errorf("%w: cluster %q is not enrolled", domain.ErrNotFound, clusterID)
	}

	// The match is made anew from what the hub and the store say now.
	idx := newTargetIndex(targets)
	var found *observedCluster
	byNameOnly := ""
	for _, raw := range hw.informer.GetStore().List() {
		obj, ok := raw.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		c := parseManagedCluster(obj)
		link := idx.match(hw.id, c)
		if link.target != targetID {
			continue
		}
		if link.link.Basis == BasisName {
			byNameOnly = c.name
			continue
		}
		found = &c
		break
	}
	switch {
	case found != nil:
	case byNameOnly != "":
		return "", hubErr(kubernetesaddon.ErrEnrollmentConflict, "weak-link",
			"cluster %q of hub %q is taken to be cluster %q by its name alone; that is too little to put the hub's facts on it", byNameOnly, hubID, clusterID)
	default:
		return "", hubErr(kubernetesaddon.ErrEnrollmentConflict, "cluster-not-on-hub",
			"hub %q has no cluster that is cluster %q: the hub must have enrolled it, publish its identifier, or record its API address and CA", hubID, clusterID)
	}
	if _, err := s.enrollment.SetExternalLabels(ctx, clusterID, found.targetLabels(hw.id)); err != nil {
		return "", err
	}
	s.markLinksDirty()
	s.logger.Info("hub facts of a cluster moved to a hub connection", "hub", hubID, "cluster", clusterID, "managed_cluster", found.name, "by", actor)
	return found.name, nil
}

// ---------------------------------------------------------------------
// Vouching for presented clusters
// ---------------------------------------------------------------------

// VerifyPresentation implements [kubernetesaddon.PresentationVerifier]
// for bundles scoped to a hub. It vouches for a cluster only when the
// hub knows it, has accepted it, and records an API address for it.
//
// Where the hub records a CA for an address, the CA the cluster's side
// presented must be exactly that set. The address returned is the hub's
// record; the CA returned is the one presented, which the hub's record
// confirms — a record that trusted more would otherwise be handed the
// honest cluster's token.
//
// An address the hub records without a CA is usable as well, because a
// registration agent may be given the address alone. The hub then holds
// no set for the presented CA to equal: as the design puts it, the
// equality applies only where the verifier holds a CA, and the presented
// CA is the probe's only trust root. An address recorded with the
// presented CA set is preferred to one recorded without a CA.
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
	// An address whose recorded CA set is the one presented is the
	// strongest evidence in the record: use it when there is one.
	for _, cfg := range c.configs {
		if kubernetesaddon.SameCASet(cfg.ca, presentedCA) {
			return kubernetesaddon.VerifiedCluster{
				ClusterID: ClusterIDFor(scope, clusterName),
				APIServer: cfg.url,
				CACert:    presentedCA,
				Labels:    c.targetLabels(scope),
			}, nil
		}
	}
	// No recorded set matched. An address the hub records without a CA
	// still applies: the hub records nothing the presentation
	// contradicts, and the presented CA is the probe's only trust root.
	for _, cfg := range c.configs {
		if len(cfg.ca) == 0 {
			return kubernetesaddon.VerifiedCluster{
				ClusterID: ClusterIDFor(scope, clusterName),
				APIServer: cfg.url,
				CACert:    presentedCA,
				Labels:    c.targetLabels(scope),
			}, nil
		}
	}
	return kubernetesaddon.VerifiedCluster{}, hubErr(kubernetesaddon.ErrEnrollmentUnverified, "ca-mismatch",
		"the CA presented for cluster %q is not the set of CAs hub %q records for any of its addresses", clusterName, scope)
}

var _ kubernetesaddon.PresentationVerifier = (*Service)(nil)

// ExplainUnknownTarget tells a caller who named an observed cluster as
// a deployment target why that is not a target. Every connected hub
// that has a cluster of the name is named: one name can be a cluster
// on several hubs. It returns "" for a name the integration does not
// know.
func (s *Service) ExplainUnknownTarget(_ context.Context, id domain.TargetID) string {
	name := strings.TrimPrefix(string(id), "k8s-")
	var found []string
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
			found = append(found, fmt.Sprintf("cluster %q of hub %q is known to the resource manager as target %q", candidate, hw.id, link.link.Target))
		default:
			found = append(found, fmt.Sprintf("cluster %q of hub %q is observed, not enrolled: the resource manager cannot deliver to it until it is enrolled (its target will then be %q)",
				candidate, hw.id, kubernetesaddon.TargetIDForCluster(ClusterIDFor(hw.id, candidate))))
		}
	}
	return strings.Join(found, "; ")
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

// withdrawnCluster is a cluster whose record says it was withdrawn:
// the id the record has, and what the record says identifies the
// cluster on a network, so that an observed cluster can be recognized
// as it.
type withdrawnCluster struct {
	id            string
	apiServer     string
	caFingerprint string
}

// withdrawnClusters returns the clusters whose record says they were
// withdrawn.
func (s *Service) withdrawnClusters(ctx context.Context) ([]withdrawnCluster, error) {
	if s.enrollment == nil {
		return nil, nil
	}
	records, err := s.enrollment.ListClusters(ctx)
	if err != nil {
		return nil, err
	}
	var out []withdrawnCluster
	for _, rec := range records {
		if rec.Observation.State != kubernetesaddon.ClusterStateWithdrawn {
			continue
		}
		out = append(out, withdrawnCluster{
			id:            rec.ID,
			apiServer:     kubernetesaddon.NormalizeAPIServerURL(rec.Observation.APIServer),
			caFingerprint: rec.Observation.CAFingerprint,
		})
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
