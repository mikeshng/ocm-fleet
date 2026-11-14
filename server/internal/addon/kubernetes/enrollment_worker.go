package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

const (
	// enrollmentCheckInterval is how often every enrolled cluster's
	// credential is checked.
	enrollmentCheckInterval = 30 * time.Second
	// rejectionsBeforeWithdrawal and rejectionWindow decide when a
	// cluster that no longer recognizes its credential is taken to
	// have withdrawn: several consecutive checks, spread over time, so
	// that one bad answer during an API server restart is not enough.
	rejectionsBeforeWithdrawal = 3
	rejectionWindow            = 30 * time.Second
	// indexerEnsureInterval is how often a healthy cluster's indexer is
	// checked to be running.
	indexerEnsureInterval = 5 * time.Minute
	// orphanSweepInterval is how often indexed objects of clusters
	// that are gone are looked for, and orphanSweepDelay how long after
	// start the first look happens (startup replay starts indexers
	// first).
	orphanSweepInterval = 2 * time.Minute
	orphanSweepDelay    = 45 * time.Second
)

// probeHistory is what the worker remembers about one cluster between
// checks. It is not persisted: after a restart the counts start again.
type probeHistory struct {
	rejections      int
	firstRejection  time.Time
	lastIndexEnsure time.Time
}

func (s *EnrollmentService) history(clusterID string) *probeHistory {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.probes[clusterID]
	if !ok {
		h = &probeHistory{}
		s.probes[clusterID] = h
	}
	return h
}

func (s *EnrollmentService) forgetProbeHistory(clusterID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.probes, clusterID)
}

// CheckCluster looks after one enrolled cluster's credential: it
// renews the credential when a third of its lifetime is left, takes
// the target out of placement when the credential lapsed, treats a
// cluster that consistently refuses a credential that has not expired
// as withdrawn by its own side, and keeps the indexer running.
func (s *EnrollmentService) CheckCluster(ctx context.Context, clusterID string) error {
	lock := s.lockCluster(clusterID)
	defer lock.Unlock()

	target, found, err := s.getTarget(ctx, TargetIDForCluster(clusterID))
	if err != nil {
		return err
	}
	if !found || !IsEnrolledTarget(target) {
		return nil
	}
	now := s.now().UTC()
	hist := s.history(clusterID)

	token, err := s.storedToken(ctx, target)
	if errors.Is(err, domain.ErrNotFound) {
		return s.lapse(ctx, clusterID, target, "Missing", "no credential is stored for the cluster; its side has to present one", now)
	}
	if err != nil {
		return err
	}
	issuedAt, expiresAt := TokenLifetime(string(token))
	if !expiresAt.IsZero() && !now.Before(expiresAt) {
		return s.lapse(ctx, clusterID, target, "Expired",
			fmt.Sprintf("the stored credential expired at %s; the cluster's side has to present a new one", expiresAt.Format(time.RFC3339)), now)
	}

	cannotRenew := ""
	rejected := false
	if !expiresAt.IsZero() && !issuedAt.IsZero() && expiresAt.Sub(now) < expiresAt.Sub(issuedAt)/3 {
		newToken, err := s.renew(ctx, clusterID, target, token)
		switch {
		case err == nil:
			token = newToken
			_, expiresAt = TokenLifetime(string(token))
		case errors.Is(err, ErrClusterRejectedCredential):
			rejected = true
		case errors.Is(err, ErrClusterDeniedRequest):
			cannotRenew = fmt.Sprintf("the cluster does not let the access account request its own token; its side has to present a new credential before %s", expiresAt.Format(time.RFC3339))
		default:
			s.logger.Warn("credential renewal failed; will retry", "cluster", clusterID, "error", err)
		}
	}

	state := s.loadClusterState(ctx, clusterID)
	before := state.fingerprint()
	state.obs = observationFromTarget(target, state.obs)
	if state.obs.State == "" || state.obs.State == ClusterStateNeedsEnrollment {
		state.obs.State = ClusterStateEnrolled
	}
	state.set(ClusterConditionEnrolled, domain.ConditionTrue, "Enrolled", "the cluster's side presented a credential that the cluster confirms", now)
	if state.obs.CredentialExpiresAt == nil {
		state.obs.CredentialExpiresAt = timePtr(expiresAt)
	}

	var probeErr error
	if !rejected {
		conn := ClusterConnection{APIServer: target.Properties()[PropAPIServer], CACert: []byte(target.Properties()[PropCACert]), Token: string(token)}
		_, probeErr = s.prober.Identity(ctx, conn)
		rejected = errors.Is(probeErr, ErrClusterRejectedCredential)
	}
	switch {
	case rejected:
		if hist.rejections == 0 {
			hist.firstRejection = now
		}
		hist.rejections++
		if hist.rejections >= rejectionsBeforeWithdrawal && now.Sub(hist.firstRejection) >= rejectionWindow {
			_, err := s.release(ctx, clusterID, target, "cluster",
				"the cluster no longer recognizes the credential: its side removed the access account")
			return err
		}
		state.set(ClusterConditionReachable, domain.ConditionTrue, "Reachable", "the cluster's API answered", now)
		state.set(ClusterConditionCredentialValid, domain.ConditionFalse, "Rejected",
			fmt.Sprintf("the cluster did not recognize the stored credential (%d consecutive checks)", hist.rejections), now)
	case probeErr != nil:
		hist.rejections = 0
		state.set(ClusterConditionReachable, domain.ConditionFalse, "Unreachable", truncate(probeErr.Error(), 300), now)
	default:
		hist.rejections = 0
		state.set(ClusterConditionReachable, domain.ConditionTrue, "Reachable", "the cluster's API answered", now)
		if cannotRenew != "" {
			state.set(ClusterConditionCredentialValid, domain.ConditionTrue, "CannotRenew", cannotRenew, now)
			if state.obs.Rights != nil {
				rights := *state.obs.Rights
				rights.CanRenew = false
				state.obs.Rights = &rights
			}
		} else {
			state.set(ClusterConditionCredentialValid, domain.ConditionTrue, "Valid", "the cluster recognizes the stored credential", now)
		}
	}

	if target.State() != domain.TargetStateReady && !rejected && probeErr == nil {
		ready := domain.NewTargetInfo(target.ID(), target.Type(), target.Name(), domain.TargetStateReady,
			target.Labels(), target.Properties(), target.AcceptedManifestTypes())
		if err := s.putTarget(ctx, ready, now); err != nil {
			return err
		}
		target = ready
	}

	if s.indexing != nil && !rejected && probeErr == nil &&
		(state.status(ClusterConditionIndexing) != domain.ConditionTrue || now.Sub(hist.lastIndexEnsure) >= indexerEnsureInterval) {
		hist.lastIndexEnsure = now
		state.setIndexing(true, s.ensureIndexer(ctx, target, token), now)
	} else if s.indexing == nil {
		state.setIndexing(false, nil, now)
	}
	state.setReady(now)
	if state.fingerprint() == before {
		return nil
	}
	return s.writeClusterState(ctx, clusterID, target.Labels(), state, now)
}

// lapse takes a cluster whose credential is gone or expired out of
// placement until its side presents a new one. Nothing is removed: the
// target, its labels, and the indexed objects stay.
func (s *EnrollmentService) lapse(ctx context.Context, clusterID string, target domain.TargetInfo, reason, message string, now time.Time) error {
	if target.State() != domain.TargetStateInitializing {
		waiting := domain.NewTargetInfo(target.ID(), target.Type(), target.Name(), domain.TargetStateInitializing,
			target.Labels(), target.Properties(), target.AcceptedManifestTypes())
		if err := s.putTarget(ctx, waiting, now); err != nil {
			return err
		}
		if s.indexing != nil {
			if err := s.indexing.StopIndexer(ctx, target.ID()); err != nil {
				s.logger.Warn("could not stop indexer of a cluster whose credential lapsed", "cluster", clusterID, "error", err)
			}
		}
		s.logger.Warn("cluster credential lapsed; target is out of placement until a new credential is presented",
			"cluster", clusterID, "reason", reason)
	}
	state := s.loadClusterState(ctx, clusterID)
	before := state.fingerprint()
	state.obs = observationFromTarget(target, state.obs)
	state.obs.State = ClusterStateNeedsEnrollment
	state.set(ClusterConditionEnrolled, domain.ConditionTrue, "Enrolled", "the cluster is enrolled; its credential has to be presented again", now)
	state.set(ClusterConditionCredentialValid, domain.ConditionFalse, reason, message, now)
	state.set(ClusterConditionIndexing, domain.ConditionFalse, "NoCredential", "indexing stopped with the credential", now)
	state.setReady(now)
	if state.fingerprint() == before {
		return nil
	}
	return s.writeClusterState(ctx, clusterID, target.Labels(), state, now)
}

// fingerprint summarizes a record so that an unchanged one is not
// written again on every check.
func (c *clusterState) fingerprint() string {
	type cond struct {
		Type, Status, Reason, Message string
	}
	types := make([]string, 0, len(c.conds))
	for t := range c.conds {
		types = append(types, string(t))
	}
	sort.Strings(types)
	conds := make([]cond, 0, len(types))
	for _, t := range types {
		cc := c.conds[domain.ConditionType(t)]
		conds = append(conds, cond{string(cc.Type()), string(cc.Status()), cc.Reason(), cc.Message()})
	}
	raw, _ := json.Marshal(struct {
		Obs   ClusterObservation
		Conds []cond
	}{c.obs, conds})
	return string(raw)
}

// IndexedClusterLister reports which clusters currently have an
// indexer. [KubernetesInProcessIndexHost] implements it.
type IndexedClusterLister interface {
	IndexedClusters() []domain.ResourceName
}

// OrphanedObjectClusters returns the cluster ids (as they appear in
// object names) that own indexed objects but have neither a kubernetes
// target nor an indexer. Such objects belong to clusters that are
// gone: nothing will ever update or remove them.
func (s *EnrollmentService) OrphanedObjectClusters(ctx context.Context, indexed IndexedClusterLister) ([]string, error) {
	collections, err := s.records.ListCollections(ctx, ObjectResourceType)
	if err != nil {
		return nil, fmt.Errorf("list object collections: %w", err)
	}
	owners := map[string]struct{}{}
	prefix := string(ClusterCollectionID) + "/"
	for _, name := range collections {
		rest, ok := strings.CutPrefix(string(name), prefix)
		if !ok {
			continue
		}
		if segment, _, ok := strings.Cut(rest, "/"); ok && segment != "" {
			owners[segment] = struct{}{}
		}
	}
	if len(owners) == 0 {
		return nil, nil
	}

	targets, err := s.listTargets(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range targets {
		if t.Type() != TargetType {
			continue
		}
		if id, ok := ClusterIDFromTarget(t); ok {
			delete(owners, url.PathEscape(id))
		}
	}
	if indexed != nil {
		for _, name := range indexed.IndexedClusters() {
			delete(owners, url.PathEscape(string(name.ID())))
		}
	}
	out := make([]string, 0, len(owners))
	for segment := range owners {
		out = append(out, segment)
	}
	sort.Strings(out)
	return out, nil
}

// purgeObjectsOf removes the indexed objects under one cluster segment.
func (s *EnrollmentService) purgeObjectsOf(ctx context.Context, segment string) (int64, error) {
	return s.records.PurgeCollectionPrefix(ctx, ObjectResourceType, string(ClusterCollectionID)+"/"+segment+"/")
}

// EnrollmentWorker runs the periodic work of enrollment: credential
// checks for every enrolled cluster, and removal of indexed objects
// whose cluster is gone.
type EnrollmentWorker struct {
	svc     *EnrollmentService
	indexed IndexedClusterLister

	checkInterval time.Duration
	sweepInterval time.Duration
	sweepDelay    time.Duration

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	suspect map[string]struct{}
}

// NewEnrollmentWorker builds a worker. indexed may be nil.
func NewEnrollmentWorker(svc *EnrollmentService, indexed IndexedClusterLister) *EnrollmentWorker {
	return &EnrollmentWorker{
		svc:           svc,
		indexed:       indexed,
		checkInterval: enrollmentCheckInterval,
		sweepInterval: orphanSweepInterval,
		sweepDelay:    orphanSweepDelay,
		suspect:       map[string]struct{}{},
	}
}

// Start launches the worker under ctx. It returns at once.
func (w *EnrollmentWorker) Start(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done != nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.done = make(chan struct{})
	go w.run(runCtx, w.done)
}

// Close stops the worker and waits for it, bounded by ctx.
func (w *EnrollmentWorker) Close(ctx context.Context) error {
	w.mu.Lock()
	cancel, done := w.cancel, w.done
	w.mu.Unlock()
	if done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("enrollment worker did not stop: %w", ctx.Err())
	}
}

func (w *EnrollmentWorker) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	check := time.NewTicker(w.checkInterval)
	defer check.Stop()
	sweep := time.NewTimer(w.sweepDelay)
	defer sweep.Stop()

	w.CheckAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-check.C:
			w.CheckAll(ctx)
		case <-sweep.C:
			w.Sweep(ctx)
			sweep.Reset(w.sweepInterval)
		}
	}
}

// CheckAll checks every enrolled cluster once.
func (w *EnrollmentWorker) CheckAll(ctx context.Context) {
	targets, err := w.svc.EnrolledTargets(ctx)
	if err != nil {
		if ctx.Err() == nil {
			w.svc.logger.Warn("could not list enrolled clusters", "error", err)
		}
		return
	}
	for _, t := range targets {
		if ctx.Err() != nil {
			return
		}
		id, ok := ClusterIDFromTarget(t)
		if !ok {
			continue
		}
		if err := w.svc.CheckCluster(ctx, id); err != nil && ctx.Err() == nil {
			w.svc.logger.Warn("cluster check failed", "cluster", id, "error", err)
		}
	}
}

// Sweep removes the indexed objects of clusters that are gone. A
// cluster's objects are removed only when it was found orphaned on two
// consecutive sweeps, so that a cluster in the middle of being
// registered is never mistaken for one that left.
func (w *EnrollmentWorker) Sweep(ctx context.Context) {
	orphans, err := w.svc.OrphanedObjectClusters(ctx, w.indexed)
	if err != nil {
		if ctx.Err() == nil {
			w.svc.logger.Warn("could not look for indexed objects of clusters that are gone", "error", err)
		}
		return
	}
	next := make(map[string]struct{}, len(orphans))
	for _, segment := range orphans {
		if _, seen := w.suspect[segment]; !seen {
			next[segment] = struct{}{}
			continue
		}
		n, err := w.svc.purgeObjectsOf(ctx, segment)
		if err != nil {
			if ctx.Err() == nil {
				w.svc.logger.Warn("could not remove indexed objects of a cluster that is gone", "cluster", segment, "error", err)
			}
			next[segment] = struct{}{}
			continue
		}
		w.svc.logger.Info("removed indexed objects of a cluster that is gone", "cluster", segment, "objects", n)
	}
	w.suspect = next
}
