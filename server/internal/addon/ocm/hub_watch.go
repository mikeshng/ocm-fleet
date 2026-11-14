package ocm

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// Intervals of a hub's watch. They are variables so that tests can
// shorten them; nothing else changes them.
var (
	// hubTickInterval is how often a hub's watch looks for work.
	hubTickInterval = time.Second
	// hubProbeInterval is how often the hub is asked a small question
	// to learn whether it still answers and still knows the credential.
	hubProbeInterval = 30 * time.Second
	// hubLinkInterval is how often links to the resource manager's own
	// clusters are recomputed when nothing asked for it sooner.
	hubLinkInterval = 30 * time.Second
	// hubRecordInterval is how often the hub's record is written when
	// nothing but its last-seen time changed.
	hubRecordInterval = 5 * time.Minute
	// hubRenewRetryInterval spaces failed credential renewals.
	hubRenewRetryInterval = 5 * time.Minute
)

const (
	// hubFlushBatch is how many cluster records one write carries.
	hubFlushBatch = 200
	// hubFailuresBeforeStale is how many consecutive failed probes
	// make the hub's observed clusters stale.
	hubFailuresBeforeStale = 2
)

// hubWatch observes one hub: it lists and watches the hub's
// ManagedCluster objects and keeps one record per cluster.
type hubWatch struct {
	svc *Service
	id  string

	client   dynamic.Interface
	informer cache.SharedIndexInformer
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	// writeMu serializes writes of the hub's own record.
	writeMu sync.Mutex

	mu         sync.Mutex
	conn       HubConnection
	obs        HubObservation
	conds      map[domain.ConditionType]domain.Condition
	dirty      map[string]struct{}
	written    map[string][sha256.Size]byte
	links      map[string]clusterLink
	linksDirty bool
	synced     bool
	// staleSince is when the hub stopped answering; zero while it answers.
	staleSince time.Time
	failures   int
	lastProbe  time.Time
	lastLinks  time.Time
	lastRenew  time.Time
	lastWrite  time.Time
	hubHash    [sha256.Size]byte
}

func newHubWatch(svc *Service, hubID string, conn HubConnection, obs HubObservation) *hubWatch {
	obs.ID = hubID
	return &hubWatch{
		svc:        svc,
		id:         hubID,
		conn:       conn,
		obs:        obs,
		conds:      map[domain.ConditionType]domain.Condition{},
		dirty:      map[string]struct{}{},
		written:    map[string][sha256.Size]byte{},
		links:      map[string]clusterLink{},
		linksDirty: true,
	}
}

func (hw *hubWatch) token() string {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.conn.Token
}

func (hw *hubWatch) connection() HubConnection {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.conn
}

func (hw *hubWatch) observation() HubObservation {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.obs
}

func (hw *hubWatch) isSynced() bool {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.synced
}

func (hw *hubWatch) markLinksDirty() {
	hw.mu.Lock()
	hw.linksDirty = true
	hw.mu.Unlock()
}

func (hw *hubWatch) linkOf(name string) clusterLink {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.links[name]
}

// cluster returns a cluster of the hub as last seen.
func (hw *hubWatch) cluster(name string) (observedCluster, bool) {
	raw, exists, err := hw.informer.GetStore().GetByKey(name)
	if err != nil || !exists {
		return observedCluster{}, false
	}
	obj, ok := raw.(*unstructured.Unstructured)
	if !ok {
		return observedCluster{}, false
	}
	return parseManagedCluster(obj), true
}

// start begins the watch. It returns once the watch is running, not
// once the hub's clusters have been read.
func (hw *hubWatch) start(parent context.Context) error {
	client, err := hw.svc.clients.Dynamic(hw.connection(), hw.token)
	if err != nil {
		return err
	}
	hw.client = client
	hw.informer = dynamicinformer.NewFilteredDynamicInformer(
		client, managedClusterGVR, metav1.NamespaceAll, 0, cache.Indexers{}, nil,
	).Informer()
	if err := hw.informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		hw.svc.logger.Debug("hub watch interrupted; it is retried", "hub", hw.id, "error", err)
	}); err != nil {
		return fmt.Errorf("configure hub watch: %w", err)
	}
	if _, err := hw.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    hw.onEvent,
		UpdateFunc: func(_, obj any) { hw.onEvent(obj) },
		DeleteFunc: hw.onEvent,
	}); err != nil {
		return fmt.Errorf("configure hub watch: %w", err)
	}

	ctx, cancel := context.WithCancel(parent)
	hw.cancel = cancel
	hw.setCond(HubConditionSynced, domain.ConditionFalse, "Reading", "the hub's clusters are being read", hw.svc.now().UTC())
	hw.wg.Add(2)
	go func() {
		defer hw.wg.Done()
		hw.informer.Run(ctx.Done())
	}()
	go func() {
		defer hw.wg.Done()
		hw.loop(ctx)
	}()
	return nil
}

// stop ends the watch and waits for it.
func (hw *hubWatch) stop() {
	if hw.cancel != nil {
		hw.cancel()
	}
	hw.wg.Wait()
}

// onEvent notes that a cluster changed. The record is written by the
// loop, so that a burst of changes becomes a few batched writes.
func (hw *hubWatch) onEvent(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	hw.mu.Lock()
	hw.dirty[u.GetName()] = struct{}{}
	hw.linksDirty = true
	hw.mu.Unlock()
}

func (hw *hubWatch) loop(ctx context.Context) {
	ticker := time.NewTicker(hubTickInterval)
	defer ticker.Stop()
	for {
		hw.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// tick does whatever is due.
func (hw *hubWatch) tick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	now := hw.svc.now().UTC()

	hw.mu.Lock()
	probeDue := hw.lastProbe.IsZero() || now.Sub(hw.lastProbe) >= hubProbeInterval
	justSynced := !hw.synced && hw.informer.HasSynced()
	hw.mu.Unlock()

	if probeDue {
		hw.probe(ctx, now)
		hw.renewIfDue(ctx, now)
	}
	if justSynced {
		if err := hw.afterSync(ctx, now); err != nil {
			hw.svc.logger.Warn("could not reconcile the hub's observed clusters; will retry", "hub", hw.id, "error", err)
			return
		}
	}

	hw.mu.Lock()
	synced := hw.synced
	linksDue := synced && (hw.linksDirty || now.Sub(hw.lastLinks) >= hubLinkInterval)
	hw.mu.Unlock()
	if linksDue {
		if err := hw.recomputeLinks(ctx, now); err != nil {
			hw.svc.logger.Warn("could not match the hub's clusters with the resource manager's; will retry", "hub", hw.id, "error", err)
		}
	}
	if synced {
		if err := hw.flush(ctx, now); err != nil && ctx.Err() == nil {
			hw.svc.logger.Warn("could not write observed clusters; will retry", "hub", hw.id, "error", err)
		}
	}
	if err := hw.writeHubRecord(ctx, now, false); err != nil && ctx.Err() == nil {
		hw.svc.logger.Warn("could not write the hub's record; will retry", "hub", hw.id, "error", err)
	}
}

// afterSync runs once the hub's clusters have been read: records of
// clusters the hub no longer has are removed, and every cluster's
// record is written.
func (hw *hubWatch) afterSync(ctx context.Context, now time.Time) error {
	recorded, err := hw.svc.recordedClusterNames(ctx, hw.id)
	if err != nil {
		return err
	}
	store := hw.informer.GetStore()
	hw.mu.Lock()
	defer hw.mu.Unlock()
	for _, name := range recorded {
		if _, exists, _ := store.GetByKey(name); !exists {
			// Known to have a record: the flush deletes it.
			hw.written[name] = [sha256.Size]byte{}
			hw.dirty[name] = struct{}{}
		}
	}
	for _, key := range store.ListKeys() {
		hw.dirty[key] = struct{}{}
	}
	hw.synced = true
	hw.linksDirty = true
	hw.setCondLocked(HubConditionSynced, domain.ConditionTrue, "Synced", "the hub's clusters have been read and are watched", now)
	hw.svc.logger.Info("hub clusters read", "hub", hw.id, "clusters", len(store.ListKeys()))
	return nil
}

// probe asks the hub a small question and records how it went.
func (hw *hubWatch) probe(ctx context.Context, now time.Time) {
	pctx, cancel := context.WithTimeout(ctx, hubRequestTimeout)
	defer cancel()
	_, err := hw.client.Resource(managedClusterGVR).List(pctx, metav1.ListOptions{Limit: 1})
	if ctx.Err() != nil {
		return
	}

	hw.mu.Lock()
	defer hw.mu.Unlock()
	hw.lastProbe = now
	switch {
	case err == nil:
		hw.failures = 0
		seen := now
		hw.obs.LastSeenAt = &seen
		hw.setCondLocked(HubConditionConnected, domain.ConditionTrue, "Connected", "the hub's API answers", now)
		if c, ok := hw.conds[HubConditionCredentialValid]; !ok || c.Status() != domain.ConditionTrue {
			hw.setCondLocked(HubConditionCredentialValid, domain.ConditionTrue, "Valid", "the hub recognizes the credential", now)
		}
		if !hw.staleSince.IsZero() {
			hw.staleSince = time.Time{}
			hw.markAllDirtyLocked()
			hw.svc.logger.Info("hub answers again; observed clusters are current", "hub", hw.id)
		}
		return
	case apierrors.IsUnauthorized(err):
		hw.setCondLocked(HubConditionCredentialValid, domain.ConditionFalse, "Rejected",
			"the hub does not recognize the credential; connect the hub again with a new one", now)
		hw.setCondLocked(HubConditionConnected, domain.ConditionFalse, "CredentialRejected", "the hub refuses the credential", now)
	case apierrors.IsForbidden(err):
		hw.setCondLocked(HubConditionConnected, domain.ConditionFalse, "AccessDenied",
			"the credential may no longer list ManagedCluster objects on the hub", now)
	default:
		hw.setCondLocked(HubConditionConnected, domain.ConditionFalse, "Unreachable", truncate(err.Error(), 300), now)
	}
	hw.failures++
	if hw.failures >= hubFailuresBeforeStale && hw.staleSince.IsZero() {
		hw.staleSince = now
		hw.markAllDirtyLocked()
		hw.svc.logger.Warn("hub does not answer; its observed clusters are marked stale", "hub", hw.id, "error", err)
	}
}

func (hw *hubWatch) markAllDirtyLocked() {
	for _, key := range hw.informer.GetStore().ListKeys() {
		hw.dirty[key] = struct{}{}
	}
}

// renewIfDue asks the hub for a new credential when a third of the
// current one's lifetime is left.
func (hw *hubWatch) renewIfDue(ctx context.Context, now time.Time) {
	hw.mu.Lock()
	conn := hw.conn
	sa, hasSA := kubernetesaddon.ParseServiceAccountRef(hw.obs.ServiceAccount)
	lastRenew := hw.lastRenew
	hw.mu.Unlock()

	issuedAt, expiresAt := kubernetesaddon.TokenLifetime(conn.Token)
	if expiresAt.IsZero() {
		return
	}
	if !now.Before(expiresAt) {
		hw.setCond(HubConditionCredentialValid, domain.ConditionFalse, "Expired",
			fmt.Sprintf("the credential expired at %s; connect the hub again with a new one", expiresAt.Format(time.RFC3339)), now)
		return
	}
	if issuedAt.IsZero() || expiresAt.Sub(now) >= expiresAt.Sub(issuedAt)/3 {
		return
	}
	if !lastRenew.IsZero() && now.Sub(lastRenew) < hubRenewRetryInterval {
		return
	}
	hw.mu.Lock()
	hw.lastRenew = now
	hw.mu.Unlock()

	cannot := func(why string) {
		hw.setCond(HubConditionCredentialValid, domain.ConditionTrue, "CannotRenew",
			fmt.Sprintf("%s; connect the hub again with a new credential before %s", why, expiresAt.Format(time.RFC3339)), now)
	}
	if !hasSA {
		cannot("the credential is not a service account token and cannot be renewed")
		return
	}
	rctx, cancel := context.WithTimeout(ctx, hubRequestTimeout)
	defer cancel()
	newToken, newExpiry, err := hw.svc.clients.RenewToken(rctx, conn, sa, expiresAt.Sub(issuedAt))
	switch {
	case err == nil:
	case errors.Is(err, kubernetesaddon.ErrClusterDeniedRequest):
		cannot("the hub does not let the service account request its own token")
		return
	default:
		hw.svc.logger.Warn("hub credential renewal failed; will retry", "hub", hw.id, "error", err)
		return
	}
	conn.Token = newToken
	if err := hw.svc.storeConnection(ctx, hw.id, conn); err != nil {
		hw.svc.logger.Warn("could not store the renewed hub credential; will retry", "hub", hw.id, "error", err)
		return
	}
	if _, exp := kubernetesaddon.TokenLifetime(newToken); !exp.IsZero() {
		newExpiry = exp
	}
	hw.mu.Lock()
	hw.conn = conn
	if !newExpiry.IsZero() {
		exp := newExpiry.UTC()
		hw.obs.CredentialExpiresAt = &exp
	}
	hw.setCondLocked(HubConditionCredentialValid, domain.ConditionTrue, "Renewed", "the hub issued a new credential", now)
	hw.mu.Unlock()
	hw.svc.logger.Info("hub credential renewed", "hub", hw.id, "expires_at", newExpiry.UTC().Format(time.RFC3339))
}

// recomputeLinks matches every cluster of the hub with the clusters
// the resource manager knows, and keeps the hub's facts on the targets
// of the enrolled ones.
func (hw *hubWatch) recomputeLinks(ctx context.Context, now time.Time) error {
	hw.mu.Lock()
	hw.linksDirty = false
	hw.lastLinks = now
	hw.mu.Unlock()
	fail := func(err error) error {
		hw.markLinksDirty()
		return err
	}

	targets, err := hw.svc.listTargets(ctx)
	if err != nil {
		return fail(err)
	}
	withdrawn, err := hw.svc.withdrawnClusters(ctx)
	if err != nil {
		return fail(err)
	}
	idx := newTargetIndex(targets)
	byID := make(map[domain.TargetID]domain.TargetInfo, len(targets))
	for _, t := range targets {
		byID[t.ID()] = t
	}

	type labelUpdate struct {
		clusterID string
		labels    map[string]string
	}
	var updates []labelUpdate
	links := map[string]clusterLink{}
	var accepted, available, enrolled int
	objects := hw.informer.GetStore().List()
	for _, raw := range objects {
		obj, ok := raw.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		c := parseManagedCluster(obj)
		link := idx.match(hw.id, c)
		if link.link.State == LinkObserved && link.conflict == "" {
			if _, was := withdrawn[ClusterIDFor(hw.id, c.name)]; was {
				link.link = EnrollmentLink{
					State:   LinkWithdrawn,
					Cluster: string(kubernetesaddon.ClusterRecordName(ClusterIDFor(hw.id, c.name))),
				}
			}
		}
		links[c.name] = link
		if c.accepted {
			accepted++
		}
		if c.available == domain.ConditionTrue {
			available++
		}
		if link.link.State != LinkEnrolled {
			continue
		}
		enrolled++
		target := byID[link.target]
		clusterID, ok := kubernetesaddon.ClusterIDFromTarget(target)
		if !ok || link.link.Basis == BasisName {
			continue
		}
		current := externalLabelsOf(target)
		// A cluster enrolled by hand that several hubs know keeps the
		// facts of the hub that labeled it first.
		if owner := current[LabelHub]; owner != "" && owner != hw.id {
			continue
		}
		if desired := c.targetLabels(hw.id); !sameLabels(current, desired) {
			updates = append(updates, labelUpdate{clusterID: clusterID, labels: desired})
		}
	}

	hw.mu.Lock()
	for name, link := range links {
		if prev, ok := hw.links[name]; !ok || prev != link {
			hw.dirty[name] = struct{}{}
		}
	}
	hw.links = links
	hw.obs.ClusterCount = len(objects)
	hw.obs.AcceptedCount = accepted
	hw.obs.AvailableCount = available
	hw.obs.EnrolledCount = enrolled
	hw.mu.Unlock()

	if hw.svc.enrollment == nil {
		return nil
	}
	for _, u := range updates {
		changed, err := hw.svc.enrollment.SetExternalLabels(ctx, u.clusterID, u.labels)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			return fail(fmt.Errorf("update labels of cluster %q: %w", u.clusterID, err))
		}
		if changed {
			hw.svc.logger.Info("hub facts applied to enrolled cluster", "hub", hw.id, "cluster", u.clusterID, "labels", len(u.labels))
		}
	}
	return nil
}

// flush writes the records of clusters that changed, a batch at a
// time, and removes the records of clusters the hub no longer has.
func (hw *hubWatch) flush(ctx context.Context, now time.Time) error {
	for ctx.Err() == nil {
		hw.mu.Lock()
		names := make([]string, 0, hubFlushBatch)
		for name := range hw.dirty {
			names = append(names, name)
			delete(hw.dirty, name)
			if len(names) == hubFlushBatch {
				break
			}
		}
		staleSince := hw.staleSince
		links := make(map[string]clusterLink, len(names))
		known := make(map[string][sha256.Size]byte, len(names))
		for _, name := range names {
			links[name] = hw.links[name]
			if h, ok := hw.written[name]; ok {
				known[name] = h
			}
		}
		hw.mu.Unlock()
		if len(names) == 0 {
			return nil
		}
		sort.Strings(names)

		var reports []kubernetesaddon.InventoryObjectReport
		hashes := map[string][sha256.Size]byte{}
		var deleted []string
		for _, name := range names {
			c, ok := hw.cluster(name)
			if !ok {
				if _, had := known[name]; had {
					reports = append(reports, kubernetesaddon.InventoryObjectReport{
						Name: ManagedClusterRecordName(hw.id, name), IsDelete: true,
					})
					deleted = append(deleted, name)
				}
				continue
			}
			report, hash, err := hw.report(c, links[name], staleSince, now)
			if err != nil {
				hw.svc.logger.Warn("could not build the record of an observed cluster", "hub", hw.id, "cluster", name, "error", err)
				continue
			}
			if prev, ok := known[name]; ok && prev == hash {
				continue
			}
			reports = append(reports, report)
			hashes[name] = hash
		}
		if len(reports) == 0 {
			continue
		}
		if err := hw.svc.records.ReplaceBatch(ctx, ManagedClusterResourceType, reports); err != nil {
			hw.mu.Lock()
			for _, name := range names {
				hw.dirty[name] = struct{}{}
			}
			hw.mu.Unlock()
			return err
		}
		hw.mu.Lock()
		for name, hash := range hashes {
			hw.written[name] = hash
		}
		for _, name := range deleted {
			delete(hw.written, name)
			delete(hw.links, name)
		}
		hw.mu.Unlock()
	}
	return nil
}

// report builds the record of one observed cluster and a hash of its
// content, so that an unchanged record is not written again.
func (hw *hubWatch) report(c observedCluster, link clusterLink, staleSince, now time.Time) (kubernetesaddon.InventoryObjectReport, [sha256.Size]byte, error) {
	obs := c.observation(hw.id, link)
	raw, err := json.Marshal(obs)
	if err != nil {
		return kubernetesaddon.InventoryObjectReport{}, [sha256.Size]byte{}, err
	}
	conds := append([]domain.Condition{}, c.conditions...)
	if !staleSince.IsZero() {
		cond, _ := domain.NewCondition(ConditionStale, domain.ConditionTrue, "HubUnreachable",
			"the hub does not answer; this is the last state seen", staleSince)
		conds = append(conds, cond)
	}

	h := sha256.New()
	h.Write(raw)
	labelKeys := make([]string, 0, len(c.labels))
	for k := range c.labels {
		labelKeys = append(labelKeys, k)
	}
	sort.Strings(labelKeys)
	for _, k := range labelKeys {
		h.Write(fmt.Appendf(nil, "\x00l:%s=%s", k, c.labels[k]))
	}
	for _, cond := range conds {
		h.Write(fmt.Appendf(nil, "\x00c:%s=%s|%s|%s|%d", cond.Type(), cond.Status(), cond.Reason(), cond.Message(), cond.LastTransitionTime().Unix()))
	}
	var hash [sha256.Size]byte
	copy(hash[:], h.Sum(nil))

	msg := json.RawMessage(raw)
	labels := make(map[string]string, len(c.labels))
	for k, v := range c.labels {
		labels[k] = v
	}
	return kubernetesaddon.InventoryObjectReport{
		Name:        ManagedClusterRecordName(hw.id, c.name),
		Labels:      labels,
		Observation: &msg,
		Conditions:  conds,
		ObservedAt:  now,
	}, hash, nil
}

// ---------------------------------------------------------------------
// The hub's own record
// ---------------------------------------------------------------------

func (hw *hubWatch) setCond(t domain.ConditionType, status domain.ConditionStatus, reason, message string, now time.Time) {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	hw.setCondLocked(t, status, reason, message, now)
}

// setCondLocked records a condition; the transition time moves only
// when the status changes.
func (hw *hubWatch) setCondLocked(t domain.ConditionType, status domain.ConditionStatus, reason, message string, now time.Time) {
	at := now
	if prev, ok := hw.conds[t]; ok && prev.Status() == status {
		at = prev.LastTransitionTime()
	}
	cond, _ := domain.NewCondition(t, status, reason, message, at)
	hw.conds[t] = cond
}

// snapshotLocked returns the hub's observation and its conditions,
// with the summary condition derived.
func (hw *hubWatch) snapshotLocked(now time.Time) (HubObservation, []domain.Condition) {
	ready, reason, message := domain.ConditionTrue, "Observing", "the hub is connected and its clusters are current"
	for _, t := range []domain.ConditionType{HubConditionCredentialValid, HubConditionConnected, HubConditionSynced} {
		c, ok := hw.conds[t]
		if !ok {
			ready, reason, message = domain.ConditionUnknown, "Starting", "the hub connection is starting"
			break
		}
		if c.Status() != domain.ConditionTrue {
			ready, reason, message = domain.ConditionFalse, c.Reason(), c.Message()
			break
		}
	}
	hw.setCondLocked(HubConditionReady, ready, reason, message, now)

	obs := hw.obs
	obs.SearchTerms = kubernetesaddon.SearchTerms(hw.id, nil, obs.DisplayName)
	types := make([]string, 0, len(hw.conds))
	for t := range hw.conds {
		types = append(types, string(t))
	}
	sort.Strings(types)
	conds := make([]domain.Condition, 0, len(types))
	for _, t := range types {
		conds = append(conds, hw.conds[domain.ConditionType(t)])
	}
	return obs, conds
}

// record returns the hub's record for a caller.
func (hw *hubWatch) record() HubRecord {
	hw.mu.Lock()
	obs, conds := hw.snapshotLocked(hw.svc.now().UTC())
	hw.mu.Unlock()
	out := HubRecord{
		ID:          hw.id,
		Name:        string(HubRecordName(hw.id)),
		Observation: obs,
		Conditions:  map[string]ConditionJS{},
	}
	for _, c := range conds {
		out.Conditions[string(c.Type())] = ConditionJS{Status: string(c.Status()), Reason: c.Reason(), Message: c.Message()}
	}
	return out
}

// writeHubRecord writes the hub's record when it changed, or when only
// its last-seen time changed and the record is due.
func (hw *hubWatch) writeHubRecord(ctx context.Context, now time.Time, force bool) error {
	hw.writeMu.Lock()
	defer hw.writeMu.Unlock()

	hw.mu.Lock()
	obs, conds := hw.snapshotLocked(now)
	lastWrite, lastHash := hw.lastWrite, hw.hubHash
	hw.mu.Unlock()

	raw, err := json.Marshal(obs)
	if err != nil {
		return fmt.Errorf("encode record of hub %q: %w", hw.id, err)
	}
	// The last-seen time alone does not make the record different.
	stable := obs
	stable.LastSeenAt = nil
	stableRaw, err := json.Marshal(stable)
	if err != nil {
		return fmt.Errorf("encode record of hub %q: %w", hw.id, err)
	}
	h := sha256.New()
	h.Write(stableRaw)
	for _, c := range conds {
		h.Write(fmt.Appendf(nil, "\x00c:%s=%s|%s|%s", c.Type(), c.Status(), c.Reason(), c.Message()))
	}
	var hash [sha256.Size]byte
	copy(hash[:], h.Sum(nil))
	if !force && hash == lastHash && now.Sub(lastWrite) < hubRecordInterval {
		return nil
	}

	msg := json.RawMessage(raw)
	labels := map[string]string{}
	if err := hw.svc.records.ReplaceBatch(ctx, HubResourceType, []kubernetesaddon.InventoryObjectReport{{
		Name:        HubRecordName(hw.id),
		Labels:      labels,
		Observation: &msg,
		Conditions:  conds,
		ObservedAt:  now,
	}}); err != nil {
		return fmt.Errorf("write record of hub %q: %w", hw.id, err)
	}
	hw.mu.Lock()
	hw.lastWrite, hw.hubHash = now, hash
	hw.mu.Unlock()
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
