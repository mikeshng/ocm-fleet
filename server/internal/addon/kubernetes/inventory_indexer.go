package kubernetes

import (
	"context"
	"log/slog"
	"time"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// IndexConfig holds configuration for the indexer delegate.
type IndexConfig struct {
	Schema          IndexSchema
	DenyList        []Resource
	AllowList       []Resource
	NamespaceFilter *NamespaceFilterConfig
	BatchInterval   time.Duration
	// SettleCheckInterval is how often a starting indexer looks whether
	// it has read everything it can, after which it removes what it did
	// not find again (see [StaleObjectPurger]). Zero means 5 seconds.
	SettleCheckInterval time.Duration
}

// indexerDelegate holds indexing-specific state for one managed cluster.
// It manages the informer-to-writer pipeline that watches Kubernetes
// resources and reports inventory through an [InventoryReporter].
type indexerDelegate struct {
	clusterResourceName domain.ResourceName
	dynClient           dynamic.Interface
	discClient          discovery.DiscoveryInterface
	reporter            InventoryReporter
	edgeSink            EdgeSink
	cfg                 IndexConfig
	logger              *slog.Logger
	done                chan struct{}
}

// newIndexerDelegate creates an indexerDelegate. A zero batchInterval
// in cfg defaults to 5 seconds. If edgeSink is nil, [NoopEdgeSink] is
// used. clusterResourceName is the managed cluster (clusters/{id}) used
// for object resource-name parents and edge-delta keys.
func newIndexerDelegate(
	clusterResourceName domain.ResourceName,
	dynClient dynamic.Interface,
	discClient discovery.DiscoveryInterface,
	reporter InventoryReporter,
	edgeSink EdgeSink,
	cfg IndexConfig,
	logger *slog.Logger,
) *indexerDelegate {
	if cfg.BatchInterval == 0 {
		cfg.BatchInterval = 5 * time.Second
	}
	if cfg.SettleCheckInterval == 0 {
		cfg.SettleCheckInterval = 5 * time.Second
	}
	if edgeSink == nil {
		edgeSink = NoopEdgeSink{}
	}
	return &indexerDelegate{
		clusterResourceName: clusterResourceName,
		dynClient:           dynClient,
		discClient:          discClient,
		reporter:            reporter,
		edgeSink:            edgeSink,
		cfg:                 cfg,
		logger:              logger,
		done:                make(chan struct{}),
	}
}

// start runs the informer manager and writer until ctx is cancelled.
// It discovers all supported GVRs, filters them through deny/allow lists,
// and uses RunContinuous to watch for CRD changes and re-reconcile.
//
// On shutdown, done closes only after ordinary and CRD informers have been
// awaited and the writer has finished its final flush under a shared
// shutdown deadline. That means no further inventory reporter / database
// calls are made for this target after done closes.
func (ic *indexerDelegate) start(ctx context.Context) {
	defer close(ic.done)
	startedAt := time.Now()

	schemaMap := ic.cfg.Schema.Entries

	var nsFilter *NamespaceFilter
	if ic.cfg.NamespaceFilter != nil {
		var err error
		nsFilter, err = NewNamespaceFilter(*ic.cfg.NamespaceFilter)
		if err != nil {
			ic.logger.Error("invalid namespace filter config; indexing will not start", "error", err)
			return
		}
	}

	w := NewWriter(ic.clusterResourceName, ic.reporter, ic.edgeSink, schemaMap, ic.cfg.BatchInterval, ic.logger)

	mgr := NewInformerManager(
		ic.dynClient,
		ic.discClient,
		w.EventCh(),
		w.ResyncCh(),
		w.RemoveCh(),
		nsFilter,
		ic.logger,
	)

	// Writer is not a child of ctx: ctx cancel must stop producers first,
	// then stop the writer under the shared shutdown budget.
	writerCtx, writerCancel := context.WithCancel(context.Background())
	defer writerCancel()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w.Run(writerCtx)
	}()

	// What this run does not find again is removed once it has read
	// everything it can (see [StaleObjectPurger]). startedAt was taken
	// before the writer and the informers existed, so every object this
	// run reports is written after it.
	purgeDone := make(chan struct{})
	go func() {
		defer close(purgeDone)
		ic.purgeUnconfirmedObjects(ctx, mgr, startedAt)
	}()

	// RunContinuous blocks until ctx is cancelled, performing initial
	// reconciliation and re-reconciling when CRDs change.
	mgr.RunContinuous(ctx, ic.cfg.DenyList, ic.cfg.AllowList)
	<-purgeDone

	// Context is done; clean up informers and writer under one budget.
	// StopAll does not emit RemoveGVR events, so shutdown does not treat
	// local cache eviction as a desired-set GVR removal.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultStopTimeout)
	defer shutdownCancel()

	if err := mgr.StopAll(shutdownCtx); err != nil {
		ic.logger.Warn("stop informers on shutdown incomplete", "error", err)
	}

	// Prefer Stop so the final flush inherits shutdownCtx's deadline.
	// Only cancel the run context if the shared budget expires first.
	w.Stop(shutdownCtx)
	select {
	case <-writerDone:
	case <-shutdownCtx.Done():
		writerCancel()
		<-writerDone
	}
}

// purgeUnconfirmedObjects waits until the informers have a first
// complete picture of the cluster, then removes, once, the cluster's
// objects that nothing has written since startedAt. It returns when
// that is done or ctx ends; a failed removal is retried.
func (ic *indexerDelegate) purgeUnconfirmedObjects(ctx context.Context, mgr *InformerManager, startedAt time.Time) {
	purger, ok := ic.reporter.(StaleObjectPurger)
	if !ok {
		return
	}
	ticker := time.NewTicker(ic.cfg.SettleCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if !mgr.Settled() {
			continue
		}
		removed, err := purger.PurgeObjectsNotReportedSince(ctx, ic.clusterResourceName, startedAt)
		if err != nil {
			if ctx.Err() == nil {
				ic.logger.Warn("could not remove indexed objects that no longer exist; will retry", "error", err)
			}
			continue
		}
		if removed > 0 {
			ic.logger.Info("removed indexed objects that no longer exist in the cluster", "objects", removed)
		}
		return
	}
}
