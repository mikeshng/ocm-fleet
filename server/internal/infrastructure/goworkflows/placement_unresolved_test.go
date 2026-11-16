package goworkflows_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	wfsqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/workflow"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/delivery"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/goworkflows"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

// retryDelayStartObserver reports when an orchestration run reaches its
// retry delay, which is where a failed pass ends.
type retryDelayStartObserver struct {
	domain.NoOpFulfillmentObserver
	once    sync.Once
	started chan struct{}
}

func (o *retryDelayStartObserver) RunStarted(ctx context.Context, _ domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	return ctx, &retryDelayStartProbe{observer: o}
}

type retryDelayStartProbe struct {
	domain.NoOpFulfillmentRunProbe
	observer *retryDelayStartObserver
}

func (p *retryDelayStartProbe) RetryDelayStarted(time.Duration) {
	p.observer.once.Do(func() { close(p.observer.started) })
}

// A static placement that names a target that is not registered fails
// the same way however often it is resolved against the same targets.
// The engine must hand the failure to the workflow at once, so that the
// pass ends, the lock is released, and a deletion is not kept waiting
// for retries that cannot succeed.
func TestOrchestration_UnresolvedPlacement_IsNotRetriedInPlace(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	depName := domain.ResourceName("deployments/unresolved")
	f := domain.NewFulfillment(domain.FulfillmentID(depName), domain.DeliveryAuth{}, nil, nil, now)
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}, now)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"not-registered"},
	}, now)
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Fulfillments().Create(ctx, f); err != nil {
		t.Fatalf("create fulfillment: %v", err)
	}
	if err := tx.Deployments().Create(ctx, domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
		Name:          depName,
		UID:           domain.NewDeploymentUID(),
		FulfillmentID: f.ID(),
		CreatedAt:     now,
		UpdatedAt:     now,
	})); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// With these options one in-place retry already waits longer than
	// the test allows the whole pass.
	const inPlaceRetryInterval = 10 * time.Second
	backend := wfsqlite.NewInMemoryBackend()
	registry := &goworkflows.Registry{
		Worker:  startWorker(t, backend),
		Client:  client.New(backend),
		Timeout: 10 * time.Second,
		ActivityOptions: &workflow.ActivityOptions{
			RetryOptions: workflow.RetryOptions{
				MaxAttempts:        3,
				FirstRetryInterval: inPlaceRetryInterval,
				MaxRetryInterval:   inPlaceRetryInterval,
				BackoffCoefficient: 1,
			},
		},
	}
	observer := &retryDelayStartObserver{started: make(chan struct{})}
	spec := domain.NewOrchestrationWorkflowSpec(
		store, delivery.NewRoutingDeliveryService(), domain.StrategyFactory{Store: store}, registry,
		domain.WithRetryDelay(time.Hour),
		domain.WithFulfillmentObserver(observer),
	)
	orchestration, err := registry.RegisterOrchestration(spec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	if _, err := orchestration.Start(ctx, f.ID()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-observer.started:
	case <-time.After(inPlaceRetryInterval / 2):
		t.Fatal("the pass did not end: the unresolved placement is being retried in place")
	}
}
