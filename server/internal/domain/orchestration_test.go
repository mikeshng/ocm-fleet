package domain_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/delivery"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/memworkflow"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

// ---------------------------------------------------------------------------
// Test helpers: real SQLite store + seeding
// ---------------------------------------------------------------------------

func setupStore(t *testing.T) (domain.Store, domain.Vault) {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	return &sqlite.Store{DB: db}, &sqlite.VaultStore{DB: db}
}

// testContext returns a context with a 5-second deadline for
// orchestration tests. These tests run against in-memory stores and
// synchronous records, so they complete in milliseconds under normal
// conditions. A short deadline ensures quick failure (rather than
// hanging for the global go test timeout) when a signal is lost.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func seedFulfillmentAndDeployment(t *testing.T, store domain.Store, depName domain.ResourceName, seed domain.FulfillmentSnapshot) {
	t.Helper()
	defaultTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if seed.CreatedAt.IsZero() {
		seed.CreatedAt = defaultTime
	}
	if seed.UpdatedAt.IsZero() {
		seed.UpdatedAt = seed.CreatedAt
	}
	seed.ID = domain.FulfillmentID(depName)

	// Use Advance* to populate pending strategy records so the version
	// tables get rows on Create. The initial create always lands at
	// generation 1 (advanceGeneration bumps once per loaded state).
	wantGen := seed.Generation
	ms, ps, rs := seed.ManifestStrategy, seed.PlacementStrategy, seed.RolloutStrategy
	seed.ManifestStrategy = domain.ManifestStrategySpec{}
	seed.PlacementStrategy = domain.PlacementStrategySpec{}
	seed.RolloutStrategy = nil
	seed.ManifestStrategyVersion = 0
	seed.PlacementStrategyVersion = 0
	seed.RolloutStrategyVersion = 0
	seed.Generation = 0

	f := domain.FulfillmentFromSnapshot(seed)
	f.AdvanceManifestStrategy(ms, seed.CreatedAt)
	f.AdvancePlacementStrategy(ps, seed.CreatedAt)
	if rs != nil {
		f.AdvanceRolloutStrategy(rs, seed.CreatedAt)
	}

	dep := domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
		Name:          depName,
		UID:           domain.NewDeploymentUID(),
		FulfillmentID: f.ID(),
		CreatedAt:     seed.CreatedAt,
		UpdatedAt:     seed.UpdatedAt,
	})
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Fulfillments().Create(context.Background(), f); err != nil {
		t.Fatalf("seed fulfillment %q: %v", f.ID(), err)
	}
	if err := tx.Deployments().Create(context.Background(), dep); err != nil {
		t.Fatalf("seed deployment %q: %v", depName, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Reach the desired generation through real update cycles. Each
	// transaction loads, touches, and saves — exactly what would happen
	// in production. This keeps the test grounded in reachable state.
	for gen := f.Generation(); gen < wantGen; gen++ {
		tx2, err := store.Begin(context.Background())
		if err != nil {
			t.Fatalf("begin touch tx: %v", err)
		}
		defer tx2.RollbackOnExit()
		loaded, err := tx2.Fulfillments().Get(context.Background(), f.ID())
		if err != nil {
			t.Fatalf("reload fulfillment: %v", err)
		}
		loaded.Touch(seed.CreatedAt)
		if err := tx2.Fulfillments().Update(context.Background(), loaded); err != nil {
			t.Fatalf("touch fulfillment: %v", err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatalf("commit touch: %v", err)
		}
	}
}

func seedSignerEnrollment(t *testing.T, store domain.Store, se domain.SignerEnrollment) {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.SignerEnrollments().Create(context.Background(), se); err != nil {
		t.Fatalf("seed signer enrollment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func seedTargets(t *testing.T, store domain.Store, targets ...domain.TargetInfo) {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	for _, tgt := range targets {
		if err := tx.Targets().Create(context.Background(), tgt); err != nil {
			t.Fatalf("seed target %q: %v", tgt.ID(), err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func getFulfillment(t *testing.T, store domain.Store, id domain.ResourceName) domain.Fulfillment {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID(id))
	if err != nil {
		t.Fatalf("get fulfillment for deployment %q: %v", id, err)
	}
	return *f
}

func getThinDeployment(t *testing.T, store domain.Store, id domain.ResourceName) domain.Deployment {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	dep, err := tx.Deployments().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get deployment %q: %v", id, err)
	}
	return dep
}

func getTarget(t *testing.T, store domain.Store, id domain.TargetID) domain.TargetInfo {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	tgt, err := tx.Targets().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get target %q: %v", id, err)
	}
	return tgt
}

func getInventoryItem(t *testing.T, store domain.Store, id domain.InventoryItemID) domain.InventoryItem {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	item, err := tx.Inventory().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get inventory item %q: %v", id, err)
	}
	return item
}

func getDeliveries(t *testing.T, store domain.Store, depName domain.ResourceName) []domain.Delivery {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	records, err := tx.Deliveries().ListByFulfillment(context.Background(), domain.FulfillmentID(depName))
	if err != nil {
		t.Fatalf("list deliveries for %q: %v", depName, err)
	}
	return records
}

func getDelivery(t *testing.T, store domain.Store, id domain.DeliveryID) domain.Delivery {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	d, err := tx.Deliveries().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get delivery %q: %v", id, err)
	}
	return d
}

func seedDelivery(t *testing.T, store domain.Store, d domain.Delivery) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Recording observer for intermediate state assertions
// ---------------------------------------------------------------------------

type recordingObserver struct {
	domain.NoOpFulfillmentObserver
	mu        sync.Mutex
	states    []domain.FulfillmentState
	filtered  []filteredEvent
	acks      []domain.DeliveryID
	stale     []domain.FulfillmentEvent
	completed []domain.DeliveryState
}

type filteredEvent struct {
	TargetID domain.TargetID
	Total    int
	Accepted int
}

func (o *recordingObserver) RunStarted(ctx context.Context, _ domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	return ctx, &recordingProbe{observer: o}
}

type recordingProbe struct {
	domain.NoOpFulfillmentRunProbe
	observer *recordingObserver
}

func (p *recordingProbe) StateChanged(state domain.FulfillmentState) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.states = append(p.observer.states, state)
}

func (p *recordingProbe) ManifestsFiltered(target domain.TargetInfo, total, accepted int) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.filtered = append(p.observer.filtered, filteredEvent{
		TargetID: target.ID(),
		Total:    total,
		Accepted: accepted,
	})
}

func (p *recordingProbe) DispatchCycleStarted(int, domain.Generation) domain.DispatchCycleProbe {
	return &recordingDispatchProbe{observer: p.observer}
}

// recordingDispatchProbe captures stale-event discards and accepted
// completions within one dispatch cycle.
type recordingDispatchProbe struct {
	domain.NoOpDispatchCycleProbe
	observer *recordingObserver
}

func (p *recordingDispatchProbe) StaleEventDiscarded(event domain.FulfillmentEvent, _ domain.Generation) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.stale = append(p.observer.stale, event)
}

func (p *recordingDispatchProbe) AckReceived(id domain.DeliveryID) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.acks = append(p.observer.acks, id)
}

func (p *recordingDispatchProbe) Completed(_ domain.DeliveryID, state domain.DeliveryState) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.completed = append(p.observer.completed, state)
}

// ---------------------------------------------------------------------------
// Workflow record fakes
// ---------------------------------------------------------------------------

// recordingRecord wraps a [domain.Record] and records activity names
// and target-related inputs so tests can assert execution sequence.
type recordingRecord struct {
	ctx      context.Context
	records  []activityRecord
	delegate domain.Record
}

type activityRecord struct {
	Name     string
	TargetID domain.TargetID
}

func (r *recordingRecord) ID() string               { return r.delegate.ID() }
func (r *recordingRecord) Context() context.Context { return r.ctx }

func (r *recordingRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	name := activity.Name()
	var targetID domain.TargetID
	switch v := in.(type) {
	case domain.RemoveInput:
		targetID = v.Target.ID()
	case domain.GenerateManifestsInput:
		targetID = v.Target.ID()
	case domain.DeliverInput:
		targetID = v.Target.ID()
	}
	r.records = append(r.records, activityRecord{Name: name, TargetID: targetID})
	return r.delegate.Run(activity, in)
}

func (r *recordingRecord) Await(signalName string) (any, error) {
	return r.delegate.Await(signalName)
}
func (r *recordingRecord) AwaitWithTimeout(signalName string, timeout time.Duration) (any, error) {
	return r.delegate.AwaitWithTimeout(signalName, timeout)
}
func (r *recordingRecord) Sleep(d time.Duration) error {
	return r.delegate.Sleep(d)
}

func (r *recordingRecord) activityNames() []string {
	names := make([]string, len(r.records))
	for i, rec := range r.records {
		names[i] = rec.Name
	}
	return names
}

// simpleRecord runs activities synchronously and delivers delivery
// completion events from the events channel. Used by most tests.
type simpleRecord struct {
	ctx    context.Context
	events <-chan domain.FulfillmentEvent
}

func (r *simpleRecord) ID() string               { return "test-simple" }
func (r *simpleRecord) Context() context.Context { return r.ctx }
func (r *simpleRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}
func (r *simpleRecord) Await(_ string) (any, error) {
	select {
	case e := <-r.events:
		return e, nil
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}
func (r *simpleRecord) AwaitWithTimeout(_ string, timeout time.Duration) (any, error) {
	if timeout == 0 {
		select {
		case e := <-r.events:
			return e, nil
		default:
			return nil, domain.ErrSignalTimeout
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case e := <-r.events:
		return e, nil
	case <-timer.C:
		return nil, domain.ErrSignalTimeout
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}
func (r *simpleRecord) Sleep(_ time.Duration) error {
	return nil
}

// ---------------------------------------------------------------------------
// Signal routing
// ---------------------------------------------------------------------------

type stubRegistry struct {
	events chan domain.FulfillmentEvent
}

func (r *stubRegistry) SignalFulfillmentEvent(_ context.Context, _ domain.FulfillmentID, event domain.FulfillmentEvent) error {
	r.events <- event
	return nil
}

func (r *stubRegistry) RegisterOrchestration(_ *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterCreateDeployment(_ *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterDeleteDeployment(_ *domain.DeleteDeploymentWorkflowSpec) (domain.DeleteDeploymentWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterDeleteDeploymentCleanup(_ *domain.DeleteDeploymentCleanupWorkflowSpec) (domain.DeleteDeploymentCleanupWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterDeleteManagedResourceCleanup(_ *domain.DeleteManagedResourceCleanupWorkflowSpec) (domain.DeleteManagedResourceCleanupWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterResumeDeployment(_ *domain.ResumeDeploymentWorkflowSpec) (domain.ResumeDeploymentWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterProvisionIdP(_ *domain.ProvisionIdPWorkflowSpec) (domain.ProvisionIdPWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterCreateManagedResource(_ *domain.CreateManagedResourceWorkflowSpec) (domain.CreateManagedResourceWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterDeleteManagedResource(_ *domain.DeleteManagedResourceWorkflowSpec) (domain.DeleteManagedResourceWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) SignalDeleteCleanupComplete(_ context.Context, _ domain.FulfillmentID, _ domain.DeleteCleanupCompleteEvent) error {
	return nil
}

func (r *stubRegistry) RegisterResumeManagedResource(_ *domain.ResumeManagedResourceWorkflowSpec) (domain.ResumeManagedResourceWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) Start(context.Context) error { return nil }
func (r *stubRegistry) Wait(context.Context) error  { return nil }
func (r *stubRegistry) Close(context.Context) error { return nil }

// ---------------------------------------------------------------------------
// Delivery agent fakes
// ---------------------------------------------------------------------------

type noopDelivery struct {
	events chan<- domain.FulfillmentEvent
}

func (d noopDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (d noopDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// asyncDelivery signals the workflow via the events channel in a
// goroutine and optionally closes a done channel for test
// synchronization.
type asyncDelivery struct {
	events chan<- domain.FulfillmentEvent
	done   chan struct{}
}

func (a *asyncDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
		if a.done != nil {
			close(a.done)
		}
	}()
	return nil
}

func (a *asyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// emittingAsyncDelivery is like asyncDelivery but also emits a
// progress event (ignored by the domain workflow, exercised
// indirectly). The signal is the important part.
type emittingAsyncDelivery struct {
	events chan<- domain.FulfillmentEvent
	done   chan struct{}
}

func (a *emittingAsyncDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
		if a.done != nil {
			close(a.done)
		}
	}()
	return nil
}

func (a *emittingAsyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// outputProducingDelivery reports through the real [domain.DeliveryReporter]
// so the delivery report service commits the structured outputs in the same
// transaction as the delivery state, exactly as an addon does.
type outputProducingDelivery struct {
	reporter   domain.DeliveryReporter
	onReported func()
	targets    []domain.ProvisionedTarget
	secrets    []domain.ProducedSecret
}

func (d *outputProducingDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	err := d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
		State:              domain.DeliveryStateDelivered,
		Operation:          domain.DeliveryOperationDeliver,
		ProvisionedTargets: d.targets,
		ProducedSecrets:    d.secrets,
	})
	if err == nil && d.onReported != nil {
		d.onReported()
	}
	return err
}

func (d *outputProducingDelivery) Remove(ctx context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	return d.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationRemove,
	})
}

type failingRemoveDelivery struct {
	events chan<- domain.FulfillmentEvent
	err    error
}

func (f *failingRemoveDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		f.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (f *failingRemoveDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	state := domain.DeliveryStateFailed
	if domain.IsAuthExpired(f.err) {
		state = domain.DeliveryStateAuthFailed
	}
	go func() {
		f.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: state, Message: f.err.Error()},
			},
		}
	}()
	return nil
}

type authFailingDelivery struct {
	events chan<- domain.FulfillmentEvent
}

func (d authFailingDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result: domain.DeliveryResult{
					State:   domain.DeliveryStateAuthFailed,
					Message: "401 Unauthorized",
				},
			},
		}
	}()
	return nil
}

func (d authFailingDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

type recordingDelivery struct {
	events chan<- domain.FulfillmentEvent
	mu     sync.Mutex
	// delivered tracks which targets received deliveries.
	delivered []domain.TargetID
}

func (d *recordingDelivery) Deliver(_ context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	d.mu.Lock()
	d.delivered = append(d.delivered, target.ID())
	d.mu.Unlock()
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (d *recordingDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// dispatchErrorDelivery fails a dispatch before the addon has received
// anything, returning its error synchronously from the activity. This
// is how an expired credential surfaces when the delivery agent cannot
// even hand the work over: there is no completion signal to wait for,
// so the pause has to come from the dispatch error itself.
type dispatchErrorDelivery struct {
	deliverErr error
	removeErr  error
}

func (d dispatchErrorDelivery) Deliver(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	return d.deliverErr
}

func (d dispatchErrorDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	return d.removeErr
}

// ---------------------------------------------------------------------------
// Helper to build a standard workflow spec for tests
// ---------------------------------------------------------------------------

func newTestWorkflow(store domain.Store, delivery domain.DeliveryAgent, events chan domain.FulfillmentEvent, opts ...func(*domain.OrchestrationWorkflowSpec)) *domain.OrchestrationWorkflowSpec {
	reg := &stubRegistry{events: events}
	wf := domain.NewOrchestrationWorkflowSpec(
		store, delivery, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
		domain.WithRetryDelay(1*time.Millisecond),
	)
	for _, opt := range opts {
		opt(wf)
	}
	return wf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestOrchestration_BasicPipeline_ReachesActive(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
	if dep.ObservedGeneration() != 1 {
		t.Errorf("ObservedGeneration = %d, want 1", dep.ObservedGeneration())
	}

	deliveries := getDeliveries(t, store, "deployments/d1")
	if len(deliveries) != 2 {
		t.Fatalf("expected 2 deliveries, got %d", len(deliveries))
	}
}

func TestOrchestration_RemoveStepsRunBeforeDeliverSteps(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:      1,
		ResolvedTargets: []domain.TargetID{"old1"},
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"new1", "new2"},
		},
		State: domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "old1", Name: "old1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new1", Name: "new1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new2", Name: "new2", Type: "test"}),
	)

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var removeOld1At, generateNew1At int = -1, -1
	for i, rec := range recorder.records {
		if rec.Name == "remove-from-target" && rec.TargetID == "old1" {
			removeOld1At = i
			break
		}
	}
	for i, rec := range recorder.records {
		if rec.Name == "generate-manifests" && rec.TargetID == "new1" {
			generateNew1At = i
			break
		}
	}
	if removeOld1At < 0 {
		t.Fatal("remove-from-target for old1 never recorded")
	}
	if generateNew1At < 0 {
		t.Fatal("generate-manifests for new1 never recorded")
	}
	if removeOld1At >= generateNew1At {
		t.Errorf("removals must run before delivery: remove(old1) at %d, generate(new1) at %d",
			removeOld1At, generateNew1At)
	}
}

// TestOrchestration_RolloutRemove_AuthExpiredDispatch_Pauses verifies
// that a removal step which cannot even be dispatched because the
// target's credentials are expired pauses the fulfillment. The delete
// pipeline already turns this dispatch failure into a pause; a removal
// planned as a rollout step must do the same instead of continuing as
// new and retrying with the same unusable credentials.
func TestOrchestration_RolloutRemove_AuthExpiredDispatch_Pauses(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ResolvedTargets:   []domain.TargetID{"old1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"new1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "old1", Name: "old1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new1", Name: "new1", Type: "test"}),
	)
	// The target was delivered to before placement dropped it, so the
	// removal has a record to withdraw and a dispatch to make.
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:old1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "old1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, dispatchErrorDelivery{
		removeErr: fmt.Errorf("%w: the fleetlet gateway refused the removal", domain.ErrAuthExpired),
	}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}

	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
		done <- err
	}()
	select {
	case err := <-done:
		var canErr *domain.ContinueAsNewError
		if errors.As(err, &canErr) {
			t.Fatalf("Run continued as new instead of pausing for fresh credentials: %v", err)
		}
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		dep := getFulfillment(t, store, "deployments/d1")
		if !dep.Paused() {
			t.Fatal("expected fulfillment to be paused after an auth-expired removal dispatch")
		}
		if !strings.Contains(dep.PauseReason(), "old1") {
			t.Errorf("PauseReason = %q, want it to name target old1", dep.PauseReason())
		}
		if !strings.Contains(dep.PauseReason(), "delivery auth expired") {
			t.Errorf("PauseReason = %q, want it to carry the dispatch error", dep.PauseReason())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("orchestration hung on an auth-expired removal dispatch")
	}
}

// TestOrchestration_DeletePipeline_AuthExpiredDispatch_Pauses covers
// the same dispatch failure in the delete pipeline, whose callback
// converts it to a pause, as the rollout paths do.
func TestOrchestration_DeletePipeline_AuthExpiredDispatch_Pauses(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, dispatchErrorDelivery{
		removeErr: fmt.Errorf("%w: the fleetlet gateway refused the removal", domain.ErrAuthExpired),
	}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}

	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
		done <- err
	}()
	select {
	case err := <-done:
		var canErr *domain.ContinueAsNewError
		if errors.As(err, &canErr) {
			t.Fatalf("Run continued as new instead of pausing for fresh credentials: %v", err)
		}
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		dep := getFulfillment(t, store, "deployments/d1")
		if !dep.Paused() {
			t.Fatal("expected fulfillment to be paused after an auth-expired removal dispatch")
		}
		if dep.State() != domain.FulfillmentStateDeleting {
			t.Errorf("State = %q, want deleting (pause must preserve lifecycle state)", dep.State())
		}
		if !strings.Contains(dep.PauseReason(), "delivery auth expired") {
			t.Errorf("PauseReason = %q, want it to carry the dispatch error", dep.PauseReason())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("orchestration hung on an auth-expired removal dispatch")
	}
}

// TestOrchestration_DeliverStep_AuthExpiredDispatch_Pauses verifies the
// same for a delivery step: the addon reported no AuthFailed (it never
// received the work), and the activity error says the credentials are
// expired, so the fulfillment pauses rather than continuing as new.
func TestOrchestration_DeliverStep_AuthExpiredDispatch_Pauses(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, dispatchErrorDelivery{
		deliverErr: fmt.Errorf("%w: the fleetlet gateway refused the delivery", domain.ErrAuthExpired),
	}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}

	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
		done <- err
	}()
	select {
	case err := <-done:
		var canErr *domain.ContinueAsNewError
		if errors.As(err, &canErr) {
			t.Fatalf("Run continued as new instead of pausing for fresh credentials: %v", err)
		}
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		dep := getFulfillment(t, store, "deployments/d1")
		if !dep.Paused() {
			t.Fatal("expected fulfillment to be paused after an auth-expired delivery dispatch")
		}
		if !strings.Contains(dep.PauseReason(), "t1") {
			t.Errorf("PauseReason = %q, want it to name target t1", dep.PauseReason())
		}
		if !strings.Contains(dep.PauseReason(), "delivery auth expired") {
			t.Errorf("PauseReason = %q, want it to carry the dispatch error", dep.PauseReason())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("orchestration hung on an auth-expired delivery dispatch")
	}
}

func TestOrchestration_PlacementAndRolloutRunAsActivities(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	names := recorder.activityNames()
	if !contains(names, "resolve-placement") {
		t.Error("resolve-placement not recorded as activity")
	}
	if !contains(names, "plan-rollout") {
		t.Error("plan-rollout not recorded as activity")
	}
}

func TestOrchestration_ZeroTargets_ActiveWithEmptySet(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategySelector, TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test", Labels: map[string]string{"env": "dev"}}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
	if len(dep.ResolvedTargets()) != 0 {
		t.Errorf("ResolvedTargets = %v, want empty", dep.ResolvedTargets())
	}
	if dep.ActiveWorkflowGen() != nil {
		t.Errorf("ActiveWorkflowGen should be nil, got %v", dep.ActiveWorkflowGen())
	}
	if dep.ObservedGeneration() != 1 {
		t.Errorf("ObservedGeneration = %d, want 1", dep.ObservedGeneration())
	}
}

func TestOrchestration_DeliveryOutputs_RegistersTargetAndStoresSecret(t *testing.T) {
	store, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"name":"new-cluster"}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"provisioner"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "provisioner", Name: "provisioner", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	reporter := application.NewDeliveryReportService(store, &stubRegistry{events: events},
		application.WithDeliveryReportVault(vault))
	wf := newTestWorkflow(store, &outputProducingDelivery{
		reporter: reporter,
		targets: []domain.ProvisionedTarget{{
			ID: "k8s-new-cluster", Type: "kubernetes", Name: "new-cluster",
			Properties: map[string]string{"kubeconfig_ref": "targets/k8s-new-cluster/kubeconfig"},
		}},
		secrets: []domain.ProducedSecret{{
			Ref: "targets/k8s-new-cluster/kubeconfig", Value: []byte("fake-kubeconfig-data"),
		}},
	}, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Vault = vault
	})

	rec := &simpleRecord{ctx: testContext(t), events: events}

	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tgt := getTarget(t, store, "k8s-new-cluster")
	if tgt.Type() != "kubernetes" {
		t.Errorf("target type = %q, want kubernetes", tgt.Type())
	}

	item := getInventoryItem(t, store, "target:k8s-new-cluster")
	if item.SourceDeliveryID() == nil || *item.SourceDeliveryID() != "deployments/d1:provisioner" {
		t.Fatalf("inventory SourceDeliveryID = %v, want deployments/d1:provisioner", item.SourceDeliveryID())
	}

	if vault != nil {
		secret, err := vault.Get(context.Background(), "targets/k8s-new-cluster/kubeconfig")
		if err != nil {
			t.Fatalf("vault get: %v", err)
		}
		if string(secret) != "fake-kubeconfig-data" {
			t.Errorf("secret = %q, want fake-kubeconfig-data", secret)
		}
	}
}

func TestOrchestration_AsyncDelivery_ReachesActive(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, &asyncDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
}

func TestOrchestration_EmittingAsyncDelivery_ReachesActive(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, &emittingAsyncDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
}

func TestOrchestration_AuthFailure_SetsPausedAuth(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if !dep.Paused() {
		t.Error("expected fulfillment to be paused after auth failure")
	}
	if dep.State() != domain.FulfillmentStateCreating {
		t.Errorf("State = %q, want creating (pause must preserve lifecycle state)", dep.State())
	}
}

func TestOrchestration_DeletePipeline_RemovesFromTargets(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t2", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	names := recorder.activityNames()
	removeCount := 0
	for _, name := range names {
		if name == "remove-from-target" {
			removeCount++
		}
	}
	if removeCount != 2 {
		t.Errorf("expected 2 remove-from-target, got %d (activities: %v)", removeCount, names)
	}

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
}

func TestOrchestration_DeletePipeline_ResetsDeliveryForRemove(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})

	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var observedState domain.DeliveryState
	var removeCalled bool
	delivery := &deliveryStateChecker{
		store:  store,
		events: events,
		onRemove: func(deliveryID domain.DeliveryID) {
			removeCalled = true
			tx, err := store.BeginReadOnly(context.Background())
			if err != nil {
				t.Fatalf("read delivery in Remove: %v", err)
			}
			defer tx.RollbackOnExit()
			d, err := tx.Deliveries().Get(context.Background(), deliveryID)
			if err != nil {
				t.Fatalf("get delivery in Remove: %v", err)
			}
			observedState = d.State()
		},
	}
	wf := newTestWorkflow(store, delivery, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !removeCalled {
		t.Fatal("Remove was never called")
	}
	if observedState != domain.DeliveryStatePending {
		t.Errorf("delivery state inside Remove = %q, want %q", observedState, domain.DeliveryStatePending)
	}
}

// deliveryStateChecker is a DeliveryService stub that calls onDeliver or
// onRemove inside the respective dispatch so the test can inspect
// delivery record state.
type deliveryStateChecker struct {
	store     domain.Store
	events    chan<- domain.FulfillmentEvent
	onDeliver func(deliveryID domain.DeliveryID)
	onRemove  func(deliveryID domain.DeliveryID)
}

func (d *deliveryStateChecker) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	if d.onDeliver != nil {
		d.onDeliver(deliveryID)
	}
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (d *deliveryStateChecker) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	if d.onRemove != nil {
		d.onRemove(deliveryID)
	}
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// TestOrchestration_DeletePipeline_WaitsForProgressingDelivery seeds a
// delivery in Progressing state (acked but not completed — as happens after a
// crash) and verifies that RemoveFromTarget does NOT re-dispatch via
// Agent.Remove. Instead it returns Dispatched=true so dispatchAndAwait
// waits for the completion signal from RecoverActiveDeliveries.
func TestOrchestration_DeletePipeline_WaitsForProgressingDelivery(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})

	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 2,
		State:      domain.DeliveryStateProgressing,
		Operation:  domain.DeliveryOperationRemove,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var removeCalled bool
	delivery := &deliveryStateChecker{
		store:  store,
		events: events,
		onRemove: func(_ domain.DeliveryID) {
			removeCalled = true
		},
	}
	wf := newTestWorkflow(store, delivery, events)

	// Simulate RecoverActiveDeliveries sending the completion signal
	// before the ack-timeout fires: inject a completion event into the
	// channel so dispatchAndAwait can pick it up.
	go func() {
		events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: "deployments/d1:t1",
				Generation: 2,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered, Operation: domain.DeliveryOperationRemove},
			},
		}
	}()

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if removeCalled {
		t.Fatal("Remove was called — Progressing delivery should wait for completion signal, not re-dispatch")
	}

	// The completion signal must have been consumed by dispatchAndAwait.
	// If RemoveFromTarget returned Dispatched=false the delivery would be
	// skipped, the signal would remain unconsumed, and cleanup would run
	// prematurely without the addon finishing the real work.
	select {
	case <-events:
		t.Fatal("completion signal not consumed — delivery was likely skipped (Dispatched=false)")
	default:
	}
}

// TestOrchestration_DeletePipeline_TerminalRemoval_IsNotRedispatched
// verifies that a removal which already finished at the pipeline's
// generation is left alone: re-dispatching it would duplicate the
// removal the addon already performed.
func TestOrchestration_DeletePipeline_TerminalRemoval_IsNotRedispatched(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 2,
		State:      domain.DeliveryStateDelivered,
		Operation:  domain.DeliveryOperationRemove,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	agent := &recordingRemoveDelivery{events: events}
	wf := newTestWorkflow(store, agent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	agent.mu.Lock()
	removed := append([]domain.TargetID(nil), agent.removed...)
	agent.mu.Unlock()
	if len(removed) != 0 {
		t.Errorf("Remove called for %v, want none: the removal finished at this generation", removed)
	}
	if deliveries := getDeliveries(t, store, "deployments/d1"); len(deliveries) != 0 {
		t.Errorf("delivery records remaining = %d, want 0 after cleanup", len(deliveries))
	}
	if dep := getFulfillment(t, store, "deployments/d1"); dep.State() != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting", dep.State())
	}
}

func TestOrchestration_DeletePipeline_HardDeletesRecord(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})

	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t2", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
	deliveries, err := tx.Deliveries().ListByFulfillment(context.Background(), domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 0 {
		t.Errorf("expected 0 delivery records, got %d", len(deliveries))
	}
}

func TestOrchestration_DeletePipeline_NoTargets_HardDeletes(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   nil,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic},
		State:             domain.FulfillmentStateDeleting,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
}

func TestOrchestration_DeletePipeline_CleansUpOwnedOutputsAndSecrets(t *testing.T) {
	store, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:      2,
		ResolvedTargets: []domain.TargetID{"kind-provider"},
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ManifestType: "api.kind.cluster",
				Raw:          json.RawMessage(`{"name":"guest-cluster"}`),
			}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"kind-provider"},
		},
		State: domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "kind-provider", Name: "kind-provider", Type: "kind"}))

	deliveryID := domain.DeliveryID("deployments/d1:kind-provider")
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            deliveryID,
		FulfillmentID: domain.FulfillmentID("deployments/d1"),
		TargetID:      "kind-provider",
		Manifests: []domain.Manifest{{
			ManifestType: "api.kind.cluster",
			Raw:          json.RawMessage(`{"name":"guest-cluster"}`),
		}},
		State: domain.DeliveryStateDelivered,
	}))

	ctx := testContext(t)
	if err := vault.Put(ctx, "targets/k8s-guest-cluster/sa-token", []byte("fake-sa-token")); err != nil {
		t.Fatalf("vault put: %v", err)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()

	if err := tx.Inventory().Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
		ID:               "target:k8s-guest-cluster",
		Type:             "kubernetes",
		Name:             "guest-cluster",
		Properties:       json.RawMessage(`{"api_server":"https://guest.example:6443","service_account_token_ref":"targets/k8s-guest-cluster/sa-token"}`),
		SourceDeliveryID: &deliveryID,
		CreatedAt:        now,
		UpdatedAt:        now,
	})); err != nil {
		t.Fatalf("seed inventory item: %v", err)
	}
	if err := tx.Targets().Create(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:              "k8s-guest-cluster",
		InventoryItemID: "target:k8s-guest-cluster",
		Type:            "kubernetes",
		Name:            "guest-cluster",
		Properties: map[string]string{
			"api_server":                "https://guest.example:6443",
			"service_account_token_ref": "targets/k8s-guest-cluster/sa-token",
		},
	})); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seeded output: %v", err)
	}

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Vault = vault
	})

	rec := &simpleRecord{ctx: ctx, events: events}
	_, err = wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	_ = getTarget(t, store, "kind-provider")

	readTx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}

	if _, err := readTx.Targets().Get(ctx, "k8s-guest-cluster"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected guest target to be deleted, got err=%v", err)
	}
	if _, err := readTx.Inventory().Get(ctx, "target:k8s-guest-cluster"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected guest inventory item to be deleted, got err=%v", err)
	}
	deliveries, err := readTx.Deliveries().ListByFulfillment(ctx, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("expected delivery records to be deleted, got %d", len(deliveries))
	}
	f, err := readTx.Fulfillments().Get(ctx, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatalf("close read tx: %v", err)
	}

	if _, err := vault.Get(ctx, "targets/k8s-guest-cluster/sa-token"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected owned vault secret to be deleted, got err=%v", err)
	}
}

// TestOrchestration_DeletePipeline_MissingTargetRowForOwnedInventoryItem_SkipsNotFails
// pins that a delivery-owned inventory item whose "target:{id}" points at a
// target row that is already gone must be skipped, not treated as an error.
// The delivery-owned inventory item itself is still deleted normally, since
// that deletion does not depend on the target row's existence.
func TestOrchestration_DeletePipeline_MissingTargetRowForOwnedInventoryItem_SkipsNotFails(t *testing.T) {
	store, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:      2,
		ResolvedTargets: []domain.TargetID{"kind-provider"},
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ManifestType: "api.kind.cluster",
				Raw:          json.RawMessage(`{"name":"guest-cluster"}`),
			}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"kind-provider"},
		},
		State: domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "kind-provider", Name: "kind-provider", Type: "kind"}))

	deliveryID := domain.DeliveryID("deployments/d1:kind-provider")
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            deliveryID,
		FulfillmentID: domain.FulfillmentID("deployments/d1"),
		TargetID:      "kind-provider",
		Manifests: []domain.Manifest{{
			ManifestType: "api.kind.cluster",
			Raw:          json.RawMessage(`{"name":"guest-cluster"}`),
		}},
		State: domain.DeliveryStateDelivered,
	}))

	ctx := testContext(t)
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	// Deliberately no corresponding tx.Targets().Create call: this
	// inventory item's target row is already gone through some other path.
	if err := tx.Inventory().Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
		ID:               "target:ghost-target",
		Type:             "kind",
		Name:             "ghost",
		SourceDeliveryID: &deliveryID,
	})); err != nil {
		t.Fatalf("seed inventory item: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seeded output: %v", err)
	}

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Vault = vault
	})

	rec := &simpleRecord{ctx: ctx, events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	readTx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}
	defer readTx.RollbackOnExit()
	if _, err := readTx.Inventory().Get(ctx, "target:ghost-target"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ghost inventory item to be deleted, got err=%v", err)
	}
	f, err := readTx.Fulfillments().Get(ctx, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
}

func TestOrchestration_DeletePipeline_MissingDeliveryRecord_Skips(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
}

func TestOrchestration_DeletePipeline_RemoveFailure_KeepsRecord(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	failingAgent := &failingRemoveDelivery{events: events, err: fmt.Errorf("network timeout")}
	wf := newTestWorkflow(store, failingAgent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err == nil {
		t.Fatal("expected error from Remove failure")
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting", dep.State())
	}
}

func TestOrchestration_CompleteReconciliation_LoopsOnNewGeneration(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	// Intercepting record bumps generation after the first load,
	// simulating a concurrent external mutation. The workflow should
	// loop: first iteration reconciles gen 1 and sees gen 3 has
	// arrived, second iteration reconciles gen 3 and exits.
	rec := &simpleRecord{ctx: testContext(t), events: events}
	interceptor := &afterLoadBumpGenRecord{
		delegate: rec,
		store:    store,
		depName:  "deployments/d1",
		bumps:    2,
	}

	_, err := wf.Run(interceptor, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.ObservedGeneration() != 3 {
		t.Errorf("ObservedGeneration = %d, want 3 (loop should reconcile up to bumped generation)", dep.ObservedGeneration())
	}
}

// afterLoadBumpGenRecord wraps a record and bumps the deployment's
// generation after the load-deployment-and-pool activity runs. This
// simulates a concurrent mutation arriving mid-workflow.
type afterLoadBumpGenRecord struct {
	delegate domain.Record
	store    domain.Store
	depName  domain.ResourceName
	bumps    int
	loaded   bool
}

func (r *afterLoadBumpGenRecord) ID() string                    { return r.delegate.ID() }
func (r *afterLoadBumpGenRecord) Context() context.Context      { return r.delegate.Context() }
func (r *afterLoadBumpGenRecord) Await(sig string) (any, error) { return r.delegate.Await(sig) }
func (r *afterLoadBumpGenRecord) AwaitWithTimeout(sig string, timeout time.Duration) (any, error) {
	return r.delegate.AwaitWithTimeout(sig, timeout)
}
func (r *afterLoadBumpGenRecord) Sleep(d time.Duration) error { return r.delegate.Sleep(d) }

func (r *afterLoadBumpGenRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	out, err := r.delegate.Run(activity, in)
	if err != nil {
		return out, err
	}
	if !r.loaded && activity.Name() == "acquire-lock-and-load" {
		r.loaded = true
		tx, txErr := r.store.Begin(context.Background())
		if txErr != nil {
			return out, txErr
		}
		thinDep, txErr := tx.Deployments().Get(context.Background(), r.depName)
		if txErr != nil {
			tx.RollbackOnExit()
			return out, txErr
		}
		// Simulate N separate transactions each advancing generation.
		// Each iteration re-loads (hydrating loadedGeneration from the
		// repo) then calls Touch to bump generation via the real domain
		// contract.
		for range r.bumps {
			fulf, txErr := tx.Fulfillments().Get(context.Background(), thinDep.FulfillmentID())
			if txErr != nil {
				tx.RollbackOnExit()
				return out, txErr
			}
			fulf.Touch(time.Now().UTC())
			if txErr = tx.Fulfillments().Update(context.Background(), fulf); txErr != nil {
				tx.RollbackOnExit()
				return out, txErr
			}
		}
		tx.Commit()
	}
	return out, nil
}

func TestOrchestration_ResourceTypeFiltering(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation: 1,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{Raw: json.RawMessage(`{}`), ManifestType: "kubernetes.manifest"},
			},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"k8s", "plain"},
		},
		State: domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k8s", Name: "k8s", Type: "kubernetes", AcceptedManifestTypes: []domain.ManifestType{"kubernetes.manifest"}}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "plain", Name: "plain", Type: "test"}),
	)

	events := make(chan domain.FulfillmentEvent, 16)
	obs := &recordingObserver{}
	rd := &recordingDelivery{events: events}
	wf := newTestWorkflow(store, rd, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Observer = obs
	})

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(obs.filtered) < 2 {
		t.Fatalf("expected 2 filter events, got %d", len(obs.filtered))
	}

	rd.mu.Lock()
	deliveredTo := rd.delivered
	rd.mu.Unlock()

	if len(deliveredTo) != 2 {
		t.Errorf("expected 2 deliveries, got %d: %v", len(deliveredTo), deliveredTo)
	}
}

// ---------------------------------------------------------------------------
// Attestation assembly
// ---------------------------------------------------------------------------

// attestationCapturingRecord wraps a Record and captures the full
// DeliverInput and RemoveInput passed to delivery/removal activities.
type attestationCapturingRecord struct {
	delegate domain.Record
	mu       sync.Mutex
	delivers []domain.DeliverInput
	removes  []domain.RemoveInput
}

func (r *attestationCapturingRecord) ID() string               { return r.delegate.ID() }
func (r *attestationCapturingRecord) Context() context.Context { return r.delegate.Context() }
func (r *attestationCapturingRecord) Await(sig string) (any, error) {
	return r.delegate.Await(sig)
}
func (r *attestationCapturingRecord) AwaitWithTimeout(sig string, timeout time.Duration) (any, error) {
	return r.delegate.AwaitWithTimeout(sig, timeout)
}
func (r *attestationCapturingRecord) Sleep(d time.Duration) error {
	return r.delegate.Sleep(d)
}

func (r *attestationCapturingRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	switch v := in.(type) {
	case domain.DeliverInput:
		r.mu.Lock()
		r.delivers = append(r.delivers, v)
		r.mu.Unlock()
	case domain.RemoveInput:
		r.mu.Lock()
		r.removes = append(r.removes, v)
		r.mu.Unlock()
	}
	return r.delegate.Run(activity, in)
}

func testProvenance(depName domain.ResourceName, ms domain.ManifestStrategySpec, ps domain.PlacementStrategySpec) *domain.Provenance {
	return &domain.Provenance{
		Content: domain.DeploymentContent{
			Name:              depName,
			ManifestStrategy:  ms,
			PlacementStrategy: ps,
		},
		Sig: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "test-signer", Issuer: "https://issuer.example.com"},
			ContentHash:    []byte("content-hash"),
			SignatureBytes: []byte("sig-bytes"),
		},
		ValidUntil:         time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpectedGeneration: 1,
	}
}

func testSignerEnrollment(id domain.SignerEnrollmentID, subjectID domain.SubjectID) domain.SignerEnrollment {
	return domain.SignerEnrollmentFromSnapshot(domain.SignerEnrollmentSnapshot{
		ID: id,
		FederatedIdentity: domain.FederatedIdentity{
			Subject: subjectID,
			Issuer:  "https://issuer.example.com",
		},
		IdentityToken:   "test-id-token",
		RegistrySubject: domain.RegistrySubject("gh-" + string(subjectID)),
		RegistryID:      "github.com",
		CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:       time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC),
	})
}

func TestOrchestration_DeliverWithProvenance_AssemblesAttestation(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{ManifestType: "test.resource", Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}
	prov := testProvenance("deployments/attested-dep", ms, ps)

	seedSignerEnrollment(t, store, testSignerEnrollment("se-test", "test-signer"))
	seedFulfillmentAndDeployment(t, store, "deployments/attested-dep", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("deployments/attested-dep"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	delivers := capRec.delivers
	capRec.mu.Unlock()

	if len(delivers) != 1 {
		t.Fatalf("expected 1 deliver input, got %d", len(delivers))
	}

	att := delivers[0].Attestation
	if att == nil {
		t.Fatal("Attestation is nil; expected it to be assembled from Provenance")
	}

	depContent, ok := att.Input.Provenance.Content.(domain.DeploymentContent)
	if !ok {
		t.Fatalf("Input.Provenance.Content is %T, want DeploymentContent", att.Input.Provenance.Content)
	}
	if depContent.Name != "deployments/attested-dep" {
		t.Errorf("DeploymentContent.Name = %q, want %q", depContent.Name, "deployments/attested-dep")
	}
	if att.Input.Provenance.Sig.Signer != prov.Sig.Signer {
		t.Errorf("Input.Provenance.Sig.Signer = %v, want %v", att.Input.Provenance.Sig.Signer, prov.Sig.Signer)
	}
	if att.Input.Signer.RegistrySubject != "gh-test-signer" {
		t.Errorf("Input.Signer.RegistrySubject = %q, want %q", att.Input.Signer.RegistrySubject, "gh-test-signer")
	}
	if string(depContent.ManifestStrategy.Type) != string(ms.Type) {
		t.Errorf("DeploymentContent.ManifestStrategy.Type = %q, want %q",
			depContent.ManifestStrategy.Type, ms.Type)
	}
	if string(depContent.PlacementStrategy.Type) != string(ps.Type) {
		t.Errorf("DeploymentContent.PlacementStrategy.Type = %q, want %q",
			depContent.PlacementStrategy.Type, ps.Type)
	}
	put, ok := att.Output.(*domain.PutManifests)
	if !ok {
		t.Fatalf("Attestation.Output is %T, want *PutManifests", att.Output)
	}
	if len(put.Manifests) == 0 {
		t.Error("PutManifests.Manifests is empty")
	}
}

// A fulfillment whose provenance names a signer that has no enrollment
// cannot assemble the signer assertion an attested delivery needs. That
// is an auth pause (the deployment resumes with a signature by an
// enrolled signer; a delete still has to withdraw what reached the
// target, so it pauses too), not a pipeline error to retry invisibly.
func TestOrchestration_DeliverMissingSignerEnrollment_PausesAuth(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer", ms, ps),
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer")
	if !f.Paused() {
		t.Fatal("expected the fulfillment to pause when the signer's enrollment is gone")
	}
	if reason := f.PauseReason(); !strings.Contains(reason, "signer enrollment") {
		t.Errorf("PauseReason = %q, want it to name the missing signer enrollment", reason)
	}
	if got := len(getDeliveries(t, store, "deployments/lost-signer")); got != 0 {
		t.Errorf("deliveries = %d, want none dispatched without an attestation", got)
	}
}

// A delivery that is already terminal in this cycle has nothing to
// dispatch, so a missing signer enrollment must not hold it: the
// reconciliation observes convergence and keeps the record as it was.
func TestOrchestration_DeliverMissingSignerEnrollment_DeliveredRecord_Completes(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer-done", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer-done", ms, ps),
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "deployments/lost-signer-done:t1",
		FulfillmentID: domain.FulfillmentID("deployments/lost-signer-done"),
		TargetID:      "t1",
		Manifests:     []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation:    1,
		State:         domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-done")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer-done")
	if f.Paused() {
		t.Fatalf("a delivered record must not pause for a missing signer enrollment: %q", f.PauseReason())
	}
	if f.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", f.State())
	}
	if got := len(getDeliveries(t, store, "deployments/lost-signer-done")); got != 1 {
		t.Errorf("deliveries = %d, want the one delivered record kept", got)
	}
}

// A target that accepts none of the delivery's manifest types receives
// nothing, so the pass needs no assertion and the reconciliation
// completes.
func TestOrchestration_DeliverMissingSignerEnrollment_ZeroAcceptedManifests_Completes(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{ManifestType: "test.resource", Raw: json.RawMessage(`{}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer-none", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer-none", ms, ps),
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Name: "t1", Type: "test", AcceptedManifestTypes: []domain.ManifestType{"other.type"},
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-none")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer-none")
	if f.Paused() {
		t.Fatalf("a target that accepts no manifests must not pause for a missing signer enrollment: %q", f.PauseReason())
	}
	if f.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", f.State())
	}
	if got := len(getDeliveries(t, store, "deployments/lost-signer-none")); got != 0 {
		t.Errorf("deliveries = %d, want none for a target that accepts nothing", got)
	}
}

func TestOrchestration_DeliverWithoutProvenance_NilAttestation(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/no-prov-dep", domain.FulfillmentSnapshot{
		Generation: 1,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
		State: domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("deployments/no-prov-dep"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	delivers := capRec.delivers
	capRec.mu.Unlock()

	if len(delivers) != 1 {
		t.Fatalf("expected 1 deliver input, got %d", len(delivers))
	}
	if delivers[0].Attestation != nil {
		t.Error("Attestation should be nil for token-passthrough deployments (no provenance)")
	}
}

// authFailingNoSignalDelivery signals [domain.DeliveryStateAuthFailed]
// through the events channel. This verifies that auth failures flowing
// through the async path (dispatchAndAwait) correctly pause the
// fulfillment. In the previous architecture this fake returned auth
// failure synchronously without signaling; the async delivery model
// eliminated that dual-path.
type authFailingNoSignalDelivery struct {
	events chan<- domain.FulfillmentEvent
}

func (d authFailingNoSignalDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result: domain.DeliveryResult{
					State:   domain.DeliveryStateAuthFailed,
					Message: "attestation verification failed: target has no trust_bundle property",
				},
			},
		}
	}()
	return nil
}

func (d authFailingNoSignalDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func TestOrchestration_AuthFailureNoSignal_DoesNotHang(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingNoSignalDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}

	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned unexpected error: %v", err)
		}
		dep := getFulfillment(t, store, "deployments/d1")
		if !dep.Paused() {
			t.Error("expected fulfillment to be paused after auth failure")
		}
		if dep.State() != domain.FulfillmentStateCreating {
			t.Errorf("State = %q, want creating (pause must preserve lifecycle state)", dep.State())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("orchestration hung: deliver-to-target returned auth_failed without signaling Done, dispatchAndAwait blocked forever")
	}
}

func TestOrchestration_RemoveWithProvenance_AssemblesRemoveAttestation(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"new1"},
	}
	prov := testProvenance("deployments/rm-attested", ms, ps)

	seedSignerEnrollment(t, store, testSignerEnrollment("se-rm", "test-signer"))
	seedFulfillmentAndDeployment(t, store, "deployments/rm-attested", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"old1"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "old1", Name: "old1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new1", Name: "new1", Type: "test"}),
	)

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("deployments/rm-attested"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	removes := capRec.removes
	capRec.mu.Unlock()

	if len(removes) != 1 {
		t.Fatalf("expected 1 remove input, got %d", len(removes))
	}

	att := removes[0].Attestation
	if att == nil {
		t.Fatal("Attestation is nil; expected remove attestation")
	}
	depContent, ok := att.Input.Provenance.Content.(domain.DeploymentContent)
	if !ok {
		t.Fatalf("Input.Provenance.Content is %T, want DeploymentContent", att.Input.Provenance.Content)
	}
	if depContent.Name != "deployments/rm-attested" {
		t.Errorf("DeploymentContent.Name = %q, want %q", depContent.Name, "deployments/rm-attested")
	}
	rm, ok := att.Output.(*domain.RemoveByDeploymentName)
	if !ok {
		t.Fatalf("Attestation.Output is %T, want *RemoveByDeploymentName", att.Output)
	}
	if rm.Name != "deployments/rm-attested" {
		t.Errorf("RemoveByDeploymentName.Name = %q, want %q", rm.Name, "deployments/rm-attested")
	}
}

func TestOrchestration_DeleteWithProvenance_AssemblesRemoveAttestation(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1", "t2"},
	}
	prov := testProvenance("deployments/del-attested", ms, ps)

	seedSignerEnrollment(t, store, testSignerEnrollment("se-del", "test-signer"))
	seedFulfillmentAndDeployment(t, store, "deployments/del-attested", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/del-attested:t1", FulfillmentID: domain.FulfillmentID("deployments/del-attested"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/del-attested:t2", FulfillmentID: domain.FulfillmentID("deployments/del-attested"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("deployments/del-attested"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	removes := capRec.removes
	capRec.mu.Unlock()

	if len(removes) != 2 {
		t.Fatalf("expected 2 remove inputs (one per target), got %d", len(removes))
	}

	for i, rm := range removes {
		if rm.Attestation == nil {
			t.Errorf("remove[%d]: Attestation is nil; expected attestation with signer assertion", i)
			continue
		}
		att := rm.Attestation
		depContent, ok := att.Input.Provenance.Content.(domain.DeploymentContent)
		if !ok {
			t.Errorf("remove[%d]: Input.Provenance.Content is %T, want DeploymentContent", i, att.Input.Provenance.Content)
			continue
		}
		if depContent.Name != "deployments/del-attested" {
			t.Errorf("remove[%d]: DeploymentContent.Name = %q, want %q", i, depContent.Name, "deployments/del-attested")
		}
		if att.Input.Signer.RegistrySubject != "gh-test-signer" {
			t.Errorf("remove[%d]: RegistrySubject = %q, want %q", i, att.Input.Signer.RegistrySubject, "gh-test-signer")
		}
		rmOut, ok := att.Output.(*domain.RemoveByDeploymentName)
		if !ok {
			t.Errorf("remove[%d]: Output is %T, want *RemoveByDeploymentName", i, att.Output)
			continue
		}
		if rmOut.Name != "deployments/del-attested" {
			t.Errorf("remove[%d]: RemoveByDeploymentName.Name = %q, want %q",
				i, rmOut.Name, "deployments/del-attested")
		}
	}

	// Orchestration cleans up delivery data but leaves the fulfillment
	// row; the DeleteDeploymentCleanupWorkflow deletes both rows after
	// receiving the signal.
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("deployments/del-attested"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist after orchestration cleanup, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
	deliveries, err := tx.Deliveries().ListByFulfillment(context.Background(), domain.FulfillmentID("deployments/del-attested"))
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	if len(deliveries) != 0 {
		t.Errorf("expected delivery records cleaned up, still have %d", len(deliveries))
	}
}

// A deletion with no targets left needs no attestation, so a signer
// whose enrollment is gone must not trap the deployment in Deleting:
// the reconciliation completes and the cleanup workflow can remove the
// rows.
func TestOrchestration_DeleteMissingSignerEnrollment_NoTargets_Completes(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer-del", domain.FulfillmentSnapshot{
		Generation:        2,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer-del", ms, ps),
		State:             domain.FulfillmentStateDeleting,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-del")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer-del")
	if f.Paused() {
		t.Fatalf("deletion with no targets must not pause for a missing signer enrollment: %q", f.PauseReason())
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting", f.State())
	}
}

// A removal from a target still on record needs the signer assertion.
// With the signer's enrollment gone the reconciliation pauses with a
// reason the operator can act on (withdraw the target, or enroll the
// signer again), rather than retrying invisibly.
func TestOrchestration_DeleteMissingSignerEnrollment_TargetsRemain_PausesAuth(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer-rm", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer-rm", ms, ps),
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	// The target was delivered to, so the removal has a record to
	// withdraw and a dispatch to make.
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "deployments/lost-signer-rm:t1",
		FulfillmentID: domain.FulfillmentID("deployments/lost-signer-rm"),
		TargetID:      "t1",
		Manifests:     []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:         domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-rm")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer-rm")
	if !f.Paused() {
		t.Fatal("expected the deletion to pause when a removal needs the missing signer's assertion")
	}
	if reason := f.PauseReason(); !strings.Contains(reason, "signer enrollment") {
		t.Errorf("PauseReason = %q, want it to name the missing signer enrollment", reason)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting (pause must preserve lifecycle state)", f.State())
	}
}

// A target with no delivery record has nothing to remove, so the
// deletion needs no assertion and must proceed. The record can be absent
// when the delivery failed before it was persisted.
func TestOrchestration_DeleteMissingSignerEnrollment_TargetsRemain_NoRecord_Completes(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer-rm-none", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer-rm-none", ms, ps),
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-rm-none")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer-rm-none")
	if f.Paused() {
		t.Fatalf("a removal with no delivery record must not pause for a missing signer enrollment: %q", f.PauseReason())
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting", f.State())
	}
}

// A rollout step that removes a target with no delivery record has
// nothing to dispatch, so the plan finishes and the deployment stays
// active. The new target's delivery is already delivered in this cycle,
// so it does not dispatch either.
func TestOrchestration_RolloutRemoveMissingSignerEnrollment_NoRecord_Completes(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"new1"}}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer-roll-none", domain.FulfillmentSnapshot{
		Generation:        1,
		ResolvedTargets:   []domain.TargetID{"old1"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer-roll-none", ms, ps),
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "old1", Name: "old1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new1", Name: "new1", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "deployments/lost-signer-roll-none:new1",
		FulfillmentID: domain.FulfillmentID("deployments/lost-signer-roll-none"),
		TargetID:      "new1",
		Manifests:     []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation:    1,
		State:         domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-roll-none")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer-roll-none")
	if f.Paused() {
		t.Fatalf("a rollout removal with no delivery record must not pause for a missing signer enrollment: %q", f.PauseReason())
	}
	if f.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", f.State())
	}
}

// A rollout removal with a delivered record would dispatch, so the
// missing signer's assertion pauses the fulfillment with a reason that
// names what is missing.
func TestOrchestration_RolloutRemoveMissingSignerEnrollment_PausesAuth(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"new1"}}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer-roll", domain.FulfillmentSnapshot{
		Generation:        1,
		ResolvedTargets:   []domain.TargetID{"old1"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer-roll", ms, ps),
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "old1", Name: "old1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new1", Name: "new1", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "deployments/lost-signer-roll:old1",
		FulfillmentID: domain.FulfillmentID("deployments/lost-signer-roll"),
		TargetID:      "old1",
		Manifests:     []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:         domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-roll")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer-roll")
	if !f.Paused() {
		t.Fatal("expected the rollout removal to pause when the signer's enrollment is gone")
	}
	if reason := f.PauseReason(); !strings.Contains(reason, "signer enrollment") {
		t.Errorf("PauseReason = %q, want it to name the missing signer enrollment", reason)
	}
	if f.State() != domain.FulfillmentStateCreating {
		t.Errorf("State = %q, want creating (pause must preserve lifecycle state)", f.State())
	}
}

// A pass that pauses has not run its plan: the removal of old1 never
// dispatched. Recording the placement it was converging to (new1)
// would compute the next delta from new1 and never plan the removal
// again, so after a resume the delivered target could only be dropped
// by the delete path, which hard-deletes its record without a removal.
// The paused pass must keep the placement it has not finished leaving.
func TestOrchestration_RolloutRemoveMissingSignerEnrollment_PausedPass_KeepsUnremovedTargets(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"new1"}}
	seedFulfillmentAndDeployment(t, store, "deployments/lost-signer-keep", domain.FulfillmentSnapshot{
		Generation:        1,
		ResolvedTargets:   []domain.TargetID{"old1"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Provenance:        testProvenance("deployments/lost-signer-keep", ms, ps),
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "old1", Name: "old1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new1", Name: "new1", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "deployments/lost-signer-keep:old1",
		FulfillmentID: domain.FulfillmentID("deployments/lost-signer-keep"),
		TargetID:      "old1",
		Manifests:     []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation:    1,
		State:         domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	rd := &recordingRemoveDelivery{events: events}
	wf := newTestWorkflow(store, rd, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-keep")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := getFulfillment(t, store, "deployments/lost-signer-keep")
	if !f.Paused() {
		t.Fatal("expected the rollout removal to pause when the signer's enrollment is gone")
	}
	if got := f.ResolvedTargets(); !slices.Equal(got, []domain.TargetID{"old1"}) {
		t.Fatalf("ResolvedTargets after the paused pass = %v, want [old1]: the removal never dispatched", got)
	}

	// The operator enrolls the signer again and resumes with a fresh
	// signature, as the pause reason asks.
	seedSignerEnrollment(t, store, testSignerEnrollment("se-keep", "test-signer"))
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	loaded, err := tx.Fulfillments().Get(ctx, "deployments/lost-signer-keep")
	if err != nil {
		t.Fatalf("reload fulfillment: %v", err)
	}
	if err := loaded.Resume(loaded.Auth(), loaded.Provenance()); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if err := tx.Fulfillments().Update(ctx, loaded); err != nil {
		t.Fatalf("update fulfillment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit resume: %v", err)
	}

	// The resumed pass plans the interrupted removal again and makes it.
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/lost-signer-keep")); err != nil {
		t.Fatalf("Run after resume: %v", err)
	}

	rd.mu.Lock()
	removed := slices.Clone(rd.removed)
	rd.mu.Unlock()
	if !slices.Contains(removed, domain.TargetID("old1")) {
		t.Errorf("removals dispatched after resume = %v, want old1: the paused pass lost the pending removal", removed)
	}
	f = getFulfillment(t, store, "deployments/lost-signer-keep")
	if f.Paused() {
		t.Fatalf("expected the resumed pass to reconcile, paused with %q", f.PauseReason())
	}
	if got := f.ResolvedTargets(); !slices.Equal(got, []domain.TargetID{"new1"}) {
		t.Errorf("ResolvedTargets after the resumed pass = %v, want [new1]", got)
	}
}

func TestOrchestration_DeleteWithoutProvenance_NilAttestation(t *testing.T) {
	store, _ := setupStore(t)

	seedFulfillmentAndDeployment(t, store, "deployments/del-no-prov", domain.FulfillmentSnapshot{
		Generation:      2,
		ResolvedTargets: []domain.TargetID{"t1"},
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
		State: domain.FulfillmentStateDeleting,
	})

	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/del-no-prov:t1", FulfillmentID: domain.FulfillmentID("deployments/del-no-prov"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("deployments/del-no-prov"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	removes := capRec.removes
	capRec.mu.Unlock()

	if len(removes) != 1 {
		t.Fatalf("expected 1 remove input, got %d", len(removes))
	}
	if removes[0].Attestation != nil {
		t.Error("Attestation should be nil for deployments without provenance")
	}
}

// ---------------------------------------------------------------------------
// Fault-injection store wrapper
// ---------------------------------------------------------------------------

// commitFaultStore wraps a [domain.Store] and injects a single transient
// commit failure on the first write-transaction commit after [Arm] is
// called. Once the fault fires it is permanently disarmed.
type commitFaultStore struct {
	domain.Store
	mu      sync.Mutex
	armed   bool
	tripped bool
	err     error
}

// Arm enables the fault. The very next write-tx Commit will fail.
func (s *commitFaultStore) Arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.tripped {
		s.armed = true
	}
}

func (s *commitFaultStore) Begin(ctx context.Context) (domain.Tx, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &commitFaultTx{Tx: tx, store: s}, nil
}

type commitFaultTx struct {
	domain.Tx
	store *commitFaultStore
}

func (tx *commitFaultTx) Commit() error {
	tx.store.mu.Lock()
	shouldFault := tx.store.armed && !tx.store.tripped
	if shouldFault {
		tx.store.armed = false
		tx.store.tripped = true
	}
	tx.store.mu.Unlock()

	if shouldFault {
		tx.RollbackOnExit()
		return tx.store.err
	}
	return tx.Tx.Commit()
}

// ---------------------------------------------------------------------------
// Output durability across workflow restarts
// ---------------------------------------------------------------------------

func TestOrchestration_DeliveryOutputs_SurviveTransientReconciliationFailure(t *testing.T) {
	realStore, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, realStore, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"name":"new-cluster"}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"provisioner"}},
		State:             domain.FulfillmentStateCreating,
	})

	seedTargets(t, realStore, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "provisioner", Name: "provisioner", Type: "test"}))

	// The commit fault is armed right after the delivery report commits, so
	// the very next write transaction (reconciliation completion) fails
	// transiently and the workflow restarts. The outputs must survive the
	// restart because they committed with the report.
	store := &commitFaultStore{
		Store: realStore,
		err:   fmt.Errorf("transient DB error"),
	}
	deliveryAgent := &outputProducingDelivery{
		onReported: store.Arm,
		targets: []domain.ProvisionedTarget{{
			ID: "k8s-new-cluster", Type: "kubernetes", Name: "new-cluster",
			Properties: map[string]string{"kubeconfig_ref": "targets/k8s-new-cluster/kubeconfig"},
		}},
		secrets: []domain.ProducedSecret{{
			Ref: "targets/k8s-new-cluster/kubeconfig", Value: []byte("fake-kubeconfig-data"),
		}},
	}

	// Simulate the engine's ContinueAsNew loop: re-run the workflow on
	// each restart, up to a bounded number of attempts. A healthy
	// pipeline should converge within two attempts (one fault + one
	// successful retry).
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		events := make(chan domain.FulfillmentEvent, 16)
		deliveryAgent.reporter = application.NewDeliveryReportService(store, &stubRegistry{events: events},
			application.WithDeliveryReportVault(vault))
		wf := newTestWorkflow(store, deliveryAgent, events, func(wf *domain.OrchestrationWorkflowSpec) {
			wf.Vault = vault
		})
		rec := &simpleRecord{ctx: testContext(t), events: events}
		_, lastErr = wf.Run(rec, domain.FulfillmentID("deployments/d1"))
		if lastErr == nil {
			break
		}
		var canErr *domain.ContinueAsNewError
		if !errors.As(lastErr, &canErr) {
			t.Fatalf("attempt %d: unexpected non-ContinueAsNew error: %v", attempt+1, lastErr)
		}
	}
	if lastErr != nil {
		t.Fatalf("workflow did not converge after %d attempts "+
			"(stuck in ContinueAsNew loop from replayed output registration): %v",
			maxAttempts, lastErr)
	}

	dep := getFulfillment(t, realStore, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active after replay recovery", dep.State())
	}

	// The outputs committed with the report, so they are present after the
	// restart even though the retry pass skips the terminal delivery.
	item := getInventoryItem(t, realStore, "target:k8s-new-cluster")
	if item.SourceDeliveryID() == nil || *item.SourceDeliveryID() != "deployments/d1:provisioner" {
		t.Fatalf("inventory SourceDeliveryID = %v, want deployments/d1:provisioner", item.SourceDeliveryID())
	}
	if vault != nil {
		secret, err := vault.Get(context.Background(), "targets/k8s-new-cluster/kubeconfig")
		if err != nil {
			t.Fatalf("vault get: %v", err)
		}
		if string(secret) != "fake-kubeconfig-data" {
			t.Errorf("secret = %q, want fake-kubeconfig-data", secret)
		}
	}
}

// ---------------------------------------------------------------------------
// ResolvedTargets across failure/auth-pause
// ---------------------------------------------------------------------------

// A pass that pauses has not finished its plan, so it does not record the
// placement it was converging to: that set would claim deliveries and
// removals that never dispatched. The attempt itself is still on the
// books, and the delivery record is what a delete acts on.
func TestOrchestration_AuthFailure_PausedPass_LeavesPlacementUnrecorded(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if !dep.Paused() {
		t.Fatalf("expected fulfillment to be paused after auth failure")
	}
	if dep.State() != domain.FulfillmentStateCreating {
		t.Fatalf("State = %q, want creating (pause must preserve lifecycle state)", dep.State())
	}
	if len(dep.ResolvedTargets()) != 0 {
		t.Errorf("ResolvedTargets = %v, want empty: the pass paused before finishing its plan", dep.ResolvedTargets())
	}

	// The auth-failed attempt is still recorded, and that record is what
	// tells a delete the target may hold platform work.
	deliveries := getDeliveries(t, store, "deployments/d1")
	if len(deliveries) != 1 {
		t.Fatalf("delivery records = %d, want 1 for the dispatched attempt", len(deliveries))
	}
	if deliveries[0].TargetID() != "t1" {
		t.Fatalf("delivery target = %q, want t1", deliveries[0].TargetID())
	}
	if !deliveries[0].NeedsWithdrawal() {
		t.Errorf("auth-failed delivery must need withdrawal; state=%q operation=%q",
			deliveries[0].State(), deliveries[0].Operation())
	}
}

func TestOrchestration_DeleteAfterAuthPause_CallsRemove(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	// Phase 1: delivery auth-fails → PausedAuth. The paused pass records
	// no placement; the auth-failed delivery record still names t1.
	ctx := testContext(t)
	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingDelivery{events: events}, events)
	rec := &simpleRecord{ctx: ctx, events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run (create): %v", err)
	}

	// Phase 2: transition to deleting (simulates user calling Delete).
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, err := tx.Fulfillments().Get(ctx, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		tx.RollbackOnExit()
		t.Fatal(err)
	}
	f.TransitionToDeleting(f.Auth())
	if err := tx.Fulfillments().Update(ctx, f); err != nil {
		tx.RollbackOnExit()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Phase 3: run orchestration again — should call Remove on t1, which
	// the delete learns about from the auth-failed delivery record.
	events2 := make(chan domain.FulfillmentEvent, 16)
	removeAgent := &recordingRemoveDelivery{events: events2}
	wf2 := newTestWorkflow(store, removeAgent, events2)
	rec2 := &simpleRecord{ctx: ctx, events: events2}
	recorder := &recordingRecord{ctx: rec2.ctx, delegate: rec2}
	if _, err := wf2.Run(recorder, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run (delete): %v", err)
	}

	removeAgent.mu.Lock()
	removed := removeAgent.removed
	removeAgent.mu.Unlock()

	if len(removed) != 1 || removed[0] != "t1" {
		t.Errorf("Remove called for targets %v, want [t1]; delete must clean up targets the paused attempt reached", removed)
	}
}

// recordingRemoveDelivery tracks which targets had Remove called.
type recordingRemoveDelivery struct {
	events  chan<- domain.FulfillmentEvent
	mu      sync.Mutex
	removed []domain.TargetID
}

func (d *recordingRemoveDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (d *recordingRemoveDelivery) Remove(_ context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	d.mu.Lock()
	d.removed = append(d.removed, target.ID())
	d.mu.Unlock()
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func TestOrchestration_DeletePipeline_AuthExpired_SetsPausedAuth(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	authErr := fmt.Errorf("%w: STS token exchange failed: invalid_grant", domain.ErrAuthExpired)
	wf := newTestWorkflow(store, &failingRemoveDelivery{events: events, err: authErr}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if !dep.Paused() {
		t.Errorf("expected fulfillment to be paused after auth failure")
	}
	if dep.State() != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting (pause must preserve lifecycle state)", dep.State())
	}
	if len(dep.ResolvedTargets()) != 1 || dep.ResolvedTargets()[0] != "t1" {
		t.Errorf("ResolvedTargets = %v, want [t1]; targets must be preserved across auth pause", dep.ResolvedTargets())
	}
}

// TestOrchestration_DeletePipeline_AuthFailedRemoval_PausesWithoutRedispatch
// verifies that a removal which already reported auth_failed at the
// pipeline's generation re-pauses the fulfillment for fresh
// credentials instead of waiting on or retrying the expired ones. The
// pause reason must never be empty, even when the report carried no
// message.
func TestOrchestration_DeletePipeline_AuthFailedRemoval_PausesWithoutRedispatch(t *testing.T) {
	cases := []struct {
		name       string
		message    string
		wantReason string
	}{
		{name: "carries the report message", message: "token expired", wantReason: "token expired"},
		{name: "falls back on an empty message", message: "", wantReason: "authentication failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := setupStore(t)
			seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
				Generation:        2,
				ResolvedTargets:   []domain.TargetID{"t1"},
				ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
				PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
				State:             domain.FulfillmentStateDeleting,
			})
			seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
			seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
				Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
				Generation: 2,
				State:      domain.DeliveryStateAuthFailed,
				Operation:  domain.DeliveryOperationRemove,
				Message:    tc.message,
				Attempts:   1,
			}))

			events := make(chan domain.FulfillmentEvent, 16)
			agent := &recordingRemoveDelivery{events: events}
			wf := newTestWorkflow(store, agent, events)

			rec := &simpleRecord{ctx: testContext(t), events: events}
			if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
				t.Fatalf("Run: %v", err)
			}

			agent.mu.Lock()
			removed := append([]domain.TargetID(nil), agent.removed...)
			agent.mu.Unlock()
			if len(removed) != 0 {
				t.Errorf("Remove called for %v, want none: auth_failed pauses rather than retrying expired credentials", removed)
			}
			dep := getFulfillment(t, store, "deployments/d1")
			if !dep.Paused() {
				t.Fatal("fulfillment was not paused after an auth_failed removal")
			}
			if !strings.Contains(dep.PauseReason(), tc.wantReason) {
				t.Errorf("PauseReason = %q, want it to carry %q", dep.PauseReason(), tc.wantReason)
			}
			if dep.State() != domain.FulfillmentStateDeleting {
				t.Errorf("State = %q, want deleting (a pause does not change lifecycle state)", dep.State())
			}
			if d := getDelivery(t, store, "deployments/d1:t1"); d.State() != domain.DeliveryStateAuthFailed {
				t.Errorf("delivery State = %q, want %q (the terminal observation is kept)", d.State(), domain.DeliveryStateAuthFailed)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Delete-pause-resume: state preserved, resume takes delete path
// ---------------------------------------------------------------------------

// TestOrchestration_DeletePausedResume_StaysDeleting verifies that a
// fulfillment paused during the delete pipeline correctly re-enters
// the delete path (not the create/update path) after resume.
func TestOrchestration_DeletePausedResume_StaysDeleting(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	ctx := testContext(t)

	// Phase 1: orchestration attempts delete, auth fails → paused.
	events1 := make(chan domain.FulfillmentEvent, 16)
	authErr := fmt.Errorf("%w: STS token exchange failed: invalid_grant", domain.ErrAuthExpired)
	wf1 := newTestWorkflow(store, &failingRemoveDelivery{events: events1, err: authErr}, events1)
	rec1 := &simpleRecord{ctx: ctx, events: events1}
	if _, err := wf1.Run(rec1, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run (delete attempt): %v", err)
	}

	f := getFulfillment(t, store, "deployments/d1")
	if !f.Paused() {
		t.Fatalf("expected fulfillment to be paused after auth failure")
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("State = %q, want %q; pause must not overwrite lifecycle state",
			f.State(), domain.FulfillmentStateDeleting)
	}

	// Phase 2: resume with fresh auth.
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fPtr, err := tx.Fulfillments().Get(ctx, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		tx.RollbackOnExit()
		t.Fatal(err)
	}
	if err := fPtr.Resume(domain.DeliveryAuth{Token: "fresh-token"}, nil); err != nil {
		tx.RollbackOnExit()
		t.Fatalf("Resume: %v", err)
	}
	if err := tx.Fulfillments().Update(ctx, fPtr); err != nil {
		tx.RollbackOnExit()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Phase 3: re-run orchestration. It must take the delete path (Remove),
	// not the default create/update path (Deliver).
	events2 := make(chan domain.FulfillmentEvent, 16)
	removeAgent := &recordingRemoveDelivery{events: events2}
	wf2 := newTestWorkflow(store, removeAgent, events2)
	rec2 := &simpleRecord{ctx: ctx, events: events2}
	if _, err := wf2.Run(rec2, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run (after resume): %v", err)
	}

	removeAgent.mu.Lock()
	removed := removeAgent.removed
	removeAgent.mu.Unlock()

	if len(removed) != 1 || removed[0] != "t1" {
		t.Errorf("Remove called for targets %v, want [t1]; resumed delete must take the delete path", removed)
	}

	f = getFulfillment(t, store, "deployments/d1")
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("final State = %q, want deleting", f.State())
	}
	if f.Paused() {
		t.Errorf("fulfillment should not be paused after successful reconciliation")
	}
}

// TestOrchestration_DeleteDuringPass_IsCarriedOut verifies that a
// delete committed while a reconciliation pass is running is not
// undone when that pass persists its result: the pass that follows
// takes the delete path, with the credentials the delete brought.
func TestOrchestration_DeleteDuringPass_IsCarriedOut(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
		Auth:              domain.DeliveryAuth{Token: "old-token"},
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	agent := &recordingRemoveDelivery{events: events}
	wf := newTestWorkflow(store, agent, events)

	// The delete is committed right after the pass has loaded the
	// fulfillment. The pass delivers to t1 for a fulfillment that is
	// not being deleted, and reports that one generation late.
	rec := &deleteAfterLoadRecord{
		Record: &simpleRecord{ctx: testContext(t), events: events},
		store:  store,
		id:     "deployments/d1",
		auth:   domain.DeliveryAuth{Token: "fresh-token"},
	}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	agent.mu.Lock()
	removed := slices.Clone(agent.removed)
	agent.mu.Unlock()
	if len(removed) != 1 || removed[0] != "t1" {
		t.Errorf("Remove called for targets %v, want [t1]; the pass after the delete must take the delete path", removed)
	}

	f := getFulfillment(t, store, "deployments/d1")
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("final State = %q, want deleting; the pass the delete overtook must not undo it", f.State())
	}
	if f.Auth().Token != "fresh-token" {
		t.Errorf("Auth.Token = %q, want the delete's fresh-token", f.Auth().Token)
	}
	if f.ObservedGeneration() != 2 {
		t.Errorf("ObservedGeneration = %d, want 2 (the delete's generation)", f.ObservedGeneration())
	}
}

// deleteAfterLoadRecord wraps a record and commits a delete right
// after the first acquire-lock-and-load activity: the mutation of a
// delete request, arriving while the pass that has just loaded the
// fulfillment is still running.
type deleteAfterLoadRecord struct {
	domain.Record
	store   domain.Store
	id      domain.FulfillmentID
	auth    domain.DeliveryAuth
	deleted bool
}

func (r *deleteAfterLoadRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	out, err := r.Record.Run(activity, in)
	if err != nil || r.deleted || activity.Name() != "acquire-lock-and-load" {
		return out, err
	}
	r.deleted = true

	ctx := context.Background()
	tx, err := r.store.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.RollbackOnExit()
	f, err := tx.Fulfillments().Get(ctx, r.id)
	if err != nil {
		return out, err
	}
	f.TransitionToDeleting(r.auth)
	if err := tx.Fulfillments().Update(ctx, f); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

// ---------------------------------------------------------------------------
// Async delivery failure → reset-to-Pending → retry
// ---------------------------------------------------------------------------

func TestOrchestration_DeliverTerminalDelivery_ResetsAndRedispatches(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateFailed,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active (terminal delivery should be reset and re-dispatched)", dep.State())
	}
}

// TestOrchestration_DeliverProgressingDelivery_WaitsForCompletion seeds a
// delivery in Progressing state at the same generation (acked but not
// completed — as happens after a crash) and verifies that DeliverToTarget
// does NOT re-dispatch. Instead dispatchAndAwait waits for the completion
// signal from RecoverActiveDeliveries.
func TestOrchestration_DeliverProgressingDelivery_WaitsForCompletion(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateProgressing,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var deliverCalled bool
	delivery := &deliveryStateChecker{
		store:  store,
		events: events,
		onRemove: func(_ domain.DeliveryID) {
			// Remove should not be called for a create flow
			t.Fatal("unexpected Remove call")
		},
	}
	// Wrap Deliver to detect re-dispatch
	agent := &deliverCallTracker{inner: delivery, called: &deliverCalled}

	wf := newTestWorkflow(store, agent, events)

	// Simulate RecoverActiveDeliveries sending the completion signal. The
	// signal waits on the channel before Run, so a run that skips the
	// delivery instead of waiting for it leaves the signal unconsumed and
	// the assertion after Run fails.
	events <- domain.FulfillmentEvent{
		DeliveryCompleted: &domain.DeliveryCompletionEvent{
			DeliveryID: "deployments/d1:t1",
			Generation: 1,
			Operation:  domain.DeliveryOperationDeliver,
			Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered, Operation: domain.DeliveryOperationDeliver},
		},
	}

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if deliverCalled {
		t.Error("Deliver was called — Progressing delivery should wait for completion signal, not re-dispatch")
	}

	select {
	case ev := <-events:
		t.Fatalf("completion signal not consumed, so the delivery was skipped rather than awaited: %+v", ev)
	default:
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}

	// Verify delivery was NOT reset to Pending — it should still be
	// Progressing because orchestration waited for the signal instead
	// of mutating the delivery state.
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	d, err := tx.Deliveries().Get(context.Background(), "deployments/d1:t1")
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if d.State() != domain.DeliveryStateProgressing {
		t.Errorf("delivery state = %q, want %q (should not be reset to Pending)", d.State(), domain.DeliveryStateProgressing)
	}
}

// staleDeliveryObserver records deliver activities skipped because the
// delivery record belongs to a newer generation than the cycle that
// reached it.
type staleDeliveryObserver struct {
	domain.NoOpFulfillmentObserver
	mu    sync.Mutex
	stale []domain.Generation
}

func (o *staleDeliveryObserver) DeliverStarted(ctx context.Context, _ domain.DeliverInput) (context.Context, domain.DeliverProbe) {
	return ctx, &staleDeliveryProbe{observer: o}
}

func (o *staleDeliveryObserver) skippedStale(gen domain.Generation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stale = append(o.stale, gen)
}

type staleDeliveryProbe struct {
	domain.NoOpDeliverProbe
	observer *staleDeliveryObserver
}

func (p *staleDeliveryProbe) SkippedStale(gen domain.Generation) {
	p.observer.skippedStale(gen)
}

// TestOrchestration_DeliverStaleGeneration_IsSkipped seeds a delivery
// record whose generation is ahead of the fulfillment dispatching it, so
// the record belongs to a newer cycle. The target must be skipped — not
// dispatched, and not awaited at the older generation — and left for the
// next reconciliation.
func TestOrchestration_DeliverStaleGeneration_IsSkipped(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 2,
		Operation:  domain.DeliveryOperationDeliver,
		State:      domain.DeliveryStatePending,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var deliverCalled bool
	observer := &staleDeliveryObserver{}
	wf := newTestWorkflow(store, &deliverCallTracker{inner: noopDelivery{events: events}, called: &deliverCalled}, events,
		domain.WithFulfillmentObserver(observer))

	rec := &simpleRecord{ctx: testContext(t), events: events}
	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run waited for a completion signal at the stale generation")
	}

	if deliverCalled {
		t.Error("Deliver was called for a delivery record at a newer generation")
	}
	observer.mu.Lock()
	stale := append([]domain.Generation(nil), observer.stale...)
	observer.mu.Unlock()
	if len(stale) != 1 || stale[0] != 2 {
		t.Errorf("SkippedStale calls = %v, want [2]", stale)
	}

	d := getDelivery(t, store, "deployments/d1:t1")
	if d.Generation() != 2 || d.State() != domain.DeliveryStatePending {
		t.Errorf("delivery = generation %d state %q, want generation 2 pending (left for the next reconciliation)", d.Generation(), d.State())
	}
}

// TestOrchestration_DispatchAndAwait_DiscardsEventsFromOtherOperation
// verifies the cycle fence on signals: a target's put and removal share
// a delivery ID, so an ack or completion of the other operation must be
// discarded rather than acknowledged or applied to this cycle.
func TestOrchestration_DispatchAndAwait_DiscardsEventsFromOtherOperation(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateProgressing,
		Operation:  domain.DeliveryOperationDeliver,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	// Signals of the removal operation at the same generation: the
	// delivery record is in a put cycle, so both must be discarded.
	events <- domain.FulfillmentEvent{DeliveryAcked: &domain.DeliveryAckedEvent{
		DeliveryID: "deployments/d1:t1", Generation: 1, Operation: domain.DeliveryOperationRemove,
	}}
	events <- domain.FulfillmentEvent{DeliveryCompleted: &domain.DeliveryCompletionEvent{
		DeliveryID: "deployments/d1:t1", Generation: 1, Operation: domain.DeliveryOperationRemove,
		Result: domain.DeliveryResult{State: domain.DeliveryStateDelivered, Operation: domain.DeliveryOperationRemove},
	}}
	events <- domain.FulfillmentEvent{DeliveryCompleted: &domain.DeliveryCompletionEvent{
		DeliveryID: "deployments/d1:t1", Generation: 1, Operation: domain.DeliveryOperationDeliver,
		Result: domain.DeliveryResult{State: domain.DeliveryStateDelivered, Operation: domain.DeliveryOperationDeliver},
	}}

	obs := &recordingObserver{}
	wf := newTestWorkflow(store, noopDelivery{events: events}, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Observer = obs
	})

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	obs.mu.Lock()
	staleCount := len(obs.stale)
	completed := append([]domain.DeliveryState(nil), obs.completed...)
	obs.mu.Unlock()
	if staleCount != 2 {
		t.Errorf("stale events discarded = %d, want 2 (the ack and completion of the removal operation)", staleCount)
	}
	if len(completed) != 1 || completed[0] != domain.DeliveryStateDelivered {
		t.Errorf("completed states = %v, want [delivered]", completed)
	}
	if dep := getFulfillment(t, store, "deployments/d1"); dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
}

// silentDelivery accepts dispatches without signaling; tests using it
// supply the ack and completion events through the events channel.
type silentDelivery struct{}

func (silentDelivery) Deliver(context.Context, domain.TargetInfo, domain.DeliveryID, []domain.Manifest, domain.DeliveryAuth, *domain.Attestation, domain.Generation) error {
	return nil
}

func (silentDelivery) Remove(context.Context, domain.TargetInfo, domain.DeliveryID, []domain.Manifest, domain.DeliveryAuth, *domain.Attestation, domain.Generation) error {
	return nil
}

// TestOrchestration_DispatchAndAwait_AcceptsMatchingAck verifies the
// positive side of the cycle fence: an ack whose generation and
// operation match the dispatch cycle is accepted — it acknowledges the
// target without being counted stale and without holding the cycle to
// the ack retry timeout.
func TestOrchestration_DispatchAndAwait_AcceptsMatchingAck(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStatePending,
		Operation:  domain.DeliveryOperationDeliver,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	events <- domain.FulfillmentEvent{DeliveryAcked: &domain.DeliveryAckedEvent{
		DeliveryID: "deployments/d1:t1", Generation: 1, Operation: domain.DeliveryOperationDeliver,
	}}
	events <- domain.FulfillmentEvent{DeliveryCompleted: &domain.DeliveryCompletionEvent{
		DeliveryID: "deployments/d1:t1", Generation: 1, Operation: domain.DeliveryOperationDeliver,
		Result: domain.DeliveryResult{State: domain.DeliveryStateDelivered, Operation: domain.DeliveryOperationDeliver},
	}}

	obs := &recordingObserver{}
	wf := newTestWorkflow(store, silentDelivery{}, events, domain.WithFulfillmentObserver(obs))

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	obs.mu.Lock()
	acks := append([]domain.DeliveryID(nil), obs.acks...)
	stale := len(obs.stale)
	completed := append([]domain.DeliveryState(nil), obs.completed...)
	obs.mu.Unlock()
	if len(acks) != 1 || acks[0] != "deployments/d1:t1" {
		t.Errorf("accepted acks = %v, want [deployments/d1:t1]", acks)
	}
	if stale != 0 {
		t.Errorf("stale events discarded = %d, want 0: the matching ack and completion belong to this cycle", stale)
	}
	if len(completed) != 1 || completed[0] != domain.DeliveryStateDelivered {
		t.Errorf("completed states = %v, want [delivered]", completed)
	}
	if dep := getFulfillment(t, store, "deployments/d1"); dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
}

// deliverCallTracker wraps a DeliveryAgent and tracks whether Deliver was called.
type deliverCallTracker struct {
	inner  domain.DeliveryAgent
	called *bool
}

func (d *deliverCallTracker) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, attestation *domain.Attestation, generation domain.Generation) error {
	*d.called = true
	return d.inner.Deliver(ctx, target, deliveryID, manifests, auth, attestation, generation)
}

func (d *deliverCallTracker) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, attestation *domain.Attestation, generation domain.Generation) error {
	return d.inner.Remove(ctx, target, deliveryID, manifests, auth, attestation, generation)
}

// TestOrchestration_DeliverPartialDelivery_IsNotRedispatched verifies the
// contract for a terminal partial delivery: its reason is reported per
// target, but the delivery is not retried until a new generation is
// dispatched.
func TestOrchestration_DeliverPartialDelivery_IsNotRedispatched(t *testing.T) {
	const message = "apply object 2: conflict with \"work-agent\""
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStatePartial,
		Operation:  domain.DeliveryOperationDeliver,
		Message:    message,
		Attempts:   1,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var deliverCalled bool
	agent := &deliverCallTracker{inner: noopDelivery{events: events}, called: &deliverCalled}
	wf := newTestWorkflow(store, agent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if deliverCalled {
		t.Error("Deliver was called — a partial delivery is terminal and must not be retried at the same generation")
	}
	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
	d := getDelivery(t, store, "deployments/d1:t1")
	if d.State() != domain.DeliveryStatePartial {
		t.Errorf("delivery State = %q, want %q", d.State(), domain.DeliveryStatePartial)
	}
	if d.Message() != message || d.Attempts() != 1 {
		t.Errorf("delivery details = (%q, %d), want (%q, 1): the one partial observation must not be repeated",
			d.Message(), d.Attempts(), message)
	}
}

// TestOrchestration_DeliverAuthFailedDelivery_PausesWithoutRedispatch
// verifies that an auth_failed delivery re-pauses the fulfillment instead
// of being retried with the same expired credentials.
func TestOrchestration_DeliverAuthFailedDelivery_PausesWithoutRedispatch(t *testing.T) {
	const message = "token expired"
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateAuthFailed,
		Operation:  domain.DeliveryOperationDeliver,
		Message:    message,
		Attempts:   1,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var deliverCalled bool
	agent := &deliverCallTracker{inner: noopDelivery{events: events}, called: &deliverCalled}
	wf := newTestWorkflow(store, agent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if deliverCalled {
		t.Error("Deliver was called — an auth_failed delivery pauses for fresh credentials instead of retrying")
	}
	dep := getFulfillment(t, store, "deployments/d1")
	if !dep.Paused() {
		t.Fatal("fulfillment was not paused after an auth_failed delivery")
	}
	if !strings.Contains(dep.PauseReason(), message) {
		t.Errorf("PauseReason = %q, want it to carry %q", dep.PauseReason(), message)
	}
	if dep.State() != domain.FulfillmentStateCreating {
		t.Errorf("State = %q, want creating (a pause does not change lifecycle state)", dep.State())
	}
}

// TestOrchestration_DeliverAuthFailedDelivery_EmptyMessage_StillPauses
// verifies the pause-reason fallback for a put cycle: a report without
// a message must still pause the fulfillment, otherwise the delivery
// would look permanently outstanding.
func TestOrchestration_DeliverAuthFailedDelivery_EmptyMessage_StillPauses(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateAuthFailed,
		Operation:  domain.DeliveryOperationDeliver,
		Attempts:   1,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var deliverCalled bool
	agent := &deliverCallTracker{inner: noopDelivery{events: events}, called: &deliverCalled}
	wf := newTestWorkflow(store, agent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if deliverCalled {
		t.Error("Deliver was called — an auth_failed delivery pauses for fresh credentials instead of retrying")
	}
	dep := getFulfillment(t, store, "deployments/d1")
	if !dep.Paused() {
		t.Fatal("fulfillment was not paused after an auth_failed delivery without a message")
	}
	if !strings.Contains(dep.PauseReason(), "authentication failed") {
		t.Errorf("PauseReason = %q, want the %q fallback", dep.PauseReason(), "authentication failed")
	}
	if dep.State() != domain.FulfillmentStateCreating {
		t.Errorf("State = %q, want creating (a pause does not change lifecycle state)", dep.State())
	}
}

// TestOrchestration_DeliverDeliveredAtSameGeneration_IsNotRedispatched
// verifies that a delivery already delivered in this cycle is not
// re-dispatched when the workflow restarts at the same generation.
func TestOrchestration_DeliverDeliveredAtSameGeneration_IsNotRedispatched(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateDelivered,
		Operation:  domain.DeliveryOperationDeliver,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var deliverCalled bool
	agent := &deliverCallTracker{inner: noopDelivery{events: events}, called: &deliverCalled}
	wf := newTestWorkflow(store, agent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if deliverCalled {
		t.Error("Deliver was called — a delivered target at the same generation must not be re-dispatched")
	}
	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
	if d := getDelivery(t, store, "deployments/d1:t1"); d.State() != domain.DeliveryStateDelivered {
		t.Errorf("delivery State = %q, want %q", d.State(), domain.DeliveryStateDelivered)
	}
}

// TestOrchestration_DeliverAfterSameGenerationRemoval_RecordsPutCycle
// covers a target that re-enters placement at the generation its removal
// completed at: the dispatch must flip the record to a put cycle instead
// of reusing the removal operation.
func TestOrchestration_DeliverAfterSameGenerationRemoval_RecordsPutCycle(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateDelivered,
		Operation:  domain.DeliveryOperationRemove,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var dispatchOperation domain.DeliveryOperation
	var dispatchState domain.DeliveryState
	agent := &deliveryStateChecker{
		store:  store,
		events: events,
		onDeliver: func(deliveryID domain.DeliveryID) {
			d := getDelivery(t, store, deliveryID)
			dispatchOperation = d.Operation()
			dispatchState = d.State()
		},
	}
	wf := newTestWorkflow(store, agent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if dispatchOperation != domain.DeliveryOperationDeliver {
		t.Errorf("operation during dispatch = %q, want %q: the target re-entered placement",
			dispatchOperation, domain.DeliveryOperationDeliver)
	}
	if dispatchState != domain.DeliveryStatePending {
		t.Errorf("state during dispatch = %q, want %q", dispatchState, domain.DeliveryStatePending)
	}
}

// TestOrchestration_RemovePendingDeliverRecord_RecordsRemovalCycle covers
// a removal dispatched while the put cycle is still Pending (e.g. the
// workflow crashed before the addon acked): the record must switch to a
// removal cycle before the dispatch, so the API reports the removal.
func TestOrchestration_RemovePendingDeliverRecord_RecordsRemovalCycle(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: domain.FulfillmentID("deployments/d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 2,
		State:      domain.DeliveryStatePending,
		Operation:  domain.DeliveryOperationDeliver,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var dispatchOperation domain.DeliveryOperation
	var dispatchState domain.DeliveryState
	agent := &deliveryStateChecker{
		store:  store,
		events: events,
		onRemove: func(deliveryID domain.DeliveryID) {
			d := getDelivery(t, store, deliveryID)
			dispatchOperation = d.Operation()
			dispatchState = d.State()
		},
	}
	wf := newTestWorkflow(store, agent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("deployments/d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if dispatchOperation != domain.DeliveryOperationRemove {
		t.Errorf("operation during removal dispatch = %q, want %q",
			dispatchOperation, domain.DeliveryOperationRemove)
	}
	if dispatchState != domain.DeliveryStatePending {
		t.Errorf("state during removal dispatch = %q, want %q", dispatchState, domain.DeliveryStatePending)
	}
}

func TestOrchestration_AsyncDeliveryFailure_ConvergesViaRetry(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	agent := &asyncFailOnceThenSucceedAgent{store: store}

	const maxAttempts = 3
	var lastErr error
	for attempt := range maxAttempts {
		events := make(chan domain.FulfillmentEvent, 16)
		agent.events = events
		wf := newTestWorkflow(store, agent, events)
		rec := &simpleRecord{ctx: testContext(t), events: events}
		_, lastErr = wf.Run(rec, domain.FulfillmentID("deployments/d1"))
		if lastErr == nil {
			break
		}
		var canErr *domain.ContinueAsNewError
		if !errors.As(lastErr, &canErr) {
			t.Fatalf("attempt %d: unexpected non-ContinueAsNew error: %v", attempt+1, lastErr)
		}
	}
	if lastErr != nil {
		t.Fatalf("workflow did not converge after %d attempts: %v", maxAttempts, lastErr)
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active (async failure should be retried via reset-to-Pending)", dep.State())
	}
}

// asyncFailOnceThenSucceedAgent simulates a delivery agent that fails
// on the first attempt (transitioning the delivery to Failed in the DB
// and signaling the workflow, like production's [DeliveryReportService])
// then succeeds on subsequent attempts. This tests the full ContinueAsNew
// recovery path: first run fails → ContinueAsNew → second run resets
// the terminal delivery to Pending → re-dispatches → addon succeeds.
type asyncFailOnceThenSucceedAgent struct {
	store  domain.Store
	events chan<- domain.FulfillmentEvent
	mu     sync.Mutex
	calls  int
}

func (a *asyncFailOnceThenSucceedAgent) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	a.mu.Lock()
	a.calls++
	n := a.calls
	a.mu.Unlock()

	if n == 1 {
		ctx := context.Background()
		tx, err := a.store.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.RollbackOnExit()
		d, err := tx.Deliveries().Get(ctx, deliveryID)
		if err != nil {
			return err
		}
		if err := d.TransitionTo(domain.DeliveryStateFailed, time.Now()); err != nil {
			return err
		}
		if err := tx.Deliveries().Put(ctx, d); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		go func() {
			a.events <- domain.FulfillmentEvent{
				DeliveryCompleted: &domain.DeliveryCompletionEvent{
					DeliveryID: deliveryID,
					Generation: generation,
					Operation:  domain.DeliveryOperationDeliver,
					Result: domain.DeliveryResult{
						State:   domain.DeliveryStateFailed,
						Message: "async delivery failed",
					},
				},
			}
		}()
		return nil
	}

	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (a *asyncFailOnceThenSucceedAgent) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationRemove,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// ---------------------------------------------------------------------------
// Deleted fulfillment → workflow stops cleanly
// ---------------------------------------------------------------------------

func TestOrchestration_DeletedFulfillment_StopsCleanly(t *testing.T) {
	store, _ := setupStore(t)
	// No fulfillment seeded — simulates a hard-deleted record.
	// The workflow should terminate cleanly, not loop via ContinueAsNew.

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("nonexistent"))
	if err != nil {
		t.Fatalf("Run should return nil for deleted fulfillment, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Late target registration
// ---------------------------------------------------------------------------

// retryDelayObserver signals when orchestration begins a durable retry
// delay, then holds the workflow until release is closed. Tests use
// this to land side effects (e.g. late target registration) inside
// the RetryDelay / ContinueAsNew window.
type retryDelayObserver struct {
	domain.NoOpFulfillmentObserver
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func newRetryDelayObserver() *retryDelayObserver {
	return &retryDelayObserver{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (o *retryDelayObserver) RunStarted(ctx context.Context, _ domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	return ctx, &retryDelayProbe{observer: o, ctx: ctx}
}

type retryDelayProbe struct {
	domain.NoOpFulfillmentRunProbe
	observer *retryDelayObserver
	ctx      context.Context
}

func (p *retryDelayProbe) RetryDelayStarted(time.Duration) {
	p.observer.once.Do(func() { close(p.observer.started) })
	select {
	case <-p.observer.release:
	case <-p.ctx.Done():
	}
}

// TestOrchestration_StaticTarget_LateRegistration verifies that a
// deployment targeting a not-yet-registered target eventually reaches
// active once the target appears in the pool.
//
// Scenario:
//  1. Create a fulfillment with static placement targeting "late-target"
//  2. "late-target" does NOT exist in the target pool
//  3. Start orchestration (runs asynchronously via memworkflow)
//  4. Wait for RetryDelayStarted, then register "late-target" during
//     the durable retry delay
//  5. Verify the fulfillment reaches active with "late-target" resolved
func TestOrchestration_StaticTarget_LateRegistration(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	depName := domain.ResourceName("deployments/late-dep")
	fID := domain.FulfillmentID(depName)

	seedFulfillmentAndDeployment(t, store, depName, domain.FulfillmentSnapshot{
		Generation: 1,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"late-target"},
		},
		State:     domain.FulfillmentStateCreating,
		CreatedAt: now,
		UpdatedAt: now,
	})

	reg := &memworkflow.Registry{}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return now },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register("test", recordingAgent)

	reporter := application.NewDeliveryReportService(store, reg)
	recordingAgent.Reporter = reporter

	obs := newRetryDelayObserver()
	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
		domain.WithRetryDelay(10*time.Millisecond),
		domain.WithFulfillmentObserver(obs),
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exec, err := orchWf.Start(ctx, fID)
	if err != nil {
		t.Fatalf("Start orchestration: %v", err)
	}

	// Wait for the durable retry delay so registration lands in the
	// RetryDelay / ContinueAsNew window — simulating a kind cluster
	// finishing provisioning while orchestration is already running.
	select {
	case <-obs.started:
	case <-ctx.Done():
		t.Fatal("timed out waiting for retry delay to start")
	}

	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx for late target: %v", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Targets().Create(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "late-target",
		Type: "test",
		Name: "late-cluster",
	})); err != nil {
		t.Fatalf("create late target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit late target: %v", err)
	}
	close(obs.release)

	if _, err := exec.AwaitResult(ctx); err != nil {
		t.Fatalf("orchestration should complete successfully, got: %v", err)
	}

	f := getFulfillment(t, store, depName)
	if f.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want %q", f.State(), domain.FulfillmentStateActive)
	}
	if len(f.ResolvedTargets()) != 1 || f.ResolvedTargets()[0] != "late-target" {
		t.Errorf("ResolvedTargets = %v, want [late-target]", f.ResolvedTargets())
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Failed delivery backoff
// ---------------------------------------------------------------------------

// sleepRecordingRecord wraps [simpleRecord] and records every sleep the
// workflow requests instead of actually sleeping.
type sleepRecordingRecord struct {
	*simpleRecord
	mu     sync.Mutex
	sleeps []time.Duration
}

func (r *sleepRecordingRecord) Sleep(d time.Duration) error {
	r.mu.Lock()
	r.sleeps = append(r.sleeps, d)
	r.mu.Unlock()
	return nil
}

// sleepFnRecord wraps [sleepRecordingRecord] and runs onSleep after the
// first recorded sleep, letting a test simulate a concurrent mutation.
type sleepFnRecord struct {
	*sleepRecordingRecord
	onSleep func()
}

func (r *sleepFnRecord) Sleep(d time.Duration) error {
	err := r.sleepRecordingRecord.Sleep(d)
	if r.onSleep != nil {
		fn := r.onSleep
		r.onSleep = nil
		fn()
	}
	return err
}

// failingDeliveryAgent persists a failed delivery result (recording the
// message and attempt count, like [DeliveryReportService]) and signals
// the workflow.
type failingDeliveryAgent struct {
	store   domain.Store
	events  chan<- domain.FulfillmentEvent
	message string
}

func (a *failingDeliveryAgent) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	ctx := context.Background()
	tx, err := a.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackOnExit()
	d, err := tx.Deliveries().Get(ctx, deliveryID)
	if err != nil {
		return err
	}
	if err := d.ApplyResult(domain.DeliveryResult{
		State:   domain.DeliveryStateFailed,
		Message: a.message,
	}, time.Now()); err != nil {
		return err
	}
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Operation:  domain.DeliveryOperationDeliver,
				Result: domain.DeliveryResult{
					State:   domain.DeliveryStateFailed,
					Message: a.message,
				},
			},
		}
	}()
	return nil
}

func (a *failingDeliveryAgent) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	return nil
}

func TestOrchestration_DeliveryFailure_BacksOffByAttempts(t *testing.T) {
	const (
		seedAttempts = 2
		seedMessage  = "apply object 1: conflict with \"work-agent\": .data.owner"
		failureMsg   = "apply object 1: conflict with \"work-agent\": .data.owner (again)"
	)

	tests := []struct {
		name          string
		retryDelay    time.Duration
		maxRetryDelay time.Duration
		wantSleep     time.Duration
	}{
		{
			name:       "grows exponentially with failed attempts",
			retryDelay: 2 * time.Millisecond,
			// attempt 3 after this failure: 2ms * 2^2 = 8ms
			wantSleep: 8 * time.Millisecond,
		},
		{
			name:          "capped at max retry delay",
			retryDelay:    2 * time.Millisecond,
			maxRetryDelay: 5 * time.Millisecond,
			wantSleep:     5 * time.Millisecond,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := setupStore(t)
			seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
				Generation:        1,
				ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
				PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
				State:             domain.FulfillmentStateCreating,
			})
			seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

			now := time.Now().UTC().Truncate(time.Second)
			seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID:            "deployments/d1:t1",
				FulfillmentID: "deployments/d1",
				TargetID:      "t1",
				Generation:    1,
				State:         domain.DeliveryStateFailed,
				Message:       seedMessage,
				Attempts:      seedAttempts,
				CreatedAt:     now,
				UpdatedAt:     now,
			}))

			events := make(chan domain.FulfillmentEvent, 16)
			agent := &failingDeliveryAgent{store: store, events: events, message: failureMsg}
			opts := []func(*domain.OrchestrationWorkflowSpec){domain.WithRetryDelay(tc.retryDelay)}
			if tc.maxRetryDelay > 0 {
				opts = append(opts, domain.WithMaxRetryDelay(tc.maxRetryDelay))
			}
			wf := newTestWorkflow(store, agent, events, opts...)

			rec := &sleepRecordingRecord{simpleRecord: &simpleRecord{ctx: testContext(t), events: events}}
			_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
			var canErr *domain.ContinueAsNewError
			if !errors.As(err, &canErr) {
				t.Fatalf("Run error = %v, want ContinueAsNewError after a failed delivery", err)
			}

			if len(rec.sleeps) != 1 {
				t.Fatalf("Sleep called %d times (%v), want exactly one backoff sleep", len(rec.sleeps), rec.sleeps)
			}
			if rec.sleeps[0] != tc.wantSleep {
				t.Errorf("Sleep(%v), want %v for attempt %d", rec.sleeps[0], tc.wantSleep, seedAttempts+1)
			}

			d := getDelivery(t, store, "deployments/d1:t1")
			if d.State() != domain.DeliveryStateFailed {
				t.Errorf("delivery State = %q, want failed", d.State())
			}
			if d.Message() != failureMsg {
				t.Errorf("delivery Message = %q, want %q", d.Message(), failureMsg)
			}
			if d.Attempts() != seedAttempts+1 {
				t.Errorf("delivery Attempts = %d, want %d", d.Attempts(), seedAttempts+1)
			}

			dep := getFulfillment(t, store, "deployments/d1")
			if dep.State() != domain.FulfillmentStateCreating {
				t.Errorf("fulfillment State = %q, want creating (not active after failure)", dep.State())
			}
		})
	}
}

// TestOrchestration_DeliveryFailure_BackoffStopsWhenGenerationAdvances
// verifies that a mutation arriving during the retry backoff is noticed
// between sleep slices: the rest of the delay is abandoned so the
// restart reconciles the new generation instead of holding this
// orchestration execution active for the whole backoff.
func TestOrchestration_DeliveryFailure_BackoffStopsWhenGenerationAdvances(t *testing.T) {
	const (
		seedAttempts = 2
		seedMessage  = "apply object 1: conflict"
		failureMsg   = "apply object 1: still conflicting"
		retryDelay   = 6 * time.Second // attempt 3 waits 24s, longer than one slice
	)

	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	now := time.Now().UTC().Truncate(time.Second)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "deployments/d1:t1",
		FulfillmentID: "deployments/d1",
		TargetID:      "t1",
		Generation:    1,
		State:         domain.DeliveryStateFailed,
		Operation:     domain.DeliveryOperationDeliver,
		Message:       seedMessage,
		Attempts:      seedAttempts,
		CreatedAt:     now,
		UpdatedAt:     now,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	agent := &failingDeliveryAgent{store: store, events: events, message: failureMsg}
	wf := newTestWorkflow(store, agent, events, domain.WithRetryDelay(retryDelay))

	rec := &sleepFnRecord{sleepRecordingRecord: &sleepRecordingRecord{
		simpleRecord: &simpleRecord{ctx: testContext(t), events: events},
	}}
	rec.onSleep = func() {
		// A mutation advances the generation while the workflow is
		// backing off, as Delete or Resume does.
		ctx := context.Background()
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		defer tx.RollbackOnExit()
		f, err := tx.Fulfillments().Get(ctx, "deployments/d1")
		if err != nil {
			t.Fatalf("get fulfillment: %v", err)
		}
		f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		}, time.Now())
		if err := tx.Fulfillments().Update(ctx, f); err != nil {
			t.Fatalf("update fulfillment: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	var canErr *domain.ContinueAsNewError
	if !errors.As(err, &canErr) {
		t.Fatalf("Run error = %v, want ContinueAsNewError after a failed delivery", err)
	}

	if len(rec.sleeps) != 1 {
		t.Fatalf("Sleep called %d times (%v), want one slice before noticing the mutation", len(rec.sleeps), rec.sleeps)
	}
	if rec.sleeps[0] >= retryDelay {
		t.Errorf("Sleep(%v) covered the whole backoff; want it sliced so a mutation is noticed sooner", rec.sleeps[0])
	}

	dep := getFulfillment(t, store, "deployments/d1")
	if dep.Generation() != 2 {
		t.Errorf("Generation = %d, want 2 (the simulated mutation)", dep.Generation())
	}
}

// failingSleepRecord records the sleep and refuses it, simulating a
// durable execution whose timer cannot be started.
type failingSleepRecord struct {
	*sleepRecordingRecord
	err error
}

func (r *failingSleepRecord) Sleep(d time.Duration) error {
	r.mu.Lock()
	r.sleeps = append(r.sleeps, d)
	r.mu.Unlock()
	return r.err
}

// unreadableStore fails BeginReadOnly once armed, simulating a store that
// can no longer answer the generation check while the workflow backs off.
type unreadableStore struct {
	domain.Store
	mu   sync.Mutex
	fail bool
}

func (s *unreadableStore) arm() {
	s.mu.Lock()
	s.fail = true
	s.mu.Unlock()
}

func (s *unreadableStore) BeginReadOnly(ctx context.Context) (domain.Tx, error) {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return nil, errors.New("store unavailable")
	}
	return s.Store.BeginReadOnly(ctx)
}

// seedFailedDeliveryBackoff seeds the fulfillment, target, and failed
// delivery that a retry backoff test starts from.
func seedFailedDeliveryBackoff(t *testing.T, store domain.Store) {
	t.Helper()
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	now := time.Now().UTC().Truncate(time.Second)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "deployments/d1:t1",
		FulfillmentID: "deployments/d1",
		TargetID:      "t1",
		Generation:    1,
		State:         domain.DeliveryStateFailed,
		Operation:     domain.DeliveryOperationDeliver,
		Message:       "apply object 1: conflict",
		Attempts:      2,
		CreatedAt:     now,
		UpdatedAt:     now,
	}))
}

// TestOrchestration_DeliveryFailure_BackoffSleepErrorEndsTheWait verifies
// that a durable timer failure during the retry backoff ends the wait
// after the failed slice instead of sleeping again. The delivery failure
// stays recorded and the run restarts to retry later.
func TestOrchestration_DeliveryFailure_BackoffSleepErrorEndsTheWait(t *testing.T) {
	const retryDelay = 6 * time.Second // attempt 3 waits 24s, longer than one slice

	store, _ := setupStore(t)
	seedFailedDeliveryBackoff(t, store)

	events := make(chan domain.FulfillmentEvent, 16)
	agent := &failingDeliveryAgent{store: store, events: events, message: "apply object 1: still conflicting"}
	wf := newTestWorkflow(store, agent, events, domain.WithRetryDelay(retryDelay))

	rec := &failingSleepRecord{
		sleepRecordingRecord: &sleepRecordingRecord{
			simpleRecord: &simpleRecord{ctx: testContext(t), events: events},
		},
		err: errors.New("durable timer unavailable"),
	}
	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	var canErr *domain.ContinueAsNewError
	if !errors.As(err, &canErr) {
		t.Fatalf("Run error = %v, want ContinueAsNewError after a failed delivery", err)
	}

	if len(rec.sleeps) != 1 {
		t.Fatalf("Sleep called %d times (%v), want exactly one before the timer failure ended the wait", len(rec.sleeps), rec.sleeps)
	}

	d := getDelivery(t, store, "deployments/d1:t1")
	if d.State() != domain.DeliveryStateFailed || d.Attempts() != 3 {
		t.Errorf("delivery = %q with %d attempts, want failed with 3", d.State(), d.Attempts())
	}
}

// TestOrchestration_DeliveryFailure_BackoffCheckGenerationErrorEndsTheWait
// verifies that a generation check that cannot be answered — for example
// because the fulfillment was deleted while the workflow backed off — ends
// the wait instead of sleeping through the rest of the delay on stale
// state.
func TestOrchestration_DeliveryFailure_BackoffCheckGenerationErrorEndsTheWait(t *testing.T) {
	const retryDelay = 6 * time.Second // attempt 3 waits 24s, longer than one slice

	base, _ := setupStore(t)
	seedFailedDeliveryBackoff(t, base)
	store := &unreadableStore{Store: base}

	events := make(chan domain.FulfillmentEvent, 16)
	agent := &failingDeliveryAgent{store: base, events: events, message: "apply object 1: still conflicting"}
	wf := newTestWorkflow(store, agent, events, domain.WithRetryDelay(retryDelay))

	rec := &sleepFnRecord{sleepRecordingRecord: &sleepRecordingRecord{
		simpleRecord: &simpleRecord{ctx: testContext(t), events: events},
	}}
	// After the first slice the generation becomes unreadable, as it does
	// when the fulfillment is deleted mid-backoff.
	rec.onSleep = store.arm

	_, err := wf.Run(rec, domain.FulfillmentID("deployments/d1"))
	var canErr *domain.ContinueAsNewError
	if !errors.As(err, &canErr) {
		t.Fatalf("Run error = %v, want ContinueAsNewError after a failed delivery", err)
	}

	if len(rec.sleeps) != 1 {
		t.Fatalf("Sleep called %d times (%v), want one slice before the unreadable generation ended the wait", len(rec.sleeps), rec.sleeps)
	}

	d := getDelivery(t, base, "deployments/d1:t1")
	if d.State() != domain.DeliveryStateFailed || d.Attempts() != 3 {
		t.Errorf("delivery = %q with %d attempts, want failed with 3", d.State(), d.Attempts())
	}
}
