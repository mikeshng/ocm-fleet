package domain_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// stubRecord is a minimal Record that runs activities synchronously.
type stubRecord struct {
	ctx context.Context
}

func (r *stubRecord) ID() string               { return "create-test" }
func (r *stubRecord) Context() context.Context { return r.ctx }
func (r *stubRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}
func (r *stubRecord) Await(_ string) (any, error) {
	return nil, nil
}
func (r *stubRecord) AwaitWithTimeout(_ string, _ time.Duration) (any, error) {
	return nil, domain.ErrSignalTimeout
}
func (r *stubRecord) Sleep(_ time.Duration) error {
	return nil
}

// fakeOrchestrationWorkflow records the fulfillment ID it was started with.
type fakeOrchestrationWorkflow struct {
	started domain.FulfillmentID
}

// startErrorOrchestration reports a fixed error from Start.
type startErrorOrchestration struct {
	err error
}

func (o startErrorOrchestration) Start(context.Context, domain.FulfillmentID) (domain.Execution[struct{}], error) {
	return nil, o.err
}

func (f *fakeOrchestrationWorkflow) Start(_ context.Context, fulfillmentID domain.FulfillmentID) (domain.Execution[struct{}], error) {
	f.started = fulfillmentID
	return &immediateExecution[struct{}]{}, nil
}

type immediateExecution[T any] struct {
	val T
}

func (e *immediateExecution[T]) WorkflowID() string                       { return "fake" }
func (e *immediateExecution[T]) AwaitResult(_ context.Context) (T, error) { return e.val, nil }

func TestCreateDeploymentWorkflow_PersistsThenStartsOrchestration(t *testing.T) {
	store, _ := setupStore(t)
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

	fakeOrch := &fakeOrchestrationWorkflow{}

	wf := &domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: fakeOrch,
		Now:           func() time.Time { return fixedTime },
	}

	ctx := context.Background()
	rec := &stubRecord{ctx: ctx}

	input := domain.CreateDeploymentInput{
		Name: "deployments/d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
	}

	dep, err := wf.Run(rec, input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if dep.Deployment.Name() != "deployments/d1" {
		t.Errorf("Deployment.Name = %q, want %q", dep.Deployment.Name(), "deployments/d1")
	}
	if dep.Fulfillment.State() != domain.FulfillmentStateCreating {
		t.Errorf("Fulfillment.State = %q, want %q", dep.Fulfillment.State(), domain.FulfillmentStateCreating)
	}
	if dep.Deployment.UID().IsZero() {
		t.Error("Deployment.UID is zero, want non-zero UUID")
	}
	if dep.Deployment.CreatedAt().IsZero() {
		t.Error("Deployment.CreatedAt is zero, want non-zero")
	}
	if !dep.Deployment.CreatedAt().Equal(fixedTime) {
		t.Errorf("Deployment.CreatedAt = %v, want %v", dep.Deployment.CreatedAt(), fixedTime)
	}
	if !dep.Deployment.UpdatedAt().Equal(fixedTime) {
		t.Errorf("Deployment.UpdatedAt = %v, want %v", dep.Deployment.UpdatedAt(), fixedTime)
	}
	if dep.Etag() == "" {
		t.Error("DeploymentView.Etag() is empty, want non-empty")
	}

	persisted := getThinDeployment(t, store, "deployments/d1")
	if persisted.Name() != "deployments/d1" {
		t.Errorf("persisted Name = %q, want %q", persisted.Name(), "deployments/d1")
	}

	if fakeOrch.started != dep.Deployment.FulfillmentID() {
		t.Errorf("Orchestration.Start called with %q, want %q", fakeOrch.started, dep.Deployment.FulfillmentID())
	}
}

func TestCreateDeploymentWorkflow_PersistReplaysWithoutDuplicating(t *testing.T) {
	store, _ := setupStore(t)
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	wf := &domain.CreateDeploymentWorkflowSpec{
		Store: store,
		Now:   func() time.Time { return fixedTime },
	}
	ctx := context.Background()
	rec := &stubRecord{ctx: ctx}
	input := domain.CreateDeploymentInput{
		Name:              "deployments/again",
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll},
	}

	first, err := domain.RunActivity(rec, wf.PersistDeployment(), input)
	if err != nil {
		t.Fatalf("first persist: %v", err)
	}
	// At-least-once: the engine may run a committed activity again.
	second, err := domain.RunActivity(rec, wf.PersistDeployment(), input)
	if err != nil {
		t.Fatalf("replayed persist: %v", err)
	}

	if second.Deployment.UID() != first.Deployment.UID() ||
		second.Deployment.FulfillmentID() != first.Deployment.FulfillmentID() ||
		second.Fulfillment.ID() != first.Fulfillment.ID() {
		t.Fatalf("replay returned different records: first %s/%s, second %s/%s",
			first.Deployment.UID(), first.Fulfillment.ID(),
			second.Deployment.UID(), second.Fulfillment.ID())
	}

	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	defer tx.RollbackOnExit()
	views, err := tx.Deployments().ListView(ctx)
	if err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("deployments after a replayed persist = %d, want 1", len(views))
	}
}

func TestCreateManagedResourceWorkflow_PersistReplaysWithoutDuplicating(t *testing.T) {
	store, _ := setupStore(t)
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	rt := domain.ResourceType("kind.open-cluster-management.io/Cluster")
	typeDef := domain.NewExtensionResourceType(rt, "v1", "clusters", fixedTime,
		domain.WithManagement(
			domain.NewRegisteredSelfTarget("target-kind", "managed-resource"),
			domain.Signature{Signer: domain.FederatedIdentity{Subject: "addon", Issuer: "https://issuer.example.com"}},
		))
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.ExtensionResources().CreateType(ctx, typeDef); err != nil {
		t.Fatalf("create type: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit type: %v", err)
	}

	wf := &domain.CreateManagedResourceWorkflowSpec{
		Store: store,
		Now:   func() time.Time { return fixedTime },
	}
	rec := &stubRecord{ctx: ctx}
	input := domain.CreateManagedResourceInput{
		ResourceType: rt,
		Name:         "clusters/dev",
		Spec:         json.RawMessage(`{"replicas":1}`),
		TypeDef:      typeDef,
	}

	first, err := domain.RunActivity(rec, wf.PersistManagedResource(), input)
	if err != nil {
		t.Fatalf("first persist: %v", err)
	}
	// At-least-once: the engine may run a committed activity again.
	second, err := domain.RunActivity(rec, wf.PersistManagedResource(), input)
	if err != nil {
		t.Fatalf("replayed persist: %v", err)
	}

	if second.Resource.UID() != first.Resource.UID() ||
		second.Fulfillment.ID() != first.Fulfillment.ID() {
		t.Fatalf("replay returned different records: first %s/%s, second %s/%s",
			first.Resource.UID(), first.Fulfillment.ID(),
			second.Resource.UID(), second.Fulfillment.ID())
	}
	if second.Intent == nil || second.Intent.Version != first.Intent.Version {
		t.Fatalf("replay intent = %+v, want version %d", second.Intent, first.Intent.Version)
	}

	readTx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	defer readTx.RollbackOnExit()
	views, err := readTx.ExtensionResources().ListViewsByType(ctx, rt)
	if err != nil {
		t.Fatalf("list managed resources: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("managed resources after a replayed persist = %d, want 1", len(views))
	}
}

func TestCreateDeploymentWorkflow_StartOrchestrationReplayIsSafe(t *testing.T) {
	wf := &domain.CreateDeploymentWorkflowSpec{
		Orchestration: startErrorOrchestration{err: domain.ErrAlreadyRunning},
	}
	rec := &stubRecord{ctx: context.Background()}
	if _, err := domain.RunActivity(rec, wf.StartOrchestration(), domain.FulfillmentID("f1")); err != nil {
		t.Fatalf("StartOrchestration on an already started orchestration: %v", err)
	}

	wf.Orchestration = startErrorOrchestration{err: errors.New("engine unavailable")}
	if _, err := domain.RunActivity(rec, wf.StartOrchestration(), domain.FulfillmentID("f1")); err == nil {
		t.Fatal("StartOrchestration: expected the engine error, got nil")
	}
}

func TestCreateManagedResourceWorkflow_StartOrchestrationReplayIsSafe(t *testing.T) {
	wf := &domain.CreateManagedResourceWorkflowSpec{
		Orchestration: startErrorOrchestration{err: domain.ErrAlreadyRunning},
	}
	rec := &stubRecord{ctx: context.Background()}
	if _, err := domain.RunActivity(rec, wf.StartOrchestration(), domain.FulfillmentID("f1")); err != nil {
		t.Fatalf("StartOrchestration on an already started orchestration: %v", err)
	}

	wf.Orchestration = startErrorOrchestration{err: errors.New("engine unavailable")}
	if _, err := domain.RunActivity(rec, wf.StartOrchestration(), domain.FulfillmentID("f1")); err == nil {
		t.Fatal("StartOrchestration: expected the engine error, got nil")
	}
}
