package application_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// recordingOrchestration records orchestration starts and answers with
// a per-fulfillment error when the test configures one.
type recordingOrchestration struct {
	mu      sync.Mutex
	started []domain.FulfillmentID
	errs    map[domain.FulfillmentID]error
}

func (r *recordingOrchestration) Start(_ context.Context, id domain.FulfillmentID) (domain.Execution[struct{}], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = append(r.started, id)
	if err := r.errs[id]; err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *recordingOrchestration) startedIDs() []domain.FulfillmentID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]domain.FulfillmentID(nil), r.started...)
}

// deploymentFulfillment returns the stored fulfillment of a deployment.
func deploymentFulfillment(t *testing.T, store domain.Store, name domain.ResourceName) *domain.Fulfillment {
	t.Helper()
	ctx := context.Background()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.RollbackOnExit()
	view, err := tx.Deployments().GetView(ctx, name)
	if err != nil {
		t.Fatalf("GetView(%s): %v", name, err)
	}
	return &view.Fulfillment
}

// placedDeployment describes a deployment as an orchestration pass left
// it: active, with a materialized placement.
type placedDeployment struct {
	name      string
	placement domain.PlacementStrategySpec
	held      []domain.TargetID
	// generation defaults to 1; observed defaults to generation.
	generation domain.Generation
	observed   domain.Generation
	state      domain.FulfillmentState
	paused     string
}

func seedPlacedDeployment(t *testing.T, store domain.Store, d placedDeployment) domain.FulfillmentID {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if d.generation == 0 {
		d.generation = 1
	}
	if d.observed == 0 {
		d.observed = d.generation
	}
	if d.state == "" {
		d.state = domain.FulfillmentStateActive
	}
	fID := domain.FulfillmentID("ful-deployments/" + d.name)
	f := domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID:                 fID,
		State:              d.state,
		PauseReason:        d.paused,
		ResolvedTargets:    d.held,
		Generation:         d.generation - 1,
		ObservedGeneration: d.observed,
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	f.AdvanceManifestStrategy(defaultManifestStrategy(), now)
	f.AdvancePlacementStrategy(d.placement, now)

	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Fulfillments().Create(ctx, f); err != nil {
		t.Fatalf("Create fulfillment: %v", err)
	}
	dep := domain.NewDeployment(domain.ResourceName("deployments/"+d.name), domain.NewDeploymentUID(), fID, now)
	if err := tx.Deployments().Create(ctx, dep); err != nil {
		t.Fatalf("Create deployment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return fID
}

func labelSelector(key, value string) domain.PlacementStrategySpec {
	return domain.PlacementStrategySpec{
		Type:           domain.PlacementStrategySelector,
		TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{key: value}},
	}
}

func fleetTarget(id domain.TargetID, state domain.TargetState, labels map[string]string) domain.TargetInfo {
	return domain.NewTargetInfo(id, "test", string(id), state, labels, nil, nil)
}

// Re-evaluation starts exactly the deployments whose placement no
// longer matches the fleet, and changes none of them: every generation
// stays where it was.
func TestPlacementReevaluation_StartsOnlyDeploymentsWhosePlacementMoved(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	prod := map[string]string{"env": "prod"}
	seedTarget(t, store, fleetTarget("t1", domain.TargetStateReady, prod))
	seedTarget(t, store, fleetTarget("lapsed", domain.TargetStateInitializing, prod))
	seedTarget(t, store, fleetTarget("leaving", domain.TargetStateTerminated, prod))

	selector := labelSelector("env", "prod")
	static := func(ids ...domain.TargetID) domain.PlacementStrategySpec {
		return domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: ids}
	}
	cases := []struct {
		deployment placedDeployment
		wantStart  bool
	}{
		{placedDeployment{name: "settled", placement: selector, held: []domain.TargetID{"t1"}}, false},
		{placedDeployment{name: "target-joined", placement: selector}, true},
		{placedDeployment{name: "target-unregistered", placement: selector, held: []domain.TargetID{"t1", "gone"}}, true},
		{placedDeployment{name: "target-leaving", placement: selector, held: []domain.TargetID{"t1", "leaving"}}, true},
		{placedDeployment{name: "target-deselected", placement: labelSelector("env", "dev"), held: []domain.TargetID{"t1"}}, true},
		// A held target that is not ready keeps its placement.
		{placedDeployment{name: "target-lapsed", placement: selector, held: []domain.TargetID{"t1", "lapsed"}}, false},
		{placedDeployment{name: "static-settled", placement: static("t1"), held: []domain.TargetID{"t1"}}, false},
		// No pass could change what an unresolved placement holds.
		{placedDeployment{name: "static-unregistered", placement: static("gone"), held: []domain.TargetID{"gone"}}, false},
		{placedDeployment{name: "paused", placement: selector, paused: "delivery auth expired"}, false},
		{placedDeployment{name: "creating", placement: selector, state: domain.FulfillmentStateCreating}, false},
		{placedDeployment{name: "failed", placement: selector, state: domain.FulfillmentStateFailed}, false},
		// Its own pass is due and loads the fleet as it is.
		{placedDeployment{name: "reconciling", placement: selector, generation: 2, observed: 1}, false},
	}
	want := map[domain.FulfillmentID]bool{}
	generations := map[string]domain.Generation{}
	for _, c := range cases {
		id := seedPlacedDeployment(t, store, c.deployment)
		if c.wantStart {
			want[id] = true
		}
		generations[c.deployment.name] = deploymentFulfillment(t, store, domain.ResourceName("deployments/"+c.deployment.name)).Generation()
	}

	orch := &recordingOrchestration{}
	svc := &application.PlacementReevaluationService{Store: store, Orchestration: orch}
	if err := svc.ReevaluatePlacement(ctx); err != nil {
		t.Fatalf("ReevaluatePlacement: %v", err)
	}

	started := map[domain.FulfillmentID]bool{}
	for _, id := range orch.startedIDs() {
		if started[id] {
			t.Errorf("fulfillment %s started twice", id)
		}
		started[id] = true
	}
	for _, c := range cases {
		id := domain.FulfillmentID("ful-deployments/" + c.deployment.name)
		if started[id] != want[id] {
			t.Errorf("%s: started = %v, want %v", c.deployment.name, started[id], want[id])
		}
		name := domain.ResourceName("deployments/" + c.deployment.name)
		if got := deploymentFulfillment(t, store, name).Generation(); got != generations[c.deployment.name] {
			t.Errorf("%s: generation = %d, want %d (re-evaluation must not change a fulfillment)",
				c.deployment.name, got, generations[c.deployment.name])
		}
	}
}

func TestPlacementReevaluation_ToleratesAlreadyRunningAndReportsStartFailures(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	seedTarget(t, store, fleetTarget("t1", domain.TargetStateReady, map[string]string{"env": "prod"}))

	selector := labelSelector("env", "prod")
	runningID := seedPlacedDeployment(t, store, placedDeployment{name: "running", placement: selector})
	failingID := seedPlacedDeployment(t, store, placedDeployment{name: "failing", placement: selector})

	orch := &recordingOrchestration{errs: map[domain.FulfillmentID]error{
		runningID: domain.ErrAlreadyRunning,
		failingID: errors.New("workflow backend unavailable"),
	}}
	svc := &application.PlacementReevaluationService{Store: store, Orchestration: orch}
	err := svc.ReevaluatePlacement(ctx)
	if err == nil {
		t.Fatal("expected the failed start to be reported")
	}
	if !strings.Contains(err.Error(), "workflow backend unavailable") {
		t.Errorf("error %q does not carry the start failure", err)
	}
	if strings.Contains(err.Error(), string(runningID)) {
		t.Errorf("error %q reports ErrAlreadyRunning as a failure", err)
	}
	if started := orch.startedIDs(); len(started) != 2 {
		t.Errorf("started = %v, want both fulfillments attempted", started)
	}

	// Nothing was written, so the comparison finds the same two again:
	// a start that did not get through is made up by the next run.
	if err := svc.ReevaluatePlacement(ctx); err == nil {
		t.Fatal("expected the second run to attempt, and report, the failed start again")
	}
	if started := orch.startedIDs(); len(started) != 4 {
		t.Errorf("started after two runs = %v, want both fulfillments attempted twice", started)
	}
}

func TestPlacementReevaluation_WithoutOrchestrationIsANoOp(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	seedTarget(t, store, fleetTarget("t1", domain.TargetStateReady, map[string]string{"env": "prod"}))
	seedPlacedDeployment(t, store, placedDeployment{name: "moved", placement: labelSelector("env", "prod")})

	svc := &application.PlacementReevaluationService{Store: store}
	if err := svc.ReevaluatePlacement(ctx); err != nil {
		t.Fatalf("ReevaluatePlacement: %v", err)
	}
}

// Run follows a fleet change that nobody announced: a target that is
// registered while it runs is found on a later interval.
func TestPlacementReevaluation_Run_FollowsAnUnannouncedFleetChange(t *testing.T) {
	store := openTestStore(t)
	id := seedPlacedDeployment(t, store, placedDeployment{name: "waiting", placement: labelSelector("env", "prod")})

	orch := &recordingOrchestration{}
	svc := &application.PlacementReevaluationService{
		Store: store, Orchestration: orch, Interval: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Run(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after its context was cancelled")
		}
	}()

	// With no matching target the placement holds: nothing starts.
	time.Sleep(50 * time.Millisecond)
	if started := orch.startedIDs(); len(started) != 0 {
		t.Fatalf("started with no matching target = %v, want none", started)
	}

	seedTarget(t, store, fleetTarget("t1", domain.TargetStateReady, map[string]string{"env": "prod"}))

	deadline := time.After(5 * time.Second)
	for {
		if started := orch.startedIDs(); len(started) > 0 {
			if started[0] != id {
				t.Fatalf("started = %v, want %s", started, id)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for Run to start the deployment whose target joined")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
