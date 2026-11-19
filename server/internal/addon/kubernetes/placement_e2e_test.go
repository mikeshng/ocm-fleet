package kubernetes

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/delivery"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/memworkflow"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

// deploymentResolvedTargets reads a deployment's materialized placement.
func deploymentResolvedTargets(t *testing.T, store domain.Store, name domain.ResourceName) []domain.TargetID {
	t.Helper()
	ctx := context.Background()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.RollbackOnExit()
	view, err := tx.Deployments().GetView(ctx, name)
	if err != nil {
		t.Fatalf("GetView(%s): %v", name, err)
	}
	return view.Fulfillment.ResolvedTargets()
}

// deploymentGeneration reads a deployment's generation.
func deploymentGeneration(t *testing.T, store domain.Store, name domain.ResourceName) domain.Generation {
	t.Helper()
	ctx := context.Background()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.RollbackOnExit()
	view, err := tx.Deployments().GetView(ctx, name)
	if err != nil {
		t.Fatalf("GetView(%s): %v", name, err)
	}
	return view.Fulfillment.Generation()
}

// awaitDeploymentTarget waits for the deployment's placement to be
// exactly the wanted target.
func awaitDeploymentTarget(ctx context.Context, t *testing.T, store domain.Store, name domain.ResourceName, want domain.TargetID) {
	t.Helper()
	for {
		got := deploymentResolvedTargets(t, store, name)
		if len(got) == 1 && got[0] == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s to resolve to %q; resolved = %v", name, want, got)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestPlacementReevaluation_JoiningClusterReceivesConvergedDeployment
// proves the end-to-end scenario behind the re-evaluation mechanism: a
// deployment whose selector matches no target converges with an empty
// target set, and its orchestration returns. A cluster carrying the
// selector label then enrolls. The enrollment restarts the deployment's
// orchestration, which resolves the new target and delivers to it. The
// deployment itself is not changed: its generation stays.
func TestPlacementReevaluation_JoiningClusterReceivesConvergedDeployment(t *testing.T) {
	h := newEnrollHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	router := delivery.NewRoutingDeliveryService()
	agent := &sqlite.RecordingDeliveryService{Store: h.store, Now: h.clock.Now}
	router.Register(TargetType, agent)
	reg := &memworkflow.Registry{}
	agent.Reporter = application.NewDeliveryReportService(h.store, reg)
	orchWf, err := reg.RegisterOrchestration(domain.NewOrchestrationWorkflowSpec(
		h.store, router, domain.StrategyFactory{Store: h.store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
		domain.WithNow(h.clock.Now),
	))
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	h.seedDeployment("d1", domain.PlacementStrategySpec{
		Type:           domain.PlacementStrategySelector,
		TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}},
	})
	fid := domain.FulfillmentID("f-d1")

	// First pass: no target carries the label, so the deployment
	// converges with an empty target set and its workflow returns.
	initial, err := orchWf.Start(ctx, fid)
	if err != nil {
		t.Fatalf("start orchestration before enrollment: %v", err)
	}
	if _, err := initial.AwaitResult(ctx); err != nil {
		t.Fatalf("initial orchestration: %v", err)
	}
	if got := deploymentResolvedTargets(t, h.store, "deployments/d1"); len(got) != 0 {
		t.Fatalf("resolved targets before enrollment = %v, want none", got)
	}

	svc := h.serviceWithPlacement(&application.PlacementReevaluationService{
		Store: h.store, Orchestration: orchWf,
	})
	api, ca, token := h.addCluster("west", time.Hour)
	if _, err := svc.Enroll(ctx, EnrollRequest{
		ClusterID: "west", APIServer: api, CACert: ca, Token: token,
		Labels: map[string]string{"env": "prod"},
	}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}

	want := TargetIDForCluster("west")
	awaitDeploymentTarget(ctx, t, h.store, "deployments/d1", want)
	if got := deploymentGeneration(t, h.store, "deployments/d1"); got != 1 {
		t.Errorf("generation after the cluster joined = %d, want 1: a fleet change does not change a deployment", got)
	}

	// The joining target was delivered to, not merely recorded as
	// resolved.
	tx, err := h.store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.RollbackOnExit()
	deliveries, err := tx.Deliveries().ListByFulfillment(ctx, fid)
	if err != nil {
		t.Fatalf("ListByFulfillment: %v", err)
	}
	targets := make([]domain.TargetID, 0, len(deliveries))
	for _, d := range deliveries {
		if d.TargetID() == want {
			return
		}
		targets = append(targets, d.TargetID())
	}
	t.Fatalf("no delivery to %q; delivered to %v", want, targets)
}

// dispatchRecorder counts what is dispatched to each target on its way
// to the delivery agent.
type dispatchRecorder struct {
	domain.DeliveryAgent
	mu        sync.Mutex
	delivered map[domain.TargetID]int
	removed   map[domain.TargetID]int
}

func (r *dispatchRecorder) Deliver(ctx context.Context, target domain.TargetInfo, id domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	r.mu.Lock()
	r.delivered[target.ID()]++
	r.mu.Unlock()
	return r.DeliveryAgent.Deliver(ctx, target, id, manifests, auth, att, generation)
}

func (r *dispatchRecorder) Remove(ctx context.Context, target domain.TargetInfo, id domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, generation domain.Generation) error {
	r.mu.Lock()
	r.removed[target.ID()]++
	r.mu.Unlock()
	return r.DeliveryAgent.Remove(ctx, target, id, manifests, auth, att, generation)
}

func (r *dispatchRecorder) counts(id domain.TargetID) (delivered, removed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.delivered[id], r.removed[id]
}

// startCounter counts orchestration starts on their way to the engine.
type startCounter struct {
	domain.OrchestrationWorkflow
	starts atomic.Int32
}

func (c *startCounter) Start(ctx context.Context, id domain.FulfillmentID) (domain.Execution[struct{}], error) {
	c.starts.Add(1)
	return c.OrchestrationWorkflow.Start(ctx, id)
}

// placementFixture runs the real store, orchestration, and placement
// re-evaluation behind an enrollment service, with a delivery agent
// that records what reaches it.
type placementFixture struct {
	h          *enrollHarness
	svc        *EnrollmentService
	dispatches *dispatchRecorder
	orch       *startCounter
}

func newPlacementFixture(t *testing.T) *placementFixture {
	t.Helper()
	h := newEnrollHarness(t)
	router := delivery.NewRoutingDeliveryService()
	agent := &sqlite.RecordingDeliveryService{Store: h.store, Now: h.clock.Now}
	dispatches := &dispatchRecorder{
		DeliveryAgent: agent,
		delivered:     map[domain.TargetID]int{},
		removed:       map[domain.TargetID]int{},
	}
	router.Register(TargetType, dispatches)
	reg := &memworkflow.Registry{}
	agent.Reporter = application.NewDeliveryReportService(h.store, reg)
	orchWf, err := reg.RegisterOrchestration(domain.NewOrchestrationWorkflowSpec(
		h.store, router, domain.StrategyFactory{Store: h.store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
		domain.WithNow(h.clock.Now),
	))
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}
	orch := &startCounter{OrchestrationWorkflow: orchWf}
	return &placementFixture{
		h:          h,
		dispatches: dispatches,
		orch:       orch,
		svc: h.serviceWithPlacement(&application.PlacementReevaluationService{
			Store: h.store, Orchestration: orch,
		}),
	}
}

// enroll takes a cluster labelled env=prod in.
func (fx *placementFixture) enroll(ctx context.Context, t *testing.T, name string, lifetime time.Duration) (apiServer string) {
	t.Helper()
	api, ca, token := fx.h.addCluster(name, lifetime)
	if _, err := fx.svc.Enroll(ctx, EnrollRequest{
		ClusterID: name, APIServer: api, CACert: ca, Token: token,
		Labels: map[string]string{"env": "prod"},
	}); err != nil {
		t.Fatalf("Enroll %s: %v", name, err)
	}
	return api
}

// deploy stores a deployment placed by the label env=prod and runs its
// first orchestration pass to the end.
func (fx *placementFixture) deploy(ctx context.Context, t *testing.T, name string) {
	t.Helper()
	fx.h.seedDeployment(name, domain.PlacementStrategySpec{
		Type:           domain.PlacementStrategySelector,
		TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}},
	})
	first, err := fx.orch.OrchestrationWorkflow.Start(ctx, domain.FulfillmentID("f-"+name))
	if err != nil {
		t.Fatalf("start the first pass of %s: %v", name, err)
	}
	if _, err := first.AwaitResult(ctx); err != nil {
		t.Fatalf("first pass of %s: %v", name, err)
	}
}

// awaitPlacement waits for the deployment's placement to be exactly the
// wanted targets, in any order.
func awaitPlacement(ctx context.Context, t *testing.T, store domain.Store, name domain.ResourceName, want ...domain.TargetID) {
	t.Helper()
	slices.Sort(want)
	for {
		got := deploymentResolvedTargets(t, store, name)
		slices.Sort(got)
		if slices.Equal(got, want) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s to resolve to %v; resolved = %v", name, want, got)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// awaitIdle waits until no orchestration of the deployment is running.
func (fx *placementFixture) awaitIdle(ctx context.Context, t *testing.T, name string) {
	t.Helper()
	for {
		tx, err := fx.h.store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("BeginReadOnly: %v", err)
		}
		view, err := tx.Deployments().GetView(ctx, domain.ResourceName("deployments/"+name))
		tx.RollbackOnExit()
		if err != nil {
			t.Fatalf("GetView: %v", err)
		}
		if view.Fulfillment.ActiveWorkflowGen() == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for the orchestration of %s to finish", name)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// A cluster that joins gets the deployment, and the clusters that
// already hold it are left alone: nothing is dispatched to them again.
// A cluster that is withdrawn leaves the placement, and what was
// delivered to it stays: no removal is dispatched, although the
// deployment still runs elsewhere. Neither change touches the
// deployment's generation or pauses it.
func TestPlacementReevaluation_JoinAndWithdrawalDispatchOnlyWhatMoved(t *testing.T) {
	fx := newPlacementFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	east, west := TargetIDForCluster("east"), TargetIDForCluster("west")

	fx.enroll(ctx, t, "east", time.Hour)
	fx.deploy(ctx, t, "d1")
	awaitPlacement(ctx, t, fx.h.store, "deployments/d1", east)

	fx.enroll(ctx, t, "west", time.Hour)
	awaitPlacement(ctx, t, fx.h.store, "deployments/d1", east, west)
	fx.awaitIdle(ctx, t, "d1")
	if delivered, _ := fx.dispatches.counts(east); delivered != 1 {
		t.Errorf("deliveries dispatched to east = %d, want 1: a cluster that joined is no reason to deliver again where the deployment already is", delivered)
	}
	if delivered, _ := fx.dispatches.counts(west); delivered != 1 {
		t.Errorf("deliveries dispatched to west = %d, want 1", delivered)
	}

	if _, err := fx.svc.Withdraw(ctx, "west", WithdrawRequest{Actor: "ops", LeaveDeployments: true}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	awaitPlacement(ctx, t, fx.h.store, "deployments/d1", east)
	fx.awaitIdle(ctx, t, "d1")

	if _, removed := fx.dispatches.counts(west); removed != 0 {
		t.Errorf("removals dispatched to the withdrawn cluster = %d, want 0: a withdrawal never removes what was delivered", removed)
	}
	if delivered, removed := fx.dispatches.counts(east); delivered != 1 || removed != 0 {
		t.Errorf("dispatched to east: %d deliveries, %d removals; want 1 and 0", delivered, removed)
	}
	if got := deploymentGeneration(t, fx.h.store, "deployments/d1"); got != 1 {
		t.Errorf("generation = %d, want 1: a fleet change does not change a deployment", got)
	}
	if _, found := fx.h.target("west"); found {
		t.Error("the withdrawn cluster's target is still registered")
	}
}

// A cluster whose credential lapsed keeps the deployments it holds:
// nothing is removed and no pass is started for it. When it presents a
// credential again it is as it was.
func TestPlacementReevaluation_LapseKeepsPlacementAndRemovesNothing(t *testing.T) {
	fx := newPlacementFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	west := TargetIDForCluster("west")

	api := fx.enroll(ctx, t, "west", time.Hour)
	fx.deploy(ctx, t, "d1")
	awaitPlacement(ctx, t, fx.h.store, "deployments/d1", west)
	starts := fx.orch.starts.Load()

	fx.h.cluster(api).denyRenew = true
	fx.h.clock.Advance(61 * time.Minute)
	if err := fx.svc.CheckCluster(ctx, "west"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	if target, _ := fx.h.target("west"); target.State() != domain.TargetStateInitializing {
		t.Fatalf("target state after the lapse = %q, want %q", target.State(), domain.TargetStateInitializing)
	}
	if got := fx.orch.starts.Load(); got != starts {
		t.Errorf("orchestration starts after the lapse = %d, want %d: a lapse moves no placement", got, starts)
	}
	awaitPlacement(ctx, t, fx.h.store, "deployments/d1", west)

	// The cluster's side presents a new credential.
	fx.h.cluster(api).denyRenew = false
	if _, err := fx.svc.Enroll(ctx, EnrollRequest{
		ClusterID: "west", APIServer: api, CACert: fx.h.cluster(api).ca,
		Token:  fx.h.mintToken(api, "fresh", time.Hour),
		Labels: map[string]string{"env": "prod"},
	}); err != nil {
		t.Fatalf("Enroll with a new credential: %v", err)
	}
	if target, _ := fx.h.target("west"); target.State() != domain.TargetStateReady {
		t.Fatalf("target state after the new credential = %q, want %q", target.State(), domain.TargetStateReady)
	}
	fx.awaitIdle(ctx, t, "d1")
	awaitPlacement(ctx, t, fx.h.store, "deployments/d1", west)

	if delivered, removed := fx.dispatches.counts(west); delivered != 1 || removed != 0 {
		t.Errorf("dispatched to west over the lapse: %d deliveries, %d removals; want 1 and 0", delivered, removed)
	}
	if got := deploymentGeneration(t, fx.h.store, "deployments/d1"); got != 1 {
		t.Errorf("generation = %d, want 1", got)
	}
}
