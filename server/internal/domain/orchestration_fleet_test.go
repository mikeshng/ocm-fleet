package domain_test

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// ---------------------------------------------------------------------------
// Placement when the target fleet changes under a fulfillment
// ---------------------------------------------------------------------------

// fleetDelivery records which targets a pass delivered to and removed
// from, and completes every dispatch as delivered.
type fleetDelivery struct {
	events    chan<- domain.FulfillmentEvent
	mu        sync.Mutex
	delivered []domain.TargetID
	removed   []domain.TargetID
}

func (d *fleetDelivery) Deliver(_ context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	d.mu.Lock()
	d.delivered = append(d.delivered, target.ID())
	d.mu.Unlock()
	d.complete(deliveryID, generation, domain.DeliveryOperationDeliver)
	return nil
}

func (d *fleetDelivery) Remove(_ context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	d.mu.Lock()
	d.removed = append(d.removed, target.ID())
	d.mu.Unlock()
	d.complete(deliveryID, generation, domain.DeliveryOperationRemove)
	return nil
}

func (d *fleetDelivery) complete(id domain.DeliveryID, generation domain.Generation, op domain.DeliveryOperation) {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: id,
				Generation: generation,
				Operation:  op,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
}

func (d *fleetDelivery) calls() (delivered, removed []domain.TargetID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.delivered), slices.Clone(d.removed)
}

// fleetFixture is an active fulfillment that a completed pass placed
// on the held targets, each with a delivered record at generation 1.
type fleetFixture struct {
	store    domain.Store
	name     domain.ResourceName
	delivery *fleetDelivery
	events   chan domain.FulfillmentEvent
}

func newFleetFixture(t *testing.T, name string, placement domain.PlacementStrategySpec, state domain.FulfillmentState, held []domain.TargetID, targets ...domain.TargetInfo) *fleetFixture {
	t.Helper()
	store, _ := setupStore(t)
	depName := domain.ResourceName("deployments/" + name)
	manifests := []domain.Manifest{{Raw: json.RawMessage(`{}`)}}
	seedFulfillmentAndDeployment(t, store, depName, domain.FulfillmentSnapshot{
		Generation:         1,
		ObservedGeneration: 1,
		ResolvedTargets:    held,
		ManifestStrategy:   domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: manifests},
		PlacementStrategy:  placement,
		State:              state,
	})
	seedTargets(t, store, targets...)
	for _, id := range held {
		seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:            domain.DeliveryID(string(depName) + ":" + string(id)),
			FulfillmentID: domain.FulfillmentID(depName),
			TargetID:      id,
			Manifests:     manifests,
			Generation:    1,
			Operation:     domain.DeliveryOperationDeliver,
			State:         domain.DeliveryStateDelivered,
		}))
	}
	events := make(chan domain.FulfillmentEvent, 16)
	return &fleetFixture{
		store:    store,
		name:     depName,
		delivery: &fleetDelivery{events: events},
		events:   events,
	}
}

// run makes one orchestration pass and returns the fulfillment after it.
func (fx *fleetFixture) run(t *testing.T) domain.Fulfillment {
	t.Helper()
	wf := newTestWorkflow(fx.store, fx.delivery, fx.events)
	rec := &simpleRecord{ctx: testContext(t), events: fx.events}
	if _, err := wf.Run(rec, domain.FulfillmentID(fx.name)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return getFulfillment(t, fx.store, fx.name)
}

func fleetTarget(id domain.TargetID, state domain.TargetState, labels map[string]string) domain.TargetInfo {
	return domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: id, Name: string(id), Type: "test", State: state, Labels: labels,
	})
}

func prodSelector() domain.PlacementStrategySpec {
	return domain.PlacementStrategySpec{
		Type:           domain.PlacementStrategySelector,
		TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}},
	}
}

func sortedTargets(ids []domain.TargetID) []domain.TargetID {
	out := slices.Clone(ids)
	slices.Sort(out)
	return out
}

// A target that stops being ready keeps the work it holds: the pass
// plans no removal for it and it stays in the placement. Readiness
// decides who gets new work, not who loses what they have.
func TestOrchestration_HeldTargetNotReady_KeepsItsPlacement(t *testing.T) {
	prod := map[string]string{"env": "prod"}
	fx := newFleetFixture(t, "held-not-ready", prodSelector(), domain.FulfillmentStateActive,
		[]domain.TargetID{"t1", "t2"},
		fleetTarget("t1", domain.TargetStateReady, prod),
		fleetTarget("t2", domain.TargetStateInitializing, prod),
	)

	f := fx.run(t)

	_, removed := fx.delivery.calls()
	if len(removed) != 0 {
		t.Fatalf("removals dispatched = %v, want none: t2 is not ready, it was not deselected", removed)
	}
	if got := sortedTargets(f.ResolvedTargets()); !slices.Equal(got, []domain.TargetID{"t1", "t2"}) {
		t.Errorf("ResolvedTargets = %v, want [t1 t2]", got)
	}
	if f.State() != domain.FulfillmentStateActive || f.Paused() {
		t.Errorf("State = %q paused=%v, want active and not paused", f.State(), f.Paused())
	}
}

// While a held target is not ready, placement neither adds it nor
// removes it, even when its labels stop matching: a removal could not
// reach it. Once it is ready again the selector decides, and the
// removal is made.
func TestOrchestration_HeldTargetNotReady_DeselectedOnlyWhenReadyAgain(t *testing.T) {
	prod := map[string]string{"env": "prod"}
	fx := newFleetFixture(t, "held-deselected", prodSelector(), domain.FulfillmentStateActive,
		[]domain.TargetID{"t1", "t2"},
		fleetTarget("t1", domain.TargetStateReady, prod),
		fleetTarget("t2", domain.TargetStateInitializing, map[string]string{"env": "dev"}),
	)

	f := fx.run(t)
	if _, removed := fx.delivery.calls(); len(removed) != 0 {
		t.Fatalf("removals dispatched while t2 is not ready = %v, want none", removed)
	}
	if got := sortedTargets(f.ResolvedTargets()); !slices.Equal(got, []domain.TargetID{"t1", "t2"}) {
		t.Fatalf("ResolvedTargets while t2 is not ready = %v, want [t1 t2]", got)
	}

	ctx := context.Background()
	tx, err := fx.store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Targets().CreateOrUpdate(ctx, fleetTarget("t2", domain.TargetStateReady, map[string]string{"env": "dev"})); err != nil {
		t.Fatalf("make t2 ready: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	f = fx.run(t)
	if _, removed := fx.delivery.calls(); !slices.Equal(removed, []domain.TargetID{"t2"}) {
		t.Fatalf("removals dispatched once t2 is ready = %v, want [t2]", removed)
	}
	if got := f.ResolvedTargets(); !slices.Equal(got, []domain.TargetID{"t1"}) {
		t.Errorf("ResolvedTargets once t2 is ready = %v, want [t1]", got)
	}
}

// A target that is not ready gets no new placement.
func TestOrchestration_NotReadyTarget_IsNotNewlyPlaced(t *testing.T) {
	prod := map[string]string{"env": "prod"}
	fx := newFleetFixture(t, "not-ready-new", prodSelector(), domain.FulfillmentStateActive,
		[]domain.TargetID{"t1"},
		fleetTarget("t1", domain.TargetStateReady, prod),
		fleetTarget("t2", domain.TargetStateInitializing, prod),
	)

	f := fx.run(t)

	delivered, _ := fx.delivery.calls()
	if slices.Contains(delivered, domain.TargetID("t2")) {
		t.Fatalf("deliveries dispatched = %v, want none to t2: it is not ready", delivered)
	}
	if got := f.ResolvedTargets(); !slices.Equal(got, []domain.TargetID{"t1"}) {
		t.Errorf("ResolvedTargets = %v, want [t1]", got)
	}
}

// A terminated target is on its way out of the fleet. It leaves the
// placement and nothing is dispatched to it: neither a delivery nor
// the removal of what it holds.
func TestOrchestration_TerminatedTarget_LeavesPlacementWithNothingDispatched(t *testing.T) {
	prod := map[string]string{"env": "prod"}
	fx := newFleetFixture(t, "terminated", prodSelector(), domain.FulfillmentStateActive,
		[]domain.TargetID{"t1", "t2"},
		fleetTarget("t1", domain.TargetStateReady, prod),
		fleetTarget("t2", domain.TargetStateTerminated, prod),
	)

	f := fx.run(t)

	delivered, removed := fx.delivery.calls()
	if len(removed) != 0 {
		t.Fatalf("removals dispatched = %v, want none: what a terminated target holds stays on it", removed)
	}
	if slices.Contains(delivered, domain.TargetID("t2")) {
		t.Fatalf("deliveries dispatched = %v, want none to t2", delivered)
	}
	if got := f.ResolvedTargets(); !slices.Equal(got, []domain.TargetID{"t1"}) {
		t.Errorf("ResolvedTargets = %v, want [t1]", got)
	}
	if f.State() != domain.FulfillmentStateActive || f.Paused() {
		t.Errorf("State = %q paused=%v, want active and not paused", f.State(), f.Paused())
	}
}

// A held target that is no longer registered cannot be reached by any
// delivery agent. It leaves the placement and no removal is dispatched
// for it.
func TestOrchestration_UnregisteredHeldTarget_LeavesPlacementWithNothingDispatched(t *testing.T) {
	prod := map[string]string{"env": "prod"}
	fx := newFleetFixture(t, "unregistered", prodSelector(), domain.FulfillmentStateActive,
		[]domain.TargetID{"t1", "gone"},
		fleetTarget("t1", domain.TargetStateReady, prod),
	)

	f := fx.run(t)

	if _, removed := fx.delivery.calls(); len(removed) != 0 {
		t.Fatalf("removals dispatched = %v, want none: target gone is not registered", removed)
	}
	if got := f.ResolvedTargets(); !slices.Equal(got, []domain.TargetID{"t1"}) {
		t.Errorf("ResolvedTargets = %v, want [t1]", got)
	}
	if f.State() != domain.FulfillmentStateActive || f.Paused() {
		t.Errorf("State = %q paused=%v, want active and not paused", f.State(), f.Paused())
	}
}

// When the only placed target stops matching the selector, what it
// holds is removed, the same as when other targets still match.
func TestOrchestration_LastTargetDeselected_IsRemoved(t *testing.T) {
	fx := newFleetFixture(t, "last-deselected", prodSelector(), domain.FulfillmentStateActive,
		[]domain.TargetID{"t1"},
		fleetTarget("t1", domain.TargetStateReady, map[string]string{"env": "dev"}),
	)

	f := fx.run(t)

	if _, removed := fx.delivery.calls(); !slices.Equal(removed, []domain.TargetID{"t1"}) {
		t.Fatalf("removals dispatched = %v, want [t1]", removed)
	}
	if got := f.ResolvedTargets(); len(got) != 0 {
		t.Errorf("ResolvedTargets = %v, want none", got)
	}
	if f.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", f.State())
	}
}

// A static placement keeps resolving while a target it holds is not
// ready: the pass does not fail on the name.
func TestOrchestration_StaticHeldTargetNotReady_StillResolves(t *testing.T) {
	fx := newFleetFixture(t, "static-not-ready",
		domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		domain.FulfillmentStateActive,
		[]domain.TargetID{"t1"},
		fleetTarget("t1", domain.TargetStateInitializing, nil),
	)

	f := fx.run(t)

	if got := f.ResolvedTargets(); !slices.Equal(got, []domain.TargetID{"t1"}) {
		t.Errorf("ResolvedTargets = %v, want [t1]", got)
	}
	if f.State() != domain.FulfillmentStateActive || f.Paused() {
		t.Errorf("State = %q paused=%v, want active and not paused", f.State(), f.Paused())
	}
	if _, removed := fx.delivery.calls(); len(removed) != 0 {
		t.Errorf("removals dispatched = %v, want none", removed)
	}
}

// Deleting a fulfillment dispatches no removal to a terminated target:
// what it holds stays, and the deletion finishes.
func TestOrchestration_DeletePipeline_SkipsTerminatedTarget(t *testing.T) {
	fx := newFleetFixture(t, "delete-terminated",
		domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		domain.FulfillmentStateDeleting,
		[]domain.TargetID{"t1", "t2"},
		fleetTarget("t1", domain.TargetStateReady, nil),
		fleetTarget("t2", domain.TargetStateTerminated, nil),
	)

	fx.run(t)

	if _, removed := fx.delivery.calls(); !slices.Equal(removed, []domain.TargetID{"t1"}) {
		t.Fatalf("removals dispatched = %v, want [t1]: t2 is terminated", removed)
	}
	if records := getDeliveries(t, fx.store, fx.name); len(records) != 0 {
		t.Errorf("delivery records after the deletion = %d, want none", len(records))
	}
}
