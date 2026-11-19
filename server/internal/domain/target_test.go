package domain_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestToPlacementTarget_OmitsProperties(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "t1",
		Name:       "cluster-a",
		Labels:     map[string]string{"env": "prod"},
		Properties: map[string]string{"region": "us-east"},
	})
	got := domain.ToPlacementTarget(target)
	if got.ID != target.ID() || got.Name != target.Name() {
		t.Errorf("ID or Name changed: got %+v", got)
	}
	if got.Labels["env"] != "prod" {
		t.Errorf("Labels[env] = %q, want prod", got.Labels["env"])
	}
	// PlacementTarget has no Properties field; conversion omits them by type.
}

func TestToPlacementTarget_PropagatesAcceptedManifestTypes(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "t1",
		Name:                  "cluster-a",
		AcceptedManifestTypes: []domain.ManifestType{"api.kind.cluster", "kubernetes"},
	})
	got := domain.ToPlacementTarget(target)
	if len(got.AcceptedManifestTypes) != 2 {
		t.Fatalf("len(AcceptedManifestTypes) = %d, want 2", len(got.AcceptedManifestTypes))
	}
	if got.AcceptedManifestTypes[0] != "api.kind.cluster" || got.AcceptedManifestTypes[1] != "kubernetes" {
		t.Errorf("AcceptedManifestTypes = %v, want [api.kind.cluster, kubernetes]", got.AcceptedManifestTypes)
	}

	// Verify it's a copy, not a shared slice.
	got.AcceptedManifestTypes[0] = "mutated"
	if target.AcceptedManifestTypes()[0] == "mutated" {
		t.Error("AcceptedManifestTypes should be copied, not shared")
	}
}

func TestPlacementTargets_PreservesOrderAndLength(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "a", Name: "n1", State: domain.TargetStateReady, Labels: map[string]string{"x": "1"}}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "b", Name: "n2", State: domain.TargetStateReady, Labels: map[string]string{"y": "2"}}),
	}
	got := domain.PlacementTargets(pool)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("order or IDs wrong: got [%s, %s]", got[0].ID, got[1].ID)
	}
}

func TestPlacementTargets_FiltersNonReadyTargets(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "a", Name: "n1", State: domain.TargetStateReady}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "b", Name: "n2", State: domain.TargetStateInitializing}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "d", Name: "n4", State: domain.TargetStateTerminated}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "e", Name: "n5", State: domain.TargetStateDiscovered}),
	}
	got := domain.PlacementTargets(pool)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1 (only ready targets)", len(got))
	}
	if got[0].ID != "a" {
		t.Errorf("got[0].ID = %s, want a", got[0].ID)
	}
}

func TestPlacementTargets_EmptyStateIsEligible(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "a", Name: "n1"}),
	}
	got := domain.PlacementTargets(pool)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1 (empty state treated as ready)", len(got))
	}
}

func TestResolvedTargetInfos_LookupAndOrder(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "c1", Labels: map[string]string{"env": "prod"}, Properties: map[string]string{"region": "us"}}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "c2", Labels: map[string]string{"env": "staging"}, Properties: map[string]string{"region": "eu"}}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t3", Name: "c3", Labels: map[string]string{"env": "prod"}, Properties: nil}),
	}
	resolved := []domain.PlacementTarget{
		{ID: "t3", Name: "c3", Labels: map[string]string{"env": "prod"}},
		{ID: "t1", Name: "c1", Labels: map[string]string{"env": "prod"}},
	}
	got := domain.ResolvedTargetInfos(resolved, pool)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].ID() != "t3" || got[1].ID() != "t1" {
		t.Errorf("order wrong: got [%s, %s], want [t3, t1]", got[0].ID(), got[1].ID())
	}
	if got[1].Properties() == nil || got[1].Properties()["region"] != "us" {
		t.Errorf("full TargetInfo from pool: got[1].Properties = %v, want map with region=us", got[1].Properties())
	}
}

func TestResolvedTargetInfos_OmitsMissingFromPool(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "c1", Labels: nil}),
	}
	resolved := []domain.PlacementTarget{
		{ID: "t1", Name: "c1", Labels: nil},
		{ID: "missing", Name: "m", Labels: nil},
	}
	got := domain.ResolvedTargetInfos(resolved, pool)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1 (missing ID omitted)", len(got))
	}
	if got[0].ID() != "t1" {
		t.Errorf("got[0].ID = %s, want t1", got[0].ID())
	}
}

func placementIDs(targets []domain.PlacementTarget) []domain.TargetID {
	ids := make([]domain.TargetID, len(targets))
	for i, t := range targets {
		ids[i] = t.ID
	}
	return ids
}

func TestNewPlacementPool_CandidatesAndRetained(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "ready", State: domain.TargetStateReady}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "stateless"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "held-waiting", State: domain.TargetStateInitializing}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new-waiting", State: domain.TargetStateInitializing}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "held-draining", State: domain.TargetStateDraining}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "held-terminated", State: domain.TargetStateTerminated}),
	}
	held := []domain.TargetID{"ready", "held-waiting", "held-draining", "held-terminated", "unregistered"}

	got := domain.NewPlacementPool(pool, held)

	if ids := placementIDs(got.Candidates); !slices.Equal(ids, []domain.TargetID{"ready", "stateless", "held-waiting", "held-draining"}) {
		t.Errorf("Candidates = %v, want [ready stateless held-waiting held-draining]", ids)
	}
	if ids := placementIDs(got.Retained); !slices.Equal(ids, []domain.TargetID{"held-waiting", "held-draining"}) {
		t.Errorf("Retained = %v, want [held-waiting held-draining]", ids)
	}
}

func TestPlacementPool_Settle_AddsRetainedTargetsLeftOut(t *testing.T) {
	pool := domain.PlacementPool{Retained: []domain.PlacementTarget{{ID: "kept"}, {ID: "picked"}}}

	got := pool.Settle([]domain.PlacementTarget{{ID: "a"}, {ID: "picked"}})

	if ids := placementIDs(got); !slices.Equal(ids, []domain.TargetID{"a", "picked", "kept"}) {
		t.Errorf("Settle = %v, want [a picked kept]", ids)
	}
	if ids := placementIDs((domain.PlacementPool{}).Settle([]domain.PlacementTarget{{ID: "a"}})); !slices.Equal(ids, []domain.TargetID{"a"}) {
		t.Errorf("Settle with nothing retained = %v, want [a]", ids)
	}
}

func TestComputeTargetDelta_RemovesOnlyTargetsThatCanBeReached(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "kept", State: domain.TargetStateReady}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "added", State: domain.TargetStateReady}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "deselected", State: domain.TargetStateReady}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "terminated", State: domain.TargetStateTerminated}),
	}
	previous := []domain.TargetID{"kept", "deselected", "terminated", "unregistered"}
	resolved := []domain.TargetInfo{pool[0], pool[1]}

	delta := domain.ComputeTargetDelta(previous, resolved, pool)

	ids := func(targets []domain.TargetInfo) []domain.TargetID {
		out := make([]domain.TargetID, len(targets))
		for i, t := range targets {
			out[i] = t.ID()
		}
		return out
	}
	if got := ids(delta.Unchanged); !slices.Equal(got, []domain.TargetID{"kept"}) {
		t.Errorf("Unchanged = %v, want [kept]", got)
	}
	if got := ids(delta.Added); !slices.Equal(got, []domain.TargetID{"added"}) {
		t.Errorf("Added = %v, want [added]", got)
	}
	if got := ids(delta.Removed); !slices.Equal(got, []domain.TargetID{"deselected"}) {
		t.Errorf("Removed = %v, want [deselected]: a terminated or unregistered target gets no removal", got)
	}
}

// --- VerifyTargetMatch tests (OME-291) ---

func TestVerifyTargetMatch_ExactMatch(t *testing.T) {
	a := domain.NewTargetInfo("t1", "kind", "Cluster A", domain.TargetStateReady,
		map[string]string{"env": "prod"}, map[string]string{"region": "us"},
		[]domain.ManifestType{"clusters", "trust"})
	b := domain.NewTargetInfo("t1", "kind", "Cluster A", domain.TargetStateReady,
		map[string]string{"env": "prod"}, map[string]string{"region": "us"},
		[]domain.ManifestType{"clusters", "trust"})
	if err := domain.VerifyTargetMatch(a, b); err != nil {
		t.Fatalf("exact match should succeed: %v", err)
	}
}

func TestVerifyTargetMatch_NilAndEmptyEquivalent(t *testing.T) {
	a := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady, nil, nil, nil)
	b := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady,
		map[string]string{}, map[string]string{}, []domain.ManifestType{})
	if err := domain.VerifyTargetMatch(a, b); err != nil {
		t.Fatalf("nil vs empty should be equivalent: %v", err)
	}
}

func TestVerifyTargetMatch_TypeDrift(t *testing.T) {
	a := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady, nil, nil, nil)
	b := domain.NewTargetInfo("t1", "kubernetes", "n", domain.TargetStateReady, nil, nil, nil)
	err := domain.VerifyTargetMatch(a, b)
	if err == nil {
		t.Fatal("expected error on type drift")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestVerifyTargetMatch_NameDrift(t *testing.T) {
	a := domain.NewTargetInfo("t1", "kind", "Name A", domain.TargetStateReady, nil, nil, nil)
	b := domain.NewTargetInfo("t1", "kind", "Name B", domain.TargetStateReady, nil, nil, nil)
	err := domain.VerifyTargetMatch(a, b)
	if err == nil {
		t.Fatal("expected error on name drift")
	}
}

func TestVerifyTargetMatch_StateDrift(t *testing.T) {
	a := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady, nil, nil, nil)
	b := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateInitializing, nil, nil, nil)
	err := domain.VerifyTargetMatch(a, b)
	if err == nil {
		t.Fatal("expected error on state drift")
	}
}

func TestVerifyTargetMatch_ManifestTypeDrift(t *testing.T) {
	a := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady, nil, nil,
		[]domain.ManifestType{"clusters"})
	b := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady, nil, nil,
		[]domain.ManifestType{"databases"})
	err := domain.VerifyTargetMatch(a, b)
	if err == nil {
		t.Fatal("expected error on manifest type drift")
	}
}

func TestVerifyTargetMatch_LabelsDrift(t *testing.T) {
	a := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady,
		map[string]string{"env": "prod"}, nil, nil)
	b := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady,
		map[string]string{"env": "stage"}, nil, nil)
	err := domain.VerifyTargetMatch(a, b)
	if err == nil {
		t.Fatal("expected error on labels drift")
	}
}

func TestVerifyTargetMatch_PropertiesDrift(t *testing.T) {
	a := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady, nil,
		map[string]string{"region": "us"}, nil)
	b := domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady, nil,
		map[string]string{"region": "eu"}, nil)
	err := domain.VerifyTargetMatch(a, b)
	if err == nil {
		t.Fatal("expected error on properties drift")
	}
}
