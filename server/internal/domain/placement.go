package domain

import (
	"context"
	"fmt"
)

// StaticPlacement selects an explicit set of targets by ID.
type StaticPlacement struct {
	Targets []TargetID
}

func (s *StaticPlacement) Resolve(_ context.Context, pool []PlacementTarget) ([]PlacementTarget, error) {
	index := make(map[TargetID]PlacementTarget, len(pool))
	for _, t := range pool {
		index[t.ID] = t
	}

	result := make([]PlacementTarget, 0, len(s.Targets))
	for _, id := range s.Targets {
		t, ok := index[id]
		if !ok {
			return nil, fmt.Errorf("%w: %w: target %q not found in pool", ErrPlacementUnresolved, ErrNotFound, id)
		}
		result = append(result, t)
	}
	return result, nil
}

// AllPlacement selects every target in the pool.
type AllPlacement struct{}

func (a *AllPlacement) Resolve(_ context.Context, pool []PlacementTarget) ([]PlacementTarget, error) {
	result := make([]PlacementTarget, len(pool))
	copy(result, pool)
	return result, nil
}

// SelectorPlacement filters the pool by label matching. All labels in the
// selector must be present and equal on the target.
type SelectorPlacement struct {
	Selector TargetSelector
}

func (s *SelectorPlacement) Resolve(_ context.Context, pool []PlacementTarget) ([]PlacementTarget, error) {
	var result []PlacementTarget
	for _, t := range pool {
		if matchLabels(t.Labels, s.Selector.MatchLabels) {
			result = append(result, t)
		}
	}
	return result, nil
}

func matchLabels(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// PlacementMoved reports whether a fulfillment's placement, resolved
// against pool the way an orchestration pass resolves it (see
// [PlacementPool]), is a different set of targets than the placement
// the fulfillment holds. It returns the resolution error when the
// placement does not resolve against pool.
func PlacementMoved(ctx context.Context, strategies StrategyFactory, f *Fulfillment, pool []TargetInfo) (bool, error) {
	strategy, err := strategies.PlacementStrategy(f.PlacementStrategy())
	if err != nil {
		return false, err
	}
	placementPool := NewPlacementPool(pool, f.ResolvedTargets())
	resolved, err := strategy.Resolve(ctx, placementPool.Candidates)
	if err != nil {
		return false, err
	}

	held := make(map[TargetID]struct{}, len(f.ResolvedTargets()))
	for _, id := range f.ResolvedTargets() {
		held[id] = struct{}{}
	}
	want := make(map[TargetID]struct{}, len(resolved))
	for _, t := range placementPool.Settle(resolved) {
		want[t.ID] = struct{}{}
	}
	if len(want) != len(held) {
		return true, nil
	}
	for id := range want {
		if _, ok := held[id]; !ok {
			return true, nil
		}
	}
	return false, nil
}
