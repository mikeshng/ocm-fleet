package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// defaultPlacementReevaluationInterval is how often [PlacementReevaluationService.Run]
// compares placement with the target fleet.
const defaultPlacementReevaluationInterval = 10 * time.Second

// PlacementReevaluationService keeps placement in step with the target
// fleet.
//
// Placement is computed against the targets that exist when a
// fulfillment's orchestration runs. A fulfillment whose orchestration
// has converged has no running workflow to notice a target that
// joined, left, or changed its labels. This service finds the
// deployments whose placement no longer matches the fleet and starts
// their orchestration.
//
// Re-evaluation changes nothing about a fulfillment. Its generation
// stays, so the pass that follows delivers to the targets that joined,
// removes from the ones that no longer match, and dispatches nothing
// to a target whose delivery is complete at that generation. Signed
// intent stays valid for the generation it was signed for, and the
// stored credentials of a target that did not move are not used again.
//
// It is a comparison of stored state, not an event: running it twice
// does nothing the second time, and a run that was missed is made up
// by the next one. Whoever changes the target fleet calls
// [PlacementReevaluationService.ReevaluatePlacement] after committing
// the change, so that placement follows at once.
// [PlacementReevaluationService.Run] repeats the comparison on an
// interval, for a change nobody announced and for a start that found
// the orchestration still finishing its previous pass.
//
// Only a deployment that has converged is looked at: active, not
// paused, and with its current generation reconciled. One that is
// still reconciling loads the fleet when its pass runs, and resuming a
// paused one starts a pass of its own. A placement that does not
// resolve against the fleet, such as a static placement that names a
// target that is no longer registered, is left as it is: no pass could
// change what it holds.
type PlacementReevaluationService struct {
	Store         domain.Store
	Orchestration domain.OrchestrationWorkflow
	// Interval is how often Run compares placement with the fleet.
	// Zero means ten seconds.
	Interval time.Duration
	// Logger defaults to [slog.Default].
	Logger *slog.Logger
}

// ReevaluatePlacement starts the orchestration of every converged
// deployment whose placement, resolved against the targets registered
// now, differs from the placement it holds. An orchestration that is
// already running is left to finish; the comparison is made again
// later. It returns the errors of the starts that failed. A nil
// Orchestration makes the call a no-op, for assemblies without
// workflows.
func (s *PlacementReevaluationService) ReevaluatePlacement(ctx context.Context) error {
	if s.Orchestration == nil {
		return nil
	}
	moved, err := s.movedFulfillments(ctx)
	if err != nil {
		return err
	}

	var errs []error
	for _, id := range moved {
		if _, err := s.Orchestration.Start(ctx, id); err != nil &&
			!errors.Is(err, domain.ErrAlreadyRunning) {
			errs = append(errs, fmt.Errorf("start orchestration of fulfillment %q: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// Run compares placement with the fleet on every interval until ctx is
// cancelled.
func (s *PlacementReevaluationService) Run(ctx context.Context) {
	interval := s.Interval
	if interval <= 0 {
		interval = defaultPlacementReevaluationInterval
	}
	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := s.ReevaluatePlacement(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("could not compare placement with the target fleet; will retry", "error", err)
		}
	}
}

// movedFulfillments returns, in deployment order, the converged
// deployment fulfillments whose placement resolves to a different
// target set than the one they hold.
func (s *PlacementReevaluationService) movedFulfillments(ctx context.Context) ([]domain.FulfillmentID, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()

	views, err := tx.Deployments().ListView(ctx)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	pool, err := tx.Targets().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list targets: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	strategies := domain.StrategyFactory{Store: s.Store}
	var ids []domain.FulfillmentID
	for i := range views {
		f := &views[i].Fulfillment
		if f.State() != domain.FulfillmentStateActive || f.Paused() ||
			f.ObservedGeneration() != f.Generation() {
			continue
		}
		moved, err := domain.PlacementMoved(ctx, strategies, f, pool)
		if err != nil {
			// The placement does not resolve against this fleet.
			continue
		}
		if moved {
			ids = append(ids, f.ID())
		}
	}
	return ids, nil
}
