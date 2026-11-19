package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// defaultDeliveryCompletionRelayInterval is how often the relay looks
// for completions whose prompt signal did not get through.
const defaultDeliveryCompletionRelayInterval = 10 * time.Second

// DeliveryCompletionRelay delivers recorded delivery completions to
// their fulfillment workflows. A terminal delivery report records its
// completion in the same transaction as the terminal state; the report
// itself sends the signal promptly and clears the record. Whatever the
// report does not clear -- because the send failed, or the process
// stopped between the commit and the send -- is found here and sent
// again.
//
// A fulfillment with no running workflow instance counts as delivered:
// no workflow can observe the signal any more, so the record is
// removed rather than retried forever (see
// [domain.ErrFulfillmentNotRunning]).
type DeliveryCompletionRelay struct {
	store    domain.Store
	signaler domain.FulfillmentSignaler
	interval time.Duration
	logger   *slog.Logger
}

// DeliveryCompletionRelayOption configures a [DeliveryCompletionRelay].
type DeliveryCompletionRelayOption func(*DeliveryCompletionRelay)

// WithDeliveryCompletionRelayInterval sets how often [Run] drains the
// outbox. Defaults to ten seconds.
func WithDeliveryCompletionRelayInterval(d time.Duration) DeliveryCompletionRelayOption {
	return func(r *DeliveryCompletionRelay) {
		if d > 0 {
			r.interval = d
		}
	}
}

// WithDeliveryCompletionRelayLogger sets the logger used by [Run].
func WithDeliveryCompletionRelayLogger(l *slog.Logger) DeliveryCompletionRelayOption {
	return func(r *DeliveryCompletionRelay) {
		if l != nil {
			r.logger = l
		}
	}
}

// NewDeliveryCompletionRelay creates a relay over the given store and
// signaler. A nil signaler makes the relay a no-op.
func NewDeliveryCompletionRelay(store domain.Store, signaler domain.FulfillmentSignaler, opts ...DeliveryCompletionRelayOption) *DeliveryCompletionRelay {
	r := &DeliveryCompletionRelay{
		store:    store,
		signaler: signaler,
		interval: defaultDeliveryCompletionRelayInterval,
		logger:   slog.Default(),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run drains the outbox immediately, then on every interval, until ctx
// is cancelled. It returns when ctx is done.
func (r *DeliveryCompletionRelay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		n, err := r.Drain(ctx)
		if err != nil && ctx.Err() == nil {
			r.logger.Warn("could not deliver recorded delivery completions; will retry", "error", err)
		} else if n > 0 {
			r.logger.Info("delivered recorded delivery completions", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Drain signals every outstanding completion once. It returns how many
// obligations were delivered and removed, and the errors of the ones
// that could not be delivered. A completion whose fulfillment workflow
// is no longer running is delivered and removed.
func (r *DeliveryCompletionRelay) Drain(ctx context.Context) (int, error) {
	if r.signaler == nil {
		return 0, nil
	}
	tx, err := r.store.BeginReadOnly(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	outstanding, err := tx.DeliveryCompletions().ListOutstanding(ctx)
	_ = tx.Rollback()
	if err != nil {
		return 0, fmt.Errorf("list outstanding completions: %w", err)
	}
	if len(outstanding) == 0 {
		return 0, nil
	}

	var delivered []domain.DeliveryCompletion
	var errs []error
	for _, c := range outstanding {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		err := r.signaler.SignalFulfillmentEvent(ctx, c.FulfillmentID, c.CompletionEvent())
		switch {
		case err == nil, errors.Is(err, domain.ErrFulfillmentNotRunning):
			delivered = append(delivered, c)
		default:
			errs = append(errs, fmt.Errorf("signal completion of delivery %s: %w", c.DeliveryID, err))
		}
	}
	if len(delivered) == 0 {
		return 0, errors.Join(errs...)
	}

	wtx, err := r.store.Begin(ctx)
	if err != nil {
		return 0, errors.Join(append(errs, fmt.Errorf("begin tx: %w", err))...)
	}
	defer wtx.RollbackOnExit()
	for _, c := range delivered {
		if err := wtx.DeliveryCompletions().Delete(ctx, c.DeliveryID, c.Generation, c.Operation); err != nil {
			errs = append(errs, fmt.Errorf("remove delivered completion %s: %w", c.DeliveryID, err))
			return 0, errors.Join(errs...)
		}
	}
	if err := wtx.Commit(); err != nil {
		errs = append(errs, fmt.Errorf("commit: %w", err))
		return 0, errors.Join(errs...)
	}
	return len(delivered), errors.Join(errs...)
}
