package application_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

func openTestStore(t *testing.T) domain.Store {
	t.Helper()
	return &sqlite.Store{DB: sqlite.OpenTestDB(t)}
}

func listCompletionObligations(t *testing.T, store domain.Store) []domain.DeliveryCompletion {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	outstanding, err := tx.DeliveryCompletions().ListOutstanding(context.Background())
	if err != nil {
		t.Fatalf("list outstanding completions: %v", err)
	}
	return outstanding
}

type failingSignal struct{ err error }

func (f failingSignal) SignalFulfillmentEvent(context.Context, domain.FulfillmentID, domain.FulfillmentEvent) error {
	return f.err
}

func seedProgressingDelivery(t *testing.T, store domain.Store) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))
}

func TestDeliveryReportService_ReportResult_ClearsCompletionAfterSignal(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	seedProgressingDelivery(t, store)

	if err := svc.ReportResult(ctx, "del-1", 0, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
	}); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	if got := listCompletionObligations(t, store); len(got) != 0 {
		t.Fatalf("expected no outstanding completion after a successful signal, got %+v", got)
	}
}

func TestDeliveryReportService_ReportResult_KeepsCompletionWhenSignalFails(t *testing.T) {
	store := openTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	svc := application.NewDeliveryReportService(store, failingSignal{err: errors.New("backend down")})
	err := svc.ReportResult(context.Background(), "del-1", 0, domain.DeliveryResult{
		State:     domain.DeliveryStateFailed,
		Message:   "the cluster refused the manifest",
		Operation: domain.DeliveryOperationDeliver,
	})
	if err == nil {
		t.Fatal("expected ReportResult to report the failed signal")
	}

	outstanding := listCompletionObligations(t, store)
	if len(outstanding) != 1 {
		t.Fatalf("expected 1 outstanding completion, got %d", len(outstanding))
	}
	c := outstanding[0]
	if c.DeliveryID != "del-1" || c.FulfillmentID != "f1" || c.State != domain.DeliveryStateFailed {
		t.Errorf("completion = %+v, want del-1/f1/failed", c)
	}
	if c.Message != "the cluster refused the manifest" {
		t.Errorf("message = %q, want the reported reason", c.Message)
	}

	// A fresh relay, as after a restart, delivers the recorded completion
	// and removes it.
	sig := &recordingSignal{}
	relay := application.NewDeliveryCompletionRelay(store, sig)
	n, err := relay.Drain(context.Background())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("delivered %d completions, want 1", n)
	}
	signals := sig.snapshot()
	if len(signals) != 1 || signals[0].Event.DeliveryCompleted == nil {
		t.Fatalf("expected one completion signal, got %+v", signals)
	}
	completed := signals[0].Event.DeliveryCompleted
	if completed.DeliveryID != "del-1" || completed.Result.State != domain.DeliveryStateFailed {
		t.Errorf("completion signal = %+v, want del-1/failed", completed)
	}
	if completed.Result.Message != "the cluster refused the manifest" {
		t.Errorf("completion message = %q, want the reported reason", completed.Result.Message)
	}
	if got := listCompletionObligations(t, store); len(got) != 0 {
		t.Fatalf("expected the obligation to be removed, got %+v", got)
	}
}

func TestDeliveryCompletionRelay_NotRunningCountsAsDelivered(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.DeliveryCompletions().Put(ctx, domain.DeliveryCompletion{
		DeliveryID: "del-1", FulfillmentID: "f1", Generation: 2,
		Operation: domain.DeliveryOperationDeliver, State: domain.DeliveryStateDelivered,
		CreatedAt: time.Now().UTC().Truncate(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	relay := application.NewDeliveryCompletionRelay(store, failingSignal{err: domain.ErrFulfillmentNotRunning})
	n, err := relay.Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("delivered %d completions, want 1", n)
	}
	if got := listCompletionObligations(t, store); len(got) != 0 {
		t.Fatalf("expected the obligation to be removed, got %+v", got)
	}
}

func TestDeliveryCompletionRelay_TransientErrorKeepsObligation(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.DeliveryCompletions().Put(ctx, domain.DeliveryCompletion{
		DeliveryID: "del-1", FulfillmentID: "f1", Generation: 2,
		Operation: domain.DeliveryOperationDeliver, State: domain.DeliveryStateDelivered,
		CreatedAt: time.Now().UTC().Truncate(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	relay := application.NewDeliveryCompletionRelay(store, failingSignal{err: errors.New("backend down")})
	n, err := relay.Drain(ctx)
	if err == nil {
		t.Fatal("expected Drain to report the failed signal")
	}
	if n != 0 {
		t.Fatalf("delivered %d completions, want 0", n)
	}
	outstanding := listCompletionObligations(t, store)
	if len(outstanding) != 1 {
		t.Fatalf("expected the obligation to survive, got %+v", outstanding)
	}
	if outstanding[0].DeliveryID != "del-1" || outstanding[0].FulfillmentID != "f1" {
		t.Errorf("obligation = %+v, want del-1/f1", outstanding[0])
	}
}

// retryingSignal fails its first calls and then delivers, counting every
// attempt so a test can see how often the relay tried.
type retryingSignal struct {
	mu      sync.Mutex
	failFor int
	calls   int
}

func (s *retryingSignal) SignalFulfillmentEvent(context.Context, domain.FulfillmentID, domain.FulfillmentEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failFor {
		return errors.New("backend down")
	}
	return nil
}

func (s *retryingSignal) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// Run is the loop production starts: it drains at once and then on every
// interval until its context is cancelled. A completion whose first signal
// fails must be retried by that loop, not merely by a second Drain call.
func TestDeliveryCompletionRelay_RunRetriesAndStopsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := openTestStore(t)
		ctx := context.Background()
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.DeliveryCompletions().Put(ctx, domain.DeliveryCompletion{
			DeliveryID: "del-1", FulfillmentID: "f1", Generation: 2,
			Operation: domain.DeliveryOperationDeliver, State: domain.DeliveryStateDelivered,
			CreatedAt: time.Now().UTC().Truncate(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		signal := &retryingSignal{failFor: 1}
		relay := application.NewDeliveryCompletionRelay(store, signal,
			application.WithDeliveryCompletionRelayInterval(time.Minute))

		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			relay.Run(runCtx)
			close(done)
		}()

		// The first drain runs at once and fails; the obligation stays.
		synctest.Wait()
		if got := signal.count(); got != 1 {
			t.Fatalf("signalled %d completions before the first interval, want 1", got)
		}
		if outstanding := listCompletionObligations(t, store); len(outstanding) != 1 {
			t.Fatalf("outstanding completions after the failed signal = %+v, want the obligation kept", outstanding)
		}

		// With every goroutine blocked, the bubble's clock reaches the
		// interval, the loop drains again, and the retry lands.
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if got := signal.count(); got != 2 {
			t.Fatalf("signalled %d completions after one interval, want 2", got)
		}
		if outstanding := listCompletionObligations(t, store); len(outstanding) != 0 {
			t.Fatalf("outstanding completions after the retry = %+v, want none", outstanding)
		}

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Run did not return after its context was cancelled")
		}
	})
}
