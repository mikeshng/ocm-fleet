package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// TestDeliveryGetForUpdateFencesReads proves the row fence the delivery
// report service relies on: a second transaction that reads the same
// delivery with GetForUpdate waits for the first to finish, so it cannot
// decide on superseded state and then write over the committed report.
// SQLite does not need the lock (write transactions are serialized), so
// this proof is specific to the backend that does.
func TestDeliveryGetForUpdateFencesReads(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

	seed, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer seed.RollbackOnExit()
	if err := seed.Deliveries().Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "f-fence:t1",
		FulfillmentID: "f-fence",
		TargetID:      "t1",
		Operation:     domain.DeliveryOperationDeliver,
		Generation:    1,
		State:         domain.DeliveryStateProgressing,
		CreatedAt:     now,
		UpdatedAt:     now,
	})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// The first reporter reads the row and changes it, but has not
	// committed: its fence is held for the rest of the transaction.
	tx1, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx1.RollbackOnExit()
	first, err := tx1.Deliveries().GetForUpdate(ctx, "f-fence:t1")
	if err != nil {
		t.Fatalf("first GetForUpdate: %v", err)
	}
	if err := first.ApplyResult(domain.DeliveryResult{State: domain.DeliveryStateDelivered}, now.Add(time.Minute)); err != nil {
		t.Fatalf("ApplyResult: %v", err)
	}
	if err := tx1.Deliveries().Put(ctx, first); err != nil {
		t.Fatalf("Put: %v", err)
	}

	type observation struct {
		state domain.DeliveryState
		err   error
	}
	observed := make(chan observation, 1)
	started := make(chan struct{})
	go func() {
		tx2, err := store.Begin(ctx)
		if err != nil {
			observed <- observation{err: err}
			return
		}
		defer tx2.RollbackOnExit()
		close(started)
		second, err := tx2.Deliveries().GetForUpdate(ctx, "f-fence:t1")
		if err != nil {
			observed <- observation{err: err}
			return
		}
		observed <- observation{state: second.State()}
	}()
	<-started

	select {
	case o := <-observed:
		t.Fatalf("the second read finished while the first transaction still held the row (state=%q, err=%v); a report could decide on superseded state", o.state, o.err)
	case <-time.After(250 * time.Millisecond):
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	select {
	case o := <-observed:
		if o.err != nil {
			t.Fatalf("second GetForUpdate: %v", o.err)
		}
		if o.state != domain.DeliveryStateDelivered {
			t.Errorf("second read saw %q, want %q: it must read the committed report", o.state, domain.DeliveryStateDelivered)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second read did not finish after the first transaction committed")
	}
}
