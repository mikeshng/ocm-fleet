// Package deliverycompletionrepotest provides contract tests for
// [domain.DeliveryCompletionRepository] implementations.
package deliverycompletionrepotest

import (
	"context"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// Factory creates a fresh [domain.DeliveryCompletionRepository] for
// each test.
type Factory func(t *testing.T) domain.DeliveryCompletionRepository

// Run exercises the [domain.DeliveryCompletionRepository] contract.
func Run(t *testing.T, factory Factory) {
	now := time.Date(2026, 3, 4, 9, 30, 0, 0, time.UTC)

	t.Run("PutAndListRoundTrip", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		completion := domain.DeliveryCompletion{
			DeliveryID:    "f1:t1",
			FulfillmentID: "f1",
			Generation:    3,
			Operation:     domain.DeliveryOperationDeliver,
			State:         domain.DeliveryStateFailed,
			Message:       `apply object 1: conflict with "work-agent": .data.owner`,
			CreatedAt:     now,
		}
		if err := repo.Put(ctx, completion); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := repo.ListOutstanding(ctx)
		if err != nil {
			t.Fatalf("ListOutstanding: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("ListOutstanding: got %d completions, want 1: %+v", len(got), got)
		}
		assertCompletion(t, "ListOutstanding", got[0], completion)
	})

	t.Run("PutReplacesThePreviousObligation", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		first := domain.DeliveryCompletion{
			DeliveryID:    "f1:t1",
			FulfillmentID: "f1",
			Generation:    3,
			Operation:     domain.DeliveryOperationDeliver,
			State:         domain.DeliveryStateDelivered,
			Message:       "delivered to 2 targets",
			CreatedAt:     now,
		}
		second := domain.DeliveryCompletion{
			DeliveryID:    "f1:t1",
			FulfillmentID: "f1",
			Generation:    4,
			Operation:     domain.DeliveryOperationRemove,
			State:         domain.DeliveryStateFailed,
			Message:       "remove object 1: refused",
			CreatedAt:     now.Add(time.Minute),
		}
		if err := repo.Put(ctx, first); err != nil {
			t.Fatalf("Put(first): %v", err)
		}
		if err := repo.Put(ctx, second); err != nil {
			t.Fatalf("Put(second): %v", err)
		}

		got, err := repo.ListOutstanding(ctx)
		if err != nil {
			t.Fatalf("ListOutstanding: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("ListOutstanding after a newer report: got %d completions, want 1: %+v", len(got), got)
		}
		assertCompletion(t, "ListOutstanding after a newer report", got[0], second)
	})

	t.Run("DeleteRequiresTheFullCycleIdentity", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		first := domain.DeliveryCompletion{
			DeliveryID:    "f1:t1",
			FulfillmentID: "f1",
			Generation:    3,
			Operation:     domain.DeliveryOperationDeliver,
			State:         domain.DeliveryStateDelivered,
			Message:       "delivered to 2 targets",
			CreatedAt:     now,
		}
		second := domain.DeliveryCompletion{
			DeliveryID:    "f1:t1",
			FulfillmentID: "f1",
			Generation:    4,
			Operation:     domain.DeliveryOperationRemove,
			State:         domain.DeliveryStateFailed,
			Message:       "remove object 1: refused",
			CreatedAt:     now.Add(time.Minute),
		}
		if err := repo.Put(ctx, first); err != nil {
			t.Fatalf("Put(first): %v", err)
		}
		if err := repo.Put(ctx, second); err != nil {
			t.Fatalf("Put(second): %v", err)
		}

		// A deletion that names any part of the stale cycle must leave
		// the newer obligation in place.
		for _, stale := range []struct {
			name       string
			generation domain.Generation
			operation  domain.DeliveryOperation
		}{
			{"stale generation and operation", first.Generation, first.Operation},
			{"current generation and stale operation", second.Generation, first.Operation},
			{"stale generation and current operation", first.Generation, second.Operation},
		} {
			if err := repo.Delete(ctx, second.DeliveryID, stale.generation, stale.operation); err != nil {
				t.Fatalf("Delete(%s): %v", stale.name, err)
			}
			got, err := repo.ListOutstanding(ctx)
			if err != nil {
				t.Fatalf("ListOutstanding after Delete(%s): %v", stale.name, err)
			}
			if len(got) != 1 {
				t.Fatalf("Delete(%s) removed the newer obligation: got %d completions, want 1", stale.name, len(got))
			}
			assertCompletion(t, "ListOutstanding after Delete("+stale.name+")", got[0], second)
		}

		if err := repo.Delete(ctx, second.DeliveryID, second.Generation, second.Operation); err != nil {
			t.Fatalf("Delete(current cycle): %v", err)
		}
		got, err := repo.ListOutstanding(ctx)
		if err != nil {
			t.Fatalf("ListOutstanding after Delete(current cycle): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ListOutstanding after Delete(current cycle): got %d completions, want 0: %+v", len(got), got)
		}
	})

	t.Run("ListIsOldestFirst", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		later := domain.DeliveryCompletion{
			DeliveryID:    "f1:t2",
			FulfillmentID: "f1",
			Generation:    3,
			Operation:     domain.DeliveryOperationDeliver,
			State:         domain.DeliveryStateDelivered,
			Message:       "delivered",
			CreatedAt:     now.Add(time.Minute),
		}
		// An earlier instant in a zone ahead of UTC: stored as the same
		// instant it was created at, it still sorts before "later".
		earlier := domain.DeliveryCompletion{
			DeliveryID:    "f1:t1",
			FulfillmentID: "f1",
			Generation:    3,
			Operation:     domain.DeliveryOperationDeliver,
			State:         domain.DeliveryStateDelivered,
			Message:       "delivered",
			CreatedAt:     now.Add(-time.Hour).In(time.FixedZone("UTC+2", 2*60*60)),
		}
		// Insert the newest first so the order can only come from the
		// repository.
		if err := repo.Put(ctx, later); err != nil {
			t.Fatalf("Put(later): %v", err)
		}
		if err := repo.Put(ctx, earlier); err != nil {
			t.Fatalf("Put(earlier): %v", err)
		}

		got, err := repo.ListOutstanding(ctx)
		if err != nil {
			t.Fatalf("ListOutstanding: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("ListOutstanding: got %d completions, want 2: %+v", len(got), got)
		}
		if got[0].DeliveryID != earlier.DeliveryID || got[1].DeliveryID != later.DeliveryID {
			t.Fatalf("ListOutstanding order = [%s %s], want [%s %s]",
				got[0].DeliveryID, got[1].DeliveryID, earlier.DeliveryID, later.DeliveryID)
		}
		if !got[0].CreatedAt.Equal(earlier.CreatedAt) {
			t.Errorf("first CreatedAt = %s, want %s", got[0].CreatedAt, earlier.CreatedAt)
		}
	})
}

func assertCompletion(t *testing.T, name string, got, want domain.DeliveryCompletion) {
	t.Helper()
	if got.DeliveryID != want.DeliveryID {
		t.Errorf("%s: DeliveryID = %q, want %q", name, got.DeliveryID, want.DeliveryID)
	}
	if got.FulfillmentID != want.FulfillmentID {
		t.Errorf("%s: FulfillmentID = %q, want %q", name, got.FulfillmentID, want.FulfillmentID)
	}
	if got.Generation != want.Generation {
		t.Errorf("%s: Generation = %d, want %d", name, got.Generation, want.Generation)
	}
	if got.Operation != want.Operation {
		t.Errorf("%s: Operation = %q, want %q", name, got.Operation, want.Operation)
	}
	if got.State != want.State {
		t.Errorf("%s: State = %q, want %q", name, got.State, want.State)
	}
	if got.Message != want.Message {
		t.Errorf("%s: Message = %q, want %q", name, got.Message, want.Message)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("%s: CreatedAt = %s, want %s", name, got.CreatedAt, want.CreatedAt)
	}
}
