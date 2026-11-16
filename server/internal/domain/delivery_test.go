package domain

import (
	"errors"
	"testing"
	"time"
)

func TestDeliveryState_IsTerminal(t *testing.T) {
	terminal := []DeliveryState{
		DeliveryStateDelivered,
		DeliveryStateFailed,
		DeliveryStatePartial,
		DeliveryStateAuthFailed,
	}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%q: want terminal", s)
		}
	}

	nonTerminal := []DeliveryState{
		DeliveryStatePending,
		DeliveryStateAccepted,
		DeliveryStateProgressing,
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%q: want non-terminal", s)
		}
	}
}

func TestDelivery_TransitionTo(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	validTransitions := []struct {
		from, to DeliveryState
	}{
		{DeliveryStatePending, DeliveryStateAccepted},
		{DeliveryStatePending, DeliveryStateProgressing},
		{DeliveryStatePending, DeliveryStateFailed},
		{DeliveryStatePending, DeliveryStateAuthFailed},
		{DeliveryStateAccepted, DeliveryStateProgressing},
		{DeliveryStateAccepted, DeliveryStateDelivered},
		{DeliveryStateAccepted, DeliveryStateFailed},
		{DeliveryStateProgressing, DeliveryStateDelivered},
		{DeliveryStateProgressing, DeliveryStateFailed},
		{DeliveryStateProgressing, DeliveryStatePartial},
	}

	for _, tt := range validTransitions {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			d := DeliveryFromSnapshot(DeliverySnapshot{State: tt.from, CreatedAt: now, UpdatedAt: now})
			if err := d.TransitionTo(tt.to, later); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d.state != tt.to {
				t.Errorf("State = %q, want %q", d.state, tt.to)
			}
			if d.updatedAt != later {
				t.Errorf("UpdatedAt = %v, want %v", d.updatedAt, later)
			}
		})
	}
}

func TestDelivery_TransitionTo_FromTerminal_Fails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	terminalStates := []DeliveryState{
		DeliveryStateDelivered,
		DeliveryStateFailed,
		DeliveryStatePartial,
		DeliveryStateAuthFailed,
	}
	for _, from := range terminalStates {
		t.Run(string(from)+"->progressing", func(t *testing.T) {
			d := DeliveryFromSnapshot(DeliverySnapshot{State: from, CreatedAt: now, UpdatedAt: now})
			err := d.TransitionTo(DeliveryStateProgressing, later)
			if err == nil {
				t.Fatal("expected error for terminal -> non-terminal transition")
			}
			if !errors.Is(err, ErrIllegalStateTransition) {
				t.Errorf("error = %v, want ErrIllegalStateTransition", err)
			}
			if d.state != from {
				t.Errorf("State changed to %q, want %q (unchanged)", d.state, from)
			}
			if d.updatedAt != now {
				t.Error("UpdatedAt changed on failed transition")
			}
		})
	}
}

func TestDelivery_TransitionTo_NoOp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	d := DeliveryFromSnapshot(DeliverySnapshot{State: DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now})
	if err := d.TransitionTo(DeliveryStateProgressing, later); err != nil {
		t.Fatalf("same-state transition should be a no-op, got: %v", err)
	}
	if d.updatedAt != now {
		t.Error("UpdatedAt changed on no-op transition")
	}
}

func TestDelivery_TransitionTo_UnknownState_Fails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	d := DeliveryFromSnapshot(DeliverySnapshot{State: DeliveryStatePending, CreatedAt: now, UpdatedAt: now})
	err := d.TransitionTo(DeliveryState("bogus"), later)
	if err == nil {
		t.Fatal("expected error for unknown target state")
	}
	if !errors.Is(err, ErrIllegalStateTransition) {
		t.Errorf("error = %v, want ErrIllegalStateTransition", err)
	}
	if d.state != DeliveryStatePending {
		t.Errorf("State changed to %q, want %q (unchanged)", d.state, DeliveryStatePending)
	}
	if d.updatedAt != now {
		t.Error("UpdatedAt changed on failed transition")
	}
}

func TestDelivery_TransitionTo_Backward_Fails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)

	d := DeliveryFromSnapshot(DeliverySnapshot{State: DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now})
	err := d.TransitionTo(DeliveryStatePending, later)
	if err == nil {
		t.Fatal("expected error for backward transition")
	}
	if !errors.Is(err, ErrIllegalStateTransition) {
		t.Errorf("error = %v, want ErrIllegalStateTransition", err)
	}
}

func TestDelivery_Redispatch(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)

	t.Run("advances generation and resets state", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			ID:         "d1",
			State:      DeliveryStateDelivered,
			Manifests:  []Manifest{{Raw: []byte("old")}},
			Generation: 1,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
		newManifests := []Manifest{{Raw: []byte("new")}}
		if err := d.Redispatch(newManifests, 2, later); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
		if d.generation != 2 {
			t.Errorf("Generation = %d, want 2", d.generation)
		}
		if len(d.manifests) != 1 || string(d.manifests[0].Raw) != "new" {
			t.Errorf("Manifests not replaced")
		}
		if d.updatedAt != later {
			t.Errorf("UpdatedAt = %v, want %v", d.updatedAt, later)
		}
	})

	t.Run("same generation put cycle fails", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			State:      DeliveryStateDelivered,
			Generation: 3,
			Operation:  DeliveryOperationDeliver,
			UpdatedAt:  now,
		})
		err := d.Redispatch(nil, 3, later)
		if err == nil {
			t.Fatal("expected error for same generation put cycle")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Errorf("error = %v, want ErrIllegalStateTransition", err)
		}
		if d.state != DeliveryStateDelivered {
			t.Errorf("State changed on failed redispatch")
		}
	})

	t.Run("same generation turns a removal cycle into a put cycle", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			ID:         "d1",
			State:      DeliveryStateDelivered,
			Generation: 3,
			Operation:  DeliveryOperationRemove,
			Manifests:  []Manifest{{Raw: []byte("removed")}},
			Message:    "remove object: forbidden",
			Attempts:   2,
			UpdatedAt:  now,
		})
		newManifests := []Manifest{{Raw: []byte("re-added")}}
		if err := d.Redispatch(newManifests, 3, later); err != nil {
			t.Fatalf("Redispatch: %v", err)
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
		if d.operation != DeliveryOperationDeliver {
			t.Errorf("Operation = %q, want %q", d.operation, DeliveryOperationDeliver)
		}
		if d.generation != 3 {
			t.Errorf("Generation = %d, want 3 (unchanged)", d.generation)
		}
		if len(d.manifests) != 1 || string(d.manifests[0].Raw) != "re-added" {
			t.Errorf("Manifests not replaced")
		}
		if d.message != "" || d.attempts != 0 {
			t.Errorf("removal failure details survived into the put cycle: Message = %q, Attempts = %d", d.message, d.attempts)
		}
	})

	t.Run("lower generation fails", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{State: DeliveryStateProgressing, Generation: 5, UpdatedAt: now})
		err := d.Redispatch(nil, 3, later)
		if err == nil {
			t.Fatal("expected error for lower generation")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Errorf("error = %v, want ErrIllegalStateTransition", err)
		}
	})

	t.Run("works from non-terminal state", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{State: DeliveryStateProgressing, Generation: 1, UpdatedAt: now})
		newManifests := []Manifest{{Raw: []byte("v2")}}
		if err := d.Redispatch(newManifests, 2, later); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
	})
}

func TestDelivery_Withdraw(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)

	t.Run("resets terminal delivery to pending", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			ID:         "d1",
			State:      DeliveryStateDelivered,
			Manifests:  []Manifest{{Raw: []byte("keep")}},
			Generation: 5,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
		modified, err := d.Withdraw(7, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true")
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
		if d.generation != 7 {
			t.Errorf("Generation = %d, want 7", d.generation)
		}
		if len(d.manifests) != 1 || string(d.manifests[0].Raw) != "keep" {
			t.Error("Manifests changed during withdraw")
		}
		if d.updatedAt != later {
			t.Errorf("UpdatedAt = %v, want %v", d.updatedAt, later)
		}
	})

	t.Run("terminal same generation resets to pending", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			State:      DeliveryStateDelivered,
			Generation: 5,
			UpdatedAt:  now,
		})
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true for same-gen terminal withdraw")
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
		if d.generation != 5 {
			t.Errorf("Generation = %d, want 5", d.generation)
		}
	})

	t.Run("terminal same generation removal is not resurrected", func(t *testing.T) {
		for _, state := range []DeliveryState{
			DeliveryStateDelivered,
			DeliveryStatePartial,
			DeliveryStateAuthFailed,
		} {
			t.Run(string(state), func(t *testing.T) {
				d := DeliveryFromSnapshot(DeliverySnapshot{
					State: state, Generation: 5, Operation: DeliveryOperationRemove, UpdatedAt: now,
				})
				modified, err := d.Withdraw(5, later)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if modified {
					t.Fatal("expected modified = false: a removal that finished at this generation is not re-dispatched")
				}
				if d.state != state {
					t.Errorf("State = %q, want %q", d.state, state)
				}
				if d.operation != DeliveryOperationRemove {
					t.Errorf("Operation = %q, want %q", d.operation, DeliveryOperationRemove)
				}
				if d.updatedAt != now {
					t.Error("UpdatedAt changed on unmodified withdraw")
				}
			})
		}
	})

	t.Run("failed same generation removal retries preserving details", func(t *testing.T) {
		const reason = "delete object 1: connection refused"
		d := DeliveryFromSnapshot(DeliverySnapshot{
			State: DeliveryStateFailed, Generation: 5, Operation: DeliveryOperationRemove,
			Message: reason, Attempts: 3, UpdatedAt: now,
		})
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true: a failed removal is retried at the same generation")
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
		if d.operation != DeliveryOperationRemove {
			t.Errorf("Operation = %q, want %q", d.operation, DeliveryOperationRemove)
		}
		if d.message != reason {
			t.Errorf("Message = %q, want %q (preserved for the API)", d.message, reason)
		}
		if d.attempts != 3 {
			t.Errorf("Attempts = %d, want 3 (preserved for the backoff)", d.attempts)
		}
		if d.updatedAt != later {
			t.Errorf("UpdatedAt = %v, want %v", d.updatedAt, later)
		}
	})

	t.Run("pending same generation removal returns unmodified", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			State:      DeliveryStatePending,
			Generation: 5,
			Operation:  DeliveryOperationRemove,
			UpdatedAt:  now,
		})
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if modified {
			t.Fatal("expected modified = false for same-gen pending removal")
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State changed unexpectedly to %q", d.state)
		}
		if d.updatedAt != now {
			t.Error("UpdatedAt changed on unmodified withdraw")
		}
	})

	t.Run("pending deliver same generation starts a removal cycle", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			State:      DeliveryStatePending,
			Generation: 5,
			Operation:  DeliveryOperationDeliver,
			Message:    "apply object 1: refused",
			Attempts:   2,
			UpdatedAt:  now,
		})
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true: the record belongs to a put cycle, not a removal")
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
		if d.operation != DeliveryOperationRemove {
			t.Errorf("Operation = %q, want %q", d.operation, DeliveryOperationRemove)
		}
		if d.message != "" || d.attempts != 0 {
			t.Errorf("put-cycle failure details survived into the removal cycle: Message = %q, Attempts = %d", d.message, d.attempts)
		}
	})

	t.Run("pending higher generation bumps generation", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			State:      DeliveryStatePending,
			Generation: 3,
			UpdatedAt:  now,
		})
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true for higher-gen pending")
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
		if d.generation != 5 {
			t.Errorf("Generation = %d, want 5", d.generation)
		}
		if d.updatedAt != later {
			t.Errorf("UpdatedAt = %v, want %v", d.updatedAt, later)
		}
	})

	t.Run("in-progress same generation removal returns unmodified", func(t *testing.T) {
		for _, state := range []DeliveryState{
			DeliveryStateAccepted,
			DeliveryStateProgressing,
		} {
			t.Run(string(state), func(t *testing.T) {
				d := DeliveryFromSnapshot(DeliverySnapshot{
					State: state, Generation: 5, Operation: DeliveryOperationRemove, UpdatedAt: now,
				})
				modified, err := d.Withdraw(5, later)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if modified {
					t.Fatal("expected modified = false for same-gen in-progress removal")
				}
				if d.state != state {
					t.Errorf("State changed to %q", d.state)
				}
				if d.updatedAt != now {
					t.Error("UpdatedAt changed on unmodified withdraw")
				}
			})
		}
	})

	t.Run("in-progress deliver same generation starts a removal cycle", func(t *testing.T) {
		for _, state := range []DeliveryState{
			DeliveryStateAccepted,
			DeliveryStateProgressing,
		} {
			t.Run(string(state), func(t *testing.T) {
				d := DeliveryFromSnapshot(DeliverySnapshot{
					State: state, Generation: 5, Operation: DeliveryOperationDeliver, UpdatedAt: now,
				})
				modified, err := d.Withdraw(5, later)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !modified {
					t.Fatal("expected modified = true: the record belongs to a put cycle, not a removal")
				}
				if d.state != DeliveryStatePending {
					t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
				}
				if d.operation != DeliveryOperationRemove {
					t.Errorf("Operation = %q, want %q", d.operation, DeliveryOperationRemove)
				}
			})
		}
	})

	t.Run("in-progress higher generation resets to pending", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			State:      DeliveryStateAccepted,
			Generation: 3,
			UpdatedAt:  now,
		})
		modified, err := d.Withdraw(5, later)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !modified {
			t.Fatal("expected modified = true for higher-gen in-progress")
		}
		if d.state != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.state, DeliveryStatePending)
		}
		if d.generation != 5 {
			t.Errorf("Generation = %d, want 5", d.generation)
		}
	})

	t.Run("fails if generation moves backwards", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			State:      DeliveryStateDelivered,
			Generation: 5,
			UpdatedAt:  now,
		})
		modified, err := d.Withdraw(3, later)
		if err == nil {
			t.Fatal("expected error for backwards generation")
		}
		if modified {
			t.Fatal("expected modified = false on error")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Errorf("error = %v, want ErrIllegalStateTransition", err)
		}
		if d.generation != 5 {
			t.Errorf("Generation changed to %d on failed withdraw", d.generation)
		}
		if d.state != DeliveryStateDelivered {
			t.Errorf("State changed on failed withdraw")
		}
	})
}

// TestDelivery_NeedsWithdrawal pins what a deletion must still dispatch:
// a put cycle always, and a removal cycle unless it finished — Delivered,
// or Partial for one that left objects in place.
func TestDelivery_NeedsWithdrawal(t *testing.T) {
	cases := []struct {
		name      string
		operation DeliveryOperation
		state     DeliveryState
		want      bool
	}{
		{"put cycle pending", DeliveryOperationDeliver, DeliveryStatePending, true},
		{"put cycle progressing", DeliveryOperationDeliver, DeliveryStateProgressing, true},
		{"put cycle delivered", DeliveryOperationDeliver, DeliveryStateDelivered, true},
		{"put cycle failed", DeliveryOperationDeliver, DeliveryStateFailed, true},
		{"removal pending", DeliveryOperationRemove, DeliveryStatePending, true},
		{"removal progressing", DeliveryOperationRemove, DeliveryStateProgressing, true},
		{"removal failed", DeliveryOperationRemove, DeliveryStateFailed, true},
		{"removal auth failed", DeliveryOperationRemove, DeliveryStateAuthFailed, true},
		{"removal delivered", DeliveryOperationRemove, DeliveryStateDelivered, false},
		{"removal partial", DeliveryOperationRemove, DeliveryStatePartial, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := DeliveryFromSnapshot(DeliverySnapshot{
				State:      c.state,
				Operation:  c.operation,
				Generation: 1,
			})
			if got := d.NeedsWithdrawal(); got != c.want {
				t.Errorf("NeedsWithdrawal() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestDelivery_Operation_NewDelivery(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := NewDelivery("d1", "f1", "t1", nil, 1, now)
	if d.Operation() != DeliveryOperationDeliver {
		t.Errorf("Operation() = %q, want %q", d.Operation(), DeliveryOperationDeliver)
	}
}

func TestDelivery_Operation_Redispatch(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := DeliveryFromSnapshot(DeliverySnapshot{
		State:      DeliveryStateDelivered,
		Generation: 1,
		Operation:  DeliveryOperationRemove,
	})
	if err := d.Redispatch(nil, 2, now); err != nil {
		t.Fatal(err)
	}
	if d.Operation() != DeliveryOperationDeliver {
		t.Errorf("Operation() = %q, want %q", d.Operation(), DeliveryOperationDeliver)
	}
}

func TestDelivery_Operation_Withdraw(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := DeliveryFromSnapshot(DeliverySnapshot{
		State:      DeliveryStateDelivered,
		Generation: 1,
		Operation:  DeliveryOperationDeliver,
	})
	modified, err := d.Withdraw(2, now)
	if err != nil {
		t.Fatal(err)
	}
	if !modified {
		t.Fatal("expected modified = true")
	}
	if d.Operation() != DeliveryOperationRemove {
		t.Errorf("Operation() = %q, want %q", d.Operation(), DeliveryOperationRemove)
	}
}

func TestDelivery_Operation_ResetForRetry_Preserves(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, op := range []DeliveryOperation{DeliveryOperationDeliver, DeliveryOperationRemove} {
		t.Run(string(op), func(t *testing.T) {
			d := DeliveryFromSnapshot(DeliverySnapshot{
				State:     DeliveryStateFailed,
				Operation: op,
			})
			if err := d.ResetForRetry(now); err != nil {
				t.Fatal(err)
			}
			if d.Operation() != op {
				t.Errorf("Operation() = %q, want %q (preserved)", d.Operation(), op)
			}
		})
	}
}

func TestDelivery_Operation_Retry_Preserves(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, op := range []DeliveryOperation{DeliveryOperationDeliver, DeliveryOperationRemove} {
		t.Run(string(op), func(t *testing.T) {
			d := DeliveryFromSnapshot(DeliverySnapshot{
				State:      DeliveryStatePending,
				Generation: 1,
				Operation:  op,
			})
			retried := d.Retry(1, now)
			if !retried {
				t.Fatal("expected retry = true")
			}
			if d.Operation() != op {
				t.Errorf("Operation() = %q, want %q (preserved)", d.Operation(), op)
			}
		})
	}
}

func TestDelivery_Operation_SnapshotRoundTrip(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := NewDelivery("d1", "f1", "t1", nil, 1, now)
	snap := d.Snapshot()
	if snap.Operation != DeliveryOperationDeliver {
		t.Errorf("Snapshot().Operation = %q, want %q", snap.Operation, DeliveryOperationDeliver)
	}
	restored := DeliveryFromSnapshot(snap)
	if restored.Operation() != DeliveryOperationDeliver {
		t.Errorf("restored Operation() = %q, want %q", restored.Operation(), DeliveryOperationDeliver)
	}
}

func TestDelivery_ResetForRetry(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)

	t.Run("resets failed delivery to pending", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			ID:         "d1",
			State:      DeliveryStateFailed,
			Generation: 5,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
		if err := d.ResetForRetry(later); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.State() != DeliveryStatePending {
			t.Errorf("State = %q, want %q", d.State(), DeliveryStatePending)
		}
		if d.UpdatedAt() != later {
			t.Errorf("UpdatedAt = %v, want %v", d.UpdatedAt(), later)
		}
		if d.Generation() != 5 {
			t.Errorf("Generation = %d, want 5 (unchanged)", d.Generation())
		}
	})

	t.Run("rejects terminal states that are not retryable", func(t *testing.T) {
		// delivered is observed convergence, partial is a terminal
		// observation of the attempt, and auth_failed waits for fresh
		// credentials: none is re-dispatched in the same cycle.
		for _, state := range []DeliveryState{
			DeliveryStateDelivered,
			DeliveryStatePartial,
			DeliveryStateAuthFailed,
		} {
			t.Run(string(state), func(t *testing.T) {
				d := DeliveryFromSnapshot(DeliverySnapshot{
					State:      state,
					Generation: 5,
					UpdatedAt:  now,
				})
				err := d.ResetForRetry(later)
				if err == nil {
					t.Fatal("expected error for a non-retryable terminal state")
				}
				if !errors.Is(err, ErrIllegalStateTransition) {
					t.Errorf("error = %v, want ErrIllegalStateTransition", err)
				}
				if d.State() != state {
					t.Errorf("State changed to %q", d.State())
				}
				if d.UpdatedAt() != now {
					t.Error("UpdatedAt changed on rejected reset")
				}
			})
		}
	})

	t.Run("rejects non-terminal delivery", func(t *testing.T) {
		for _, state := range []DeliveryState{
			DeliveryStatePending,
			DeliveryStateAccepted,
			DeliveryStateProgressing,
		} {
			t.Run(string(state), func(t *testing.T) {
				d := DeliveryFromSnapshot(DeliverySnapshot{
					State:     state,
					UpdatedAt: now,
				})
				err := d.ResetForRetry(later)
				if err == nil {
					t.Fatal("expected error for non-terminal state")
				}
				if !errors.Is(err, ErrIllegalStateTransition) {
					t.Errorf("error = %v, want ErrIllegalStateTransition", err)
				}
				if d.State() != state {
					t.Errorf("State changed to %q, want %q (unchanged)", d.State(), state)
				}
				if d.UpdatedAt() != now {
					t.Error("UpdatedAt changed on failed reset")
				}
			})
		}
	})
}

func TestDelivery_ApplyResult_RecordsFailureDetails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const reason = `apply object 1: conflict with "work-agent": .data.owner`

	t.Run("failed, partial, and auth_failed record the message and count the attempt", func(t *testing.T) {
		for _, state := range []DeliveryState{
			DeliveryStateFailed,
			DeliveryStatePartial,
			DeliveryStateAuthFailed,
		} {
			t.Run(string(state), func(t *testing.T) {
				d := DeliveryFromSnapshot(DeliverySnapshot{
					ID:        "f1:t1",
					State:     DeliveryStateProgressing,
					CreatedAt: now,
					UpdatedAt: now,
				})
				err := d.ApplyResult(DeliveryResult{State: state, Message: reason}, now.Add(time.Minute))
				if err != nil {
					t.Fatalf("ApplyResult: %v", err)
				}
				if d.State() != state {
					t.Errorf("State = %q, want %q", d.State(), state)
				}
				if d.Message() != reason {
					t.Errorf("Message = %q, want %q", d.Message(), reason)
				}
				if d.Attempts() != 1 {
					t.Errorf("Attempts = %d, want 1", d.Attempts())
				}
			})
		}
	})

	t.Run("delivered clears the message and keeps the attempt count", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			ID:        "f1:t1",
			State:     DeliveryStatePending,
			Message:   reason,
			Attempts:  2,
			CreatedAt: now,
			UpdatedAt: now,
		})
		if err := d.ApplyResult(DeliveryResult{State: DeliveryStateDelivered, Message: "done"}, now.Add(time.Minute)); err != nil {
			t.Fatalf("ApplyResult: %v", err)
		}
		if d.Message() != "" {
			t.Errorf("Message = %q, want empty after delivery", d.Message())
		}
		if d.Attempts() != 2 {
			t.Errorf("Attempts = %d, want 2 (unchanged)", d.Attempts())
		}
	})

	t.Run("a duplicate report of the same terminal state changes nothing", func(t *testing.T) {
		later := now.Add(time.Hour)
		d := DeliveryFromSnapshot(DeliverySnapshot{
			ID:        "f1:t1",
			State:     DeliveryStateFailed,
			Message:   reason,
			Attempts:  1,
			CreatedAt: now,
			UpdatedAt: now,
		})
		if err := d.ApplyResult(DeliveryResult{State: DeliveryStateFailed, Message: "other"}, later); err != nil {
			t.Fatalf("ApplyResult: %v", err)
		}
		if d.Message() != reason {
			t.Errorf("Message = %q, want the first report %q", d.Message(), reason)
		}
		if d.Attempts() != 1 {
			t.Errorf("Attempts = %d, want 1 (unchanged)", d.Attempts())
		}
		if d.UpdatedAt() != now {
			t.Error("UpdatedAt changed on a duplicate report")
		}
	})

	t.Run("an illegal transition reports the error and records nothing", func(t *testing.T) {
		d := DeliveryFromSnapshot(DeliverySnapshot{
			ID:        "f1:t1",
			State:     DeliveryStateDelivered,
			Attempts:  1,
			CreatedAt: now,
			UpdatedAt: now,
		})
		err := d.ApplyResult(DeliveryResult{State: DeliveryStateFailed, Message: reason}, now.Add(time.Minute))
		if err == nil {
			t.Fatal("expected an error from a terminal state")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Errorf("error = %v, want ErrIllegalStateTransition", err)
		}
		if d.Message() != "" || d.Attempts() != 1 {
			t.Errorf("failure details changed: Message = %q, Attempts = %d", d.Message(), d.Attempts())
		}
	})
}

func TestDelivery_ApplyResult_AttemptsAccumulateAcrossRetries(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := NewDelivery("d1", "f1", "t1", nil, 1, now)

	if err := d.ApplyResult(DeliveryResult{State: DeliveryStateFailed, Message: "first"}, now.Add(time.Minute)); err != nil {
		t.Fatalf("first ApplyResult: %v", err)
	}
	if err := d.ResetForRetry(now.Add(2 * time.Minute)); err != nil {
		t.Fatalf("ResetForRetry: %v", err)
	}
	if got := d.Message(); got != "first" {
		t.Errorf("Message after ResetForRetry = %q, want %q (preserved for the next report)", got, "first")
	}
	if err := d.ApplyResult(DeliveryResult{State: DeliveryStateFailed, Message: "second"}, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("second ApplyResult: %v", err)
	}
	if d.Attempts() != 2 {
		t.Errorf("Attempts = %d, want 2", d.Attempts())
	}
	if got := d.Message(); got != "second" {
		t.Errorf("Message = %q, want %q", got, "second")
	}
}

func TestDelivery_Redispatch_ClearsFailureDetails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := DeliveryFromSnapshot(DeliverySnapshot{
		ID:         "d1",
		Generation: 1,
		State:      DeliveryStateFailed,
		Message:    "old failure",
		Attempts:   4,
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	if err := d.Redispatch(nil, 2, now.Add(time.Minute)); err != nil {
		t.Fatalf("Redispatch: %v", err)
	}
	if d.Message() != "" || d.Attempts() != 0 {
		t.Errorf("failure details survived Redispatch: Message = %q, Attempts = %d", d.Message(), d.Attempts())
	}
}

func TestDelivery_Withdraw_ClearsFailureDetails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := DeliveryFromSnapshot(DeliverySnapshot{
		ID:         "d1",
		Generation: 1,
		State:      DeliveryStateFailed,
		Message:    "old failure",
		Attempts:   4,
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	modified, err := d.Withdraw(2, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if !modified {
		t.Fatal("expected modified = true")
	}
	if d.Message() != "" || d.Attempts() != 0 {
		t.Errorf("failure details survived Withdraw: Message = %q, Attempts = %d", d.Message(), d.Attempts())
	}
}

func TestDelivery_Withdraw_SameGenerationRetryPreservesFailureDetails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := DeliveryFromSnapshot(DeliverySnapshot{
		ID:         "d1",
		Generation: 3,
		State:      DeliveryStateFailed,
		Operation:  DeliveryOperationRemove,
		Message:    "remove object: forbidden",
		Attempts:   2,
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	modified, err := d.Withdraw(3, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if !modified {
		t.Fatal("expected modified = true")
	}
	if d.State() != DeliveryStatePending {
		t.Errorf("State = %q, want %q", d.State(), DeliveryStatePending)
	}
	if d.Operation() != DeliveryOperationRemove {
		t.Errorf("Operation = %q, want %q", d.Operation(), DeliveryOperationRemove)
	}
	if d.Message() != "remove object: forbidden" || d.Attempts() != 2 {
		t.Errorf("failure details not preserved across a same-generation removal retry: Message = %q, Attempts = %d",
			d.Message(), d.Attempts())
	}
}

func TestDelivery_Withdraw_SameGenerationDeliverToRemoveStartsClean(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := DeliveryFromSnapshot(DeliverySnapshot{
		ID:         "d1",
		Generation: 3,
		State:      DeliveryStateFailed,
		Operation:  DeliveryOperationDeliver,
		Message:    "apply: conflict with work-agent",
		Attempts:   2,
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	modified, err := d.Withdraw(3, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if !modified {
		t.Fatal("expected modified = true")
	}
	if d.Operation() != DeliveryOperationRemove {
		t.Errorf("Operation = %q, want %q", d.Operation(), DeliveryOperationRemove)
	}
	// The failure belongs to the put cycle: a removal at the same generation
	// must not report it, nor inherit its retry backoff.
	if d.Message() != "" || d.Attempts() != 0 {
		t.Errorf("put-cycle failure details carried into the removal cycle: Message = %q, Attempts = %d",
			d.Message(), d.Attempts())
	}
}

func TestDelivery_Snapshot_FailureDetailsRoundTrip(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const reason = `apply object 2: conflict with "work-agent": .metadata.labels`
	d := DeliveryFromSnapshot(DeliverySnapshot{
		ID:        "f1:t1",
		State:     DeliveryStateFailed,
		Message:   reason,
		Attempts:  3,
		CreatedAt: now,
		UpdatedAt: now,
	})
	snap := d.Snapshot()
	if snap.Message != reason || snap.Attempts != 3 {
		t.Fatalf("Snapshot() failure details = (%q, %d), want (%q, 3)", snap.Message, snap.Attempts, reason)
	}
	restored := DeliveryFromSnapshot(snap)
	if restored.Message() != reason || restored.Attempts() != 3 {
		t.Errorf("restored failure details = (%q, %d), want (%q, 3)", restored.Message(), restored.Attempts(), reason)
	}
}
