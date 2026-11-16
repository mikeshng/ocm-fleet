package memworkflow

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// countingActivity fails every attempt with err and counts the attempts.
type countingActivity struct {
	calls int
	err   error
}

func (a *countingActivity) Name() string { return "counting" }

func (a *countingActivity) Run(context.Context, any) (any, error) {
	a.calls++
	return nil, a.err
}

// An unresolved placement fails the same way on every attempt with the
// same input, so the activity is attempted once and the error reaches
// the workflow, which waits and reloads its targets.
func TestRecordRun_UnresolvedPlacement_IsAttemptedOnce(t *testing.T) {
	activity := &countingActivity{err: fmt.Errorf("resolve placement: %w", domain.ErrPlacementUnresolved)}
	record := newRecord("unresolved", context.Background(), nil)

	_, err := record.Run(activity, "input")

	if !errors.Is(err, domain.ErrPlacementUnresolved) {
		t.Fatalf("Run error = %v, want ErrPlacementUnresolved", err)
	}
	if activity.calls != 1 {
		t.Errorf("attempts = %d, want 1", activity.calls)
	}
}

// Any other failure is retried in place, up to the attempt limit.
func TestRecordRun_OtherFailure_IsRetriedInPlace(t *testing.T) {
	activity := &countingActivity{err: errors.New("database is locked")}
	record := newRecord("transient", context.Background(), nil)

	if _, err := record.Run(activity, "input"); err == nil {
		t.Fatal("Run returned no error")
	}
	if activity.calls != activityMaxAttempts {
		t.Errorf("attempts = %d, want %d", activity.calls, activityMaxAttempts)
	}
}
