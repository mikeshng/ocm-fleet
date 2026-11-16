package kubernetes

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

type activeDeliveryReporter struct {
	ids     []domain.TargetID
	active  []domain.ActiveDelivery
	listErr error
}

func (r *activeDeliveryReporter) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}

func (r *activeDeliveryReporter) ReportResult(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryResult) error {
	return nil
}

func (r *activeDeliveryReporter) ListActiveDeliveries(_ context.Context, targetIDs []domain.TargetID) ([]domain.ActiveDelivery, error) {
	r.ids = targetIDs
	return r.active, r.listErr
}

func recoveryTarget(id domain.TargetID, targetType domain.TargetType) domain.TargetInfo {
	return domain.NewTargetInfo(id, targetType, string(id), domain.TargetStateReady, nil, nil, nil)
}

func TestRecoverActiveDeliveries_AsksOnlyForThisAddonsTargetsAndRedispatches(t *testing.T) {
	now := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	reporter := &activeDeliveryReporter{active: []domain.ActiveDelivery{
		{
			Delivery: domain.NewDelivery("d1", "f1", "t1", nil, 2, now),
			Target:   recoveryTarget("t1", TargetType),
		},
		{
			Delivery: domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID: "d2", FulfillmentID: "f1", TargetID: "t1",
				Generation: 3, Operation: domain.DeliveryOperationRemove,
				State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
			}),
			Target: recoveryTarget("t1", TargetType),
		},
	}}
	agent := NewDeliveryAgent(reporter)
	lister := staticTargetLister{targets: []domain.TargetInfo{
		recoveryTarget("t1", TargetType),
		recoveryTarget("kind-local", domain.TargetType("kind.example.com/KindCluster")),
	}}

	err := agent.RecoverActiveDeliveries(context.Background(), lister)
	if err == nil {
		t.Fatal("RecoverActiveDeliveries: expected the redispatch errors for targets with no api-server property")
	}
	for _, want := range []string{"recover delivery d1 to t1", "recover removal d2 to t1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	if len(reporter.ids) != 1 || reporter.ids[0] != "t1" {
		t.Fatalf("ListActiveDeliveries target ids = %v, want [t1]", reporter.ids)
	}
}

func TestRecoverActiveDeliveries_NoTargetsOfThisTypeAsksNothing(t *testing.T) {
	reporter := &activeDeliveryReporter{listErr: errors.New("must not be called")}
	agent := NewDeliveryAgent(reporter)
	lister := staticTargetLister{targets: []domain.TargetInfo{
		recoveryTarget("kind-local", domain.TargetType("kind.example.com/KindCluster")),
	}}

	if err := agent.RecoverActiveDeliveries(context.Background(), lister); err != nil {
		t.Fatalf("RecoverActiveDeliveries: %v", err)
	}
	if reporter.ids != nil {
		t.Fatalf("ListActiveDeliveries called with %v, want no call", reporter.ids)
	}
}

func TestRecoverActiveDeliveries_TargetListFailureIsReported(t *testing.T) {
	agent := NewDeliveryAgent(&activeDeliveryReporter{})
	lister := staticTargetLister{err: errors.New("store closed")}

	err := agent.RecoverActiveDeliveries(context.Background(), lister)
	if err == nil || !strings.Contains(err.Error(), "list targets") {
		t.Fatalf("error = %v, want a wrapped target-list failure", err)
	}
}

func TestRecoverActiveDeliveries_ActiveListFailureIsReported(t *testing.T) {
	reporter := &activeDeliveryReporter{listErr: errors.New("store closed")}
	agent := NewDeliveryAgent(reporter)
	lister := staticTargetLister{targets: []domain.TargetInfo{recoveryTarget("t1", TargetType)}}

	err := agent.RecoverActiveDeliveries(context.Background(), lister)
	if err == nil || !strings.Contains(err.Error(), "recover active deliveries: list") {
		t.Fatalf("error = %v, want a wrapped active-delivery list failure", err)
	}
}
