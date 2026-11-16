package domain_test

import (
	"context"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestDeleteDeployment_MutateToDeleting_ReturnsDeliveries(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "deployments/d1", domain.FulfillmentSnapshot{
		Generation: 2,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
		ResolvedTargets: []domain.TargetID{"t1"},
		State:           domain.FulfillmentStateActive,
	})
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "deployments/d1:t1", FulfillmentID: "deployments/d1", TargetID: "t1",
		State:    domain.DeliveryStateDelivered,
		Attempts: 1,
	}))

	spec := &domain.DeleteDeploymentWorkflowSpec{Store: store}
	rec := &stubRecord{ctx: context.Background()}
	mr, err := domain.RunActivity(rec, spec.MutateToDeleting(), domain.DeleteDeploymentInput{
		Name: "deployments/d1",
		Auth: domain.DeliveryAuth{Token: "tok"},
	})
	if err != nil {
		t.Fatalf("MutateToDeleting: %v", err)
	}
	if mr.View.Fulfillment.State() != domain.FulfillmentStateDeleting {
		t.Errorf("Fulfillment.State = %q, want deleting", mr.View.Fulfillment.State())
	}
	// The response view must carry the target statuses a read would
	// report at the same moment.
	if len(mr.View.Deliveries) != 1 {
		t.Fatalf("len(Deliveries) = %d, want 1", len(mr.View.Deliveries))
	}
	got := mr.View.Deliveries[0]
	if got.TargetID() != "t1" {
		t.Errorf("TargetID = %q, want t1", got.TargetID())
	}
	if got.State() != domain.DeliveryStateDelivered {
		t.Errorf("State = %q, want delivered", got.State())
	}
}
