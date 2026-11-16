package grpc

import (
	"testing"
	"time"

	pb "github.com/stolostron/ocm-fleet/server/gen/ocm/v1alpha"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestDeploymentToProto_ReconcilingRespectsPause(t *testing.T) {
	tests := []struct {
		name            string
		state           domain.FulfillmentState
		pauseReason     string
		wantReconciling bool
	}{
		{
			name:            "creating without pause is reconciling",
			state:           domain.FulfillmentStateCreating,
			wantReconciling: true,
		},
		{
			name:            "deleting without pause is reconciling",
			state:           domain.FulfillmentStateDeleting,
			wantReconciling: true,
		},
		{
			name:            "creating with pause is not reconciling",
			state:           domain.FulfillmentStateCreating,
			pauseReason:     "credential rotation required",
			wantReconciling: false,
		},
		{
			name:            "deleting with pause is not reconciling",
			state:           domain.FulfillmentStateDeleting,
			pauseReason:     "credential rotation required",
			wantReconciling: false,
		},
		{
			name:            "active without pause is not reconciling",
			state:           domain.FulfillmentStateActive,
			wantReconciling: false,
		},
		{
			name:            "active with pause is not reconciling",
			state:           domain.FulfillmentStateActive,
			pauseReason:     "paused",
			wantReconciling: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := domain.DeploymentView{
				Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
					Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
				}),
				Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
					ID:          "f1",
					State:       tt.state,
					PauseReason: tt.pauseReason,
				}),
			}

			got := deploymentToProto(view)

			if got.GetReconciling() != tt.wantReconciling {
				t.Errorf("Reconciling = %v, want %v", got.GetReconciling(), tt.wantReconciling)
			}
			if got.GetPauseReason() != tt.pauseReason {
				t.Errorf("PauseReason = %q, want %q", got.GetPauseReason(), tt.pauseReason)
			}

			wantState := fulfillmentStateToProto(tt.state)
			if got.GetState() != wantState {
				t.Errorf("State = %v, want %v", got.GetState(), wantState)
			}
		})
	}
}

func TestDeploymentToProto_TargetStatuses(t *testing.T) {
	updated := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	view := domain.DeploymentView{
		Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
			Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
		}),
		Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
			ID: "f1", State: domain.FulfillmentStateCreating,
		}),
		Deliveries: []domain.Delivery{
			domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				TargetID: "t1", State: domain.DeliveryStateDelivered,
				Operation: domain.DeliveryOperationDeliver,
				UpdatedAt: updated,
			}),
			domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				TargetID: "t2", State: domain.DeliveryStateFailed,
				Operation: domain.DeliveryOperationDeliver,
				Message:   `apply object 1: conflict with "work-agent"`,
				Attempts:  3, UpdatedAt: updated,
			}),
		},
	}

	got := deploymentToProto(view).GetTargetStatuses()
	if len(got) != 2 {
		t.Fatalf("len(TargetStatuses) = %d, want 2", len(got))
	}

	if got[0].GetTargetId() != "t1" {
		t.Errorf("TargetStatuses[0].TargetId = %q, want t1", got[0].GetTargetId())
	}
	if got[0].GetState() != pb.TargetDeliveryStatus_STATE_DELIVERED {
		t.Errorf("TargetStatuses[0].State = %v, want STATE_DELIVERED", got[0].GetState())
	}
	if got[0].GetMessage() != "" {
		t.Errorf("TargetStatuses[0].Message = %q, want empty", got[0].GetMessage())
	}
	if got[0].GetAttempts() != 0 { // never failed
		t.Errorf("TargetStatuses[0].Attempts = %d, want 0", got[0].GetAttempts())
	}

	if got[1].GetTargetId() != "t2" {
		t.Errorf("TargetStatuses[1].TargetId = %q, want t2", got[1].GetTargetId())
	}
	if got[1].GetState() != pb.TargetDeliveryStatus_STATE_FAILED {
		t.Errorf("TargetStatuses[1].State = %v, want STATE_FAILED", got[1].GetState())
	}
	if want := `apply object 1: conflict with "work-agent"`; got[1].GetMessage() != want {
		t.Errorf("TargetStatuses[1].Message = %q, want %q", got[1].GetMessage(), want)
	}
	if got[1].GetAttempts() != 3 {
		t.Errorf("TargetStatuses[1].Attempts = %d, want 3", got[1].GetAttempts())
	}
	if got[1].GetUpdateTime() == nil || !got[1].GetUpdateTime().AsTime().Equal(updated) {
		t.Errorf("TargetStatuses[1].UpdateTime = %v, want %v", got[1].GetUpdateTime(), updated)
	}
	for i, want := range []pb.TargetDeliveryStatus_Operation{
		pb.TargetDeliveryStatus_OPERATION_DELIVER,
		pb.TargetDeliveryStatus_OPERATION_DELIVER,
	} {
		if got[i].GetOperation() != want {
			t.Errorf("TargetStatuses[%d].Operation = %v, want %v", i, got[i].GetOperation(), want)
		}
	}
}

func TestDeploymentToProto_TargetStatuses_Removal(t *testing.T) {
	// A target whose removal completed is still STATE_DELIVERED; only the
	// operation tells a client that the work is gone rather than applied.
	view := domain.DeploymentView{
		Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
			Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
		}),
		Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
			ID: "f1", State: domain.FulfillmentStateDeleting,
		}),
		Deliveries: []domain.Delivery{
			domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				TargetID: "t1", State: domain.DeliveryStateDelivered,
				Operation: domain.DeliveryOperationRemove,
			}),
			domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				TargetID: "t2", State: domain.DeliveryStateFailed,
				Operation: domain.DeliveryOperationRemove,
				Message:   `remove object 1: forbidden`,
				Attempts:  2,
			}),
		},
	}

	got := deploymentToProto(view).GetTargetStatuses()
	if len(got) != 2 {
		t.Fatalf("len(TargetStatuses) = %d, want 2", len(got))
	}
	if got[0].GetState() != pb.TargetDeliveryStatus_STATE_DELIVERED {
		t.Errorf("TargetStatuses[0].State = %v, want STATE_DELIVERED", got[0].GetState())
	}
	if got[0].GetOperation() != pb.TargetDeliveryStatus_OPERATION_REMOVE {
		t.Errorf("TargetStatuses[0].Operation = %v, want OPERATION_REMOVE", got[0].GetOperation())
	}
	if got[1].GetOperation() != pb.TargetDeliveryStatus_OPERATION_REMOVE {
		t.Errorf("TargetStatuses[1].Operation = %v, want OPERATION_REMOVE", got[1].GetOperation())
	}
	if want := `remove object 1: forbidden`; got[1].GetMessage() != want {
		t.Errorf("TargetStatuses[1].Message = %q, want %q", got[1].GetMessage(), want)
	}
}

func TestDeploymentToProto_TargetStatuses_OperationMapping(t *testing.T) {
	tests := []struct {
		operation domain.DeliveryOperation
		want      pb.TargetDeliveryStatus_Operation
	}{
		{domain.DeliveryOperationDeliver, pb.TargetDeliveryStatus_OPERATION_DELIVER},
		{domain.DeliveryOperationRemove, pb.TargetDeliveryStatus_OPERATION_REMOVE},
		{"unknown", pb.TargetDeliveryStatus_OPERATION_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(string(tt.operation), func(t *testing.T) {
			view := domain.DeploymentView{
				Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
					Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
				}),
				Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
					ID: "f1", State: domain.FulfillmentStateCreating,
				}),
				Deliveries: []domain.Delivery{
					domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
						TargetID: "t1", State: domain.DeliveryStatePending, Operation: tt.operation,
					}),
				},
			}

			got := deploymentToProto(view).GetTargetStatuses()
			if len(got) != 1 {
				t.Fatalf("len(TargetStatuses) = %d, want 1", len(got))
			}
			if got[0].GetOperation() != tt.want {
				t.Errorf("Operation = %v, want %v", got[0].GetOperation(), tt.want)
			}
		})
	}
}

func TestDeploymentToProto_TargetStatuses_StateMapping(t *testing.T) {
	tests := []struct {
		state domain.DeliveryState
		want  pb.TargetDeliveryStatus_State
	}{
		{domain.DeliveryStatePending, pb.TargetDeliveryStatus_STATE_PENDING},
		{domain.DeliveryStateAccepted, pb.TargetDeliveryStatus_STATE_ACCEPTED},
		{domain.DeliveryStateProgressing, pb.TargetDeliveryStatus_STATE_PROGRESSING},
		{domain.DeliveryStateDelivered, pb.TargetDeliveryStatus_STATE_DELIVERED},
		{domain.DeliveryStateFailed, pb.TargetDeliveryStatus_STATE_FAILED},
		{domain.DeliveryStatePartial, pb.TargetDeliveryStatus_STATE_PARTIAL},
		{domain.DeliveryStateAuthFailed, pb.TargetDeliveryStatus_STATE_AUTH_FAILED},
		{"unknown", pb.TargetDeliveryStatus_STATE_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			view := domain.DeploymentView{
				Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
					Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
				}),
				Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
					ID: "f1", State: domain.FulfillmentStateCreating,
				}),
				Deliveries: []domain.Delivery{
					domain.DeliveryFromSnapshot(domain.DeliverySnapshot{TargetID: "t1", State: tt.state}),
				},
			}

			got := deploymentToProto(view).GetTargetStatuses()
			if len(got) != 1 {
				t.Fatalf("len(TargetStatuses) = %d, want 1", len(got))
			}
			if got[0].GetState() != tt.want {
				t.Errorf("State = %v, want %v", got[0].GetState(), tt.want)
			}
		})
	}
}

func TestDeploymentToProto_NoDeliveries_NoTargetStatuses(t *testing.T) {
	view := domain.DeploymentView{
		Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
			Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
		}),
		Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
			ID: "f1", State: domain.FulfillmentStateActive,
		}),
	}

	if got := deploymentToProto(view).GetTargetStatuses(); len(got) != 0 {
		t.Errorf("len(TargetStatuses) = %d, want 0", len(got))
	}
}

func TestDeploymentToProto_PauseReasonPopulated(t *testing.T) {
	view := domain.DeploymentView{
		Deployment: domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
			Name: "deployments/d1", UID: domain.NewDeploymentUID(), FulfillmentID: "f1",
		}),
		Fulfillment: *domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
			ID:          "f1",
			State:       domain.FulfillmentStateCreating,
			PauseReason: "credential rotation required",
		}),
	}

	got := deploymentToProto(view)

	if got.GetState() != pb.Deployment_STATE_CREATING {
		t.Errorf("State = %v, want STATE_CREATING", got.GetState())
	}
	if got.GetPauseReason() != "credential rotation required" {
		t.Errorf("PauseReason = %q, want %q", got.GetPauseReason(), "credential rotation required")
	}
}
