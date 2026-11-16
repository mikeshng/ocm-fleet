package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateManagedResourceInput carries all the fields needed to create a
// managed resource instance. The application service pre-validates the
// spec against the registered JSON Schema before starting this workflow.
type CreateManagedResourceInput struct {
	ResourceType ResourceType
	Name         ResourceName
	Spec         json.RawMessage
	Labels       map[string]string
	TypeDef      ExtensionResourceType
	Provenance   *Provenance
	Auth         DeliveryAuth
}

// CreateManagedResourceWorkflowSpec persists a managed resource
// (HEAD + intent v1 + derived fulfillment) and starts orchestration.
// Follows the same structural pattern as [CreateDeploymentWorkflowSpec].
type CreateManagedResourceWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
	Now           func() time.Time
}

func (s *CreateManagedResourceWorkflowSpec) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *CreateManagedResourceWorkflowSpec) Name() string { return "create-managed-resource" }

// PersistManagedResource creates the extension resource aggregate (with
// managed state, initial intent, and derived fulfillment) in a single
// transaction.
func (s *CreateManagedResourceWorkflowSpec) PersistManagedResource() Activity[CreateManagedResourceInput, ExtensionResourceView] {
	return NewActivity("persist-managed-resource", func(ctx context.Context, in CreateManagedResourceInput) (ExtensionResourceView, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return ExtensionResourceView{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.RollbackOnExit()

		mgmt := in.TypeDef.Management()
		if mgmt == nil {
			return ExtensionResourceView{}, fmt.Errorf(
				"%w: type %q has no management metadata",
				ErrInvalidArgument, in.ResourceType)
		}

		// An activity may run more than once. If an earlier attempt
		// committed and its result was not recorded, the resource
		// already exists: that stored aggregate is this activity's
		// result, and creating another would duplicate it. A create the
		// caller must reject as a duplicate name is refused before the
		// workflow starts, and a concurrent duplicate is refused when
		// its workflow instance is started: the instance ID is derived
		// from the type and name.
		switch view, err := tx.ExtensionResources().GetView(ctx, in.ResourceType.FullName(in.Name)); {
		case err == nil:
			return view, nil
		case !errors.Is(err, ErrNotFound):
			return ExtensionResourceView{}, fmt.Errorf("look up existing managed resource: %w", err)
		}

		now := s.now()
		fID := FulfillmentID(uuid.New().String())

		// No platform-identity claim is needed here: a platform
		// resource has no UID of its own and no eager row to create --
		// its representation is derived on read, by name, once the
		// extension resource row below exists.

		// Create the extension resource with managed state.
		er := NewExtensionResource(NewExtensionResourceUID(), in.ResourceType, in.Name, now,
			WithManagedState(fID),
			WithExtensionLabels(in.Labels),
		)
		intent, err := er.RecordIntent(in.Spec, now)
		if err != nil {
			return ExtensionResourceView{}, fmt.Errorf("record intent: %w", err)
		}

		ms, ps, rs := mgmt.Relation().DeriveStrategies(intent)

		var attestRef *AttestationRef
		if in.Provenance != nil {
			rt := in.ResourceType
			attestRef = &AttestationRef{RelationRef: &rt}
		}

		f := NewFulfillment(fID, in.Auth, in.Provenance, attestRef, now)
		f.AdvanceManifestStrategy(ms, now)
		f.AdvancePlacementStrategy(ps, now)
		f.AdvanceRolloutStrategy(rs, now)

		if err := tx.Fulfillments().Create(ctx, f); err != nil {
			return ExtensionResourceView{}, fmt.Errorf("create fulfillment: %w", err)
		}

		if err := tx.ExtensionResources().Create(ctx, er); err != nil {
			return ExtensionResourceView{}, fmt.Errorf("create extension resource: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return ExtensionResourceView{}, fmt.Errorf("commit: %w", err)
		}

		return ExtensionResourceView{
			Resource:    *er,
			Intent:      &intent,
			Fulfillment: f,
		}, nil
	})
}

// StartOrchestration starts the orchestration workflow for the derived
// fulfillment. Idempotent: treats [ErrAlreadyRunning] as success so an
// activity replay after the orchestration already started does not
// fail the workflow.
func (s *CreateManagedResourceWorkflowSpec) StartOrchestration() Activity[FulfillmentID, struct{}] {
	return NewActivity("start-mr-orchestration", func(ctx context.Context, id FulfillmentID) (struct{}, error) {
		_, err := s.Orchestration.Start(ctx, id)
		if err != nil && errors.Is(err, ErrAlreadyRunning) {
			return struct{}{}, nil
		}
		return struct{}{}, err
	})
}

// Run is the workflow body: persist everything, then start orchestration.
func (s *CreateManagedResourceWorkflowSpec) Run(record Record, input CreateManagedResourceInput) (ExtensionResourceView, error) {
	view, err := RunActivity(record, s.PersistManagedResource(), input)
	if err != nil {
		return ExtensionResourceView{}, fmt.Errorf("persist managed resource: %w", err)
	}

	if _, err := RunActivity(record, s.StartOrchestration(), view.Fulfillment.ID()); err != nil {
		return ExtensionResourceView{}, fmt.Errorf("start orchestration: %w", err)
	}

	return view, nil
}
