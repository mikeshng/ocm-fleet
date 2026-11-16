package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// DeploymentService manages deployment lifecycle and triggers orchestration.
type DeploymentService struct {
	Store         domain.Store
	CreateWF      domain.CreateDeploymentWorkflow
	DeleteWF      domain.DeleteDeploymentWorkflow
	ResumeWF      domain.ResumeDeploymentWorkflow
	ProvenanceSvc *domain.ProvenanceService
	// TargetAdvisors are asked, in order, to explain a target name a
	// placement uses that is not a registered target. Optional.
	TargetAdvisors []PlacementTargetAdvisor
}

// PlacementTargetAdvisor explains a name that a caller used as a target
// but that is not one. An integration that knows of clusters which are
// not targets (for example clusters it only observes) implements it,
// so that the refusal says what the name is and what to do about it.
type PlacementTargetAdvisor interface {
	// ExplainUnknownTarget returns a sentence for the caller, or ""
	// when it knows nothing about the name.
	ExplainUnknownTarget(ctx context.Context, id domain.TargetID) string
}

// Create starts the durable create-deployment workflow, which persists
// the deployment and launches orchestration as a child workflow. A name
// that is already stored is the caller's duplicate and is refused with
// [domain.ErrAlreadyExists]; the workflow's persistence is replay safe,
// so an activity retry after a restart reports the stored deployment
// instead of failing.
func (s *DeploymentService) Create(ctx context.Context, in domain.CreateDeploymentInput) (domain.DeploymentView, error) {
	if err := s.rejectDuplicate(ctx, in.Name); err != nil {
		return domain.DeploymentView{}, err
	}
	if err := s.checkStaticPlacement(ctx, in.PlacementStrategy); err != nil {
		return domain.DeploymentView{}, err
	}

	ac := AuthFromContext(ctx)
	if ac != nil && ac.Subject != nil {
		in.Auth = domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		}
	}

	if len(in.UserSignature) > 0 {
		if ac == nil || ac.Subject == nil {
			return domain.DeploymentView{}, fmt.Errorf(
				"%w: signing a deployment requires an authenticated caller",
				domain.ErrInvalidArgument)
		}
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return domain.DeploymentView{}, fmt.Errorf("begin read tx: %w", err)
		}
		defer tx.RollbackOnExit()
		prov, err := s.ProvenanceSvc.BuildDeploymentProvenance(
			ctx, tx.SignerEnrollments(), ac.Subject,
			in.Name, in.ManifestStrategy, in.PlacementStrategy,
			1, in.UserSignature, in.ValidUntil,
		)
		if err != nil {
			return domain.DeploymentView{}, fmt.Errorf("build provenance: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.DeploymentView{}, fmt.Errorf("commit read tx: %w", err)
		}
		// TODO: I don't like this modification of the input after the fact
		in.Provenance = prov
	}

	// TODO: don't store token; keep it in memory. use peer cluster to retrieve from peers on concurrent updates.
	exec, err := s.CreateWF.Start(ctx, in)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("start create-deployment workflow: %w", err)
	}

	view, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("create-deployment workflow: %w", err)
	}

	return view, nil
}

// rejectDuplicate refuses a create whose name is already stored. The
// persist activity treats an existing deployment as its own replay and
// returns it as a success, so without this check a second caller's
// create would succeed instead of reporting [domain.ErrAlreadyExists].
func (s *DeploymentService) rejectDuplicate(ctx context.Context, name domain.ResourceName) error {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.RollbackOnExit()

	if _, err := tx.Deployments().Get(ctx, name); err == nil {
		return fmt.Errorf("%w: deployment %q", domain.ErrAlreadyExists, name)
	} else if !errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("look up deployment %q: %w", name, err)
	}
	return tx.Commit()
}

// checkStaticPlacement refuses a deployment whose static placement
// names a target that is not registered or that takes no deployments.
// Orchestration would otherwise retry such a placement without end and
// without a reason the caller can read. Placements by selector are not
// checked: matching nothing yet is a valid state for them.
func (s *DeploymentService) checkStaticPlacement(ctx context.Context, spec domain.PlacementStrategySpec) error {
	if spec.Type != domain.PlacementStrategyStatic || len(spec.Targets) == 0 {
		return nil
	}
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.RollbackOnExit()
	targets, err := tx.Targets().List(ctx)
	if err != nil {
		return fmt.Errorf("list targets: %w", err)
	}
	states := make(map[domain.TargetID]domain.TargetState, len(targets))
	for _, t := range targets {
		states[t.ID()] = t.State()
	}
	for _, id := range spec.Targets {
		state, ok := states[id]
		if !ok {
			msg := fmt.Sprintf("placement names target %q, which is not a registered target", id)
			for _, advisor := range s.TargetAdvisors {
				if hint := advisor.ExplainUnknownTarget(ctx, id); hint != "" {
					msg += ": " + hint
					break
				}
			}
			return fmt.Errorf("%w: %s", domain.ErrInvalidArgument, msg)
		}
		if state != domain.TargetStateReady && state != "" {
			return fmt.Errorf("%w: placement names target %q, which is in state %q and takes no deployments",
				domain.ErrInvalidArgument, id, state)
		}
	}
	return nil
}

// Get retrieves a deployment by resource name.
func (s *DeploymentService) Get(ctx context.Context, name domain.ResourceName) (domain.DeploymentView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()

	view, err := tx.Deployments().GetView(ctx, name)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	return view, tx.Commit()
}

// List returns all deployments (each joined with its fulfillment).
func (s *DeploymentService) List(ctx context.Context) ([]domain.DeploymentView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()

	views, err := tx.Deployments().ListView(ctx)
	if err != nil {
		return nil, err
	}
	return views, tx.Commit()
}

// ListPage returns one page of deployments, ordered by name, starting
// strictly after after, plus whether more follow. The repository applies
// the name predicate and the limit in storage, so a paged read of a large
// fleet does not load every deployment for every page.
func (s *DeploymentService) ListPage(ctx context.Context, after domain.ResourceName, limit int) (domain.DeploymentViewPage, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentViewPage{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()

	page, err := tx.Deployments().ListViewPage(ctx, after, limit)
	if err != nil {
		return domain.DeploymentViewPage{}, err
	}
	return page, tx.Commit()
}

// ResumeInput carries the optional re-signing parameters for resuming
// a deployment. When UserSignature is non-empty, the server constructs
// fresh provenance for the resuming user.
type ResumeInput struct {
	Name               domain.ResourceName
	UserSignature      []byte
	ValidUntil         time.Time
	Etag               domain.Etag
	ExpectedGeneration domain.Generation
}

// Resume resumes a deployment that is paused for authentication by
// starting a durable resume-deployment workflow. The workflow updates
// auth/provenance, bumps the generation, and guarantees orchestration
// converges the new state.
func (s *DeploymentService) Resume(ctx context.Context, in ResumeInput) (domain.DeploymentView, error) {
	ac := AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return domain.DeploymentView{}, fmt.Errorf("%w: resuming a deployment requires an authenticated caller",
			domain.ErrInvalidArgument)
	}

	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.RollbackOnExit()

	dep, err := tx.Deployments().Get(ctx, in.Name)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	fulfillment, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID())
	if err != nil {
		return domain.DeploymentView{}, err
	}
	currentGen := fulfillment.Generation()
	if err := tx.Commit(); err != nil {
		return domain.DeploymentView{}, fmt.Errorf("commit read tx: %w", err)
	}

	exec, err := s.ResumeWF.Start(ctx, domain.ResumeDeploymentInput{
		Name: in.Name,
		Auth: domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		},
		UserSignature:      in.UserSignature,
		ValidUntil:         in.ValidUntil,
		Etag:               in.Etag,
		ExpectedGeneration: in.ExpectedGeneration,
	}, currentGen)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("start resume-deployment workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("resume-deployment workflow: %w", err)
	}

	return result, nil
}

// Delete starts a durable delete-deployment workflow that transitions
// the fulfillment to [domain.FulfillmentStateDeleting], bumps its
// generation, and guarantees orchestration converges the delete. If
// the fulfillment is already deleting and not paused, the current
// view is returned without starting a new workflow (idempotent).
func (s *DeploymentService) Delete(ctx context.Context, name domain.ResourceName) (domain.DeploymentView, error) {
	var auth domain.DeliveryAuth
	ac := AuthFromContext(ctx)
	if ac != nil && ac.Subject != nil {
		auth = domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		}
	}

	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.RollbackOnExit()

	dep, err := tx.Deployments().Get(ctx, name)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	fulfillment, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID())
	if err != nil {
		return domain.DeploymentView{}, err
	}

	// The idempotent path returns the current view, including its
	// deliveries, so the response matches a read of the same resource.
	idempotent := fulfillment.State() == domain.FulfillmentStateDeleting && !fulfillment.Paused()
	var deliveries []domain.Delivery
	if idempotent {
		deliveries, err = tx.Deliveries().ListByFulfillment(ctx, dep.FulfillmentID())
		if err != nil {
			return domain.DeploymentView{}, fmt.Errorf("list deliveries: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return domain.DeploymentView{}, fmt.Errorf("commit read tx: %w", err)
	}

	if idempotent {
		return domain.DeploymentView{Deployment: dep, Fulfillment: *fulfillment, Deliveries: deliveries}, nil
	}

	exec, err := s.DeleteWF.Start(ctx, domain.DeleteDeploymentInput{
		Name: name,
		Auth: auth,
	}, fulfillment.Generation())
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("start delete-deployment workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("delete-deployment workflow: %w", err)
	}

	return result, nil
}
