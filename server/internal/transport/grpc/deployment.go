package grpc

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/validation"

	pb "github.com/stolostron/ocm-fleet/server/gen/ocm/v1alpha"
	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/kubelabels"
)

const deploymentCollectionID = "deployments"

// Deployment pages use the same default and maximum as the query surface.
const (
	defaultDeploymentPageSize = 50
	maxDeploymentPageSize     = 500
)

// DeploymentServer implements [pb.DeploymentServiceServer].
type DeploymentServer struct {
	pb.UnimplementedDeploymentServiceServer
	Deployments *application.DeploymentService
}

func (s *DeploymentServer) CreateDeployment(ctx context.Context, req *pb.CreateDeploymentRequest) (*pb.Deployment, error) {
	if req.GetDeploymentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "deployment_id is required")
	}
	if errs := validation.IsDNS1123Label(req.GetDeploymentId()); len(errs) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "deployment_id %q is not a DNS label: %s", req.GetDeploymentId(), strings.Join(errs, "; "))
	}
	if req.GetDeployment() == nil {
		return nil, status.Error(codes.InvalidArgument, "deployment is required")
	}

	input, err := createInputFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	view, err := s.Deployments.Create(ctx, input)
	if err != nil {
		return nil, domainError(err)
	}

	return deploymentToProto(view), nil
}

func (s *DeploymentServer) GetDeployment(ctx context.Context, req *pb.GetDeploymentRequest) (*pb.Deployment, error) {
	name, err := parseDeploymentName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	view, err := s.Deployments.Get(ctx, name)
	if err != nil {
		return nil, domainError(err)
	}

	return deploymentToProto(view), nil
}

func (s *DeploymentServer) ResumeDeployment(ctx context.Context, req *pb.ResumeDeploymentRequest) (*pb.Deployment, error) {
	name, err := parseDeploymentName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	in := application.ResumeInput{
		Name:               name,
		UserSignature:      req.GetUserSignature(),
		Etag:               domain.Etag(req.GetEtag()),
		ExpectedGeneration: domain.Generation(req.GetExpectedGeneration()),
	}
	if req.GetValidUntil() != nil {
		in.ValidUntil = req.GetValidUntil().AsTime()
	}

	view, err := s.Deployments.Resume(ctx, in)
	if err != nil {
		return nil, domainError(err)
	}

	return deploymentToProto(view), nil
}

func (s *DeploymentServer) ListDeployments(ctx context.Context, req *pb.ListDeploymentsRequest) (*pb.ListDeploymentsResponse, error) {
	size := int(req.GetPageSize())
	if size < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid page_size: must not be negative")
	}
	if size == 0 {
		size = defaultDeploymentPageSize
	}
	if size > maxDeploymentPageSize {
		size = maxDeploymentPageSize
	}

	// The token names the last deployment of the page just read, and the
	// repository starts strictly after that name, so a deployment deleted
	// between calls does not shift the next page.
	after := domain.ResourceName("")
	if tok := req.GetPageToken(); tok != "" {
		last, err := decodeDeploymentPageToken(tok)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		after = last
	}

	page, err := s.Deployments.ListPage(ctx, after, size)
	if err != nil {
		return nil, domainError(err)
	}

	out := make([]*pb.Deployment, 0, len(page.Views))
	for _, v := range page.Views {
		out = append(out, deploymentToProto(v))
	}
	next := ""
	if page.More && len(page.Views) > 0 {
		next = encodeDeploymentPageToken(page.Views[len(page.Views)-1].Deployment.Name())
	}
	return &pb.ListDeploymentsResponse{Deployments: out, NextPageToken: next}, nil
}

// encodeDeploymentPageToken encodes the last deployment name of a page.
func encodeDeploymentPageToken(name domain.ResourceName) string {
	return base64.RawURLEncoding.EncodeToString([]byte(name))
}

// decodeDeploymentPageToken reads a token written by
// [encodeDeploymentPageToken].
func decodeDeploymentPageToken(token string) (domain.ResourceName, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("not a deployment page token: %w", err)
	}
	if len(b) == 0 {
		return "", fmt.Errorf("not a deployment page token: empty name")
	}
	return domain.ResourceName(b), nil
}

func (s *DeploymentServer) DeleteDeployment(ctx context.Context, req *pb.DeleteDeploymentRequest) (*pb.Deployment, error) {
	name, err := parseDeploymentName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	view, err := s.Deployments.Delete(ctx, name)
	if err != nil {
		return nil, domainError(err)
	}

	return deploymentToProto(view), nil
}

// --- resource name helpers ---

func parseDeploymentName(raw string) (domain.ResourceName, error) {
	name, err := domain.ParseResourceName(raw)
	if err != nil {
		return "", fmt.Errorf("deployment name: %w", err)
	}
	if name.CollectionID() != domain.CollectionID(deploymentCollectionID) {
		return "", fmt.Errorf("deployment name: collection must be %q, got %q", deploymentCollectionID, name.CollectionID())
	}
	return name, nil
}

func buildDeploymentName(id string) (domain.ResourceName, error) {
	rid, err := domain.NewResourceID(id)
	if err != nil {
		return "", fmt.Errorf("deployment id: %w", err)
	}
	return domain.NewResourceName(domain.NewCollectionName(domain.CollectionID(deploymentCollectionID)), rid)
}

// --- proto <-> domain mapping ---

func createInputFromProto(req *pb.CreateDeploymentRequest) (domain.CreateDeploymentInput, error) {
	name, err := buildDeploymentName(req.GetDeploymentId())
	if err != nil {
		return domain.CreateDeploymentInput{}, err
	}

	d := req.GetDeployment()
	ms, err := manifestStrategyFromProto(d.GetManifestStrategy())
	if err != nil {
		return domain.CreateDeploymentInput{}, err
	}
	ps, err := placementStrategyFromProto(d.GetPlacementStrategy())
	if err != nil {
		return domain.CreateDeploymentInput{}, err
	}
	var rs *domain.RolloutStrategySpec
	if d.GetRolloutStrategy() != nil {
		v, err := rolloutStrategyFromProto(d.GetRolloutStrategy())
		if err != nil {
			return domain.CreateDeploymentInput{}, err
		}
		rs = &v
	}
	in := domain.CreateDeploymentInput{
		Name:              name,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		RolloutStrategy:   rs,
		UserSignature:     req.GetUserSignature(),
	}
	if req.GetValidUntil() != nil {
		in.ValidUntil = req.GetValidUntil().AsTime()
	}
	return in, nil
}

func manifestStrategyFromProto(p *pb.ManifestStrategy) (domain.ManifestStrategySpec, error) {
	if p == nil {
		return domain.ManifestStrategySpec{}, fmt.Errorf("manifest_strategy is required")
	}
	switch p.GetType() {
	case pb.ManifestStrategy_TYPE_INLINE:
		manifests := make([]domain.Manifest, len(p.GetManifests()))
		for i, m := range p.GetManifests() {
			manifests[i] = domain.Manifest{
				ManifestType: domain.ManifestType(m.GetManifestType()),
				Raw:          m.GetRaw(),
			}
		}
		return domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: manifests,
		}, nil
	default:
		return domain.ManifestStrategySpec{}, fmt.Errorf("unsupported manifest_strategy type: %v", p.GetType())
	}
}

func placementStrategyFromProto(p *pb.PlacementStrategy) (domain.PlacementStrategySpec, error) {
	if p == nil {
		return domain.PlacementStrategySpec{}, fmt.Errorf("placement_strategy is required")
	}
	switch p.GetType() {
	case pb.PlacementStrategy_TYPE_STATIC:
		targets := make([]domain.TargetID, len(p.GetTargetIds()))
		for i, id := range p.GetTargetIds() {
			targets[i] = domain.TargetID(id)
		}
		return domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: targets,
		}, nil
	case pb.PlacementStrategy_TYPE_ALL:
		return domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll}, nil
	case pb.PlacementStrategy_TYPE_SELECTOR:
		sel := p.GetTargetSelector()
		if sel == nil {
			return domain.PlacementStrategySpec{}, fmt.Errorf("target_selector is required for SELECTOR placement")
		}
		for k, v := range sel.GetMatchLabels() {
			if !kubelabels.Valid(k, v) {
				return domain.PlacementStrategySpec{}, fmt.Errorf("target_selector match_labels: %q=%q is not a valid Kubernetes label", k, v)
			}
		}
		return domain.PlacementStrategySpec{
			Type:           domain.PlacementStrategySelector,
			TargetSelector: &domain.TargetSelector{MatchLabels: sel.GetMatchLabels()},
		}, nil
	default:
		return domain.PlacementStrategySpec{}, fmt.Errorf("unsupported placement_strategy type: %v", p.GetType())
	}
}

func rolloutStrategyFromProto(p *pb.RolloutStrategy) (domain.RolloutStrategySpec, error) {
	switch p.GetType() {
	case pb.RolloutStrategy_TYPE_IMMEDIATE:
		return domain.RolloutStrategySpec{Type: domain.RolloutStrategyImmediate}, nil
	default:
		return domain.RolloutStrategySpec{}, fmt.Errorf("unsupported rollout_strategy type: %v", p.GetType())
	}
}

func deploymentToProto(v domain.DeploymentView) *pb.Deployment {
	d := v.Deployment
	f := v.Fulfillment
	dep := &pb.Deployment{
		Name:  string(d.Name()),
		State: fulfillmentStateToProto(f.State()),
	}

	dep.Reconciling = f.Reconciling()
	dep.PauseReason = f.PauseReason()

	dep.ManifestStrategy = manifestStrategyToProto(f.ManifestStrategy())
	dep.PlacementStrategy = placementStrategyToProto(f.PlacementStrategy())
	if rs := f.RolloutStrategy(); rs != nil {
		dep.RolloutStrategy = &pb.RolloutStrategy{
			Type: rolloutStrategyTypeToProto(rs.Type),
		}
	}

	if targets := f.ResolvedTargets(); len(targets) > 0 {
		ids := make([]string, len(targets))
		for i, t := range targets {
			ids[i] = string(t)
		}
		dep.ResolvedTargetIds = ids
	}

	if !d.CreatedAt().IsZero() {
		dep.CreateTime = timestamppb.New(d.CreatedAt())
	}
	if !d.UpdatedAt().IsZero() {
		dep.UpdateTime = timestamppb.New(d.UpdatedAt())
	}
	dep.Uid = d.UID().String()
	dep.Etag = string(v.Etag())

	if prov := f.Provenance(); prov != nil {
		dep.Provenance = provenanceToProto(prov)
	}

	dep.Generation = int64(f.Generation())

	dep.TargetStatuses = targetStatusesToProto(v.Deliveries)

	return dep
}

// targetStatusesToProto maps the fulfillment's delivery records to the
// per-target statuses the API exposes. Order is preserved from the
// repository, which orders by target ID.
func targetStatusesToProto(deliveries []domain.Delivery) []*pb.TargetDeliveryStatus {
	if len(deliveries) == 0 {
		return nil
	}
	out := make([]*pb.TargetDeliveryStatus, len(deliveries))
	for i := range deliveries {
		out[i] = targetStatusToProto(deliveries[i])
	}
	return out
}

func targetStatusToProto(d domain.Delivery) *pb.TargetDeliveryStatus {
	s := &pb.TargetDeliveryStatus{
		TargetId:  string(d.TargetID()),
		State:     deliveryStateToProto(d.State()),
		Message:   d.Message(),
		Attempts:  int32(d.Attempts()),
		Operation: deliveryOperationToProto(d.Operation()),
	}
	if !d.UpdatedAt().IsZero() {
		s.UpdateTime = timestamppb.New(d.UpdatedAt())
	}
	return s
}

func deliveryOperationToProto(o domain.DeliveryOperation) pb.TargetDeliveryStatus_Operation {
	switch o {
	case domain.DeliveryOperationDeliver:
		return pb.TargetDeliveryStatus_OPERATION_DELIVER
	case domain.DeliveryOperationRemove:
		return pb.TargetDeliveryStatus_OPERATION_REMOVE
	default:
		return pb.TargetDeliveryStatus_OPERATION_UNSPECIFIED
	}
}

func deliveryStateToProto(s domain.DeliveryState) pb.TargetDeliveryStatus_State {
	switch s {
	case domain.DeliveryStatePending:
		return pb.TargetDeliveryStatus_STATE_PENDING
	case domain.DeliveryStateAccepted:
		return pb.TargetDeliveryStatus_STATE_ACCEPTED
	case domain.DeliveryStateProgressing:
		return pb.TargetDeliveryStatus_STATE_PROGRESSING
	case domain.DeliveryStateDelivered:
		return pb.TargetDeliveryStatus_STATE_DELIVERED
	case domain.DeliveryStateFailed:
		return pb.TargetDeliveryStatus_STATE_FAILED
	case domain.DeliveryStatePartial:
		return pb.TargetDeliveryStatus_STATE_PARTIAL
	case domain.DeliveryStateAuthFailed:
		return pb.TargetDeliveryStatus_STATE_AUTH_FAILED
	default:
		return pb.TargetDeliveryStatus_STATE_UNSPECIFIED
	}
}

func provenanceToProto(p *domain.Provenance) *pb.Provenance {
	prov := &pb.Provenance{
		Signature: &pb.Signature{
			Signer: &pb.FederatedIdentity{
				Subject: string(p.Sig.Signer.Subject),
				Issuer:  string(p.Sig.Signer.Issuer),
			},
			ContentHash:    p.Sig.ContentHash,
			SignatureBytes: p.Sig.SignatureBytes,
		},
		ValidUntil:         timestamppb.New(p.ValidUntil),
		ExpectedGeneration: int64(p.ExpectedGeneration),
	}

	if len(p.OutputConstraints) > 0 {
		prov.OutputConstraints = make([]*pb.OutputConstraint, len(p.OutputConstraints))
		for i, c := range p.OutputConstraints {
			prov.OutputConstraints[i] = &pb.OutputConstraint{
				Name:       c.Name,
				Expression: c.Expression,
			}
		}
	}

	return prov
}

func fulfillmentStateToProto(s domain.FulfillmentState) pb.Deployment_State {
	switch s {
	case domain.FulfillmentStateCreating:
		return pb.Deployment_STATE_CREATING
	case domain.FulfillmentStateActive:
		return pb.Deployment_STATE_ACTIVE
	case domain.FulfillmentStateDeleting:
		return pb.Deployment_STATE_DELETING
	case domain.FulfillmentStateFailed:
		return pb.Deployment_STATE_FAILED
	default:
		return pb.Deployment_STATE_UNSPECIFIED
	}
}

func manifestStrategyToProto(s domain.ManifestStrategySpec) *pb.ManifestStrategy {
	ms := &pb.ManifestStrategy{}
	switch s.Type {
	case domain.ManifestStrategyInline:
		ms.Type = pb.ManifestStrategy_TYPE_INLINE
	}
	if len(s.Manifests) > 0 {
		ms.Manifests = make([]*pb.Manifest, len(s.Manifests))
		for i, m := range s.Manifests {
			ms.Manifests[i] = &pb.Manifest{
				ManifestType: string(m.ManifestType),
				Raw:          m.Raw,
			}
		}
	}
	return ms
}

func placementStrategyToProto(s domain.PlacementStrategySpec) *pb.PlacementStrategy {
	ps := &pb.PlacementStrategy{}
	switch s.Type {
	case domain.PlacementStrategyStatic:
		ps.Type = pb.PlacementStrategy_TYPE_STATIC
		ids := make([]string, len(s.Targets))
		for i, t := range s.Targets {
			ids[i] = string(t)
		}
		ps.TargetIds = ids
	case domain.PlacementStrategyAll:
		ps.Type = pb.PlacementStrategy_TYPE_ALL
	case domain.PlacementStrategySelector:
		ps.Type = pb.PlacementStrategy_TYPE_SELECTOR
		if s.TargetSelector != nil {
			ps.TargetSelector = &pb.TargetSelector{MatchLabels: s.TargetSelector.MatchLabels}
		}
	}
	return ps
}

func rolloutStrategyTypeToProto(t domain.RolloutStrategyType) pb.RolloutStrategy_Type {
	switch t {
	case domain.RolloutStrategyImmediate:
		return pb.RolloutStrategy_TYPE_IMMEDIATE
	default:
		return pb.RolloutStrategy_TYPE_UNSPECIFIED
	}
}

// --- error mapping ---

func domainError(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrStaleGeneration):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
