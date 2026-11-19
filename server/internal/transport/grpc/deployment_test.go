package grpc_test

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/stolostron/ocm-fleet/server/gen/ocm/v1alpha"
	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/delivery"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/memworkflow"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
	transportgrpc "github.com/stolostron/ocm-fleet/server/internal/transport/grpc"
)

const testTargetType domain.TargetType = "test"

func setup(t *testing.T) pb.DeploymentServiceClient {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(testTargetType, recordingAgent)

	reg := &memworkflow.Registry{}
	recordingAgent.Reporter = application.NewDeliveryReportService(store, reg)

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	cwfSpec := &domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createWf, err := reg.RegisterCreateDeployment(cwfSpec)
	if err != nil {
		t.Fatalf("RegisterCreateDeployment: %v", err)
	}

	cleanupSpec := &domain.DeleteDeploymentCleanupWorkflowSpec{
		Store: store,
	}
	cleanupWf, err := reg.RegisterDeleteDeploymentCleanup(cleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeploymentCleanup: %v", err)
	}

	deleteSpec := &domain.DeleteDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	}
	deleteWf, err := reg.RegisterDeleteDeployment(deleteSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeployment: %v", err)
	}

	resumeSpec := &domain.ResumeDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	resumeWf, err := reg.RegisterResumeDeployment(resumeSpec)
	if err != nil {
		t.Fatalf("RegisterResumeDeployment: %v", err)
	}

	deploymentSvc := &application.DeploymentService{
		Store:    store,
		CreateWF: createWf,
		DeleteWF: deleteWf,
		ResumeWF: resumeWf,
	}

	// Register a test target so placements resolve.
	targetSvc := &application.TargetService{Store: store}
	if err := targetSvc.Register(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "t1", Type: testTargetType, Name: "test-target",
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterDeploymentServiceServer(srv, &transportgrpc.DeploymentServer{
		Deployments: deploymentSvc,
	})

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewDeploymentServiceClient(conn)
}

func TestCreateThenGet(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"key": "value"})

	created, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
		DeploymentId: "dep-1",
		Deployment: &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type: pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{
					ManifestType: "test.resource",
					Raw:          raw,
				}},
			},
			PlacementStrategy: &pb.PlacementStrategy{
				Type:      pb.PlacementStrategy_TYPE_STATIC,
				TargetIds: []string{"t1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}

	if created.GetName() != "deployments/dep-1" {
		t.Errorf("name = %q, want %q", created.GetName(), "deployments/dep-1")
	}
	if created.GetUid() == "" {
		t.Error("created uid is empty, want non-empty UUID")
	}
	if created.GetCreateTime() == nil {
		t.Error("created create_time is nil, want non-nil")
	}
	if created.GetEtag() == "" {
		t.Error("created etag is empty, want non-empty")
	}

	got, err := client.GetDeployment(ctx, &pb.GetDeploymentRequest{
		Name: "deployments/dep-1",
	})
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}

	if got.GetName() != created.GetName() {
		t.Errorf("got name = %q, want %q", got.GetName(), created.GetName())
	}
	if got.GetManifestStrategy().GetType() != pb.ManifestStrategy_TYPE_INLINE {
		t.Errorf("manifest strategy type = %v, want INLINE", got.GetManifestStrategy().GetType())
	}
	if got.GetPlacementStrategy().GetType() != pb.PlacementStrategy_TYPE_STATIC {
		t.Errorf("placement strategy type = %v, want STATIC", got.GetPlacementStrategy().GetType())
	}
	if got.GetUid() == "" {
		t.Error("uid is empty, want non-empty UUID")
	}
	if got.GetCreateTime() == nil {
		t.Error("create_time is nil, want non-nil")
	}
	if got.GetUpdateTime() == nil {
		t.Error("update_time is nil, want non-nil")
	}
	if got.GetEtag() == "" {
		t.Error("etag is empty, want non-empty")
	}
}

func TestCreateThenList(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"k": "v"})
	for _, id := range []string{"dep-a", "dep-b"} {
		_, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
			DeploymentId: id,
			Deployment: &pb.Deployment{
				ManifestStrategy: &pb.ManifestStrategy{
					Type:      pb.ManifestStrategy_TYPE_INLINE,
					Manifests: []*pb.Manifest{{ManifestType: "t", Raw: raw}},
				},
				PlacementStrategy: &pb.PlacementStrategy{
					Type:      pb.PlacementStrategy_TYPE_STATIC,
					TargetIds: []string{"t1"},
				},
			},
		})
		if err != nil {
			t.Fatalf("CreateDeployment(%s): %v", id, err)
		}
	}

	resp, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	if err != nil {
		t.Fatalf("ListDeployments: %v", err)
	}
	if len(resp.GetDeployments()) != 2 {
		t.Fatalf("got %d deployments, want 2", len(resp.GetDeployments()))
	}
	if got := resp.GetDeployments()[0].GetName(); got != "deployments/dep-a" {
		t.Errorf("first name = %q, want deployments/dep-a (listed by name)", got)
	}
}

func TestListDeploymentsPages(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"k": "v"})
	for _, id := range []string{"dep-a", "dep-b", "dep-c"} {
		_, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
			DeploymentId: id,
			Deployment: &pb.Deployment{
				ManifestStrategy: &pb.ManifestStrategy{
					Type:      pb.ManifestStrategy_TYPE_INLINE,
					Manifests: []*pb.Manifest{{ManifestType: "t", Raw: raw}},
				},
				PlacementStrategy: &pb.PlacementStrategy{
					Type:      pb.PlacementStrategy_TYPE_STATIC,
					TargetIds: []string{"t1"},
				},
			},
		})
		if err != nil {
			t.Fatalf("CreateDeployment(%s): %v", id, err)
		}
	}

	var names []string
	token := ""
	for page := 0; ; page++ {
		if page > 3 {
			t.Fatal("paging did not terminate")
		}
		resp, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{
			PageSize:  1,
			PageToken: token,
		})
		if err != nil {
			t.Fatalf("ListDeployments(page %d): %v", page, err)
		}
		if len(resp.GetDeployments()) != 1 {
			t.Fatalf("page %d returned %d deployments, want 1", page, len(resp.GetDeployments()))
		}
		names = append(names, resp.GetDeployments()[0].GetName())
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}
	want := []string{"deployments/dep-a", "deployments/dep-b", "deployments/dep-c"}
	if len(names) != len(want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("names = %v, want %v", names, want)
		}
	}

	// A page size above the maximum still returns the whole set here,
	// and the last page carries no token.
	all, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListDeployments(oversized): %v", err)
	}
	if len(all.GetDeployments()) != 3 || all.GetNextPageToken() != "" {
		t.Errorf("oversized page = %d deployments, token %q; want 3, empty", len(all.GetDeployments()), all.GetNextPageToken())
	}

	// A page size of zero means the default, not an empty page.
	def, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{PageSize: 0})
	if err != nil {
		t.Fatalf("ListDeployments(default): %v", err)
	}
	if len(def.GetDeployments()) != 3 {
		t.Errorf("default page = %d deployments, want 3", len(def.GetDeployments()))
	}

	// A negative page size is invalid, not a request for the default.
	_, err = client.ListDeployments(ctx, &pb.ListDeploymentsRequest{PageSize: -1})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("negative page size error = %v, want InvalidArgument", err)
	}

	// A token this server did not issue is rejected.
	_, err = client.ListDeployments(ctx, &pb.ListDeploymentsRequest{PageToken: "not-a-token!"})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("invalid token error = %v, want InvalidArgument", err)
	}
}

func TestGetNotFound(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	_, err := client.GetDeployment(ctx, &pb.GetDeploymentRequest{
		Name: "deployments/nonexistent",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

func TestCreateDuplicate(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"k": "v"})
	req := &pb.CreateDeploymentRequest{
		DeploymentId: "dup",
		Deployment: &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type:      pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{ManifestType: "t", Raw: raw}},
			},
			PlacementStrategy: &pb.PlacementStrategy{
				Type:      pb.PlacementStrategy_TYPE_STATIC,
				TargetIds: []string{"t1"},
			},
		},
	}

	if _, err := client.CreateDeployment(ctx, req); err != nil {
		t.Fatalf("first create: %v", err)
	}

	_, err := client.CreateDeployment(ctx, req)
	if err == nil {
		t.Fatal("expected error on duplicate, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.AlreadyExists {
		t.Errorf("code = %v, want AlreadyExists", status.Code(err))
	}
}

func TestCreateMissingFields(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	_, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
		DeploymentId: "",
		Deployment:   &pb.Deployment{},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestCreateStateAndReconciling(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"k": "v"})
	created, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
		DeploymentId: "state-test",
		Deployment: &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type:      pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{ManifestType: "t", Raw: raw}},
			},
			PlacementStrategy: &pb.PlacementStrategy{
				Type:      pb.PlacementStrategy_TYPE_STATIC,
				TargetIds: []string{"t1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}

	// The create workflow returns the deployment in pending/CREATING state.
	if created.GetState() != pb.Deployment_STATE_CREATING {
		t.Errorf("state = %v, want STATE_CREATING", created.GetState())
	}
	if !created.GetReconciling() {
		t.Error("reconciling = false, want true for CREATING state")
	}
}

func TestDeleteDeployment(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"k": "v"})
	_, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
		DeploymentId: "del-1",
		Deployment: &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type:      pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{ManifestType: "t", Raw: raw}},
			},
			PlacementStrategy: &pb.PlacementStrategy{
				Type:      pb.PlacementStrategy_TYPE_STATIC,
				TargetIds: []string{"t1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}

	deleted, err := client.DeleteDeployment(ctx, &pb.DeleteDeploymentRequest{
		Name: "deployments/del-1",
	})
	if err != nil {
		t.Fatalf("DeleteDeployment: %v", err)
	}
	if deleted.GetState() != pb.Deployment_STATE_DELETING {
		t.Errorf("state = %v, want STATE_DELETING", deleted.GetState())
	}
}

func TestDeleteNotFound(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	_, err := client.DeleteDeployment(ctx, &pb.DeleteDeploymentRequest{
		Name: "deployments/nonexistent",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

func TestCreateDeploymentInvalidID(t *testing.T) {
	client := setup(t)
	ctx := context.Background()
	deployment := func() *pb.Deployment {
		raw, _ := json.Marshal(map[string]string{"k": "v"})
		return &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type:      pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{ManifestType: "test.resource", Raw: raw}},
			},
			PlacementStrategy: &pb.PlacementStrategy{Type: pb.PlacementStrategy_TYPE_ALL},
		}
	}
	for name, id := range map[string]string{
		"dots":          "..",
		"uppercase":     "Dep-1",
		"space":         "dep 1",
		"leading dash":  "-dep",
		"64 characters": strings.Repeat("d", 64),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{DeploymentId: id, Deployment: deployment()})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CreateDeployment id %q = %v, want InvalidArgument", id, err)
			}
		})
	}
}

func TestCreateDeploymentInvalidSelectorLabels(t *testing.T) {
	client := setup(t)
	ctx := context.Background()
	raw, _ := json.Marshal(map[string]string{"k": "v"})
	deployment := func(matchLabels map[string]string) *pb.Deployment {
		return &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type:      pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{ManifestType: "test.resource", Raw: raw}},
			},
			PlacementStrategy: &pb.PlacementStrategy{
				Type:           pb.PlacementStrategy_TYPE_SELECTOR,
				TargetSelector: &pb.TargetSelector{MatchLabels: matchLabels},
			},
		}
	}
	for name, matchLabels := range map[string]map[string]string{
		"three parts":    {"a/b/c": "v"},
		"empty name":     {"example.com/": "v"},
		"space in value": {"app": "has space"},
		"value too long": {"app": strings.Repeat("v", 64)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
				DeploymentId: "dep-labels-" + strings.ReplaceAll(name, " ", "-"),
				Deployment:   deployment(matchLabels),
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CreateDeployment selector %v = %v, want InvalidArgument", matchLabels, err)
			}
		})
	}
	if _, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
		DeploymentId: "dep-labels-ok",
		Deployment:   deployment(map[string]string{"app.kubernetes.io/name": "web", "env": "prod"}),
	}); err != nil {
		t.Fatalf("a valid selector = %v", err)
	}
}
