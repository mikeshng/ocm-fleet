package extensionresource_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	ocmaddon "github.com/stolostron/ocm-fleet/server/internal/addon/ocm"
	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
	"github.com/stolostron/ocm-fleet/server/internal/transport/dynamicapi"
	"github.com/stolostron/ocm-fleet/server/internal/transport/extensionresource"
)

// buildInventoryOnlySchemaService registers the type row for an
// inventory-only schema and builds its dynamic service.
func buildInventoryOnlySchemaService(
	t *testing.T, store domain.Store, schema domain.ExtensionResourceSchema,
) *extensionresource.RegisteredService {
	t.Helper()

	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.RollbackOnExit()
	def := domain.NewExtensionResourceType(
		schema.ResourceType,
		domain.APIVersion(schema.Version),
		domain.CollectionID(schema.CollectionID),
		time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
		domain.WithInventory(),
	)
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}
	svc, err := extensionresource.Build(&extensionresource.ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      schema.Version,
			CollectionID: schema.CollectionID,
			Singular:     schema.Singular,
			Plural:       schema.Plural,
		},
		ResourceType: schema.ResourceType,
		ProtoPackage: schema.ProtoPackage,
		Capabilities: extensionresource.ResourceCapabilities{
			Inventory: &extensionresource.InventoryCapabilityConfig{},
		},
	}, extensionresource.Deps{
		Resources: application.NewExtensionResourceService(store, nil, nil, nil, nil),
		Validator: validator,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return svc
}

// listNames calls List over gRPC and returns the resource names it
// returns, in order.
func listNames(t *testing.T, conn *grpc.ClientConn, svc *extensionresource.RegisteredService) []string {
	t.Helper()
	req := dynamicpb.NewMessage(svc.Descriptors.ListRequest)
	resp := dynamicpb.NewMessage(svc.Descriptors.ListResponse)
	method := "/" + svc.Config.GRPCServiceName() + "/List" + svc.Config.Plural
	if err := conn.Invoke(context.Background(), method, req, resp); err != nil {
		t.Fatalf("List: %v", err)
	}
	resources := resp.Get(svc.Descriptors.ListResponse.Fields().ByNumber(1)).List()
	nameField := svc.Descriptors.Resource.Fields().ByName("name")
	names := make([]string, resources.Len())
	for i := range names {
		names[i] = resources.Get(i).Message().Get(nameField).String()
	}
	return names
}

// getOverGRPC calls Get over gRPC and returns the resource name in the
// response.
func getOverGRPC(
	t *testing.T, conn *grpc.ClientConn, svc *extensionresource.RegisteredService, name string,
) (string, error) {
	t.Helper()
	req := dynamicpb.NewMessage(svc.Descriptors.GetRequest)
	req.Set(svc.Descriptors.GetRequest.Fields().ByName("name"), protoreflect.ValueOfString(name))
	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/Get" + svc.Config.Singular
	if err := conn.Invoke(context.Background(), method, req, resp); err != nil {
		return "", err
	}
	return resp.Get(svc.Descriptors.Resource.Fields().ByName("name")).String(), nil
}

// seedInventoryRecord stores one inventory record of the schema's
// resource type, so both the gRPC service and the HTTP routes read it.
func seedInventoryRecord(
	t *testing.T, store domain.Store, schema domain.ExtensionResourceSchema, record domain.ResourceName,
) {
	t.Helper()
	if err := application.NewInventoryReportService(store).ApplyDeltaBatch(
		context.Background(),
		application.InventoryDeltaBatchInput{
			Reports: []application.InventoryDeltaInput{{
				ResourceType: schema.ResourceType,
				Name:         &record,
				ObservedAt:   time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
			}},
		},
	); err != nil {
		t.Fatalf("ApplyDeltaBatch: %v", err)
	}
}

// serviceHTTP mounts the schema's activated service over HTTP on the
// gRPC server the records live on, and returns the test server's base
// URL and the service root, /apis/{service}/{version}.
func serviceHTTP(
	t *testing.T, schema domain.ExtensionResourceSchema, conn *grpc.ClientConn,
) (baseURL, root string) {
	t.Helper()

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}
	httpMux := dynamicapi.NewDynamicHTTPMux(nil, conn)
	activator := &extensionresource.DynamicSchemaActivator{
		GRPCMux:  dynamicapi.NewDynamicServiceMux(),
		HTTPMux:  httpMux,
		Deps:     extensionresource.Deps{Validator: validator},
		Registry: extensionresource.NewActiveResourceRegistry(),
	}
	if _, err := activator.Activate(context.Background(), schema); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	ts := httptest.NewServer(httpMux.ServeMux())
	t.Cleanup(ts.Close)

	root = "/apis/" + string(schema.ResourceType.ServiceName()) + "/" + schema.Version
	return ts.URL, root
}

// httpGet performs one GET and returns its status and body.
func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET %s body: %v", url, err)
	}
	return resp.StatusCode, string(body)
}

// TestDynamic_GetNestedRecordNames proves that a record stored under a
// nested collection is read back by the name List returns, both over
// gRPC and over HTTP. The HTTP URL is /apis/{service}/{version}/{resource
// name} — the resource-name design's URL — so a managed cluster named
// "hubs/{hub}/managedClusters/{name}" is read at
// .../v1alpha/hubs/{hub}/managedClusters/{name}, not below the sibling
// collection with its parent path repeated. Managed clusters and watched
// Kubernetes objects ("clusters/{cluster}/.../objects/{uid}") are the
// types that store records under nested collections.
func TestDynamic_GetNestedRecordNames(t *testing.T) {
	cases := []struct {
		name   string
		schema domain.ExtensionResourceSchema
		record domain.ResourceName
	}{
		{
			name:   "managed_cluster",
			schema: ocmaddon.ManagedClusterSchema(),
			record: ocmaddon.ManagedClusterRecordName("hub-a", "mc-a"),
		},
		{
			name:   "watched_object",
			schema: kubernetesaddon.InventorySchema(),
			record: "clusters/c1/namespaces/ns1/apiResources/apps.deployments/objects/uid-1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
			svc := buildInventoryOnlySchemaService(t, store, tc.schema)

			record := tc.record
			seedInventoryRecord(t, store, tc.schema, record)

			_, conn, _ := serveGRPCOverTCP(t, svc)

			names := listNames(t, conn, svc)
			if len(names) != 1 || names[0] != string(record) {
				t.Fatalf("List names = %v, want [%s]", names, record)
			}

			got, err := getOverGRPC(t, conn, svc, names[0])
			if err != nil {
				t.Fatalf("Get(%q) over gRPC: %v", names[0], err)
			}
			if got != string(record) {
				t.Errorf("Get name = %q, want %q", got, record)
			}

			baseURL, root := serviceHTTP(t, tc.schema, conn)

			// The collection address still lists the record.
			code, body := httpGet(t, baseURL+root+"/"+tc.schema.CollectionID)
			if code != http.StatusOK {
				t.Fatalf("GET %s/%s status = %d body = %s, want 200",
					root, tc.schema.CollectionID, code, body)
			}
			if !strings.Contains(body, `"name":"`+string(record)+`"`) {
				t.Errorf("list body = %s, want name %q", body, record)
			}

			// The record is read at the URL its resource name prescribes.
			code, body = httpGet(t, baseURL+root+"/"+string(record))
			if code != http.StatusOK {
				t.Fatalf("GET %s/%s status = %d body = %s, want 200",
					root, record, code, body)
			}
			if !strings.Contains(body, `"name":"`+string(record)+`"`) {
				t.Errorf("record body = %s, want name %q", body, record)
			}

			// The collection-prefixed spelling is not the record's URL: it
			// would route the name's parent segments through the wrong
			// collection and make the URL carry the collection twice.
			code, body = httpGet(t, baseURL+root+"/"+tc.schema.CollectionID+"/"+string(record))
			if code != http.StatusBadRequest {
				t.Errorf("GET %s/%s/%s status = %d body = %s, want 400",
					root, tc.schema.CollectionID, record, code, body)
			}
		})
	}
}

// TestDynamic_GetRejectsForeignCollectionName proves that a full name
// whose leaf collection is not this service's collection is rejected
// instead of being looked up.
func TestDynamic_GetRejectsForeignCollectionName(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	svc := buildInventoryOnlySchemaService(t, store, ocmaddon.ManagedClusterSchema())
	_, conn, _ := serveGRPCOverTCP(t, svc)

	_, err := getOverGRPC(t, conn, svc, "hubs/hub-a/hubs/other")
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("Get foreign name error = %v, want InvalidArgument", err)
	}
}
