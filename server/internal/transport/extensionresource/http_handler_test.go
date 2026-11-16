package extensionresource_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"buf.build/go/protovalidate"

	ocmaddon "github.com/stolostron/ocm-fleet/server/internal/addon/ocm"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
	"github.com/stolostron/ocm-fleet/server/internal/transport/dynamicapi"
	"github.com/stolostron/ocm-fleet/server/internal/transport/extensionresource"
)

// TestBuildHTTPHandler_UsesRegisteredPrefix proves that the prefix
// passed to BuildHTTPHandler is what gets trimmed for ID extraction.
// Activator's replace path must pass the *new* canonical prefix; using
// the old one leaves a path fragment in the ID, so Get looks up the
// wrong resource name.
func TestBuildHTTPHandler_UsesRegisteredPrefix(t *testing.T) {
	svc := buildFullClusterService(t)
	addr, conn, _ := serveGRPCOverTCP(t, svc)
	_ = addr

	const (
		oldPrefix = "/apis/kind.open-cluster-management.io/v1alpha/clusters"
		newPrefix = "/apis/kindv2.open-cluster-management.io/v1alpha/clusters"
		id        = "prefix-parse-id"
	)

	// Seed a resource under the clean ID via the correct handler.
	create := extensionresource.BuildHTTPHandler(svc, conn, newPrefix)
	createReq := httptest.NewRequest(http.MethodPost, newPrefix+"?cluster_id="+id,
		strings.NewReader(`{"spec":{"name":"`+id+`"}}`))
	createRec := httptest.NewRecorder()
	create(createRec, createReq)
	if createRec.Code != http.StatusOK && createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %s", createRec.Code, createRec.Body.String())
	}

	// Correct prefix: ID trims to "prefix-parse-id" → found.
	right := extensionresource.BuildHTTPHandler(svc, conn, newPrefix)
	reqOK := httptest.NewRequest(http.MethodGet, newPrefix+"/"+id, nil)
	recOK := httptest.NewRecorder()
	right(recOK, reqOK)
	bodyOK, _ := io.ReadAll(recOK.Result().Body)
	if recOK.Code != http.StatusOK {
		t.Fatalf("correct-prefix GET status = %d body = %s, want 200", recOK.Code, bodyOK)
	}
	if !strings.Contains(string(bodyOK), "clusters/"+id) {
		t.Fatalf("correct-prefix GET body = %s, want name clusters/%s", bodyOK, id)
	}

	// Wrong prefix (the Activate bug): TrimPrefix does not strip, so the
	// path tail becomes "apis/kindv2.../prefix-parse-id", which is not a
	// resource name; the lookup fails instead of finding the resource
	// seeded under the clean ID.
	wrong := extensionresource.BuildHTTPHandler(svc, conn, oldPrefix)
	reqBad := httptest.NewRequest(http.MethodGet, newPrefix+"/"+id, nil)
	recBad := httptest.NewRecorder()
	wrong(recBad, reqBad)
	bodyBad, _ := io.ReadAll(recBad.Result().Body)
	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("wrong-prefix GET status = %d body = %s, want 400 (malformed name)", recBad.Code, bodyBad)
	}
}

// TestBuildHTTPHandler_ListUsesCamelCasePageParameters proves the record
// list reads the API's query spelling (AIP-127: pageSize, pageToken) and
// refuses the proto spelling rather than ignoring it — a client sending
// page_size would otherwise get a silently unpaged answer while it
// believed it had set a page size.
func TestBuildHTTPHandler_ListUsesCamelCasePageParameters(t *testing.T) {
	svc := buildFullClusterService(t)
	_, conn, _ := serveGRPCOverTCP(t, svc)
	prefix := svc.Config.CanonicalHTTPPrefix()
	handler := extensionresource.BuildHTTPHandler(svc, conn, prefix)

	for _, id := range []string{"cluster-a", "cluster-b", "cluster-c"} {
		req := httptest.NewRequest(http.MethodPost, prefix+"?cluster_id="+id,
			strings.NewReader(`{"spec":{"name":"`+id+`"}}`))
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
			t.Fatalf("create %s status = %d body = %s", id, rec.Code, rec.Body.String())
		}
	}

	// The repeated field carries the collection's own name ("clusters"),
	// as the dynamic descriptor builds it.
	type listBody struct {
		Clusters []struct {
			Name string `json:"name"`
		} `json:"clusters"`
		NextPageToken string `json:"nextPageToken"`
	}
	list := func(query string) (listBody, *httptest.ResponseRecorder) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, prefix+query, nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		var body listBody
		if rec.Code == http.StatusOK {
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode list body: %v (%s)", err, rec.Body.String())
			}
		}
		return body, rec
	}

	page1, rec := list("?pageSize=2")
	if rec.Code != http.StatusOK {
		t.Fatalf("list pageSize=2 status = %d body = %s", rec.Code, rec.Body.String())
	}
	if len(page1.Clusters) != 2 || page1.NextPageToken == "" {
		t.Fatalf("first page = %d resources, token %q; want 2 and a token", len(page1.Clusters), page1.NextPageToken)
	}
	if page1.Clusters[0].Name != "clusters/cluster-a" || page1.Clusters[1].Name != "clusters/cluster-b" {
		t.Errorf("first page = %+v, want cluster-a then cluster-b", page1.Clusters)
	}
	page2, rec := list("?pageSize=2&pageToken=" + page1.NextPageToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("list second page status = %d body = %s", rec.Code, rec.Body.String())
	}
	if len(page2.Clusters) != 1 || page2.Clusters[0].Name != "clusters/cluster-c" || page2.NextPageToken != "" {
		t.Errorf("second page = %+v token %q; want [clusters/cluster-c] and no token", page2.Clusters, page2.NextPageToken)
	}

	for _, query := range []string{"?page_size=2", "?page_token=x"} {
		_, rec := list(query)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("list %s status = %d body = %s, want 400", query, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "unknown query parameter") {
			t.Errorf("list %s body = %q, want an unknown-query-parameter message", query, rec.Body.String())
		}
	}

	_, rec = list("?pageSize=two")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "invalid pageSize") {
		t.Errorf("list pageSize=two status = %d body = %q, want 400 with invalid pageSize", rec.Code, rec.Body.String())
	}

	_, rec = list("?pageSize=-1")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "must not be negative") {
		t.Errorf("list pageSize=-1 status = %d body = %q, want 400 with must not be negative", rec.Code, rec.Body.String())
	}
}

func buildInventoryOnlyNodeService(t *testing.T) *extensionresource.RegisteredService {
	t.Helper()
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}
	cfg := &extensionresource.ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      "v1",
			CollectionID: "nodes",
			Singular:     "Node",
			Plural:       "Nodes",
		},
		ResourceType: "test.open-cluster-management.io/Node",
		ProtoPackage: "ocm.test.v1alpha",
		Capabilities: extensionresource.ResourceCapabilities{
			Inventory: &extensionresource.InventoryCapabilityConfig{},
		},
	}
	svc, err := extensionresource.Build(cfg, extensionresource.Deps{Validator: validator})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return svc
}

// TestBuildHTTPHandler_InventoryOnlyUnsupportedVerbs404 proves that
// Create/Delete/Resume on an inventory-only collection return 404 from
// the capability-aware handler without panicking on nil request
// descriptors.
func TestBuildHTTPHandler_InventoryOnlyUnsupportedVerbs404(t *testing.T) {
	svc := buildInventoryOnlyNodeService(t)
	// No live gRPC needed — unsupported verbs must 404 before proxying.
	handler := extensionresource.BuildHTTPHandler(svc, nil, svc.Config.CanonicalHTTPPrefix())
	prefix := svc.Config.CanonicalHTTPPrefix()

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"create", http.MethodPost, prefix + "?node_id=n1", `{}`},
		{"delete", http.MethodDelete, prefix + "/n1", ""},
		{"resume", http.MethodPost, prefix + "/n1:resume", `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			rec := httptest.NewRecorder()
			handler(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d body = %s, want 404", rec.Code, rec.Body.String())
			}
		})
	}

	// Managed types still expose Create (smoke: descriptor present).
	managed := buildFullClusterService(t)
	if managed.Descriptors.CreateRequest == nil {
		t.Fatal("managed CreateRequest should be non-nil")
	}
	if len(managed.Desc.Methods) != 5 {
		t.Fatalf("managed method count = %d, want 5", len(managed.Desc.Methods))
	}
	if len(svc.Desc.Methods) != 2 {
		t.Fatalf("inventory-only method count = %d, want 2 (Get/List)", len(svc.Desc.Methods))
	}
}

// TestBuildHTTPHandler_CollectionRouteTakesAnIDNotAName proves that a
// collection-prefixed route reads a bare resource id: a full nested
// resource name sent there is a malformed id, not a record lookup. A
// record whose name carries parent collections is read at the service
// root, by its resource name (see the activator's route table).
func TestBuildHTTPHandler_CollectionRouteTakesAnIDNotAName(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	schema := ocmaddon.ManagedClusterSchema()
	svc := buildInventoryOnlySchemaService(t, store, schema)
	record := ocmaddon.ManagedClusterRecordName("hub-a", "mc-a")
	seedInventoryRecord(t, store, schema, record)

	_, conn, _ := serveGRPCOverTCP(t, svc)
	prefix := svc.Config.CanonicalHTTPPrefix()
	handler := extensionresource.BuildHTTPHandler(svc, conn, prefix)

	req := httptest.NewRequest(http.MethodGet, prefix+"/"+string(record), nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("GET %s/%s status = %d body = %s, want 400 (a resource name is not an id)",
			prefix, record, rec.Code, rec.Body.String())
	}
}
