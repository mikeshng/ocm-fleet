package ocm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
)

type httpHarness struct {
	*harness
	mux *http.ServeMux
}

func newHTTPHarness(t *testing.T) *httpHarness {
	h := newHarness(t)
	mux := http.NewServeMux()
	// The dynamic inventory API owns the method-less prefixes of the
	// record types; the integration's verbs must coexist with it.
	dynamic := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	mux.Handle(APIPrefix+"/hubs", dynamic)
	mux.Handle(APIPrefix+"/hubs/", dynamic)
	handlers := NewHTTP(h.svc, testEndpoint(), func(context.Context) string { return "ops@example.test" },
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	handle := func(pattern string, handler http.Handler) { mux.Handle(pattern, handler) }
	handlers.Register(handle, handle)
	return &httpHarness{harness: h, mux: mux}
}

func (h *httpHarness) call(method, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("encode body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, APIPrefix+path, reader)
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 && strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			h.t.Fatalf("%s %s: response is not JSON: %v: %s", method, path, err, rec.Body.String())
		}
	}
	return rec.Code, out
}

func TestHTTPHubAccessManifest(t *testing.T) {
	h := newHTTPHarness(t)
	code, out := h.call(http.MethodGet, "/hubAccessManifest", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d: %v", code, out)
	}
	manifest, _ := out["manifest"].(string)
	if !strings.Contains(manifest, "kind: ClusterRole") || !strings.Contains(manifest, HubReaderServiceAccount) || strings.Contains(manifest, "kind: Secret") {
		t.Errorf("manifest:\n%s", manifest)
	}
	if steps, _ := out["instructions"].([]any); len(steps) < 3 {
		t.Errorf("instructions = %v", out["instructions"])
	}
}

func TestHTTPConnectEnrollmentAddonDisconnect(t *testing.T) {
	h := newHTTPHarness(t)
	h.createCluster(managedCluster("ext"))

	code, out := h.call(http.MethodPost, "/hubs:connect", map[string]any{
		"hubId": "hub1", "displayName": "Production hub",
		"apiServer": hubAPI, "caCert": string(h.hubCA), "token": hubToken,
	})
	if code != http.StatusOK {
		t.Fatalf("connect status = %d: %v", code, out)
	}
	hub := out["hub"].(map[string]any)
	obs := hub["observation"].(map[string]any)
	if hub["id"] != "hub1" || hub["name"] != "hubs/hub1" || obs["displayName"] != "Production hub" || obs["connectedBy"] != "ops@example.test" {
		t.Errorf("connect response = %v", out)
	}
	if raw, _ := json.Marshal(out); bytes.Contains(raw, []byte(hubToken)) {
		t.Errorf("the connect response repeats the hub credential")
	}
	eventually(t, "ext recorded", func() bool { _, ok := h.observed("hub1", "ext"); return ok })

	code, out = h.call(http.MethodPost, "/hubs/hub1:enrollmentAddon", map[string]any{
		"profile": "read-only", "labels": map[string]string{"team": "blue"}, "tokenLifetimeSeconds": 3600,
	})
	if code != http.StatusOK {
		t.Fatalf("enrollmentAddon status = %d: %v", code, out)
	}
	manifest, _ := out["manifest"].(string)
	enable, _ := out["enableExample"].(string)
	if !strings.Contains(manifest, "kind: AddOnTemplate") || !strings.Contains(manifest, "kind: ClusterManagementAddOn") ||
		!strings.Contains(manifest, "--cluster-name={{CLUSTER_NAME}}") || !strings.Contains(manifest, "--token-lifetime=1h0m0s") {
		t.Errorf("manifest:\n%s", manifest)
	}
	if !strings.Contains(manifest, "name: view") || strings.Contains(manifest, "name: edit") {
		t.Errorf("manifest does not bind the read-only profile:\n%s", manifest)
	}
	if !strings.Contains(enable, "kind: ManagedClusterAddOn") {
		t.Errorf("enable example:\n%s", enable)
	}
	bundle := out["bundle"].(map[string]any)
	id, _ := bundle["id"].(string)
	if !strings.HasPrefix(id, "eb-") || bundle["profile"] != "read-only" {
		t.Errorf("bundle = %v", bundle)
	}
	// The secret is inside the manifest and nowhere else in the response.
	if !strings.Contains(manifest, "token: "+id+".") {
		t.Errorf("manifest does not carry the bundle secret")
	}
	delete(out, "manifest")
	if raw, _ := json.Marshal(out); bytes.Contains(raw, []byte(id+".")) {
		t.Errorf("the bundle secret appears outside the manifest: %s", raw)
	}

	code, out = h.call(http.MethodPost, "/hubs/hub1:disconnect", nil)
	if code != http.StatusOK {
		t.Fatalf("disconnect status = %d: %v", code, out)
	}
	if _, ok := h.observed("hub1", "ext"); ok {
		t.Errorf("observed record survived the disconnect")
	}
	if code, _ = h.call(http.MethodPost, "/hubs/hub1:disconnect", nil); code != http.StatusNotFound {
		t.Errorf("second disconnect status = %d, want 404", code)
	}
}

func TestHTTPConnectWithKubeconfig(t *testing.T) {
	h := newHTTPHarness(t)
	kubeconfig := strings.NewReplacer(
		"Q0EtUEVN", encodeBase64(h.hubCA),
		"reader-token", hubToken,
	).Replace(tokenKubeconfig)
	code, out := h.call(http.MethodPost, "/hubs:connect", map[string]any{"hubId": "hub1", "kubeconfig": kubeconfig})
	if code != http.StatusOK {
		t.Fatalf("status = %d: %v", code, out)
	}
	if conn, err := h.svc.loadConnection(h.ctx, "hub1"); err != nil || conn.APIServer != hubAPI || conn.Token != hubToken {
		t.Errorf("stored connection = %+v, %v", conn, err)
	}
}

func TestHTTPErrors(t *testing.T) {
	h := newHTTPHarness(t)
	h.connect("hub1")
	cases := map[string]struct {
		method, path string
		body         any
		status       int
		reason       string
	}{
		"kubeconfig and fields":      {http.MethodPost, "/hubs:connect", map[string]any{"hubId": "hub2", "kubeconfig": tokenKubeconfig, "token": "t"}, http.StatusBadRequest, "invalid-argument"},
		"unknown field":              {http.MethodPost, "/hubs:connect", map[string]any{"hubId": "hub2", "nope": 1}, http.StatusBadRequest, "invalid-argument"},
		"credential the hub rejects": {http.MethodPost, "/hubs:connect", map[string]any{"hubId": "hub1", "apiServer": hubAPI, "caCert": string(h.hubCA), "token": "wrong"}, http.StatusUnprocessableEntity, "credential-rejected"},
		"hub already connected":      {http.MethodPost, "/hubs:connect", map[string]any{"hubId": "hub2", "apiServer": hubAPI, "caCert": string(h.hubCA), "token": hubToken}, http.StatusConflict, "hub-already-connected"},
		"add-on for unknown hub":     {http.MethodPost, "/hubs/hub9:enrollmentAddon", map[string]any{}, http.StatusNotFound, "not-found"},
		"add-on with bad profile":    {http.MethodPost, "/hubs/hub1:enrollmentAddon", map[string]any{"profile": "root"}, http.StatusBadRequest, "invalid-argument"},
		"disconnect unknown hub":     {http.MethodPost, "/hubs/hub9:disconnect", nil, http.StatusNotFound, "not-found"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			code, out := h.call(tc.method, tc.path, tc.body)
			if code != tc.status || out["reason"] != tc.reason {
				t.Fatalf("status %d reason %v, want %d %s (%v)", code, out["reason"], tc.status, tc.reason, out)
			}
		})
	}
	if code, _ := h.call(http.MethodPost, "/hubs/hub1:explode", nil); code != http.StatusNotFound {
		t.Errorf("unknown verb status = %d, want 404", code)
	}
	// Reads stay with the dynamic inventory API.
	if code, _ := h.call(http.MethodGet, "/hubs/hub1", nil); code != http.StatusTeapot {
		t.Errorf("GET of a hub was not left to the dynamic API: status %d", code)
	}
	if code, _ := h.call(http.MethodGet, "/hubs", nil); code != http.StatusTeapot {
		t.Errorf("list of hubs was not left to the dynamic API: status %d", code)
	}
}

func TestHTTPEnrollmentAddonWithoutEnrollmentCapability(t *testing.T) {
	h := newHarness(t)
	// A resource manager with the integration but without the
	// kubernetes add-on: hubs are observed, nothing can be enrolled.
	svc := NewService(Deps{
		Store: h.store, Vault: h.vault, Records: h.records, Clients: h.clients,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := svc.Start(h.ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	if _, err := svc.Connect(h.ctx, ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken}}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	mux := http.NewServeMux()
	handle := func(pattern string, handler http.Handler) { mux.Handle(pattern, handler) }
	NewHTTP(svc, kubernetesaddon.EnrollmentEndpoint{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Register(handle, handle)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, APIPrefix+"/hubs/hub1:enrollmentAddon", strings.NewReader("{}")))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "no enrollment capability") {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}
