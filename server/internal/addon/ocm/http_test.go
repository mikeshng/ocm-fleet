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
	"time"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
)

type httpHarness struct {
	*harness
	handlers *HTTP
	mux      *http.ServeMux
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
	return &httpHarness{harness: h, handlers: handlers, mux: mux}
}

func (h *httpHarness) call(method, path string, body any) (int, map[string]any) {
	h.t.Helper()
	req := h.request(method, path, body)
	rr := httptest.NewRecorder()
	h.mux.ServeHTTP(rr, req)
	return h.parse(rr)
}

func (h *httpHarness) request(method, path string, body any) *http.Request {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("encode body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	return httptest.NewRequest(method, APIPrefix+path, reader)
}

func (h *httpHarness) parse(rr *httptest.ResponseRecorder) (int, map[string]any) {
	h.t.Helper()
	out := map[string]any{}
	if rr.Body.Len() > 0 && strings.HasPrefix(rr.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			h.t.Fatalf("status %d: response is not JSON: %v: %s", rr.Code, err, rr.Body.String())
		}
	}
	return rr.Code, out
}

// callWithin is call bounded by limit: it fails the test when the
// handler does not answer in time. Without the bound, a handler that
// waits on a peer with no budget would hang the test instead of
// failing it.
func (h *httpHarness) callWithin(limit time.Duration, method, path string, body any) (int, map[string]any) {
	h.t.Helper()
	req := h.request(method, path, body)
	rr := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		defer close(served)
		h.mux.ServeHTTP(rr, req)
	}()
	select {
	case <-served:
	case <-time.After(limit):
		h.t.Fatalf("%s %s did not answer within %s", method, path, limit)
		return 0, nil
	}
	return h.parse(rr)
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

	code, out = h.call(http.MethodPost, "/hubs/hub1:generateEnrollmentAddon", map[string]any{
		"profile": "read-only", "labels": map[string]string{"team": "blue"}, "tokenLifetimeSeconds": 3600,
	})
	if code != http.StatusOK {
		t.Fatalf("generateEnrollmentAddon status = %d: %v", code, out)
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
	if rollout, _ := out["rollout"].(map[string]any); rollout["mode"] != RolloutManual {
		t.Errorf("rollout = %v, want manual when none is asked for and none was made before", out["rollout"])
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

	// Every cluster of the hub, by one choice.
	code, out = h.call(http.MethodPost, "/hubs/hub1:generateEnrollmentAddon", map[string]any{"rollout": "every-cluster"})
	if code != http.StatusOK {
		t.Fatalf("generateEnrollmentAddon for every cluster: status = %d: %v", code, out)
	}
	manifest, _ = out["manifest"].(string)
	for _, want := range []string{"kind: Placement", "kind: ManagedClusterSetBinding", "type: Placements", "namespace: " + RolloutNamespace, "kind: AddOnTemplate"} {
		if !strings.Contains(manifest, want) {
			t.Errorf("the manifest for every cluster lacks %q:\n%s", want, manifest)
		}
	}
	if enable, _ := out["enableExample"].(string); enable != "" {
		t.Errorf("an example for one cluster comes with the add-on for every cluster:\n%s", enable)
	}
	if rollout, _ := out["rollout"].(map[string]any); rollout["mode"] != RolloutEveryCluster || rollout["placementName"] != RolloutPlacementName {
		t.Errorf("rollout = %v", out["rollout"])
	}
	// A manifest made again, with nothing said, is for the same clusters.
	code, out = h.call(http.MethodPost, "/hubs/hub1:generateEnrollmentAddon", map[string]any{})
	if rollout, _ := out["rollout"].(map[string]any); code != http.StatusOK || rollout["mode"] != RolloutEveryCluster {
		t.Errorf("a manifest made again: status %d, rollout %v, want every-cluster kept", code, out["rollout"])
	}
	for name, body := range map[string]map[string]any{
		"unknown rollout":          {"rollout": "all"},
		"placement without a name": {"rollout": "placement", "placementNamespace": "team"},
		"a name without placement": {"placementNamespace": "team", "placementName": "prod"},
	} {
		if code, out := h.call(http.MethodPost, "/hubs/hub1:generateEnrollmentAddon", body); code != http.StatusBadRequest || out["reason"] != "invalid-argument" {
			t.Errorf("%s: status %d reason %v, want 400 invalid-argument", name, code, out["reason"])
		}
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

// A hub that accepts the connection and then never answers must not
// hold the connect handler past the server's write deadline: the
// request budget ends the wait, and the route answers with the error
// it documents for a hub that does not answer.
func TestHTTPConnectAgainstAStalledHubAnswersWithinTheBudget(t *testing.T) {
	h := newHTTPHarness(t)
	h.blockProbes()
	h.handlers.requestBudget = 50 * time.Millisecond

	code, out := h.callWithin(5*time.Second, http.MethodPost, "/hubs:connect", map[string]any{
		"hubId": "hub1", "apiServer": hubAPI, "caCert": string(h.hubCA), "token": hubToken,
	})
	if code != http.StatusUnprocessableEntity || out["reason"] != "hub-unreachable" {
		t.Fatalf("connect against a hub that does not answer = %d %v, want 422 hub-unreachable", code, out)
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
		"add-on for unknown hub":     {http.MethodPost, "/hubs/hub9:generateEnrollmentAddon", map[string]any{}, http.StatusNotFound, "not-found"},
		"add-on with bad profile":    {http.MethodPost, "/hubs/hub1:generateEnrollmentAddon", map[string]any{"profile": "root"}, http.StatusBadRequest, "invalid-argument"},
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

func TestHTTPEnrollmentAddonSecondsAreBounded(t *testing.T) {
	h := newHTTPHarness(t)
	h.connect("hub1")
	cases := map[string]map[string]any{
		// 2^55+600 seconds wraps to 600 seconds when it is converted
		// to a duration; a bound on the seconds catches it.
		"token lifetime that overflows":    {"tokenLifetimeSeconds": int64(1)<<55 + 600},
		"token lifetime above the maximum": {"tokenLifetimeSeconds": int64(kubernetesaddon.MaxTokenLifetime/time.Second) + 1},
		"negative token lifetime":          {"tokenLifetimeSeconds": -1},
		"ttl that overflows":               {"ttlSeconds": int64(1)<<55 + 1},
		"ttl above the maximum":            {"ttlSeconds": int64(kubernetesaddon.MaxBundleTTL/time.Second) + 1},
		"negative ttl":                     {"ttlSeconds": -1},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			code, out := h.call(http.MethodPost, "/hubs/hub1:generateEnrollmentAddon", body)
			if code != http.StatusBadRequest || out["reason"] != "invalid-argument" {
				t.Fatalf("status %d reason %v, want 400 invalid-argument (%v)", code, out["reason"], out)
			}
		})
	}
	code, out := h.call(http.MethodPost, "/hubs/hub1:generateEnrollmentAddon", map[string]any{
		"tokenLifetimeSeconds": int64(kubernetesaddon.MaxTokenLifetime / time.Second),
		"ttlSeconds":           int64(kubernetesaddon.MaxBundleTTL / time.Second),
	})
	if code != http.StatusOK {
		t.Fatalf("the longest allowed lifetimes = %d %v, want 200", code, out)
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
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, APIPrefix+"/hubs/hub1:generateEnrollmentAddon", strings.NewReader("{}")))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "no enrollment capability") {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

// The record of a hub is written at most every few minutes while only
// its last-heard time changes. A caller that shows that time asks the
// hub's live observation instead, which answers what the watch sees now.
func TestHTTPHubObservationAnswersTheLiveWatch(t *testing.T) {
	h := newHTTPHarness(t)
	h.connect("hub1")
	hw := h.svc.hub("hub1")
	if hw == nil {
		t.Fatal("the hub is connected but has no watch")
	}
	// Store a record whose last-heard time is an hour old, as the
	// throttled write leaves it while only the time moves.
	obs, conds := hw.snapshot(h.svc.now().UTC())
	old := h.svc.now().UTC().Add(-time.Hour)
	obs.LastSeenAt = &old
	raw, err := json.Marshal(obs)
	if err != nil {
		t.Fatalf("encode the stored observation: %v", err)
	}
	if err := h.svc.storeHubRecord(h.ctx, "hub1", raw, conds, h.svc.now().UTC()); err != nil {
		t.Fatalf("store the hub record: %v", err)
	}

	code, out := h.call(http.MethodGet, "/hubs/hub1/observation", nil)
	if code != http.StatusOK {
		t.Fatalf("GET the hub's observation = %d, want 200", code)
	}
	answer, ok := out["lastSeenAt"].(string)
	if !ok || answer == "" {
		t.Fatalf("the observation answers no lastSeenAt: %v", out)
	}
	seen, err := time.Parse(time.RFC3339, answer)
	if err != nil {
		t.Fatalf("parse lastSeenAt %q: %v", answer, err)
	}
	if !seen.After(old.Add(time.Minute)) {
		t.Fatalf("the observation answered the stored record's time %s, not the watch's", seen)
	}
}

func TestHTTPHubObservationOfAConnectedHubMissingIsNotFound(t *testing.T) {
	h := newHTTPHarness(t)
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, h.request(http.MethodGet, "/hubs/no-such-hub/observation", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET an unconnected hub's observation = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.HasPrefix(body, "not found") {
		t.Fatalf("the 404 answers %q, want the record reads' text: not found", body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("the 404 answers Content-Type %q, want text/plain", ct)
	}
}
