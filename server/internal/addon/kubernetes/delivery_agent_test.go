package kubernetes_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/keyregistry"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/oidc/oidctest"
	"github.com/stolostron/ocm-fleet/server/internal/testutil"
)

// awaitDone drains one result from ch with a safety-net timeout so
// that a regression in the fake delivery pipeline hangs for at most
// [testutil.UnitTimeout] rather than the global go-test deadline.
func awaitDone(t *testing.T, ch <-chan domain.DeliveryResult) domain.DeliveryResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(testutil.UnitTimeout):
		t.Fatal("timed out waiting for delivery result")
		return domain.DeliveryResult{}
	}
}

func tlsServerCAPEM(ts *httptest.Server) string {
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: ts.Certificate().Raw,
	}))
}

// newDiscoveryTestServer serves the discovery documents the REST mapper
// asks for, and hands every other request to object.
func newDiscoveryTestServer(t *testing.T, object http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			fmt.Fprint(w, `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[]}`)
		case "/apis":
			fmt.Fprint(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`)
		case "/api/v1":
			fmt.Fprint(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":["get","delete","patch"]}]}`)
		default:
			object(w, r)
		}
	}))
}

// channelReporter implements [domain.DeliveryReporter] for tests,
// routing events and results to channels for deterministic waits.
type channelReporter struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
	ch     chan domain.DeliveryEvent
	done   chan domain.DeliveryResult
}

func newChannelReporter() *channelReporter {
	return &channelReporter{
		ch:   make(chan domain.DeliveryEvent, 100),
		done: make(chan domain.DeliveryResult, 1),
	}
}

func (r *channelReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, event domain.DeliveryEvent) error {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	r.ch <- event
	return nil
}

func (r *channelReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.done <- result
	return nil
}

func (r *channelReporter) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

// nopReporter discards all reports; used when tests don't need to
// observe async results.
type nopReporter struct{}

func (nopReporter) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}
func (nopReporter) ReportResult(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryResult) error {
	return nil
}
func (nopReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

// flakyReporter fails a configured number of ReportResult calls before
// recording one: a transient platform error must not lose the outcome.
type flakyReporter struct {
	mu       sync.Mutex
	failures int
	attempts int
	done     chan domain.DeliveryResult
}

func (r *flakyReporter) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}

func (r *flakyReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts++
	if r.failures > 0 {
		r.failures--
		return errors.New("platform unavailable")
	}
	r.done <- result
	return nil
}

func (r *flakyReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

// countingResultReporter records every delivery result without blocking,
// so a test can tell how many reports one delivery produced.
type countingResultReporter struct {
	mu      sync.Mutex
	results []domain.DeliveryResult
	done    chan domain.DeliveryResult
}

func newCountingResultReporter() *countingResultReporter {
	return &countingResultReporter{done: make(chan domain.DeliveryResult, 16)}
}

func (r *countingResultReporter) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}

func (r *countingResultReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.mu.Lock()
	r.results = append(r.results, result)
	r.mu.Unlock()
	r.done <- result
	return nil
}

func (r *countingResultReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

func (r *countingResultReporter) resultCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.results)
}

// TestAgent_Deliver_SucceedsWhenIndexerAbsent asserts that the Kubernetes
// DeliveryAgent does not consult IndexingRuntime. Deliver still accepts a
// valid request and reports success when no indexer is running.
func TestAgent_Deliver_SucceedsWhenIndexerAbsent(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	// Empty manifests: client construction succeeds without dialing, and
	// the apply loop is a no-op that reports Delivered. No indexer is
	// constructed or consulted.
	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver with indexer absent: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateDelivered {
		t.Errorf("async State = %q, want %q", asyncResult.State, domain.DeliveryStateDelivered)
	}
}

// TestAgent_Deliver_SucceedsWhenIndexerEnsureFails asserts that a
// failing EnsureIndexer path does not block delivery. Delivery and
// indexing are separate types for the Kubernetes delivery agent.
func TestAgent_Deliver_SucceedsWhenIndexerEnsureFails(t *testing.T) {
	ctx := context.Background()
	host := kubernetes.NewKubernetesInProcessIndexHost(
		ctx,
		nil,
		nil,
		failingIndexerClients{},
		slog.New(slog.DiscardHandler),
	)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":            "https://127.0.0.1:6443",
			"service_account_token": "token",
		},
	})
	input, err := kubernetes.NewIndexRuntimeInput(
		target.ID(),
		domain.ResourceName("clusters/test"),
		"https://127.0.0.1:6443",
		"",
		[]byte("token"),
		"",
		1,
		kubernetes.IndexConfig{},
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	err = host.EnsureIndexer(ctx, input)
	if err == nil {
		t.Fatal("expected EnsureIndexer to fail when clients are down")
	}

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)
	err = agent.Deliver(ctx, target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver with failed indexer: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateDelivered {
		t.Errorf("async State = %q, want %q", asyncResult.State, domain.DeliveryStateDelivered)
	}
}

type failingIndexerClients struct{}

func (failingIndexerClients) Dynamic(*rest.Config) (dynamic.Interface, error) {
	return nil, errors.New("indexer intentionally down")
}

func (failingIndexerClients) Discovery(*rest.Config) (discovery.DiscoveryInterface, error) {
	return nil, errors.New("indexer intentionally down")
}

func TestAgent_Deliver_MissingAPIServer(t *testing.T) {
	agent := kubernetes.NewDeliveryAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{},
	})

	auth := domain.DeliveryAuth{Token: "some-token"}
	err := agent.Deliver(context.Background(), target, "d1", nil, auth, nil, 1)
	if err == nil {
		t.Fatal("expected error for missing api_server")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Deliver_EmptyAPIServer(t *testing.T) {
	agent := kubernetes.NewDeliveryAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{"api_server": ""},
	})

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error for empty api_server")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Remove_EmptyAPIServer(t *testing.T) {
	agent := kubernetes.NewDeliveryAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{"api_server": ""},
	})

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error for empty api_server")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Deliver_MissingToken(t *testing.T) {
	agent := kubernetes.NewDeliveryAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, nil, 1)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Deliver_BadAPIServer(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:1",
		},
	})

	auth := domain.DeliveryAuth{Token: "not-a-real-token"}
	manifests := []domain.Manifest{{
		ManifestType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateFailed {
		t.Errorf("async State = %q, want %q", asyncResult.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Remove_MissingAPIServer(t *testing.T) {
	agent := kubernetes.NewDeliveryAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{},
	})

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error for missing api_server")
	}
}

func TestAgent_Remove_EmptyManifests(t *testing.T) {
	agent := kubernetes.NewDeliveryAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	if err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1); err != nil {
		t.Fatalf("Remove with empty manifests: %v", err)
	}
}

func TestAgent_Deliver_Unauthorized_ReportsAuthFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})

	auth := domain.DeliveryAuth{Token: "expired-token"}
	manifests := []domain.Manifest{{
		ManifestType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateAuthFailed {
		t.Errorf("async State = %q, want %q; message: %s", asyncResult.State, domain.DeliveryStateAuthFailed, asyncResult.Message)
	}
}

func TestAgent_Deliver_Forbidden_ReportsAuthFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Forbidden","reason":"Forbidden","code":403}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})

	auth := domain.DeliveryAuth{Token: "some-token"}
	manifests := []domain.Manifest{{
		ManifestType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateAuthFailed {
		t.Errorf("async State = %q, want %q; message: %s", asyncResult.State, domain.DeliveryStateAuthFailed, asyncResult.Message)
	}
}

func TestAgent_Deliver_AttestationFailure_ReturnsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	})

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateAuthFailed, result.Message)
	}
}

func TestAgent_Deliver_WithAttestation_NoTrustBundle_ReturnsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestAgent_Deliver_VerifierCacheReuse(t *testing.T) {
	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	})

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	_ = agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	result1 := awaitDone(t, reporter.done)
	if result1.State != domain.DeliveryStateAuthFailed {
		t.Errorf("first: State = %q, want AuthFailed", result1.State)
	}

	_ = agent.Deliver(context.Background(), target, "d2", nil, domain.DeliveryAuth{}, att, 1)
	result2 := awaitDone(t, reporter.done)
	if result2.State != domain.DeliveryStateAuthFailed {
		t.Errorf("second: State = %q, want AuthFailed", result2.State)
	}
}

func TestAgent_Deliver_WithAttestation_NoTokenRequired(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	})

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestAgent_Remove_AttestationFailure_ReportsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	})

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Remove should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
	if !strings.Contains(result.Message, "attestation verification failed") {
		t.Errorf("expected attestation verification error in message, got: %q", result.Message)
	}
}

func TestAgent_Remove_WithAttestation_NoTrustBundle_ReportsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Remove should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
	if !strings.Contains(result.Message, "trust_bundle") {
		t.Errorf("expected trust_bundle error in message, got: %q", result.Message)
	}
}

func TestAgent_Remove_DeleteFailure_ReportsFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"boom","reason":"InternalError","code":500}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})
	err := agent.Remove(context.Background(), target, "d1", []domain.Manifest{{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"rm-fail","namespace":"default"}}`),
	}}, domain.DeliveryAuth{Token: "tok"}, nil, 1)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateFailed, result.Message)
	}
}

func TestAgent_Remove_MissingToken(t *testing.T) {
	agent := kubernetes.NewDeliveryAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, nil, 1)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Remove_Unauthorized_ReportsAuthFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	obj := json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"rm-unauth","namespace":"default"}}`)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})

	err := agent.Remove(context.Background(), target, "d1", []domain.Manifest{{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          obj,
	}}, domain.DeliveryAuth{Token: "bad-token"}, nil, 1)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateAuthFailed, result.Message)
	}
}

func TestAgent_Remove_EmptyManifests_ReportsDelivered(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	if err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateDelivered, result.Message)
	}
}

// A removal that finishes with an object it may not touch reports
// Partial: the removal is over, but something the delivery may have
// applied still stands, and the message names it.
func TestAgent_Remove_LeftoverObject_ReportsPartial(t *testing.T) {
	bundle := buildUnitTestAttestation(t)
	ts := newDiscoveryTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/default/configmaps/left-behind" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"forbidden","reason":"Forbidden","code":403}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"not found","reason":"NotFound","code":404}`)
	})
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter,
		kubernetes.WithKeyResolver(bundle.keyResolver),
		kubernetes.WithHTTPClient(bundle.httpClient),
	)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":            ts.URL,
			"ca_cert":               tlsServerCAPEM(ts),
			"trust_bundle":          bundle.trustBundleJSON,
			"service_account_token": "platform-tok",
		},
	})

	manifest := domain.Manifest{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"left-behind","namespace":"default"}}`),
	}
	if err := agent.Remove(context.Background(), target, "d1", []domain.Manifest{manifest}, domain.DeliveryAuth{}, bundle.attestation, 1); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.Operation != domain.DeliveryOperationRemove {
		t.Errorf("Operation = %q, want %q", result.Operation, domain.DeliveryOperationRemove)
	}
	if result.State != domain.DeliveryStatePartial {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStatePartial, result.Message)
	}
	want := "left as they are: ConfigMap default/left-behind (the stored credential may neither read nor delete it)"
	if result.Message != want {
		t.Errorf("Message = %q, want %q", result.Message, want)
	}
}

// A removal with the caller's credential that finds an object another
// manager wrote leaves it in place and says so.
func TestAgent_Remove_LeftoverObject_CallerCredential_ReportsPartial(t *testing.T) {
	ts := newDiscoveryTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/default/configmaps/left-behind" {
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"left-behind","namespace":"default","managedFields":[{"manager":"someone-else","operation":"Apply"}]},"data":{"from":"someone"}}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"not found","reason":"NotFound","code":404}`)
	})
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})

	manifest := domain.Manifest{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"left-behind","namespace":"default"}}`),
	}
	if err := agent.Remove(context.Background(), target, "d1", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.Operation != domain.DeliveryOperationRemove {
		t.Errorf("Operation = %q, want %q", result.Operation, domain.DeliveryOperationRemove)
	}
	if result.State != domain.DeliveryStatePartial {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStatePartial, result.Message)
	}
	want := "left as they are: ConfigMap default/left-behind (not written by this resource manager)"
	if result.Message != want {
		t.Errorf("Message = %q, want %q", result.Message, want)
	}
}

// closingProbe reports that a Close has started waiting for in-flight
// work, so a test can look at Close's result without a scheduling window.
type closingProbe struct {
	once    sync.Once
	closing chan struct{}
}

func (p *closingProbe) Closing() {
	p.once.Do(func() { close(p.closing) })
}

// Close waits for a delivery already running and refuses new dispatches.
func TestAgent_Close_JoinsInFlightDelivery(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	started := make(chan struct{})
	var once sync.Once
	ts := newDiscoveryTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			once.Do(func() { close(started) })
			<-release
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"forbidden","reason":"Forbidden","code":403}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"not found","reason":"NotFound","code":404}`)
	})
	// A failure before the release must still unblock the handler, or the
	// server's Close would wait for it forever. Cleanups run last
	// registered first, so ts.Close is registered before unblock.
	t.Cleanup(ts.Close)
	t.Cleanup(unblock)

	probe := &closingProbe{closing: make(chan struct{})}
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter, kubernetes.WithAgentProbe(probe))
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})
	manifest := domain.Manifest{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"close-test","namespace":"default"}}`),
	}
	if err := agent.Deliver(context.Background(), target, "d1", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	<-started

	closed := make(chan error, 1)
	go func() { closed <- agent.Close(context.Background()) }()
	// With no context deadline Close can return only after the running
	// delivery finishes, so once the probe reports it started waiting, a
	// non-blocking look at its result is conclusive.
	<-probe.closing
	select {
	case err := <-closed:
		t.Fatalf("Close returned while a delivery was still running: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the delivery finished")
	}
	if result := awaitDone(t, reporter.done); result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateAuthFailed, result.Message)
	}
	if err := agent.Deliver(context.Background(), target, "d2", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err == nil || !strings.Contains(err.Error(), "closing") {
		t.Errorf("Deliver after Close = %v, want an error saying the agent is closing", err)
	}
	if err := agent.Remove(context.Background(), target, "d2", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err == nil || !strings.Contains(err.Error(), "closing") {
		t.Errorf("Remove after Close = %v, want an error saying the agent is closing", err)
	}
}

// Close respects its deadline while a delivery is still running.
func TestAgent_Close_DeadlineIsRespected(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	ts := newDiscoveryTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			once.Do(func() { close(started) })
			<-release
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"forbidden","reason":"Forbidden","code":403}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"not found","reason":"NotFound","code":404}`)
	})
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})
	manifest := domain.Manifest{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"close-deadline","namespace":"default"}}`),
	}
	if err := agent.Deliver(context.Background(), target, "d1", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	<-started

	closeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := agent.Close(closeCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close = %v, want a wrapped context.DeadlineExceeded", err)
	}
	close(release)
	awaitDone(t, reporter.done)
}

// A second Deliver for a delivery already being applied is skipped: the
// platform provides at-least-once delivery, and a startup recovery plus a
// replayed workflow activity would otherwise run the same apply twice and
// report twice.
func TestAgent_Deliver_SkipsDuplicateWhileInFlight(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	started := make(chan struct{})
	var patches atomic.Int32
	var once sync.Once
	ts := newDiscoveryTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			patches.Add(1)
			once.Do(func() { close(started) })
			<-release
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"forbidden","reason":"Forbidden","code":403}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"not found","reason":"NotFound","code":404}`)
	})
	t.Cleanup(ts.Close)
	t.Cleanup(unblock)

	reporter := newCountingResultReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})
	manifest := domain.Manifest{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"in-flight-deliver","namespace":"default"}}`),
	}
	ctx := context.Background()
	if err := agent.Deliver(ctx, target, "d1", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	<-started

	if err := agent.Deliver(ctx, target, "d1", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("duplicate Deliver: %v", err)
	}
	unblock()
	if result := awaitDone(t, reporter.done); result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateAuthFailed, result.Message)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := agent.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := patches.Load(); n != 1 {
		t.Errorf("apply requests = %d, want 1: a duplicate dispatch must be skipped", n)
	}
	if n := reporter.resultCount(); n != 1 {
		t.Errorf("result reports = %d, want 1: a duplicate dispatch must not report", n)
	}
}

// A second Remove for a delivery already being removed is skipped, for
// the same reason as Deliver.
func TestAgent_Remove_SkipsDuplicateWhileInFlight(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	started := make(chan struct{})
	var gets atomic.Int32
	var once sync.Once
	ts := newDiscoveryTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/default/configmaps/in-flight-remove" {
			gets.Add(1)
			once.Do(func() { close(started) })
			<-release
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"not found","reason":"NotFound","code":404}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"not found","reason":"NotFound","code":404}`)
	})
	t.Cleanup(ts.Close)
	t.Cleanup(unblock)

	reporter := newCountingResultReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})
	manifest := domain.Manifest{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"in-flight-remove","namespace":"default"}}`),
	}
	ctx := context.Background()
	if err := agent.Remove(ctx, target, "d1", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	<-started

	if err := agent.Remove(ctx, target, "d1", []domain.Manifest{manifest}, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("duplicate Remove: %v", err)
	}
	unblock()
	if result := awaitDone(t, reporter.done); result.State != domain.DeliveryStateDelivered {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateDelivered, result.Message)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := agent.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := gets.Load(); n != 1 {
		t.Errorf("object reads = %d, want 1: a duplicate dispatch must be skipped", n)
	}
	if n := reporter.resultCount(); n != 1 {
		t.Errorf("result reports = %d, want 1: a duplicate dispatch must not report", n)
	}
}

// A transient failure reporting a delivery outcome is retried rather
// than dropped.
func TestAgent_Deliver_ResultReportingRetriesTransientFailure(t *testing.T) {
	reporter := &flakyReporter{failures: 2, done: make(chan domain.DeliveryResult, 1)}
	agent := kubernetes.NewDeliveryAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	if err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "tok"}, nil, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.Operation != domain.DeliveryOperationDeliver {
		t.Errorf("Operation = %q, want %q", result.Operation, domain.DeliveryOperationDeliver)
	}
	if result.State != domain.DeliveryStateDelivered {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateDelivered, result.Message)
	}
	reporter.mu.Lock()
	attempts := reporter.attempts
	reporter.mu.Unlock()
	if attempts != 3 {
		t.Errorf("ReportResult attempts = %d, want 3 (two transient failures, then success)", attempts)
	}
}

// When the cluster refuses the caller's own credential for a removal,
// the refusal stands even on a target whose stored credential the
// cluster would accept: that credential is not tried in the caller's
// place, not even to look whether anything is there.
func TestAgent_Remove_CallerRefused_StoredCredentialIsNotTried(t *testing.T) {
	var storedSeen atomic.Int32
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") == "Bearer stored" {
			storedSeen.Add(1)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","reason":"NotFound","code":404}`)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":            ts.URL,
			"ca_cert":               tlsServerCAPEM(ts),
			"service_account_token": "stored",
		},
	})

	err := agent.Remove(context.Background(), target, "d1", []domain.Manifest{{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"never-created","namespace":"default"}}`),
	}}, domain.DeliveryAuth{Token: "a-token-the-cluster-does-not-know"}, nil, 1)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateAuthFailed, result.Message)
	}
	if n := storedSeen.Load(); n != 0 {
		t.Errorf("the stored credential was shown to the cluster %d time(s)", n)
	}
}

func TestAgent_Deliver_AttestedPlatform_MissingCredentials_ReportsFailed(t *testing.T) {
	bundle := buildUnitTestAttestation(t)
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter,
		kubernetes.WithKeyResolver(bundle.keyResolver),
		kubernetes.WithHTTPClient(bundle.httpClient),
	)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": bundle.trustBundleJSON,
			// intentionally no service_account_token / ref
		},
	})

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, bundle.attestation, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateFailed, result.Message)
	}
	if !strings.Contains(result.Message, "service_account_token") && !strings.Contains(result.Message, "platform") {
		t.Errorf("expected platform credential error, got: %q", result.Message)
	}
}

func TestAgent_Deliver_AttestedPlatform_DirectToken_EmptyManifests(t *testing.T) {
	bundle := buildUnitTestAttestation(t)
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter,
		kubernetes.WithKeyResolver(bundle.keyResolver),
		kubernetes.WithHTTPClient(bundle.httpClient),
	)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":            "https://127.0.0.1:6443",
			"trust_bundle":          bundle.trustBundleJSON,
			"service_account_token": "platform-tok",
		},
	})

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, bundle.attestation, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateDelivered, result.Message)
	}
}

func TestAgent_Deliver_AttestedPlatform_VaultToken_EmptyManifests(t *testing.T) {
	bundle := buildUnitTestAttestation(t)
	vault := &mapVault{secrets: map[domain.SecretRef][]byte{
		"targets/k8s-test/sa": []byte("vault-platform-tok"),
	}}
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter,
		kubernetes.WithKeyResolver(bundle.keyResolver),
		kubernetes.WithHTTPClient(bundle.httpClient),
		kubernetes.WithVault(vault),
	)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":                "https://127.0.0.1:6443",
			"trust_bundle":              bundle.trustBundleJSON,
			"service_account_token_ref": "targets/k8s-test/sa",
		},
	})

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, bundle.attestation, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateDelivered, result.Message)
	}
}

func TestAgent_Remove_AttestedPlatform_MissingCredentials_ReportsFailed(t *testing.T) {
	bundle := buildUnitTestAttestation(t)
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter,
		kubernetes.WithKeyResolver(bundle.keyResolver),
		kubernetes.WithHTTPClient(bundle.httpClient),
	)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": bundle.trustBundleJSON,
		},
	})

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, bundle.attestation, 1)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateFailed, result.Message)
	}
}

func TestAgent_Remove_AttestedPlatform_DirectToken_EmptyManifests(t *testing.T) {
	bundle := buildUnitTestAttestation(t)
	reporter := newChannelReporter()
	agent := kubernetes.NewDeliveryAgent(reporter,
		kubernetes.WithKeyResolver(bundle.keyResolver),
		kubernetes.WithHTTPClient(bundle.httpClient),
	)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":            "https://127.0.0.1:6443",
			"trust_bundle":          bundle.trustBundleJSON,
			"service_account_token": "platform-tok",
		},
	})

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, bundle.attestation, 1)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateDelivered, result.Message)
	}
}

type mapVault struct {
	secrets map[domain.SecretRef][]byte
}

func (v *mapVault) Get(_ context.Context, ref domain.SecretRef) ([]byte, error) {
	val, ok := v.secrets[ref]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return val, nil
}
func (v *mapVault) Put(_ context.Context, ref domain.SecretRef, val []byte) error {
	v.secrets[ref] = val
	return nil
}
func (v *mapVault) Delete(_ context.Context, ref domain.SecretRef) error {
	delete(v.secrets, ref)
	return nil
}

type unitTestAttestationBundle struct {
	attestation     *domain.Attestation
	keyResolver     *domain.KeyResolver
	httpClient      *http.Client
	trustBundleJSON string
}

func buildUnitTestAttestation(t *testing.T) unitTestAttestationBundle {
	t.Helper()

	provider := oidctest.Start(t, oidctest.WithAudience("ocm-fleet-enroll"))
	signerID := domain.SubjectID("unit-test-user")
	issuer := provider.IssuerURL()
	registrySubject := domain.RegistrySubject("gh-unit-test-user")

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	identityToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  string(signerID),
		Audience: "ocm-fleet-enroll",
		Extra:    map[string]any{"preferred_username": registrySubject},
	})

	fakeReg := keyregistry.NewFake()
	fakeReg.Register("https://api.github.com", registrySubject, &privKey.PublicKey)

	manifests := []domain.Manifest{{
		ManifestType: kubernetes.ManifestManifestType,
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"unit","namespace":"default"}}`),
	}}
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: manifests,
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"k8s-test"},
	}
	validUntil := time.Now().Add(24 * time.Hour)
	gen := domain.Generation(1)

	envelope, err := domain.BuildSignedInputEnvelope("deployments/unit-dep", ms, ps, validUntil, nil, gen)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	envelopeHash := domain.HashIntent(envelope)

	hash := sha256.Sum256(envelope)
	sigBytes, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign envelope: %v", err)
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Content: domain.DeploymentContent{
					Name:              "deployments/unit-dep",
					ManifestStrategy:  ms,
					PlacementStrategy: ps,
				},
				Sig: domain.Signature{
					Signer:         domain.FederatedIdentity{Subject: signerID, Issuer: issuer},
					ContentHash:    envelopeHash,
					SignatureBytes: sigBytes,
				},
				ValidUntil:         validUntil,
				ExpectedGeneration: gen,
			},
			Signer: domain.SignerAssertion{
				IdentityToken:   domain.RawToken(identityToken),
				RegistryID:      "github.com",
				RegistrySubject: registrySubject,
			},
		},
		Output: &domain.PutManifests{Manifests: manifests},
	}

	keyResolver := &domain.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: fakeReg,
		},
	}

	jwksURI := string(issuer) + "/jwks"
	trustBundle := []domain.TrustBundleEntry{{
		IssuerURL:          issuer,
		JWKSURI:            domain.EndpointURL(jwksURI),
		EnrollmentAudience: "ocm-fleet-enroll",
		RegistrySubjectMapping: &domain.RegistrySubjectMapping{
			RegistryID: "github.com",
			Expression: `claims.preferred_username`,
		},
	}}
	trustJSON, err := json.Marshal(trustBundle)
	if err != nil {
		t.Fatalf("marshal trust bundle: %v", err)
	}

	return unitTestAttestationBundle{
		attestation:     att,
		keyResolver:     keyResolver,
		httpClient:      provider.HTTPClient(),
		trustBundleJSON: string(trustJSON),
	}
}
