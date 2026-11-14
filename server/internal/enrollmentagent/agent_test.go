package enrollmentagent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeSource struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeSource) AccessToken(_ context.Context, namespace, name string, lifetime time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	return "token-for-" + namespace + "/" + name + "-" + lifetime.String(), nil
}

func (f *fakeSource) ClusterCA() ([]byte, error) { return []byte("CLUSTER-CA-PEM"), nil }
func (f *fakeSource) Namespace() (string, error) { return "own-namespace", nil }

// resourceManager is a TLS test server standing in for the resource
// manager, with its certificate written to a file the agent trusts.
type resourceManager struct {
	*httptest.Server
	caFile   string
	requests atomic.Int32
}

func newResourceManager(t *testing.T, handler http.HandlerFunc) *resourceManager {
	t.Helper()
	rm := &resourceManager{}
	rm.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rm.requests.Add(1)
		handler(w, r)
	}))
	t.Cleanup(rm.Close)
	rm.caFile = filepath.Join(t.TempDir(), "ca.crt")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rm.Certificate().Raw})
	if err := os.WriteFile(rm.caFile, pemBytes, 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	return rm
}

func bundleFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write bundle token: %v", err)
	}
	return path
}

func newAgent(t *testing.T, cfg Config, source TokenSource) *Agent {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	agent, err := New(cfg, source, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return agent
}

func accepted(w http.ResponseWriter, renewAfter int64) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(presentResponse{Cluster: "clusters/hub1.ext", Target: "k8s-hub1.ext", State: "enrolled", RenewAfterSeconds: renewAfter})
}

func TestRunOnce_PresentsTokenAndCA(t *testing.T) {
	var got presentRequest
	var auth, path, method string
	rm := newResourceManager(t, func(w http.ResponseWriter, r *http.Request) {
		auth, path, method = r.Header.Get("Authorization"), r.URL.Path, r.Method
		_ = json.NewDecoder(r.Body).Decode(&got)
		accepted(w, 14400)
	})
	source := &fakeSource{}
	agent := newAgent(t, Config{
		ServerURL: rm.URL, ServerCAFile: rm.caFile, BundleTokenFile: bundleFile(t, "eb-0123456789ab.secret\n"),
		ServiceAccount: "ocm-fleet-access", ClusterName: "ext", TokenLifetime: 2 * time.Hour, Once: true,
	}, source)

	if err := agent.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if method != http.MethodPost || path != PresentPath {
		t.Errorf("request = %s %s", method, path)
	}
	if auth != "Bearer eb-0123456789ab.secret" {
		t.Errorf("Authorization = %q", auth)
	}
	if got.ClusterName != "ext" || got.CACert != "CLUSTER-CA-PEM" || got.Token != "token-for-own-namespace/ocm-fleet-access-2h0m0s" {
		t.Errorf("presented = %+v", got)
	}
	if got.ServiceAccount["namespace"] != "own-namespace" || got.ServiceAccount["name"] != "ocm-fleet-access" {
		t.Errorf("service account = %v", got.ServiceAccount)
	}
	if rm.requests.Load() != 1 || source.calls != 1 {
		t.Errorf("%d requests and %d token requests, want one of each", rm.requests.Load(), source.calls)
	}
}

func TestConnectAddress_DialsElsewhereButNamesTheServer(t *testing.T) {
	var host string
	rm := newResourceManager(t, func(w http.ResponseWriter, r *http.Request) {
		host = r.Host
		accepted(w, 3600)
	})
	listen, _ := url.Parse(rm.URL)
	// The test certificate is valid for example.com, which does not
	// resolve to the test server: only the connect address reaches it.
	agent := newAgent(t, Config{
		ServerURL: "https://example.com:8085", ServerCAFile: rm.caFile, ConnectAddress: listen.Host,
		BundleTokenFile: bundleFile(t, "b.s"), ServiceAccount: "ocm-fleet-access", Namespace: "elsewhere", Once: true,
	}, &fakeSource{})
	if err := agent.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if host != "example.com:8085" {
		t.Errorf("Host header = %q, want the server's public name", host)
	}
}

func TestServerCertificateIsVerified(t *testing.T) {
	rm := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) { accepted(w, 3600) })
	agent := newAgent(t, Config{
		ServerURL: rm.URL, ServerCAFile: unrelatedCAFile(t), BundleTokenFile: bundleFile(t, "b.s"),
		ServiceAccount: "ocm-fleet-access", Once: true,
	}, &fakeSource{})
	agent.sleep = func(context.Context, time.Duration) error { return nil }
	err := agent.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("Run with a CA that did not sign the server = %v, want a certificate error", err)
	}
	if rm.requests.Load() != 0 {
		t.Errorf("the bundle secret was sent to a server whose certificate does not verify")
	}
}

func TestRunOnce_GivesUpAfterRefusals(t *testing.T) {
	rm := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"hub \"hub1\" has not accepted cluster \"ext\"","reason":"cluster-not-accepted"}`))
	})
	agent := newAgent(t, Config{
		ServerURL: rm.URL, ServerCAFile: rm.caFile, BundleTokenFile: bundleFile(t, "b.s"),
		ServiceAccount: "ocm-fleet-access", Once: true,
	}, &fakeSource{})
	var waits []time.Duration
	agent.sleep = func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }

	err := agent.Run(context.Background())
	var refused *refusedError
	if !errors.As(err, &refused) || refused.status != http.StatusForbidden || refused.reason != "cluster-not-accepted" {
		t.Fatalf("Run = %v, want the refusal", err)
	}
	if !strings.Contains(err.Error(), "has not accepted") {
		t.Errorf("the error does not carry the resource manager's explanation: %v", err)
	}
	if int(rm.requests.Load()) != onceAttempts || len(waits) != onceAttempts-1 {
		t.Fatalf("%d attempts with %d waits, want %d and %d", rm.requests.Load(), len(waits), onceAttempts, onceAttempts-1)
	}
	// Refusals are retried slowly, and more slowly each time.
	for i, w := range waits {
		if w < refusedRetryMin*9/10 {
			t.Errorf("wait %d = %s, shorter than a refusal's first delay", i, w)
		}
		if i > 0 && w < waits[i-1] {
			t.Errorf("wait %d = %s is shorter than the one before (%s)", i, w, waits[i-1])
		}
	}
}

func TestRun_UnreachableServerIsRetriedSoonerThanARefusal(t *testing.T) {
	rm := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) { accepted(w, 3600) })
	caFile, address := rm.caFile, rm.URL
	rm.Close()
	agent := newAgent(t, Config{
		ServerURL: address, ServerCAFile: caFile, BundleTokenFile: bundleFile(t, "b.s"),
		ServiceAccount: "ocm-fleet-access", Once: true,
	}, &fakeSource{})
	var waits []time.Duration
	agent.sleep = func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }
	err := agent.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "reach the resource manager") {
		t.Fatalf("Run = %v", err)
	}
	if len(waits) == 0 || waits[0] > retryMin*11/10 {
		t.Errorf("first wait = %v, want about %s", waits, retryMin)
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	elsewhere := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) { accepted(w, 3600) })
	rm := newResourceManager(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+PresentPath, http.StatusTemporaryRedirect)
	})
	agent := newAgent(t, Config{
		ServerURL: rm.URL, ServerCAFile: rm.caFile, BundleTokenFile: bundleFile(t, "b.s"),
		ServiceAccount: "ocm-fleet-access", Once: true,
	}, &fakeSource{})
	agent.sleep = func(context.Context, time.Duration) error { return nil }
	var refused *refusedError
	if err := agent.Run(context.Background()); !errors.As(err, &refused) || refused.status != http.StatusTemporaryRedirect {
		t.Fatalf("Run = %v, want the redirect reported as a refusal", err)
	}
	if elsewhere.requests.Load() != 0 {
		t.Errorf("the bundle secret followed a redirect to another server")
	}
}

func TestRun_KeepsPresentingAndReportsReadiness(t *testing.T) {
	var fail atomic.Bool
	rm := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		accepted(w, 7200)
	})
	agent := newAgent(t, Config{
		ServerURL: rm.URL, ServerCAFile: rm.caFile, BundleTokenFile: bundleFile(t, "b.s"), ServiceAccount: "ocm-fleet-access",
	}, &fakeSource{})
	if agent.Ready() {
		t.Fatal("ready before any presentation")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var waits []time.Duration
	var readyAfterFirst bool
	agent.sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		switch len(waits) {
		case 1:
			readyAfterFirst = agent.Ready()
			fail.Store(true)
		case 3:
			cancel()
			return context.Canceled
		}
		return nil
	}
	if err := agent.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want it to run until cancelled", err)
	}
	if !readyAfterFirst {
		t.Errorf("not ready after a successful presentation")
	}
	if len(waits) != 3 || waits[0] < 2*time.Hour*9/10 || waits[0] > 2*time.Hour*11/10 {
		t.Fatalf("waits = %v, want the first to be about the two hours the resource manager asked for", waits)
	}
	// A server that is unwell is asked again soon; it has not refused anything.
	if waits[1] > retryMin*11/10 {
		t.Errorf("wait after a failed re-presentation = %s, want about %s", waits[1], retryMin)
	}
	// One failed re-presentation does not make the cluster unready: the credential it holds is still good.
	if !agent.Ready() {
		t.Errorf("unready right after one failed re-presentation")
	}
}

func TestRenewAfterHasAFloor(t *testing.T) {
	rm := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) { accepted(w, 0) })
	agent := newAgent(t, Config{
		ServerURL: rm.URL, ServerCAFile: rm.caFile, BundleTokenFile: bundleFile(t, "b.s"), ServiceAccount: "ocm-fleet-access",
	}, &fakeSource{})
	var first time.Duration
	agent.sleep = func(_ context.Context, d time.Duration) error { first = d; return context.Canceled }
	_ = agent.Run(context.Background())
	if first < minRenewAfter*9/10 {
		t.Fatalf("next presentation in %s, want at least about %s", first, minRenewAfter)
	}
}

func TestPresent_LocalFailures(t *testing.T) {
	rm := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) { accepted(w, 3600) })
	base := Config{ServerURL: rm.URL, ServerCAFile: rm.caFile, ServiceAccount: "ocm-fleet-access", Once: true}

	t.Run("empty bundle token", func(t *testing.T) {
		cfg := base
		cfg.BundleTokenFile = bundleFile(t, "  \n")
		if _, err := newAgent(t, cfg, &fakeSource{}).present(context.Background()); err == nil || !strings.Contains(err.Error(), "empty") {
			t.Fatalf("present = %v", err)
		}
	})
	t.Run("missing bundle token", func(t *testing.T) {
		cfg := base
		cfg.BundleTokenFile = filepath.Join(t.TempDir(), "absent")
		if _, err := newAgent(t, cfg, &fakeSource{}).present(context.Background()); err == nil {
			t.Fatal("present succeeded without a bundle token")
		}
	})
	t.Run("the cluster refuses the token request", func(t *testing.T) {
		cfg := base
		cfg.BundleTokenFile = bundleFile(t, "b.s")
		_, err := newAgent(t, cfg, &fakeSource{err: errors.New("the cluster answered 403: forbidden")}).present(context.Background())
		if err == nil || !strings.Contains(err.Error(), "request a token for own-namespace/ocm-fleet-access") {
			t.Fatalf("present = %v", err)
		}
	})
	if rm.requests.Load() != 0 {
		t.Errorf("the resource manager was called although nothing could be presented")
	}
}

func TestConfigValidate(t *testing.T) {
	good := Config{ServerURL: "https://fleet.example:8085/", BundleTokenFile: "/t", ServiceAccount: "sa"}
	cfg := good
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg.ServerURL != "https://fleet.example:8085" || cfg.TokenLifetime != 12*time.Hour {
		t.Errorf("defaults not applied: %+v", cfg)
	}
	bad := map[string]func(*Config){
		"cleartext":           func(c *Config) { c.ServerURL = "http://fleet.example" },
		"no host":             func(c *Config) { c.ServerURL = "https://" },
		"path":                func(c *Config) { c.ServerURL = "https://fleet.example/api" },
		"credentials":         func(c *Config) { c.ServerURL = "https://u:p@fleet.example" },
		"connect address":     func(c *Config) { c.ConnectAddress = "fleet-internal" },
		"no bundle token":     func(c *Config) { c.BundleTokenFile = "" },
		"no service account":  func(c *Config) { c.ServiceAccount = "" },
		"lifetime too short":  func(c *Config) { c.TokenLifetime = time.Minute },
		"server URL is empty": func(c *Config) { c.ServerURL = "" },
	}
	for name, change := range bad {
		t.Run(name, func(t *testing.T) {
			cfg := good
			change(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate accepted %+v", cfg)
			}
		})
	}
}

func TestServeHealth(t *testing.T) {
	rm := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) { accepted(w, 3600) })
	cfg := Config{ServerURL: rm.URL, ServerCAFile: rm.caFile, BundleTokenFile: bundleFile(t, "b.s"), ServiceAccount: "ocm-fleet-access"}
	// Without a health address nothing is served and the call returns when the context ends.
	quiet := newAgent(t, cfg, &fakeSource{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- quiet.ServeHealth(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeHealth without an address = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeHealth without an address did not return")
	}
	// An address that cannot be bound is an error, not a silent agent without probes.
	cfg.HealthAddr = "256.0.0.1:1"
	if err := newAgent(t, cfg, &fakeSource{}).ServeHealth(context.Background()); err == nil {
		t.Fatal("ServeHealth on an impossible address returned no error")
	}
}

func TestServeHealth_ReadyOnlyAfterAPresentation(t *testing.T) {
	rm := newResourceManager(t, func(w http.ResponseWriter, _ *http.Request) { accepted(w, 3600) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()
	agent := newAgent(t, Config{
		ServerURL: rm.URL, ServerCAFile: rm.caFile, BundleTokenFile: bundleFile(t, "b.s"),
		ServiceAccount: "ocm-fleet-access", HealthAddr: addr,
	}, &fakeSource{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- agent.ServeHealth(ctx) }()

	get := func(path string) int {
		for i := 0; i < 200; i++ {
			resp, err := http.Get("http://" + addr + path)
			if err == nil {
				resp.Body.Close()
				return resp.StatusCode
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("health endpoint %s never answered", path)
		return 0
	}
	if code := get("/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d", code)
	}
	if code := get("/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz before a presentation = %d, want 503", code)
	}
	agent.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	_ = agent.Run(ctx)
	if code := get("/readyz"); code != http.StatusOK {
		t.Errorf("/readyz after a presentation = %d, want 200", code)
	}
	cancel()
	if err := <-served; err != nil {
		t.Errorf("ServeHealth = %v", err)
	}
}

// unrelatedCAFile writes a self-signed certificate that signed nothing.
func unrelatedCAFile(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "unrelated"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "unrelated.crt")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	return path
}
