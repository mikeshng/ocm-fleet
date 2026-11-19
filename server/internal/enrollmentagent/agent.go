// Package enrollmentagent is the small program an enrollment puts on a
// cluster next to the access piece. It asks the cluster for a token of
// the access service account, reads the cluster's CA, and presents
// both to the resource manager with the enrollment bundle's secret.
// Run once, it enrolls the cluster and exits. Kept running, it presents
// again before the credential would lapse, so that a cluster whose
// credential the resource manager could not renew repairs itself.
//
// It depends on nothing but the standard library: it runs on every
// cluster of a fleet and should be small and easy to read in full.
package enrollmentagent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// PresentPath is the resource manager's route for presentations.
const PresentPath = "/apis/kubernetes.open-cluster-management.io/v1alpha/enrollments:present"

const (
	// serviceAccountDir is where a pod finds its own identity and the
	// cluster's CA.
	serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	requestTimeout    = 30 * time.Second
	// maxResponseBytes bounds what is read of any response.
	maxResponseBytes = 1 << 20

	// Presentations that fail are retried with a growing delay. A
	// refusal (the resource manager answered, and said no) is retried
	// more slowly than a failure to reach it: a refusal seldom changes
	// by itself, and each attempt costs the resource manager a check
	// against the cluster.
	retryMin        = 5 * time.Second
	retryMax        = 5 * time.Minute
	refusedRetryMin = 30 * time.Second
	refusedRetryMax = 10 * time.Minute
	// minRenewAfter keeps a misconfigured answer from making the agent
	// present in a tight loop.
	minRenewAfter = time.Minute
	// onceAttempts is how often a one-shot run tries before it exits
	// with an error and leaves the retry to whatever started it.
	onceAttempts = 6
)

// Config is what the agent is started with.
type Config struct {
	// ServerURL is the resource manager's https address.
	ServerURL string
	// ServerCAFile is a PEM file with the CA that signs the resource
	// manager's certificate. Empty means the system's roots.
	ServerCAFile string
	// ConnectAddress is a host:port to dial instead of resolving the
	// host of ServerURL. The request still names ServerURL's host, in
	// the TLS handshake and in the Host header.
	ConnectAddress string
	// BundleTokenFile holds the enrollment bundle's bearer value.
	BundleTokenFile string
	// Namespace and ServiceAccount name the access account. Namespace
	// defaults to the agent's own.
	Namespace      string
	ServiceAccount string
	// ClusterName is the cluster's name within the bundle's scope, for
	// a bundle that covers more than one cluster.
	ClusterName string
	// TokenLifetime is the lifetime asked for the access token.
	TokenLifetime time.Duration
	// HealthAddr is where /healthz and /readyz are served; empty
	// serves neither.
	HealthAddr string
	// Once makes the agent exit after the first successful
	// presentation.
	Once bool
}

// Validate checks the configuration and applies defaults.
func (c *Config) Validate() error {
	u, err := url.Parse(c.ServerURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("--server-url must be an https URL with a host, got %q", c.ServerURL)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return fmt.Errorf("--server-url must not carry a path, query, or credentials")
	}
	c.ServerURL = "https://" + u.Host
	if c.ConnectAddress != "" {
		if _, _, err := net.SplitHostPort(c.ConnectAddress); err != nil {
			return fmt.Errorf("--connect-address must be host:port: %v", err)
		}
	}
	if c.BundleTokenFile == "" {
		return errors.New("--bundle-token-file is required")
	}
	if c.ServiceAccount == "" {
		return errors.New("--service-account is required")
	}
	if c.TokenLifetime == 0 {
		c.TokenLifetime = 12 * time.Hour
	}
	if c.TokenLifetime < 10*time.Minute {
		return errors.New("--token-lifetime must be at least 10m")
	}
	return nil
}

// TokenSource obtains what the agent presents: a token of the access
// account and the cluster's CA.
type TokenSource interface {
	// AccessToken asks the cluster for a new token of the account.
	AccessToken(ctx context.Context, namespace, name string, lifetime time.Duration) (string, error)
	// ClusterCA returns the PEM CA of the cluster's API server.
	ClusterCA() ([]byte, error)
	// Namespace returns the namespace the agent runs in.
	Namespace() (string, error)
}

// presentRequest and presentResponse are the route's JSON bodies.
type presentRequest struct {
	ClusterName    string            `json:"clusterName,omitempty"`
	CACert         string            `json:"caCert"`
	Token          string            `json:"token"`
	ServiceAccount map[string]string `json:"serviceAccount"`
}

type presentResponse struct {
	Cluster           string `json:"cluster"`
	Target            string `json:"target"`
	State             string `json:"state"`
	RenewAfterSeconds int64  `json:"renewAfterSeconds"`
}

// refusedError is an answer from the resource manager that is not a
// success: it was reached and it declined.
type refusedError struct {
	status int
	reason string
	detail string
}

func (e *refusedError) Error() string {
	if e.reason != "" {
		return fmt.Sprintf("the resource manager answered %d (%s): %s", e.status, e.reason, e.detail)
	}
	return fmt.Sprintf("the resource manager answered %d: %s", e.status, e.detail)
}

// Agent presents a cluster to a resource manager.
type Agent struct {
	cfg    Config
	source TokenSource
	client *http.Client
	logger *slog.Logger

	mu        sync.Mutex
	readyTill time.Time
	// sleep waits for d or until ctx ends; tests replace it.
	sleep func(ctx context.Context, d time.Duration) error
}

// New builds an agent. cfg must have passed [Config.Validate].
func New(cfg Config, source TokenSource, logger *slog.Logger) (*Agent, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.ServerCAFile != "" {
		pem, err := os.ReadFile(cfg.ServerCAFile)
		if err != nil {
			return nil, fmt.Errorf("read --server-ca-file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--server-ca-file %s holds no certificate", cfg.ServerCAFile)
		}
		tlsConfig.RootCAs = pool
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	transport := &http.Transport{
		TLSClientConfig:       tlsConfig,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: requestTimeout,
		MaxIdleConns:          1,
		IdleConnTimeout:       30 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if cfg.ConnectAddress != "" {
				addr = cfg.ConnectAddress
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}
	return &Agent{
		cfg:    cfg,
		source: source,
		logger: logger,
		client: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,
			// The bundle secret goes to the configured address only.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		sleep: sleepContext,
	}, nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Ready reports whether the last presentation succeeded and the next
// one is not overdue.
func (a *Agent) Ready() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return time.Now().Before(a.readyTill)
}

// present requests a token and presents it once.
func (a *Agent) present(ctx context.Context) (presentResponse, error) {
	namespace := a.cfg.Namespace
	if namespace == "" {
		ns, err := a.source.Namespace()
		if err != nil {
			return presentResponse{}, fmt.Errorf("find own namespace: %w", err)
		}
		namespace = ns
	}
	bundle, err := os.ReadFile(a.cfg.BundleTokenFile)
	if err != nil {
		return presentResponse{}, fmt.Errorf("read bundle token: %w", err)
	}
	bearer := strings.TrimSpace(string(bundle))
	if bearer == "" {
		return presentResponse{}, errors.New("the bundle token file is empty")
	}
	ca, err := a.source.ClusterCA()
	if err != nil {
		return presentResponse{}, fmt.Errorf("read cluster CA: %w", err)
	}
	token, err := a.source.AccessToken(ctx, namespace, a.cfg.ServiceAccount, a.cfg.TokenLifetime)
	if err != nil {
		return presentResponse{}, fmt.Errorf("request a token for %s/%s: %w", namespace, a.cfg.ServiceAccount, err)
	}

	body, err := json.Marshal(presentRequest{
		ClusterName:    a.cfg.ClusterName,
		CACert:         string(ca),
		Token:          token,
		ServiceAccount: map[string]string{"namespace": namespace, "name": a.cfg.ServiceAccount},
	})
	if err != nil {
		return presentResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.ServerURL+PresentPath, bytes.NewReader(body))
	if err != nil {
		return presentResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := a.client.Do(req)
	if err != nil {
		return presentResponse{}, fmt.Errorf("reach the resource manager at %s: %w", a.cfg.ServerURL, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode != http.StatusOK {
		var failure struct {
			Error  string `json:"error"`
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(raw, &failure)
		detail := failure.Error
		if detail == "" {
			detail = strings.TrimSpace(string(raw))
			if len(detail) > 200 {
				detail = detail[:200]
			}
		}
		return presentResponse{}, &refusedError{status: resp.StatusCode, reason: failure.Reason, detail: detail}
	}
	var out presentResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return presentResponse{}, fmt.Errorf("the resource manager's answer is not understood: %w", err)
	}
	return out, nil
}

// Run presents until it succeeds, and then, unless the agent runs
// once, again whenever the resource manager asked for it. It returns
// nil after the one success of a one-shot run, an error when a
// one-shot run gives up, and ctx's error when ctx ends.
func (a *Agent) Run(ctx context.Context) error {
	delay, refusedDelay := retryMin, refusedRetryMin
	failures := 0
	for {
		out, err := a.present(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			failures++
			wait := delay
			// A 5xx answer is the server (or something in front of it)
			// being unwell, not a decision about this cluster.
			var refused *refusedError
			if errors.As(err, &refused) && refused.status < http.StatusInternalServerError {
				wait = refusedDelay
				refusedDelay = min(refusedDelay*2, refusedRetryMax)
			} else {
				delay = min(delay*2, retryMax)
			}
			if a.cfg.Once && failures >= onceAttempts {
				return fmt.Errorf("giving up after %d attempts: %w", failures, err)
			}
			wait = jitter(wait)
			a.logger.Warn("presentation failed; will retry", "error", err, "retry_in", wait.Round(time.Second).String())
			if err := a.sleep(ctx, wait); err != nil {
				return err
			}
			continue
		}
		delay, refusedDelay, failures = retryMin, refusedRetryMin, 0
		renewAfter := max(time.Duration(out.RenewAfterSeconds)*time.Second, minRenewAfter)
		a.logger.Info("cluster presented", "cluster", out.Cluster, "target", out.Target, "state", out.State,
			"next_in", renewAfter.Round(time.Second).String())
		if a.cfg.Once {
			return nil
		}
		// Ready until the next presentation is clearly overdue.
		a.mu.Lock()
		a.readyTill = time.Now().Add(renewAfter + renewAfter/2 + retryMax)
		a.mu.Unlock()
		if err := a.sleep(ctx, jitter(renewAfter)); err != nil {
			return err
		}
	}
}

// jitter spreads waits so that a fleet's agents do not all call at the
// same moment: between 90% and 110% of d.
func jitter(d time.Duration) time.Duration {
	return d - d/10 + time.Duration(rand.Int64N(int64(d/5)+1))
}

// ServeHealth serves /healthz (the process runs) and /readyz (the
// cluster is enrolled and its next presentation is not overdue) until
// ctx ends.
func (a *Agent) ServeHealth(ctx context.Context) error {
	if a.cfg.HealthAddr == "" {
		<-ctx.Done()
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if a.Ready() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "the cluster has not been presented successfully", http.StatusServiceUnavailable)
	})
	server := &http.Server{Addr: a.cfg.HealthAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve health on %s: %w", a.cfg.HealthAddr, err)
	}
	return nil
}
