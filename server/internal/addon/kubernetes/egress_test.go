package kubernetes

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
)

func TestEgressPolicyPermit(t *testing.T) {
	allowMetadata := []netip.Prefix{netip.MustParsePrefix("169.254.169.254/32")}
	tests := []struct {
		name  string
		allow []netip.Prefix
		ip    string
		want  bool
	}{
		// Ranges a cluster's API server is not at.
		{"loopback v4", nil, "127.0.0.1", false},
		{"loopback v6", nil, "::1", false},
		{"unspecified v4", nil, "0.0.0.0", false},
		{"unspecified v6", nil, "::", false},
		{"link-local v4", nil, "169.254.1.1", false},
		{"instance metadata", nil, "169.254.169.254", false},
		{"link-local v6", nil, "fe80::1", false},
		{"site-local v6", nil, "fec0::1", false},
		{"shared address space", nil, "100.100.100.200", false},
		{"azure platform address", nil, "168.63.129.16", false},
		{"benchmarking", nil, "198.18.0.1", false},
		{"multicast v4", nil, "224.0.0.1", false},
		{"multicast v6", nil, "ff02::1", false},
		{"broadcast", nil, "255.255.255.255", false},
		{"v4-mapped loopback", nil, "::ffff:127.0.0.1", false},
		{"nat64 loopback", nil, "64:ff9b::7f00:1", false},
		{"nat64 instance metadata", nil, "64:ff9b::a9fe:a9fe", false},

		// Places a cluster legitimately lives.
		{"private 10/8", nil, "10.0.0.5", true},
		{"private 172.16/12", nil, "172.16.4.1", true},
		{"private 192.168/16", nil, "192.168.1.10", true},
		{"unique-local v6", nil, "fd00::1", true},
		{"public v4", nil, "203.0.113.10", true},
		{"public v6", nil, "2001:db8::1", true},
		{"nat64 private", nil, "64:ff9b::a00:5", true},

		// An operator's allowlist wins over a refusal.
		{"allow loopback", []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, "127.0.0.1", true},
		{"allow metadata", allowMetadata, "169.254.169.254", true},
		{"allow nat64 metadata by v4", allowMetadata, "64:ff9b::a9fe:a9fe", true},
		{"allow nat64 address itself", []netip.Prefix{netip.MustParsePrefix("64:ff9b::a9fe:a9fe/128")}, "64:ff9b::a9fe:a9fe", true},
		{"allow v6 loopback", []netip.Prefix{netip.MustParsePrefix("::1/128")}, "::1", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := EgressPolicy{Allow: tt.allow}
			ip, err := netip.ParseAddr(tt.ip)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.ip, err)
			}
			if got := p.Permit(ip); got != tt.want {
				t.Errorf("Permit(%s) with allow %v = %v, want %v", tt.ip, tt.allow, got, tt.want)
			}
		})
	}
	if (EgressPolicy{}).Permit(netip.Addr{}) {
		t.Error("Permit of the zero address = true, want false")
	}
}

// TestEgressPolicyDialContext checks that the policy refuses before dialing,
// dials a checked address when permitted, and resolves a name itself rather
// than handing it to the dialer.
func TestEgressPolicyDialContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
		close(accepted)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := (EgressPolicy{}).DialContext(ctx, "tcp", ln.Addr().String()); !errors.Is(err, ErrEgressRefused) {
		t.Fatalf("DialContext to loopback with the zero policy = %v, want ErrEgressRefused", err)
	}
	select {
	case conn := <-accepted:
		if conn != nil {
			conn.Close()
		}
		t.Fatal("the zero policy dialed loopback; the listener accepted a connection")
	case <-time.After(200 * time.Millisecond):
		// No connection, as wanted.
	}

	// The name must be resolved by the policy and the checked address dialed.
	permitted := EgressPolicy{Allow: []netip.Prefix{
		netip.MustParsePrefix("127.0.0.0/8"),
		netip.MustParsePrefix("::1/128"),
	}}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split %q: %v", ln.Addr(), err)
	}
	conn, err := permitted.DialContext(ctx, "tcp", net.JoinHostPort("localhost", port))
	if err != nil {
		t.Fatalf("DialContext to an allowlisted loopback = %v", err)
	}
	conn.Close()
	select {
	case got := <-accepted:
		if got == nil {
			t.Fatal("the allowlisted policy did not connect")
		}
		got.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("the allowlisted policy did not connect")
	}
}

// TestEgressPolicyDialConfigDisablesEnvironmentProxy checks that a client-go
// config built from the policy never routes a cluster connection through the
// environment's HTTP proxy, which would hide the real destination.
func TestEgressPolicyDialConfigDisablesEnvironmentProxy(t *testing.T) {
	var cfg rest.Config
	(EgressPolicy{}).DialConfig(&cfg)
	if cfg.Dial == nil {
		t.Fatal("DialConfig left Dial nil")
	}
	if cfg.Proxy == nil {
		t.Fatal("DialConfig left Proxy nil; client-go would use the environment proxy")
	}
	proxy, err := cfg.Proxy(&http.Request{})
	if err != nil || proxy != nil {
		t.Fatalf("Proxy(request) = %v, %v; want nil, nil", proxy, err)
	}
}

// TestClientGoClusterProberEgressAtDial proves the prober's REST config
// carries the policy: the same in-process TLS server is refused by the zero
// policy and answered when its address is allowlisted, and TLS still checks
// the name from the URL because the certificate is only valid for it.
func TestClientGoClusterProberEgressAtDial(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/version" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"major":"1","minor":"31","gitVersion":"v1.31.0"}`)
	}))
	defer srv.Close()

	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	conn := ClusterConnection{APIServer: srv.URL, CACert: ca, Token: "test-token"}

	zero := ClientGoClusterProber{}
	client, err := zero.client(conn)
	if err != nil {
		t.Fatalf("build client under the zero policy: %v", err)
	}
	if _, err := client.Discovery().ServerVersion(); !errors.Is(err, ErrEgressRefused) {
		t.Fatalf("ServerVersion under the zero policy = %v, want ErrEgressRefused", err)
	}

	allowed := ClientGoClusterProber{Egress: EgressPolicy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}}
	client, err = allowed.client(conn)
	if err != nil {
		t.Fatalf("build client under the allowlist: %v", err)
	}
	version, err := client.Discovery().ServerVersion()
	if err != nil {
		t.Fatalf("ServerVersion under the allowlist = %v", err)
	}
	if version.GitVersion != "v1.31.0" {
		t.Errorf("GitVersion = %q, want v1.31.0", version.GitVersion)
	}
}

// TestEgressRefusalReachesPresenterWithoutDetail checks that a refused
// cluster connection reaches the caller as a fixed sentence naming neither
// the address dialed nor the raw probe error, while the detail is written to
// the server log.
func TestEgressRefusalReachesPresenterWithoutDetail(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})

	var logs bytes.Buffer
	svc := &EnrollmentService{
		prober: ClientGoClusterProber{},
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := svc.probe(ctx, ClusterConnection{APIServer: srv.URL, CACert: ca, Token: "test-token"})
	enrollErr(t, err, ErrEnrollmentUnverified, "cluster-unreachable")
	msg := err.Error()
	for _, leak := range []string{"127.0.0.1", srv.URL, "destination refused by egress policy"} {
		if strings.Contains(msg, leak) {
			t.Errorf("presenter-visible error %q contains %q", msg, leak)
		}
	}
	logged := logs.String()
	if !strings.Contains(logged, "destination refused by egress policy") || !strings.Contains(logged, "api_server=") {
		t.Errorf("server log %q does not carry the refusal detail", logged)
	}
}
