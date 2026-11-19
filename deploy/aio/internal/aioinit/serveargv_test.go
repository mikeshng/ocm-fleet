package aioinit_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stolostron/ocm-fleet/deploy/aio/internal/aioinit"
)

// TestServeArgs_PeerDexDefaultsForbidSandboxFlags checks Dex-on packaging
// argv (issuer, audience, UI client, registry, CA) and that serve is not
// given --sandbox or --bootstrap-auth-method.
func TestServeArgs_PeerDexDefaultsForbidSandboxFlags(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    aioinit.PeerDexIssuer,
		CAFile:    "/data/sandbox/pki/ca.crt",
	})
	args := aioinit.ServeArgs(in)
	joined := strings.Join(args, "\x00")
	for _, want := range []string{
		"serve",
		"--http-addr\x00127.0.0.1:8086",
		"--oidc-issuer\x00https://ocm-fleet.localhost:8085/idp",
		"--oidc-resource-audience\x00resource-manager",
		"--oidc-ui-client-id\x00ocm-fleet-console",
		"--oidc-ui-scope\x00" + aioinit.DefaultPeerDexUIScope,
		"--oidc-registry-id\x00github.com",
		"--oidc-registry-subject-expression\x00claims.preferred_username",
		"--oidc-ca-file\x00/data/sandbox/pki/ca.crt",
		"--log-level\x00error",
		"--addons\x00kind,kubernetes,ocm",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q in %v", want, args)
		}
	}
	for _, bad := range []string{"--sandbox", "--bootstrap-auth-method"} {
		if strings.Contains(joined, bad) {
			t.Fatalf("forbidden flag %s present", bad)
		}
	}
}

func TestApplyServeDefaults_ExplicitAddonsWin(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{Addons: "kubernetes,scripted"})
	if in.Addons != "kubernetes,scripted" {
		t.Fatalf("Addons = %q, want explicit addon selection", in.Addons)
	}
}

// TestResolveEnrollment covers where existing clusters are told to present
// themselves: the kind sandbox default, an explicit address taken as given,
// and nothing at all without either.
func TestResolveEnrollment(t *testing.T) {
	const sandboxCA = "/data/sandbox/pki/ca.crt"

	sandbox := aioinit.ResolveEnrollment(aioinit.EnrollmentConfig{AgentImage: "agent:1"}, sandboxCA, true)
	want := aioinit.EnrollmentConfig{
		ServerURL:      "https://ocm-fleet.localhost:8085",
		CAFile:         sandboxCA,
		ConnectAddress: "ocm-fleet:8085",
		AgentImage:     "agent:1",
	}
	if sandbox != want {
		t.Fatalf("kind sandbox default = %+v, want %+v", sandbox, want)
	}

	explicit := aioinit.EnrollmentConfig{ServerURL: "https://fleet.example.com", AgentImage: "agent:1"}
	if got := aioinit.ResolveEnrollment(explicit, sandboxCA, true); got != explicit {
		t.Fatalf("explicit address = %+v, want it untouched (its CA and connect address are the operator's)", got)
	}

	if got := aioinit.ResolveEnrollment(aioinit.EnrollmentConfig{}, sandboxCA, false); got != (aioinit.EnrollmentConfig{}) {
		t.Fatalf("no engine socket, nothing explicit = %+v, want empty", got)
	}
}

func TestServeArgs_Enrollment(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    aioinit.PeerDexIssuer,
		Enrollment: aioinit.EnrollmentConfig{
			ServerURL:      "https://ocm-fleet.localhost:8085",
			CAFile:         "/data/sandbox/pki/ca.crt",
			ConnectAddress: "ocm-fleet:8085",
			AgentImage:     "agent:1",
		},
	})
	joined := strings.Join(aioinit.ServeArgs(in), "\x00")
	for _, want := range []string{
		"--enrollment-server-url\x00https://ocm-fleet.localhost:8085",
		"--enrollment-ca-file\x00/data/sandbox/pki/ca.crt",
		"--enrollment-connect-address\x00ocm-fleet:8085",
		"--enrollment-agent-image\x00agent:1",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q", want)
		}
	}

	none := strings.Join(aioinit.ServeArgs(aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints, Issuer: aioinit.PeerDexIssuer,
	})), "\x00")
	if strings.Contains(none, "--enrollment-") {
		t.Fatalf("enrollment flags present without enrollment settings: %q", none)
	}
}

func TestApplyServeDefaults_ExternalIssuerUsesPortableUIScope(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    "https://idp.example.com/tenant/opaque-id",
	})
	if in.UIScope != aioinit.DefaultExternalIssuerUIScope {
		t.Fatalf("UIScope = %q, want %q", in.UIScope, aioinit.DefaultExternalIssuerUIScope)
	}
	args := aioinit.ServeArgs(in)
	joined := strings.Join(args, "\x00")
	if !strings.Contains(joined, "--oidc-issuer\x00https://idp.example.com/tenant/opaque-id") {
		t.Fatalf("external issuer path must be preserved: %v", args)
	}
	want := "--oidc-ui-scope\x00" + aioinit.DefaultExternalIssuerUIScope
	if !strings.Contains(joined, want) {
		t.Fatalf("args missing %q in %v", want, args)
	}
	if strings.Contains(joined, "audience:server:client_id:") {
		t.Fatalf("external issuer must not default Dex audience scope: %v", args)
	}
}

func TestApplyServeDefaults_ExplicitUIScopeWinsOnExternalIssuer(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    "https://idp.example.com/tenant/opaque-id",
		UIScope:   "openid profile email groups",
	})
	if in.UIScope != "openid profile email groups" {
		t.Fatalf("UIScope = %q, want explicit override", in.UIScope)
	}
}

func TestApplyServeDefaults_PublicKeySkipsRegistryDefaults(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints:     aioinit.FixedEndpoints,
		Issuer:        aioinit.PeerDexIssuer,
		PublicKeyExpr: "claims.spk",
	})
	if in.RegistryID != "" || in.RegistryExpr != "" {
		t.Fatalf("registry defaults = %q/%q, want empty when public-key claim set", in.RegistryID, in.RegistryExpr)
	}
	args := aioinit.ServeArgs(in)
	joined := strings.Join(args, "\x00")
	if !strings.Contains(joined, "--oidc-public-key-claim-expression\x00claims.spk") {
		t.Fatalf("args missing public-key claim: %v", args)
	}
	if strings.Contains(joined, "--oidc-registry-id") {
		t.Fatalf("args unexpectedly include registry: %v", args)
	}
}

func TestResolveLogLevel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		raw     string
		want    string
		wantErr string
	}{
		{name: "unset", raw: "", want: "error"},
		{name: "whitespace", raw: "  \t", want: "error"},
		{name: "error", raw: "error", want: "error"},
		{name: "warn", raw: "warn", want: "warn"},
		{name: "info", raw: "info", want: "info"},
		{name: "debug", raw: "debug", want: "debug"},
		{name: "trimmed debug", raw: "  debug  ", want: "debug"},
		{name: "invalid", raw: "verbose", wantErr: "verbose"},
		{name: "uppercase", raw: "ERROR", wantErr: "ERROR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := aioinit.ResolveLogLevel(tc.raw)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ResolveLogLevel(%q) = %q, want error identifying %q", tc.raw, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q, want it to identify %q", err, tc.wantErr)
				}
				if !strings.Contains(err.Error(), "invalid LOG_LEVEL") {
					t.Fatalf("error %q, want invalid LOG_LEVEL", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveLogLevel(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("ResolveLogLevel(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestApplyServeDefaults_LogLevel(t *testing.T) {
	cases := []struct {
		name  string
		level string
		want  string
	}{
		{name: "omitted", want: "error"},
		{name: "override", level: "info", want: "info"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
				Endpoints: aioinit.FixedEndpoints,
				Issuer:    aioinit.PeerDexIssuer,
				LogLevel:  tc.level,
			})
			if in.LogLevel != tc.want {
				t.Fatalf("LogLevel = %q, want %q", in.LogLevel, tc.want)
			}
			args := aioinit.ServeArgs(in)
			joined := strings.Join(args, "\x00")
			want := "--log-level\x00" + tc.want
			if !strings.Contains(joined, want) {
				t.Fatalf("args missing %q: %v", want, args)
			}
		})
	}
}

func TestWriteServeExecScript(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exec-serve")
	if err := aioinit.WriteServeExecScript(path, []string{
		"serve",
		"--oidc-ui-scope", "a b",
		"--oidc-issuer", "https://issuer.example/x&id",
		"--oidc-ui-client-id", "it's-me",
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	if !strings.Contains(body, "exec /usr/local/bin/resource-manager 'serve'") {
		t.Fatalf("unexpected script: %s", raw)
	}
	if !strings.Contains(body, " '--oidc-ui-scope' 'a b'") {
		t.Fatalf("scope not single-quoted in script: %s", raw)
	}
	if !strings.Contains(body, " '--oidc-issuer' 'https://issuer.example/x&id'") {
		t.Fatalf("issuer with & not single-quoted in script: %s", raw)
	}
	if !strings.Contains(body, ` '--oidc-ui-client-id' 'it'\''s-me'`) {
		t.Fatalf("embedded quote not escaped in script: %s", raw)
	}
}
