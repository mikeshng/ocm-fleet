package kubernetes_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// credentialRecorder is a stand-in cluster API that refuses everything
// and remembers which bearer credentials it was shown.
type credentialRecorder struct {
	mu     sync.Mutex
	server *httptest.Server
	seen   map[string]bool
}

func newCredentialRecorder(t *testing.T) *credentialRecorder {
	t.Helper()
	r := &credentialRecorder{seen: map[string]bool{}}
	r.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.seen[req.Header.Get("Authorization")] = true
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`)
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *credentialRecorder) credentials() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.seen))
	for c := range r.seen {
		out = append(out, c)
	}
	return out
}

func (r *credentialRecorder) target(rule string) domain.TargetInfo {
	props := map[string]string{
		"api_server":            r.server.URL,
		"ca_cert":               tlsServerCAPEM(r.server),
		"service_account_token": "stored-credential",
	}
	if rule != "" {
		props[kubernetes.PropDeliveryCredential] = rule
	}
	return domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "k8s-enrolled", Type: kubernetes.TargetType, Name: "enrolled", Properties: props,
	})
}

var credentialRuleManifests = []domain.Manifest{{
	ManifestType: "raw",
	Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
}}

// TestAgent_CredentialRule pins which credential applies unsigned
// intent: the stored one only on a target that records the explicit
// grant and only for an authenticated caller; the caller's own token
// in every other case. There is no silent fallback either way.
func TestAgent_CredentialRule(t *testing.T) {
	caller := &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "ops", Issuer: "https://issuer.test"}}
	cases := []struct {
		name string
		rule string
		auth domain.DeliveryAuth
		want string
	}{
		{"granted and authenticated uses the stored credential", kubernetes.DeliveryCredentialPlatform,
			domain.DeliveryAuth{Caller: caller, Token: "caller-token"}, "Bearer stored-credential"},
		{"granted and authenticated needs no caller token", kubernetes.DeliveryCredentialPlatform,
			domain.DeliveryAuth{Caller: caller}, "Bearer stored-credential"},
		{"granted but anonymous uses the caller's token", kubernetes.DeliveryCredentialPlatform,
			domain.DeliveryAuth{Token: "caller-token"}, "Bearer caller-token"},
		{"signed-only uses the caller's token", kubernetes.DeliveryCredentialSigned,
			domain.DeliveryAuth{Caller: caller, Token: "caller-token"}, "Bearer caller-token"},
		{"no rule uses the caller's token", "",
			domain.DeliveryAuth{Caller: caller, Token: "caller-token"}, "Bearer caller-token"},
	}
	for _, tc := range cases {
		for _, op := range []string{"deliver", "remove"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				rec := newCredentialRecorder(t)
				reporter := newChannelReporter()
				agent := kubernetes.NewDeliveryAgent(reporter)
				var err error
				if op == "deliver" {
					err = agent.Deliver(context.Background(), rec.target(tc.rule), "d1", credentialRuleManifests, tc.auth, nil, 1)
				} else {
					err = agent.Remove(context.Background(), rec.target(tc.rule), "d1", credentialRuleManifests, tc.auth, nil, 1)
				}
				if err != nil {
					t.Fatalf("%s: %v", op, err)
				}
				result := awaitDone(t, reporter.done)
				if result.State != domain.DeliveryStateAuthFailed {
					t.Fatalf("state = %q, want %q (the stand-in refuses everything)", result.State, domain.DeliveryStateAuthFailed)
				}
				seen := rec.credentials()
				if len(seen) != 1 || seen[0] != tc.want {
					t.Errorf("credentials shown to the cluster = %v, want only %q", seen, tc.want)
				}
			})
		}
	}
}

// TestAgent_CredentialRule_NoGrantNoToken: without the grant, unsigned
// intent of a caller with no token is refused before anything is sent.
func TestAgent_CredentialRule_NoGrantNoToken(t *testing.T) {
	caller := &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "ops", Issuer: "https://issuer.test"}}
	for _, rule := range []string{kubernetes.DeliveryCredentialSigned, ""} {
		rec := newCredentialRecorder(t)
		agent := kubernetes.NewDeliveryAgent(nopReporter{})
		err := agent.Deliver(context.Background(), rec.target(rule), "d1", credentialRuleManifests, domain.DeliveryAuth{Caller: caller}, nil, 1)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("rule %q deliver: error = %v, want ErrInvalidArgument", rule, err)
		}
		err = agent.Remove(context.Background(), rec.target(rule), "d1", credentialRuleManifests, domain.DeliveryAuth{Caller: caller}, nil, 1)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("rule %q remove: error = %v, want ErrInvalidArgument", rule, err)
		}
		if seen := rec.credentials(); len(seen) != 0 {
			t.Errorf("rule %q: the cluster was shown %v", rule, seen)
		}
	}
}
