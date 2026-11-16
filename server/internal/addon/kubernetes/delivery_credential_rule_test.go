package kubernetes_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
//
// It also pins what a cluster that does not recognize the credential
// means. For the caller's token it is an authentication failure, which
// pauses the fulfillment until the caller resumes it with a fresh one.
// For the stored credential it is a failed delivery, which is retried:
// no caller can replace that credential, and the platform renews it or
// the cluster's side presents a new one.
func TestAgent_CredentialRule(t *testing.T) {
	caller := &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "ops", Issuer: "https://issuer.test"}}
	cases := []struct {
		name             string
		rule             string
		auth             domain.DeliveryAuth
		want             string
		wantState        domain.DeliveryState
		wantUnsignedHint bool
	}{
		{"granted and authenticated uses the stored credential", kubernetes.DeliveryCredentialPlatform,
			domain.DeliveryAuth{Caller: caller, Token: "caller-token"}, "Bearer stored-credential", domain.DeliveryStateFailed, false},
		{"granted and authenticated needs no caller token", kubernetes.DeliveryCredentialPlatform,
			domain.DeliveryAuth{Caller: caller}, "Bearer stored-credential", domain.DeliveryStateFailed, false},
		{"granted but anonymous uses the caller's token", kubernetes.DeliveryCredentialPlatform,
			domain.DeliveryAuth{Token: "caller-token"}, "Bearer caller-token", domain.DeliveryStateAuthFailed, true},
		{"signed-only uses the caller's token", kubernetes.DeliveryCredentialSigned,
			domain.DeliveryAuth{Caller: caller, Token: "caller-token"}, "Bearer caller-token", domain.DeliveryStateAuthFailed, true},
		{"no rule uses the caller's token", "",
			domain.DeliveryAuth{Caller: caller, Token: "caller-token"}, "Bearer caller-token", domain.DeliveryStateAuthFailed, true},
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
				if result.State != tc.wantState {
					t.Fatalf("state = %q, want %q (the stand-in recognizes no credential); message: %s", result.State, tc.wantState, result.Message)
				}
				seen := rec.credentials()
				if len(seen) != 1 || seen[0] != tc.want {
					t.Errorf("credentials shown to the cluster = %v, want only %q", seen, tc.want)
				}
				if hint := strings.Contains(result.Message, "does not let the stored credential deliver"); hint != tc.wantUnsignedHint {
					t.Errorf("message names the missing grant = %v, want %v; message: %s", hint, tc.wantUnsignedHint, result.Message)
				}
			})
		}
	}
}

// TestAgent_NamespaceBeingDeletedIsRetried pins what a cluster means
// when it refuses new content in a namespace that is still being
// deleted. The answer is "forbidden", but it says nothing about the
// credential: the same call succeeds once the namespace is gone, and
// nobody has to widen any right. A deployment that leaves a cluster and
// comes back before its namespace is gone meets exactly this. So the
// delivery failed and is retried; it is not an authorization failure,
// which would pause the fulfillment until someone resumes it.
//
// Any other "forbidden" stays an authorization failure.
func TestAgent_NamespaceBeingDeletedIsRetried(t *testing.T) {
	const beingDeleted = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",` +
		`"message":"configmaps \"test\" is forbidden: unable to create new content in namespace default because it is being terminated",` +
		`"reason":"Forbidden","details":{"name":"test","kind":"configmaps",` +
		`"causes":[{"reason":"NamespaceTerminating","message":"namespace default is being terminated","field":"metadata.namespace"}]},"code":403}`
	const notAllowed = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",` +
		`"message":"configmaps \"test\" is forbidden: User \"system:serviceaccount:ocm-fleet-system:ocm-fleet-access\" cannot patch resource \"configmaps\"",` +
		`"reason":"Forbidden","details":{"name":"test","kind":"configmaps"},"code":403}`
	caller := &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "ops", Issuer: "https://issuer.test"}}
	cases := []struct {
		name      string
		answer    string
		rule      string
		auth      domain.DeliveryAuth
		wantState domain.DeliveryState
		wantHint  bool
	}{
		{"namespace being deleted, stored credential", beingDeleted, kubernetes.DeliveryCredentialPlatform,
			domain.DeliveryAuth{Caller: caller}, domain.DeliveryStateFailed, true},
		{"namespace being deleted, caller's token", beingDeleted, "",
			domain.DeliveryAuth{Caller: caller, Token: "caller-token"}, domain.DeliveryStateFailed, true},
		{"not allowed, stored credential", notAllowed, kubernetes.DeliveryCredentialPlatform,
			domain.DeliveryAuth{Caller: caller}, domain.DeliveryStateAuthFailed, false},
		{"not allowed, caller's token", notAllowed, "",
			domain.DeliveryAuth{Caller: caller, Token: "caller-token"}, domain.DeliveryStateAuthFailed, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newDiscoveryTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, tc.answer)
			})
			t.Cleanup(server.Close)
			props := map[string]string{
				"api_server":            server.URL,
				"ca_cert":               tlsServerCAPEM(server),
				"service_account_token": "stored-credential",
			}
			if tc.rule != "" {
				props[kubernetes.PropDeliveryCredential] = tc.rule
			}
			target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
				ID: "k8s-enrolled", Type: kubernetes.TargetType, Name: "enrolled", Properties: props,
			})
			reporter := newChannelReporter()
			agent := kubernetes.NewDeliveryAgent(reporter)
			if err := agent.Deliver(context.Background(), target, "d1", credentialRuleManifests, tc.auth, nil, 1); err != nil {
				t.Fatalf("deliver: %v", err)
			}
			result := awaitDone(t, reporter.done)
			if result.State != tc.wantState {
				t.Fatalf("state = %q, want %q; message: %s", result.State, tc.wantState, result.Message)
			}
			if hint := strings.Contains(result.Message, "the namespace is still being deleted"); hint != tc.wantHint {
				t.Errorf("message says the namespace is still being deleted = %v, want %v; message: %s", hint, tc.wantHint, result.Message)
			}
		})
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
