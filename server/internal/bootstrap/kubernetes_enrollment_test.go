package bootstrap

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

type staticAuthMethods struct {
	methods []domain.AuthMethod
	err     error
}

func (s staticAuthMethods) List(context.Context) ([]domain.AuthMethod, error) {
	return s.methods, s.err
}

func TestTrustBundleSource_OneEntryPerOIDCMethod(t *testing.T) {
	mapping := &domain.RegistrySubjectMapping{RegistryID: "github.com", Expression: "claims.login"}
	source := trustBundleSource(staticAuthMethods{methods: []domain.AuthMethod{
		domain.NewOIDCAuthMethod("default", &domain.OIDCConfig{
			IssuerURL:              "https://issuer.test",
			JWKSURI:                "https://issuer.test/keys",
			Audience:               "resource-manager",
			KeyEnrollmentAudience:  "enroll",
			RegistrySubjectMapping: mapping,
		}),
	}})
	entries, err := source(context.Background())
	if err != nil {
		t.Fatalf("source: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %+v, want one", entries)
	}
	e := entries[0]
	if e.IssuerURL != "https://issuer.test" || e.JWKSURI != "https://issuer.test/keys" || e.EnrollmentAudience != "enroll" || e.RegistrySubjectMapping != mapping {
		t.Errorf("entry = %+v", e)
	}

	none, err := trustBundleSource(staticAuthMethods{})(context.Background())
	if err != nil || len(none) != 0 {
		t.Errorf("no auth methods: %+v, %v", none, err)
	}
	if _, err := trustBundleSource(staticAuthMethods{err: errors.New("down")})(context.Background()); err == nil {
		t.Error("a listing failure was swallowed")
	}
}

func TestCallerFromContext(t *testing.T) {
	if got := callerFromContext(context.Background()); got != "" {
		t.Errorf("anonymous caller = %q, want empty", got)
	}
	withSubject := func(extra map[string][]string) context.Context {
		return application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
			Subject: &domain.SubjectClaims{
				FederatedIdentity: domain.FederatedIdentity{Subject: "sub-1", Issuer: "https://issuer.test"},
				Extra:             extra,
			},
		})
	}
	if got := callerFromContext(withSubject(nil)); got != "sub-1" {
		t.Errorf("caller without email = %q, want the subject", got)
	}
	if got := callerFromContext(withSubject(map[string][]string{"email": {"ops@example.test"}})); got != "ops@example.test" {
		t.Errorf("caller with email = %q", got)
	}
}

func TestRegisterAddonHTTPRoutes(t *testing.T) {
	mux := http.NewServeMux()
	// The dynamic API's method-less routes for a collection.
	dynamic := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	mux.Handle("/apis/x/v1alpha/things", dynamic)
	mux.Handle("/apis/x/v1alpha/things/", dynamic)

	authenticated := 0
	authenticate := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authenticated++
			next.ServeHTTP(w, r)
		})
	}
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	specs := []AddonSpec{
		{},
		{HTTPRoutes: func(handle, handlePublic func(string, http.Handler)) {
			handle("POST /apis/x/v1alpha/things/{idAndVerb}", ok)
			handle("POST /apis/x/v1alpha/things:make", ok)
			handlePublic("POST /apis/x/v1alpha/presentations:present", ok)
		}},
	}
	if err := registerAddonHTTPRoutes(mux, specs, authenticate); err != nil {
		t.Fatalf("registerAddonHTTPRoutes: %v", err)
	}

	serve := func(method, path string) int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		return rec.Code
	}
	if got := serve(http.MethodPost, "/apis/x/v1alpha/things/a:verb"); got != http.StatusOK || authenticated != 1 {
		t.Errorf("verb route = %d, authenticated %d times", got, authenticated)
	}
	if got := serve(http.MethodPost, "/apis/x/v1alpha/things:make"); got != http.StatusOK || authenticated != 2 {
		t.Errorf("collection verb route = %d, authenticated %d times", got, authenticated)
	}
	if got := serve(http.MethodPost, "/apis/x/v1alpha/presentations:present"); got != http.StatusOK || authenticated != 2 {
		t.Errorf("public route = %d, authenticated %d times (must not be wrapped)", got, authenticated)
	}
	if got := serve(http.MethodGet, "/apis/x/v1alpha/things/a"); got != http.StatusTeapot {
		t.Errorf("read = %d, want the dynamic API's answer", got)
	}

	conflicting := []AddonSpec{{HTTPRoutes: func(handle, _ func(string, http.Handler)) {
		handle("POST /apis/x/v1alpha/things:make", ok)
	}}}
	err := registerAddonHTTPRoutes(mux, conflicting, authenticate)
	if err == nil || !strings.Contains(err.Error(), "register add-on HTTP routes") {
		t.Fatalf("conflicting route: error = %v, want a registration error instead of a panic", err)
	}
}

func TestNewKubernetesEnrollment_NeedsARecordStore(t *testing.T) {
	if got := newKubernetesEnrollment(AddonDeps{Logger: testLogger()}); got != nil {
		t.Fatal("enrollment was assembled without a record store")
	}
}
