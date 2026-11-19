package bootstrap

import (
	"context"
	"fmt"
	"net/http"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// kubernetesEnrollment is the kubernetes add-on's enrollment capability
// as server composition wires it: the service, its periodic worker, and
// its HTTP handlers.
type kubernetesEnrollment struct {
	service  *kubernetesaddon.EnrollmentService
	worker   *kubernetesaddon.EnrollmentWorker
	http     *kubernetesaddon.EnrollmentHTTP
	endpoint kubernetesaddon.EnrollmentEndpoint
	appCtx   context.Context
}

// newKubernetesEnrollment builds the enrollment capability. It returns
// nil when the composition supplies no record store (test assemblies).
func newKubernetesEnrollment(deps AddonDeps) *kubernetesEnrollment {
	if deps.InventoryRecords == nil || deps.Store == nil || deps.Vault == nil {
		return nil
	}
	var indexing kubernetesaddon.IndexingRuntime
	var indexed kubernetesaddon.IndexedClusterLister
	if deps.Indexing != nil {
		indexing = deps.Indexing.Runtime
		indexed = deps.Indexing.Indexed
	}
	svc := kubernetesaddon.NewEnrollmentService(kubernetesaddon.EnrollmentDeps{
		Store:     deps.Store,
		Vault:     deps.Vault,
		Records:   deps.InventoryRecords,
		Indexing:  indexing,
		Prober:    kubernetesaddon.ClientGoClusterProber{Egress: kubernetesaddon.EgressPolicy{Allow: deps.Config.ClusterEgressAllow}},
		Trust:     deps.TrustBundles,
		Placement: deps.Placement,
		Logger:    deps.Logger,
	})
	endpoint := kubernetesaddon.EnrollmentEndpoint{
		ServerURL:      deps.Config.EnrollmentServerURL,
		ServerCA:       deps.Config.EnrollmentCABundle,
		ConnectAddress: deps.Config.EnrollmentConnectAddress,
		AgentImage:     deps.Config.EnrollmentAgentImage,
	}
	return &kubernetesEnrollment{
		service:  svc,
		worker:   kubernetesaddon.NewEnrollmentWorker(svc, indexed),
		http:     kubernetesaddon.NewEnrollmentHTTP(svc, endpoint, deps.Caller, deps.Logger),
		endpoint: endpoint,
		appCtx:   deps.AppCtx,
	}
}

// start launches the worker under the application context.
func (e *kubernetesEnrollment) start(context.Context) error {
	e.worker.Start(e.appCtx)
	return nil
}

// trustBundleSource builds the trust settings that targets registered by
// an add-on receive: one entry per configured OIDC auth method, the same
// content the idp-trust-bundle deployment delivers to provisioners.
func trustBundleSource(methods authMethodLister) kubernetesaddon.TrustBundleSource {
	return func(ctx context.Context) ([]domain.TrustBundleEntry, error) {
		list, err := methods.List(ctx)
		if err != nil {
			return nil, fmt.Errorf("list auth methods: %w", err)
		}
		var entries []domain.TrustBundleEntry
		for _, m := range list {
			if m.Type() != domain.AuthMethodTypeOIDC || m.OIDC() == nil {
				continue
			}
			oidc := m.OIDC()
			entries = append(entries, domain.TrustBundleEntry{
				IssuerURL:                oidc.IssuerURL,
				JWKSURI:                  oidc.JWKSURI,
				EnrollmentAudience:       oidc.KeyEnrollmentAudience,
				PublicKeyClaimExpression: oidc.PublicKeyClaimExpression,
				RegistrySubjectMapping:   oidc.RegistrySubjectMapping,
			})
		}
		return entries, nil
	}
}

// callerFromContext names the authenticated subject of a request for
// audit fields: its email when the token carries one, else its subject.
func callerFromContext(ctx context.Context) string {
	ac := application.AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return ""
	}
	if emails := ac.Subject.Extra["email"]; len(emails) > 0 && emails[0] != "" {
		return emails[0]
	}
	return string(ac.Subject.Subject)
}

// registerAddonHTTPRoutes mounts every add-on's HTTP routes on mux.
// http.ServeMux panics on a conflicting pattern; that is turned into an
// error so that Start fails cleanly instead.
func registerAddonHTTPRoutes(mux *http.ServeMux, specs []AddonSpec, authenticate func(http.Handler) http.Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("register add-on HTTP routes: %v", r)
		}
	}()
	for _, spec := range specs {
		if spec.HTTPRoutes == nil {
			continue
		}
		spec.HTTPRoutes(
			func(pattern string, handler http.Handler) { mux.Handle(pattern, authenticate(handler)) },
			func(pattern string, handler http.Handler) { mux.Handle(pattern, handler) },
		)
	}
	return nil
}
