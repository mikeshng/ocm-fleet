package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestEnroll_ByHand_RegistersTargetCredentialRecordAndIndexer(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", 12*time.Hour)

	rec, err := h.svc.Enroll(ctx, EnrollRequest{
		ClusterID: "ext", APIServer: api + "/", CACert: ca, Token: token,
		Labels: map[string]string{"env": "dev"}, Profile: AccessProfileWorkloads, Actor: "ops@example.test",
	})
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}

	target, found := h.target("ext")
	if !found {
		t.Fatal("target k8s-ext was not registered")
	}
	if target.Type() != TargetType || target.State() != domain.TargetStateReady || target.Name() != "ext" {
		t.Errorf("target = type %q state %q name %q", target.Type(), target.State(), target.Name())
	}
	if target.Labels()["env"] != "dev" {
		t.Errorf("target labels = %v, want env=dev", target.Labels())
	}
	props := target.Properties()
	want := map[string]string{
		PropAPIServer:              api,
		PropCACert:                 string(ca),
		PropServiceAccountTokenRef: "targets/k8s-ext/sa-token",
		PropClusterResourceName:    "clusters/ext",
		PropOrigin:                 OriginEnrolled,
		PropAccessForm:             AccessFormDirect,
		PropDeliveryCredential:     DeliveryCredentialSigned,
		PropServiceAccount:         AccessNamespace + "/" + AccessServiceAccount,
		PropEnrolledBy:             "ops@example.test",
		PropAccessProfile:          AccessProfileWorkloads,
	}
	for k, v := range want {
		if props[k] != v {
			t.Errorf("property %s = %q, want %q", k, props[k], v)
		}
	}
	if props[propTrustBundle] == "" || !strings.Contains(props[propTrustBundle], "https://issuer.test") {
		t.Errorf("trust_bundle = %q, want the configured issuer", props[propTrustBundle])
	}
	if _, ok := props[PropDeliveryGrantBy]; ok {
		t.Error("delivery grant recorded although none was given")
	}
	for k, v := range props {
		if strings.Contains(v, token) {
			t.Errorf("property %s holds the credential", k)
		}
	}
	if got, ok := h.vaultToken("ext"); !ok || got != token {
		t.Errorf("vault credential = %q (found %v), want the presented token", got, ok)
	}
	if cred, ok := h.indexing.credentialOf("k8s-ext"); !ok || cred != token {
		t.Errorf("indexer credential = %q (running %v), want the presented token", cred, ok)
	}

	obs := rec.Observation
	if obs.State != ClusterStateEnrolled || obs.Target != "k8s-ext" || obs.KubernetesVersion != "v1.37.0" {
		t.Errorf("observation = %+v", obs)
	}
	if obs.Rights == nil || !obs.Rights.Workloads || obs.Rights.ClusterAdmin {
		t.Errorf("rights = %+v, want workloads without cluster-admin", obs.Rights)
	}
	if obs.CredentialExpiresAt == nil || !obs.CredentialExpiresAt.Equal(h.clock.Now().Add(12*time.Hour)) {
		t.Errorf("credentialExpiresAt = %v", obs.CredentialExpiresAt)
	}
	for _, term := range []string{"ext", "dev", "env=dev"} {
		if !contains(obs.SearchTerms, term) {
			t.Errorf("searchTerms %v lack %q", obs.SearchTerms, term)
		}
	}
	raw, _ := json.Marshal(rec)
	if strings.Contains(string(raw), token) {
		t.Error("the cluster record holds the credential")
	}

	stored, err := h.svc.GetCluster(ctx, "ext")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if stored.Observation.State != ClusterStateEnrolled || stored.Labels["env"] != "dev" {
		t.Errorf("stored record = %+v", stored)
	}
	conds := h.conditions("ext")
	for _, ct := range []domain.ConditionType{ClusterConditionReady, ClusterConditionEnrolled, ClusterConditionReachable, ClusterConditionCredentialValid, ClusterConditionIndexing} {
		if conds[ct].Status() != domain.ConditionTrue {
			t.Errorf("condition %s = %q, want True", ct, conds[ct].Status())
		}
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestEnroll_GrantIsRecordedWithWhoAndWhen(t *testing.T) {
	h := newEnrollHarness(t)
	api, ca, token := h.addCluster("ext", time.Hour)
	if _, err := h.svc.Enroll(context.Background(), EnrollRequest{
		ClusterID: "ext", APIServer: api, CACert: ca, Token: token, GrantPlatformDelivery: true, Actor: "ops",
	}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	target, _ := h.target("ext")
	props := target.Properties()
	if props[PropDeliveryCredential] != DeliveryCredentialPlatform || props[PropDeliveryGrantBy] != "ops" ||
		props[PropDeliveryGrantAt] != h.clock.Now().Format(time.RFC3339) {
		t.Errorf("grant properties = %q by %q at %q", props[PropDeliveryCredential], props[PropDeliveryGrantBy], props[PropDeliveryGrantAt])
	}
}

func TestEnroll_FalseEnrollmentCreatesNothing(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(h *enrollHarness, req *EnrollRequest)
		kind   error
		reason string
	}{
		{"credential the cluster does not know", func(_ *enrollHarness, r *EnrollRequest) { r.Token = "not-issued-by-the-cluster" },
			ErrEnrollmentUnverified, "credential-rejected"},
		{"another cluster's CA", func(h *enrollHarness, r *EnrollRequest) { r.CACert = testCA(h.t, "other") },
			ErrEnrollmentUnverified, "cluster-unreachable"},
		{"cluster not reachable", func(_ *enrollHarness, r *EnrollRequest) { r.APIServer = "https://nowhere:6443" },
			ErrEnrollmentUnverified, "cluster-unreachable"},
		{"not the stated service account", func(_ *enrollHarness, r *EnrollRequest) {
			r.ServiceAccount = ServiceAccountRef{Namespace: "kube-system", Name: "other"}
		}, ErrEnrollmentUnverified, "unexpected-identity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newEnrollHarness(t)
			api, ca, token := h.addCluster("ext", time.Hour)
			req := EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token, Actor: "ops"}
			tc.mutate(h, &req)
			_, err := h.svc.Enroll(context.Background(), req)
			enrollErr(t, err, tc.kind, tc.reason)
			h.assertNothingRecorded("ext")
		})
	}
}

func (h *enrollHarness) assertNothingRecorded(clusterID string) {
	h.t.Helper()
	if _, found := h.target(clusterID); found {
		h.t.Error("a target was registered")
	}
	if _, ok := h.vaultToken(clusterID); ok {
		h.t.Error("a credential was stored")
	}
	if _, err := h.svc.GetCluster(context.Background(), clusterID); !errors.Is(err, domain.ErrNotFound) {
		h.t.Errorf("a cluster record exists (err %v)", err)
	}
	if len(h.indexing.ensures) != 0 {
		h.t.Error("an indexer was started")
	}
}

func TestEnroll_RejectsUserCredential(t *testing.T) {
	h := newEnrollHarness(t)
	api, ca, _ := h.addCluster("ext", time.Hour)
	token := makeToken("admin", h.clock.Now(), h.clock.Now().Add(time.Hour))
	h.cluster(api).tokens[token] = ClusterProbeResult{Username: "kubernetes-admin"}
	_, err := h.svc.Enroll(context.Background(), EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token})
	enrollErr(t, err, ErrEnrollmentUnverified, "not-a-service-account")
	h.assertNothingRecorded("ext")
}

func TestEnroll_InvalidInput(t *testing.T) {
	h := newEnrollHarness(t)
	api, ca, token := h.addCluster("ext", time.Hour)
	base := EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}
	cases := map[string]func(r *EnrollRequest){
		"empty id":         func(r *EnrollRequest) { r.ClusterID = "" },
		"uppercase id":     func(r *EnrollRequest) { r.ClusterID = "Ext" },
		"id with slash":    func(r *EnrollRequest) { r.ClusterID = "a/b" },
		"http address":     func(r *EnrollRequest) { r.APIServer = "http://ext:6443" },
		"address and path": func(r *EnrollRequest) { r.APIServer = api + "/api" },
		"no CA":            func(r *EnrollRequest) { r.CACert = []byte("not a certificate") },
		"no credential":    func(r *EnrollRequest) { r.Token = "" },
		"unknown profile":  func(r *EnrollRequest) { r.Profile = "everything" },
		"bad label":        func(r *EnrollRequest) { r.Labels = map[string]string{"": "x"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := base
			mutate(&req)
			if _, err := h.svc.Enroll(context.Background(), req); !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	h.assertNothingRecorded("ext")
}

func TestEnroll_OneClusterOneTarget(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("spoke", time.Hour)

	// A cluster the resource manager created: a kubernetes target
	// without the enrolled origin, with this cluster's address and CA.
	created := domain.NewTargetInfo("k8s-spoke", TargetType, "spoke", "", nil, map[string]string{
		PropAPIServer: api, PropCACert: string(ca), PropClusterResourceName: "clusters/spoke",
	}, []domain.ManifestType{ManifestManifestType})
	if err := h.svc.putTarget(ctx, created, h.clock.Now()); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	t.Run("its id cannot be taken", func(t *testing.T) {
		_, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "spoke", APIServer: api, CACert: ca, Token: token})
		ee := enrollErr(t, err, ErrEnrollmentConflict, "cluster-id-in-use")
		if ee.Details["target"] != "k8s-spoke" {
			t.Errorf("details = %v", ee.Details)
		}
	})
	t.Run("it cannot be enrolled under another id", func(t *testing.T) {
		_, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "hub1.spoke", APIServer: api, CACert: ca, Token: token})
		ee := enrollErr(t, err, ErrEnrollmentConflict, "cluster-already-known")
		if ee.Details["target"] != "k8s-spoke" || ee.Details["cluster"] != "spoke" {
			t.Errorf("details = %v", ee.Details)
		}
		if _, found := h.target("hub1.spoke"); found {
			t.Error("a second target was registered for the same cluster")
		}
	})
	t.Run("the same address with another CA is another cluster", func(t *testing.T) {
		otherCA := testCA(t, "rebuilt")
		h.cluster(api).ca = otherCA
		if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "rebuilt", APIServer: api, CACert: otherCA, Token: token}); err != nil {
			t.Fatalf("Enroll: %v", err)
		}
	})
}

func TestBundle_PresentEnrollsWithTheBundlesChoices(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext2", time.Hour)

	bundle, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{
		Scope:                 BundleScope{ClusterID: "ext2", APIServer: api},
		Profile:               AccessProfileReadOnly,
		GrantPlatformDelivery: true,
		Labels:                map[string]string{"team": "a"},
		TokenLifetime:         time.Hour,
		Actor:                 "ops",
	})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	if bundle.Route != BundleRouteManifest || bundle.ExpiresAt == nil || !bundle.ExpiresAt.Equal(h.clock.Now().Add(24*time.Hour)) {
		t.Errorf("bundle = %+v", bundle)
	}
	if !strings.HasPrefix(bearer, bundle.ID+".") {
		t.Fatalf("bearer %q does not start with the bundle id", bearer)
	}
	secret := strings.TrimPrefix(bearer, bundle.ID+".")
	stored, err := h.vault.Get(ctx, bundleSecretRef(bundle.ID))
	if err != nil {
		t.Fatalf("vault get: %v", err)
	}
	if strings.Contains(string(stored), secret) {
		t.Error("the vault holds the bundle secret itself, not a hash of it")
	}
	raw, _ := json.Marshal(bundle)
	if strings.Contains(string(raw), secret) {
		t.Error("the bundle record holds the secret")
	}

	res, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: token})
	if err != nil {
		t.Fatalf("Present: %v", err)
	}
	if res.RenewAfter != 20*time.Minute {
		t.Errorf("RenewAfter = %s, want a third of the token lifetime", res.RenewAfter)
	}
	target, found := h.target("ext2")
	if !found {
		t.Fatal("target was not registered")
	}
	props := target.Properties()
	if props[PropDeliveryCredential] != DeliveryCredentialPlatform || props[PropDeliveryGrantBy] != "ops" {
		t.Errorf("delivery rule = %q granted by %q", props[PropDeliveryCredential], props[PropDeliveryGrantBy])
	}
	if props[PropEnrollmentBundle] != bundle.ID || props[PropEnrolledBy] != "bundle:"+bundle.ID || props[PropAccessProfile] != AccessProfileReadOnly {
		t.Errorf("bundle properties = %v", props)
	}
	if target.Labels()["team"] != "a" {
		t.Errorf("labels = %v", target.Labels())
	}
	used, err := h.svc.GetBundle(ctx, bundle.ID)
	if err != nil {
		t.Fatalf("GetBundle: %v", err)
	}
	if used.Uses != 1 || used.LastUsedAt == nil {
		t.Errorf("bundle use not recorded: %+v", used)
	}
}

func TestBundle_AuthenticationFailuresLookAlike(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext2", time.Hour)
	bundle, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{ClusterID: "ext2", APIServer: api}, Actor: "ops"})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	req := PresentRequest{CACert: ca, Token: token}

	message := ""
	for name, b := range map[string]string{
		"wrong secret":   bundle.ID + ".wrong",
		"unknown bundle": "eb-000000000000.secret",
		"malformed":      "garbage",
		"empty":          "",
	} {
		_, err := h.svc.Present(ctx, b, req)
		if !errors.Is(err, ErrEnrollmentUnauthenticated) {
			t.Fatalf("%s: error = %v, want unauthenticated", name, err)
		}
		if message == "" {
			message = err.Error()
		} else if err.Error() != message {
			t.Errorf("%s: message %q differs from %q; failures must not reveal which bundles exist", name, err.Error(), message)
		}
	}
	h.assertNothingRecorded("ext2")

	t.Run("expired", func(t *testing.T) {
		h.clock.Advance(25 * time.Hour)
		_, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: h.mintToken(api, "later", time.Hour)})
		enrollErr(t, err, ErrEnrollmentUnauthenticated, "bundle-expired")
	})
	t.Run("revoked", func(t *testing.T) {
		b2, bearer2, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{ClusterID: "ext2", APIServer: api}, Actor: "ops"})
		if err != nil {
			t.Fatalf("CreateBundle: %v", err)
		}
		if err := h.svc.RevokeBundle(ctx, b2.ID, "ops"); err != nil {
			t.Fatalf("RevokeBundle: %v", err)
		}
		_, err = h.svc.Present(ctx, bearer2, PresentRequest{CACert: ca, Token: h.mintToken(api, "after-revoke", time.Hour)})
		if !errors.Is(err, ErrEnrollmentUnauthenticated) {
			t.Fatalf("error = %v, want unauthenticated", err)
		}
		if _, err := h.svc.GetBundle(ctx, b2.ID); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("revoked bundle still readable: %v", err)
		}
	})
	h.assertNothingRecorded("ext2")
}

func TestBundle_PresentRequiresTheAccessAccount(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, _ := h.addCluster("ext2", time.Hour)
	_, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{ClusterID: "ext2", APIServer: api}, Actor: "ops"})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	other := makeToken("default-sa", h.clock.Now(), h.clock.Now().Add(time.Hour))
	h.cluster(api).tokens[other] = ClusterProbeResult{
		Username: "system:serviceaccount:default:default", ServiceAccount: ServiceAccountRef{Namespace: "default", Name: "default"},
	}
	_, err = h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: other})
	enrollErr(t, err, ErrEnrollmentUnverified, "unexpected-identity")
	h.assertNothingRecorded("ext2")
}

func TestBundle_CreateValidation(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, _, _ := h.addCluster("ext2", time.Hour)
	ok := CreateBundleRequest{Scope: BundleScope{ClusterID: "ext2", APIServer: api}}
	cases := map[string]func(r *CreateBundleRequest){
		"no cluster":          func(r *CreateBundleRequest) { r.Scope = BundleScope{} },
		"no address":          func(r *CreateBundleRequest) { r.Scope.APIServer = "" },
		"both scopes":         func(r *CreateBundleRequest) { r.Scope.Verifier = "x"; r.Scope.VerifierScope = "y" },
		"unknown verifier":    func(r *CreateBundleRequest) { r.Scope = BundleScope{Verifier: "nobody", VerifierScope: "s"} },
		"unknown profile":     func(r *CreateBundleRequest) { r.Profile = "everything" },
		"custom without role": func(r *CreateBundleRequest) { r.Profile = AccessProfileCustom },
		"role without custom": func(r *CreateBundleRequest) { r.Profile = AccessProfileReadOnly; r.ClusterRole = "x" },
		"short token":         func(r *CreateBundleRequest) { r.TokenLifetime = time.Minute },
		"negative ttl":        func(r *CreateBundleRequest) { r.TTL = -time.Second },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := ok
			mutate(&req)
			if _, _, err := h.svc.CreateBundle(ctx, req); !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	t.Run("a cluster role alone means the custom profile", func(t *testing.T) {
		req := ok
		req.ClusterRole = "my-role"
		b, _, err := h.svc.CreateBundle(ctx, req)
		if err != nil {
			t.Fatalf("CreateBundle: %v", err)
		}
		if b.Profile != AccessProfileCustom || b.ClusterRole != "my-role" {
			t.Errorf("bundle = %+v", b)
		}
	})
}

// fakeVerifier vouches for the clusters in its map.
type fakeVerifier struct {
	clusters map[string]VerifiedCluster
}

func (v *fakeVerifier) VerifyPresentation(_ context.Context, scope, name string, presentedCA []byte) (VerifiedCluster, error) {
	c, ok := v.clusters[scope+"/"+name]
	if !ok {
		return VerifiedCluster{}, enrollmentErr(ErrEnrollmentRefused, "cluster-unknown", "cluster %q is not known in %q", name, scope)
	}
	if !SameCA(presentedCA, c.CACert) {
		return VerifiedCluster{}, enrollmentErr(ErrEnrollmentRefused, "ca-mismatch", "the presented CA is not the one on record for %q", name)
	}
	return c, nil
}

func TestPresent_VerifierScopedBundle(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	h.svc.RegisterVerifier("registry", &fakeVerifier{clusters: map[string]VerifiedCluster{
		"hub1/ext": {ClusterID: "hub1.ext", APIServer: api, CACert: ca, Labels: map[string]string{"region": "eu", "team": "from-registry"}},
	}})
	bundle, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{
		Scope: BundleScope{Verifier: "registry", VerifierScope: "hub1"}, Labels: map[string]string{"team": "a"}, Actor: "ops",
	})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	if bundle.Route != BundleRouteAddon || bundle.ExpiresAt != nil {
		t.Errorf("bundle = %+v, want the addon route without expiry", bundle)
	}

	t.Run("a cluster the verifier does not know is refused", func(t *testing.T) {
		_, err := h.svc.Present(ctx, bearer, PresentRequest{ClusterName: "stranger", CACert: ca, Token: token})
		enrollErr(t, err, ErrEnrollmentRefused, "cluster-unknown")
	})
	t.Run("a CA the verifier does not have on record is refused", func(t *testing.T) {
		_, err := h.svc.Present(ctx, bearer, PresentRequest{ClusterName: "ext", CACert: testCA(t, "impostor"), Token: token})
		enrollErr(t, err, ErrEnrollmentRefused, "ca-mismatch")
	})
	t.Run("a cluster name is required", func(t *testing.T) {
		if _, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: token}); !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
	h.assertNothingRecorded("hub1.ext")

	res, err := h.svc.Present(ctx, bearer, PresentRequest{ClusterName: "ext", CACert: ca, Token: token})
	if err != nil {
		t.Fatalf("Present: %v", err)
	}
	if res.Cluster.ID != "hub1.ext" {
		t.Errorf("cluster id = %q", res.Cluster.ID)
	}
	target, found := h.target("hub1.ext")
	if !found {
		t.Fatal("target was not registered")
	}
	// Both label sources are present; the enrollment's own choice wins.
	if got := target.Labels(); got["region"] != "eu" || got["team"] != "a" {
		t.Errorf("labels = %v", got)
	}
	src := res.Cluster.Observation.Source
	if src == nil || src.Verifier != "registry" || src.Scope != "hub1" || src.Name != "ext" {
		t.Errorf("source = %+v", src)
	}
	for _, term := range []string{"hub1.ext", "hub1", "ext", "eu"} {
		if !contains(res.Cluster.Observation.SearchTerms, term) {
			t.Errorf("searchTerms %v lack %q", res.Cluster.Observation.SearchTerms, term)
		}
	}
}

func TestPresent_AgainRefreshesTheCredentialAndKeepsChoices(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext2", time.Hour)
	_, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{
		Scope: BundleScope{ClusterID: "ext2", APIServer: api}, GrantPlatformDelivery: true, Labels: map[string]string{"team": "a"}, Actor: "ops",
	})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	if _, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: token}); err != nil {
		t.Fatalf("Present: %v", err)
	}
	first, _ := h.target("ext2")
	if _, err := h.svc.SetExternalLabels(ctx, "ext2", map[string]string{"region": "eu"}); err != nil {
		t.Fatalf("SetExternalLabels: %v", err)
	}

	h.clock.Advance(20 * time.Minute)
	second := h.mintToken(api, "second", time.Hour)
	if _, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: second}); err != nil {
		t.Fatalf("Present again: %v", err)
	}
	if got, _ := h.vaultToken("ext2"); got != second {
		t.Error("the stored credential was not replaced")
	}
	if cred, _ := h.indexing.credentialOf("k8s-ext2"); cred != second {
		t.Error("the indexer was not restarted with the new credential")
	}
	target, _ := h.target("ext2")
	props := target.Properties()
	if props[PropEnrolledAt] != first.Properties()[PropEnrolledAt] || props[PropDeliveryGrantAt] != first.Properties()[PropDeliveryGrantAt] {
		t.Error("a credential refresh rewrote the enrollment or grant time")
	}
	if got := target.Labels(); got["team"] != "a" || got["region"] != "eu" {
		t.Errorf("labels after refresh = %v", got)
	}

	t.Run("a different bundle applies its own choices", func(t *testing.T) {
		newer, bearer2, err := h.svc.CreateBundle(ctx, CreateBundleRequest{
			Scope: BundleScope{ClusterID: "ext2", APIServer: api}, Labels: map[string]string{"team": "b"}, Actor: "ops2",
		})
		if err != nil {
			t.Fatalf("CreateBundle: %v", err)
		}
		if _, err := h.svc.Present(ctx, bearer2, PresentRequest{CACert: ca, Token: h.mintToken(api, "third", time.Hour)}); err != nil {
			t.Fatalf("Present: %v", err)
		}
		target, _ := h.target("ext2")
		props := target.Properties()
		if props[PropDeliveryCredential] != DeliveryCredentialSigned || props[PropDeliveryGrantBy] != "" || props[PropEnrollmentBundle] != newer.ID {
			t.Errorf("properties = %v", props)
		}
		if got := target.Labels(); got["team"] != "b" || got["region"] != "eu" {
			t.Errorf("labels = %v", got)
		}
	})
}

func TestWithdraw_RemovesEverythingButTheRecord(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	_, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{ClusterID: "ext", APIServer: api}, Actor: "ops"})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	if _, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: token}); err != nil {
		t.Fatalf("Present: %v", err)
	}
	h.seedObjects("ext", 3)
	h.seedObjects("ext-neighbour", 2)

	h.clock.Advance(time.Minute)
	rec, err := h.svc.Withdraw(ctx, "ext", WithdrawRequest{Actor: "ops", Reason: "moving away"})
	if err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if rec.Observation.State != ClusterStateWithdrawn || rec.Observation.WithdrawnBy != "ops" ||
		rec.Observation.WithdrawnReason != "moving away" || rec.Observation.Target != "" {
		t.Errorf("observation = %+v", rec.Observation)
	}
	if _, found := h.target("ext"); found {
		t.Error("the target still exists")
	}
	if _, ok := h.vaultToken("ext"); ok {
		t.Error("the credential is still in the vault")
	}
	if h.hasObjects("ext") {
		t.Error("the cluster's indexed objects were not removed")
	}
	if !h.hasObjects("ext-neighbour") {
		t.Error("another cluster's indexed objects were removed")
	}
	if _, running := h.indexing.credentialOf("k8s-ext"); running {
		t.Error("the indexer is still running")
	}
	conds := h.conditions("ext")
	if conds[ClusterConditionEnrolled].Status() != domain.ConditionFalse || conds[ClusterConditionReady].Status() != domain.ConditionFalse {
		t.Errorf("conditions after withdrawal: enrolled %q ready %q", conds[ClusterConditionEnrolled].Status(), conds[ClusterConditionReady].Status())
	}

	t.Run("withdrawal is durable against the bundle that enrolled it", func(t *testing.T) {
		_, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: h.mintToken(api, "again", time.Hour)})
		enrollErr(t, err, ErrEnrollmentRefused, "cluster-withdrawn")
		if _, found := h.target("ext"); found {
			t.Error("the cluster was enrolled again")
		}
	})
	t.Run("a second withdrawal finds nothing enrolled", func(t *testing.T) {
		if _, err := h.svc.Withdraw(ctx, "ext", WithdrawRequest{Actor: "ops"}); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("error = %v, want ErrNotFound", err)
		}
	})
	t.Run("a bundle created after the withdrawal enrolls it again", func(t *testing.T) {
		h.clock.Advance(time.Minute)
		_, newer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{ClusterID: "ext", APIServer: api}, Actor: "ops"})
		if err != nil {
			t.Fatalf("CreateBundle: %v", err)
		}
		res, err := h.svc.Present(ctx, newer, PresentRequest{CACert: ca, Token: h.mintToken(api, "newer", time.Hour)})
		if err != nil {
			t.Fatalf("Present: %v", err)
		}
		if res.Cluster.Observation.State != ClusterStateEnrolled || res.Cluster.Observation.WithdrawnAt != nil {
			t.Errorf("observation = %+v", res.Cluster.Observation)
		}
	})
}

func TestForget_AllowsAnOlderBundleAgain(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	_, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{ClusterID: "ext", APIServer: api}, Actor: "ops"})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	if _, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: token}); err != nil {
		t.Fatalf("Present: %v", err)
	}
	if err := h.svc.Forget(ctx, "ext"); err == nil {
		t.Fatal("an enrolled cluster was forgotten")
	} else {
		enrollErr(t, err, ErrEnrollmentConflict, "still-enrolled")
	}
	h.clock.Advance(time.Minute)
	if _, err := h.svc.Withdraw(ctx, "ext", WithdrawRequest{Actor: "ops"}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if err := h.svc.Forget(ctx, "ext"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, err := h.svc.GetCluster(ctx, "ext"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("record after Forget: %v", err)
	}
	if err := h.svc.Forget(ctx, "ext"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("second Forget: %v, want ErrNotFound", err)
	}
	if _, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: h.mintToken(api, "after-forget", time.Hour)}); err != nil {
		t.Fatalf("Present after Forget: %v", err)
	}
}

func TestWithdraw_RefusedWhileDeploymentsUseTheTarget(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	h.seedDeployment("named", domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"k8s-ext"}})
	h.seedDeployment("selected", domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll}, "k8s-ext")
	h.seedDeployment("elsewhere", domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"k8s-other"}}, "k8s-other")

	_, err := h.svc.Withdraw(ctx, "ext", WithdrawRequest{Actor: "ops"})
	ee := enrollErr(t, err, ErrEnrollmentConflict, "deployments-resolve-to-target")
	got, _ := ee.Details["deployments"].([]string)
	if len(got) != 2 || got[0] != "deployments/named" || got[1] != "deployments/selected" {
		t.Errorf("deployments in details = %v", ee.Details["deployments"])
	}
	if _, found := h.target("ext"); !found {
		t.Error("the target was removed although the withdrawal was refused")
	}
	if _, ok := h.vaultToken("ext"); !ok {
		t.Error("the credential was removed although the withdrawal was refused")
	}

	// Asked to leave the deployments, the withdrawal proceeds and the
	// record names what it left.
	rec, err := h.svc.Withdraw(ctx, "ext", WithdrawRequest{Actor: "ops", Reason: "the cluster is gone", LeaveDeployments: true})
	if err != nil {
		t.Fatalf("Withdraw leaving deployments: %v", err)
	}
	if _, found := h.target("ext"); found {
		t.Error("the target is still there after the withdrawal")
	}
	reason := rec.Observation.WithdrawnReason
	if rec.Observation.State != ClusterStateWithdrawn || !strings.HasPrefix(reason, "the cluster is gone") ||
		!strings.Contains(reason, "deployments/named, deployments/selected") || strings.Contains(reason, "elsewhere") {
		t.Errorf("record after leaving deployments: state %q, reason %q", rec.Observation.State, reason)
	}
}

func TestWithdraw_OnlyEnrolledClusters(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	created := domain.NewTargetInfo("k8s-spoke", TargetType, "spoke", "", nil, map[string]string{
		PropAPIServer: "https://spoke:6443", PropClusterResourceName: "clusters/spoke",
	}, nil)
	if err := h.svc.putTarget(ctx, created, h.clock.Now()); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	_, err := h.svc.Withdraw(ctx, "spoke", WithdrawRequest{Actor: "ops"})
	enrollErr(t, err, ErrEnrollmentConflict, "not-enrolled")
	if _, found := h.target("spoke"); !found {
		t.Error("a cluster the resource manager created was removed by a withdrawal")
	}
}

func TestRenewCredential_ReplacesTheStoredToken(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	h.clock.Advance(10 * time.Minute)
	rec, err := h.svc.RenewCredential(ctx, "ext")
	if err != nil {
		t.Fatalf("RenewCredential: %v", err)
	}
	renewed, _ := h.vaultToken("ext")
	if renewed == token || renewed == "" {
		t.Fatal("the stored credential was not replaced")
	}
	if cred, _ := h.indexing.credentialOf("k8s-ext"); cred != renewed {
		t.Error("the indexer does not run with the renewed credential")
	}
	wantExpiry := h.clock.Now().Add(time.Hour)
	if rec.Observation.CredentialExpiresAt == nil || !rec.Observation.CredentialExpiresAt.Equal(wantExpiry) {
		t.Errorf("credentialExpiresAt = %v, want %v (same lifetime as before)", rec.Observation.CredentialExpiresAt, wantExpiry)
	}

	t.Run("refused by the cluster", func(t *testing.T) {
		h.cluster(api).denyRenew = true
		_, err := h.svc.RenewCredential(ctx, "ext")
		enrollErr(t, err, ErrEnrollmentUnverified, "renewal-denied")
		if got, _ := h.vaultToken("ext"); got != renewed {
			t.Error("a refused renewal changed the stored credential")
		}
	})
	t.Run("unknown cluster", func(t *testing.T) {
		if _, err := h.svc.RenewCredential(ctx, "nobody"); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("error = %v, want ErrNotFound", err)
		}
	})
}

func TestSetExternalLabels(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token, Labels: map[string]string{"env": "dev"}}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}

	changed, err := h.svc.SetExternalLabels(ctx, "ext", map[string]string{"region": "eu", "env": "prod"})
	if err != nil || !changed {
		t.Fatalf("SetExternalLabels = %v, %v; want changed", changed, err)
	}
	target, _ := h.target("ext")
	if got := target.Labels(); got["region"] != "eu" || got["env"] != "dev" {
		t.Errorf("labels = %v; the enrollment's own label must win", got)
	}
	rec, _ := h.svc.GetCluster(ctx, "ext")
	if rec.Labels["region"] != "eu" || !contains(rec.Observation.SearchTerms, "region=eu") {
		t.Errorf("record not updated: labels %v terms %v", rec.Labels, rec.Observation.SearchTerms)
	}

	changed, err = h.svc.SetExternalLabels(ctx, "ext", map[string]string{"region": "eu", "env": "prod"})
	if err != nil || changed {
		t.Fatalf("repeat = %v, %v; want unchanged", changed, err)
	}
	changed, err = h.svc.SetExternalLabels(ctx, "ext", nil)
	if err != nil || !changed {
		t.Fatalf("clear = %v, %v; want changed", changed, err)
	}
	target, _ = h.target("ext")
	if got := target.Labels(); len(got) != 1 || got["env"] != "dev" {
		t.Errorf("labels after clearing = %v", got)
	}
	if _, err := h.svc.SetExternalLabels(ctx, "nobody", nil); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown cluster: %v, want ErrNotFound", err)
	}
}

func TestIndexerFailureDoesNotFailEnrollment(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	h.indexing.ensureErr = domain.ErrInvalidArgument // permanent: no retries
	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	conds := h.conditions("ext")
	if conds[ClusterConditionIndexing].Status() != domain.ConditionFalse {
		t.Errorf("Indexing = %q, want False", conds[ClusterConditionIndexing].Status())
	}
	if conds[ClusterConditionReady].Status() != domain.ConditionTrue {
		t.Errorf("Ready = %q; a cluster that is not indexed can still take deployments", conds[ClusterConditionReady].Status())
	}
}

func TestOnChange_ToldWhenATargetComesAndGoes(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	var seen []string
	h.svc.OnChange(func(clusterID string) { seen = append(seen, clusterID) })
	api, ca, token := h.addCluster("ext", time.Hour)

	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if len(seen) != 1 || seen[0] != "ext" {
		t.Fatalf("after enrollment observers saw %v", seen)
	}
	// A credential refresh and a label change leave the target in place.
	if _, err := h.svc.RenewCredential(ctx, "ext"); err != nil {
		t.Fatalf("RenewCredential: %v", err)
	}
	if _, err := h.svc.SetExternalLabels(ctx, "ext", map[string]string{"region": "eu"}); err != nil {
		t.Fatalf("SetExternalLabels: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("observers were told about changes that keep the target: %v", seen)
	}
	if _, err := h.svc.Withdraw(ctx, "ext", WithdrawRequest{Actor: "ops"}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if err := h.svc.Forget(ctx, "ext"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("after withdrawal and forgetting observers saw %v", seen)
	}
	// A refused enrollment tells nobody.
	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: "nope"}); err == nil {
		t.Fatal("Enroll with a wrong credential succeeded")
	}
	if len(seen) != 3 {
		t.Fatalf("observers were told about a refused enrollment: %v", seen)
	}
}

func TestListBundles(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	if got, err := h.svc.ListBundles(ctx); err != nil || len(got) != 0 {
		t.Fatalf("ListBundles on an empty store = %v, %v", got, err)
	}
	api, _, _ := h.addCluster("ext", time.Hour)
	one, _, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{ClusterID: "ext", APIServer: api}, Actor: "ops"})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	h.svc.RegisterVerifier("registry", &fakeVerifier{})
	two, _, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{Verifier: "registry", VerifierScope: "hub1"}, Actor: "ops"})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	got, err := h.svc.ListBundles(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("ListBundles = %v, %v", got, err)
	}
	ids := map[string]BundleScope{}
	for _, b := range got {
		ids[b.ID] = b.Scope
	}
	if ids[one.ID].ClusterID != "ext" || ids[two.ID].VerifierScope != "hub1" {
		t.Errorf("bundles = %v", ids)
	}
	if err := h.svc.RevokeBundle(ctx, one.ID, "ops"); err != nil {
		t.Fatalf("RevokeBundle: %v", err)
	}
	if got, _ := h.svc.ListBundles(ctx); len(got) != 1 || got[0].ID != two.ID {
		t.Errorf("after a revocation ListBundles = %v", got)
	}
}
