package ocm

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// bundleBearer finds the bearer value the rendered add-on carries: it
// is inside the agent's Secret in the template, and nowhere else.
func bundleBearer(t *testing.T, addon EnrollmentAddon) string {
	t.Helper()
	for _, obj := range templateManifests(t, addon) {
		if obj["kind"] != "Secret" {
			continue
		}
		data, _ := obj["stringData"].(map[string]any)
		if token, _ := data["token"].(string); token != "" {
			return token
		}
	}
	t.Fatalf("the add-on template carries no bundle secret")
	return ""
}

func templateManifests(t *testing.T, addon EnrollmentAddon) []map[string]any {
	t.Helper()
	if len(addon.Hub) != 2 || addon.Hub[0]["kind"] != "AddOnTemplate" {
		t.Fatalf("add-on hub objects = %v", addon.Hub)
	}
	raw, found, err := unstructured.NestedSlice(addon.Hub[0], "spec", "agentSpec", "workload", "manifests")
	if err != nil || !found {
		t.Fatalf("template has no manifests: %v", err)
	}
	out := make([]map[string]any, 0, len(raw))
	for _, m := range raw {
		out = append(out, m.(map[string]any))
	}
	return out
}

func TestVerifyPresentation(t *testing.T) {
	h := newHarness(t)
	extCA := testCA(t, "ext")
	h.createCluster(managedCluster("ext",
		withLabels(map[string]string{clusterSetLabel: "prod", "region": "eu"}),
		withClientConfig("https://ext.test:6443/", extCA),
		withClaim("id.k8s.io", "abc-123"),
		withClaim("too-long", string(make([]byte, 300)))))
	h.createCluster(managedCluster("pending", notAccepted(), withClientConfig("https://pending.test:6443", extCA)))
	h.createCluster(managedCluster("no-address"))

	if _, err := h.svc.VerifyPresentation(h.ctx, "hub1", "ext", extCA); !hasReason(err, kubernetesaddon.ErrEnrollmentRefused, "hub-not-connected") {
		t.Fatalf("before the hub is connected: %v", err)
	}
	h.connect("hub1")
	eventually(t, "hub synced", func() bool { return h.svc.hub("hub1").isSynced() })

	refusals := map[string]struct {
		cluster string
		ca      []byte
		kind    error
		reason  string
	}{
		"unknown cluster":   {"nope", extCA, kubernetesaddon.ErrEnrollmentRefused, "cluster-not-on-hub"},
		"not accepted":      {"pending", extCA, kubernetesaddon.ErrEnrollmentRefused, "cluster-not-accepted"},
		"no address on hub": {"no-address", extCA, kubernetesaddon.ErrEnrollmentUnverified, "no-recorded-address"},
		"another CA":        {"ext", testCA(t, "impostor"), kubernetesaddon.ErrEnrollmentUnverified, "ca-mismatch"},
	}
	for name, tc := range refusals {
		t.Run(name, func(t *testing.T) {
			if _, err := h.svc.VerifyPresentation(h.ctx, "hub1", tc.cluster, tc.ca); !hasReason(err, tc.kind, tc.reason) {
				t.Fatalf("VerifyPresentation error = %v, want %s", err, tc.reason)
			}
		})
	}

	got, err := h.svc.VerifyPresentation(h.ctx, "hub1", "ext", extCA)
	if err != nil {
		t.Fatalf("VerifyPresentation: %v", err)
	}
	if got.ClusterID != "hub1.ext" || got.APIServer != "https://ext.test:6443" || !kubernetesaddon.SameCA(got.CACert, extCA) {
		t.Errorf("verified cluster = %+v", got)
	}
	want := map[string]string{
		clusterSetLabel:     "prod",
		"region":            "eu",
		"claim.id.k8s.io":   "abc-123",
		LabelHub:            "hub1",
		LabelManagedCluster: "ext",
	}
	if !sameLabels(got.Labels, want) {
		t.Errorf("labels = %v, want %v (a claim too long for a label is left out)", got.Labels, want)
	}
}

func hasReason(err, kind error, reason string) bool {
	var ee *kubernetesaddon.EnrollmentError
	return errors.Is(err, kind) && errors.As(err, &ee) && ee.Reason == reason
}

func TestEnrollThroughAHub(t *testing.T) {
	h := newHarness(t)
	api, ca, token := h.addRealCluster("ext")
	h.createCluster(managedCluster("ext",
		withLabels(map[string]string{clusterSetLabel: "prod", "region": "eu"}),
		withClientConfig(api, ca),
		withClaim("id.k8s.io", "abc-123")))
	h.connect("hub1")
	eventually(t, "ext recorded", func() bool { _, ok := h.observed("hub1", "ext"); return ok })

	res, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{
		HubID: "hub1", GrantPlatformDelivery: true, Labels: map[string]string{"team": "blue"},
		Endpoint: testEndpoint(), Actor: "ops@example.test",
	})
	if err != nil {
		t.Fatalf("EnrollmentAddon: %v", err)
	}
	if res.Bundle.Scope.Verifier != VerifierName || res.Bundle.Scope.VerifierScope != "hub1" || res.Bundle.ExpiresAt != nil {
		t.Fatalf("bundle = %+v, want one scoped to the hub that does not expire", res.Bundle)
	}
	if res.Bundle.Profile != kubernetesaddon.AccessProfileWorkloads || res.Bundle.Route != kubernetesaddon.BundleRouteAddon {
		t.Errorf("bundle profile and route = %q, %q", res.Bundle.Profile, res.Bundle.Route)
	}
	bearer := bundleBearer(t, res.Addon)

	// What the agent on the cluster does.
	out, err := h.enrollment.Present(h.ctx, bearer, kubernetesaddon.PresentRequest{ClusterName: "ext", CACert: ca, Token: token})
	if err != nil {
		t.Fatalf("Present: %v", err)
	}
	if out.Cluster.ID != "hub1.ext" || out.Cluster.Observation.Target != "k8s-hub1.ext" {
		t.Fatalf("enrolled cluster = %+v", out.Cluster)
	}
	if src := out.Cluster.Observation.Source; src == nil || *src != (kubernetesaddon.ClusterSource{Verifier: VerifierName, Scope: "hub1", Name: "ext"}) {
		t.Errorf("source = %+v", src)
	}
	target, ok := h.target("k8s-hub1.ext")
	if !ok {
		t.Fatalf("no target for the enrolled cluster")
	}
	wantLabels := map[string]string{
		"team": "blue", clusterSetLabel: "prod", "region": "eu", "claim.id.k8s.io": "abc-123",
		LabelHub: "hub1", LabelManagedCluster: "ext",
	}
	if !sameLabels(target.Labels(), wantLabels) {
		t.Errorf("target labels = %v, want %v", target.Labels(), wantLabels)
	}
	if target.Properties()[kubernetesaddon.PropDeliveryCredential] != kubernetesaddon.DeliveryCredentialPlatform {
		t.Errorf("delivery credential rule = %q", target.Properties()[kubernetesaddon.PropDeliveryCredential])
	}

	// The observed record says the cluster is enrolled, and why the two are the same.
	wantLink := EnrollmentLink{State: LinkEnrolled, Cluster: "clusters/hub1.ext", Target: "k8s-hub1.ext", Basis: BasisEnrollment}
	eventually(t, "observed record linked to the enrolled cluster", func() bool {
		rec, ok := h.observed("hub1", "ext")
		return ok && rec.obs.Enrollment == wantLink
	})
	eventually(t, "hub record counts the enrolled cluster", func() bool {
		obs, _, ok := h.hubRecordStored("hub1")
		return ok && obs.EnrolledCount == 1
	})

	// The hub's facts follow the hub.
	h.updateCluster("ext", func(obj *unstructured.Unstructured) {
		obj.SetLabels(map[string]string{clusterSetLabel: "prod", "region": "us", "tier": "gold"})
	})
	eventually(t, "target labels follow the hub", func() bool {
		target, _ := h.target("k8s-hub1.ext")
		l := target.Labels()
		return l["region"] == "us" && l["tier"] == "gold" && l["team"] == "blue" && l[LabelHub] == "hub1"
	})
	h.updateCluster("ext", func(obj *unstructured.Unstructured) {
		obj.SetLabels(map[string]string{clusterSetLabel: "prod", "region": "us"})
	})
	eventually(t, "a label removed on the hub leaves the target", func() bool {
		target, _ := h.target("k8s-hub1.ext")
		_, has := target.Labels()["tier"]
		return !has
	})
	rec, err := h.enrollment.GetCluster(h.ctx, "hub1.ext")
	if err != nil || rec.Labels["region"] != "us" {
		t.Errorf("cluster record labels = %v, %v", rec.Labels, err)
	}

	// Withdrawal shows on the observed record.
	if _, err := h.enrollment.Withdraw(h.ctx, "hub1.ext", kubernetesaddon.WithdrawRequest{Actor: "ops", Reason: "test"}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	eventually(t, "observed record says withdrawn", func() bool {
		rec, ok := h.observed("hub1", "ext")
		return ok && rec.obs.Enrollment == EnrollmentLink{State: LinkWithdrawn, Cluster: "clusters/hub1.ext"}
	})
	// The same add-on cannot take the cluster in again by itself.
	if _, err := h.enrollment.Present(h.ctx, bearer, kubernetesaddon.PresentRequest{ClusterName: "ext", CACert: ca, Token: token}); !hasReason(err, kubernetesaddon.ErrEnrollmentRefused, "cluster-withdrawn") {
		t.Fatalf("Present after withdrawal: %v", err)
	}
	// Forgetting the cluster is the explicit act that allows it.
	if err := h.enrollment.Forget(h.ctx, "hub1.ext"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	eventually(t, "observed record back to observed", func() bool {
		rec, ok := h.observed("hub1", "ext")
		return ok && rec.obs.Enrollment == EnrollmentLink{State: LinkObserved}
	})
	if _, err := h.enrollment.Present(h.ctx, bearer, kubernetesaddon.PresentRequest{ClusterName: "ext", CACert: ca, Token: token}); err != nil {
		t.Fatalf("Present after forgetting: %v", err)
	}
}

func TestPresentationsTheHubDoesNotBack(t *testing.T) {
	h := newHarness(t)
	api, ca, token := h.addRealCluster("ext")
	otherAPI, otherCA, otherToken := h.addRealCluster("other")
	h.createCluster(managedCluster("ext", withClientConfig(api, ca)))
	h.connect("hub1")
	eventually(t, "hub synced", func() bool { return h.svc.hub("hub1").isSynced() })
	res, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1", Endpoint: testEndpoint(), Actor: "ops"})
	if err != nil {
		t.Fatalf("EnrollmentAddon: %v", err)
	}
	bearer := bundleBearer(t, res.Addon)

	// A cluster the hub does not know presents itself under the hub's bundle.
	_, err = h.enrollment.Present(h.ctx, bearer, kubernetesaddon.PresentRequest{ClusterName: "other", CACert: otherCA, Token: otherToken})
	if !hasReason(err, kubernetesaddon.ErrEnrollmentRefused, "cluster-not-on-hub") {
		t.Fatalf("unknown cluster: %v", err)
	}
	// Another cluster claims to be one the hub knows.
	_, err = h.enrollment.Present(h.ctx, bearer, kubernetesaddon.PresentRequest{ClusterName: "ext", CACert: otherCA, Token: otherToken})
	if !hasReason(err, kubernetesaddon.ErrEnrollmentUnverified, "ca-mismatch") {
		t.Fatalf("impostor with its own CA: %v", err)
	}
	// It shows the right CA but its own credential: the real cluster does not recognize it.
	_, err = h.enrollment.Present(h.ctx, bearer, kubernetesaddon.PresentRequest{ClusterName: "ext", CACert: ca, Token: otherToken})
	if !hasReason(err, kubernetesaddon.ErrEnrollmentUnverified, "credential-rejected") {
		t.Fatalf("impostor with the right CA: %v", err)
	}
	for _, id := range []domain.TargetID{"k8s-hub1.ext", "k8s-hub1.other"} {
		if _, ok := h.target(id); ok {
			t.Errorf("a refused presentation created target %s", id)
		}
	}
	_ = otherAPI

	// After the hub is disconnected its bundle is worth nothing.
	if err := h.svc.Disconnect(h.ctx, "hub1", "ops"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	_, err = h.enrollment.Present(h.ctx, bearer, kubernetesaddon.PresentRequest{ClusterName: "ext", CACert: ca, Token: token})
	if !errors.Is(err, kubernetesaddon.ErrEnrollmentUnauthenticated) {
		t.Fatalf("Present with the bundle of a disconnected hub: %v", err)
	}
}

func TestEnrolledClustersOutliveTheirHubConnection(t *testing.T) {
	h := newHarness(t)
	api, ca, token := h.addRealCluster("ext")
	h.createCluster(managedCluster("ext", withLabels(map[string]string{clusterSetLabel: "prod"}), withClientConfig(api, ca)))
	h.connect("hub1")
	eventually(t, "hub synced", func() bool { return h.svc.hub("hub1").isSynced() })
	res, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1", Endpoint: testEndpoint(), Actor: "ops"})
	if err != nil {
		t.Fatalf("EnrollmentAddon: %v", err)
	}
	if _, err := h.enrollment.Present(h.ctx, bundleBearer(t, res.Addon), kubernetesaddon.PresentRequest{ClusterName: "ext", CACert: ca, Token: token}); err != nil {
		t.Fatalf("Present: %v", err)
	}

	if err := h.svc.Disconnect(h.ctx, "hub1", "ops"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	target, ok := h.target("k8s-hub1.ext")
	if !ok || target.State() != domain.TargetStateReady {
		t.Fatalf("the enrolled cluster's target did not survive the hub's disconnect")
	}
	if target.Labels()[clusterSetLabel] != "prod" || target.Labels()[LabelHub] != "hub1" {
		t.Errorf("labels after disconnect = %v, want the last facts kept", target.Labels())
	}
	if rec, err := h.enrollment.GetCluster(h.ctx, "hub1.ext"); err != nil || rec.Observation.State != kubernetesaddon.ClusterStateEnrolled {
		t.Errorf("cluster record after disconnect = %+v, %v", rec.Observation, err)
	}
}

func TestAClusterEnrolledByHandThatAHubKnows(t *testing.T) {
	h := newHarness(t)
	api, ca, token := h.addRealCluster("byhand")
	if _, err := h.enrollment.Enroll(h.ctx, kubernetesaddon.EnrollRequest{
		ClusterID: "byhand", APIServer: api, CACert: ca, Token: token, Labels: map[string]string{"owner": "me"}, Actor: "ops",
	}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	// The hub knows the same cluster under its own name.
	h.createCluster(managedCluster("edge-7", withLabels(map[string]string{clusterSetLabel: "edge"}), withClientConfig(api, ca)))
	h.connect("hub1")

	eventually(t, "linked by address and CA", func() bool {
		rec, ok := h.observed("hub1", "edge-7")
		return ok && rec.obs.Enrollment == EnrollmentLink{State: LinkEnrolled, Cluster: "clusters/byhand", Target: "k8s-byhand", Basis: BasisAddressAndCA}
	})
	eventually(t, "hub facts on the hand-enrolled target", func() bool {
		target, _ := h.target("k8s-byhand")
		l := target.Labels()
		return l["owner"] == "me" && l[clusterSetLabel] == "edge" && l[LabelHub] == "hub1" && l[LabelManagedCluster] == "edge-7"
	})

	// Enrolling it again through the hub is refused: one cluster, one target.
	eventually(t, "hub synced", func() bool { return h.svc.hub("hub1").isSynced() })
	res, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1", Endpoint: testEndpoint(), Actor: "ops"})
	if err != nil {
		t.Fatalf("EnrollmentAddon: %v", err)
	}
	_, err = h.enrollment.Present(h.ctx, bundleBearer(t, res.Addon), kubernetesaddon.PresentRequest{ClusterName: "edge-7", CACert: ca, Token: token})
	if !hasReason(err, kubernetesaddon.ErrEnrollmentConflict, "cluster-already-known") {
		t.Fatalf("second enrollment of one cluster: %v", err)
	}
}

func TestEnrollmentAddonNeedsAConnectedHubAndAnEndpoint(t *testing.T) {
	h := newHarness(t)
	if _, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1", Endpoint: testEndpoint()}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("hub not connected: %v", err)
	}
	h.connect("hub1")
	if _, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1"}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("no endpoint configured: %v", err)
	}
	if _, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1", Profile: "root", Endpoint: testEndpoint()}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("unknown profile: %v", err)
	}
	bundles, err := h.enrollment.ListBundles(h.ctx)
	if err != nil || len(bundles) != 0 {
		t.Fatalf("refused requests left bundles behind: %v, %v", bundles, err)
	}
}
