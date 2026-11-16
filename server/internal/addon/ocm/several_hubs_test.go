package ocm

import (
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// Several hubs connected to one resource manager. The single-hub tests
// say what a hub connection does; these say what holds between two.

const (
	hubTwoAPI   = "https://hub-two.test:6443"
	hubTwoToken = "hub-two-reader-token"
)

// addHubTwo makes a second hub exist and returns it with its CA.
func (h *harness) addHubTwo() (*fakeHub, []byte) {
	h.t.Helper()
	ca := testCA(h.t, "hub-two")
	return h.addHub(hubTwoAPI, ca, hubTwoToken), ca
}

// connectHubTwo connects the second hub under hubID.
func (h *harness) connectHubTwo(hubID string, ca []byte) HubRecord {
	h.t.Helper()
	rec, err := h.connectHub(hubID, hubTwoAPI, ca, hubTwoToken)
	if err != nil {
		h.t.Fatalf("Connect(%s): %v", hubID, err)
	}
	return rec
}

// addonBearer asks for a hub's enrollment add-on and returns the
// bearer value its agent presents.
func (h *harness) addonBearer(hubID string) string {
	h.t.Helper()
	eventually(h.t, "hub "+hubID+" synced", func() bool { return h.svc.hub(hubID).isSynced() })
	res, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: hubID, Endpoint: testEndpoint(), Actor: "ops@example.test"})
	if err != nil {
		h.t.Fatalf("EnrollmentAddon(%s): %v", hubID, err)
	}
	return bundleBearer(h.t, res.Addon)
}

func (h *harness) enrolledCount(hubID string) int {
	h.t.Helper()
	obs, _, ok := h.hubRecordStored(hubID)
	if !ok {
		return -1
	}
	return obs.EnrolledCount
}

func reasonOf(err error) string {
	var ee *kubernetesaddon.EnrollmentError
	if errors.As(err, &ee) {
		return ee.Reason
	}
	return ""
}

// Two hubs, each with its own cluster and each with a cluster named
// prod-1. Nothing of one hub may read as the other's, and what happens
// to one hub leaves the other whole.
func TestTwoHubsWithTheSameClusterNames(t *testing.T) {
	h := newHarness(t)
	hubTwo, hubTwoCA := h.addHubTwo()
	oneAPI, oneCA, oneToken := h.addRealCluster("prod-1-of-hub-one")
	twoAPI, twoCA, twoToken := h.addRealCluster("prod-1-of-hub-two")
	h.createCluster(managedCluster("local-cluster", withClientConfig(hubAPI, h.hubCA)))
	h.createCluster(managedCluster("prod-1", withClientConfig(oneAPI, oneCA)))
	h.createClusterOn(hubTwo, managedCluster("local-cluster", withClientConfig(hubTwoAPI, hubTwoCA)))
	h.createClusterOn(hubTwo, managedCluster("prod-1", withClientConfig(twoAPI, twoCA)))
	h.connect("hub1")
	h.connectHubTwo("hub2", hubTwoCA)

	for _, hub := range []string{"hub1", "hub2"} {
		for _, name := range []string{"local-cluster", "prod-1"} {
			eventually(t, hub+"/"+name+" recorded as a cluster of its hub", func() bool {
				rec, ok := h.observed(hub, name)
				return ok && rec.obs.Hub == hub && rec.obs.Enrollment.State == LinkObserved
			})
		}
	}

	// A name without a hub is a cluster on each of them, and the answer
	// to someone who names it as a target says so.
	explained := h.svc.ExplainUnknownTarget(h.ctx, "k8s-prod-1")
	for _, want := range []string{`hub "hub1"`, `hub "hub2"`, `"k8s-hub1.prod-1"`, `"k8s-hub2.prod-1"`} {
		if !strings.Contains(explained, want) {
			t.Errorf("ExplainUnknownTarget(k8s-prod-1) = %q, want it to name %s", explained, want)
		}
	}
	if got := h.svc.ExplainUnknownTarget(h.ctx, "k8s-hub2.prod-1"); !strings.Contains(got, `hub "hub2"`) || strings.Contains(got, `hub "hub1"`) {
		t.Errorf("ExplainUnknownTarget(k8s-hub2.prod-1) = %q, want hub2's cluster alone", got)
	}

	// Each hub's add-on enrolls that hub's clusters and no other's.
	bearerOne, bearerTwo := h.addonBearer("hub1"), h.addonBearer("hub2")
	if _, err := h.enrollment.Present(h.ctx, bearerOne, kubernetesaddon.PresentRequest{ClusterName: "prod-1", CACert: twoCA, Token: twoToken}); !hasReason(err, kubernetesaddon.ErrEnrollmentUnverified, "ca-mismatch") {
		t.Fatalf("hub two's prod-1 under hub one's add-on: %v", err)
	}
	out, err := h.enrollment.Present(h.ctx, bearerTwo, kubernetesaddon.PresentRequest{ClusterName: "prod-1", CACert: twoCA, Token: twoToken})
	if err != nil {
		t.Fatalf("Present under hub two's add-on: %v", err)
	}
	if out.Cluster.ID != "hub2.prod-1" || out.Cluster.Observation.Target != "k8s-hub2.prod-1" {
		t.Fatalf("enrolled cluster = %+v", out.Cluster)
	}
	target, _ := h.target("k8s-hub2.prod-1")
	want := map[string]string{
		clusterSetLabel: "default", LabelHub: "hub2", LabelManagedCluster: "prod-1", LabelHubClusterSet: "hub2.default",
	}
	if !sameLabels(target.Labels(), want) {
		t.Errorf("target labels = %v, want %v", target.Labels(), want)
	}
	eventually(t, "hub two counts its enrolled cluster", func() bool {
		rec, ok := h.observed("hub2", "prod-1")
		return ok && rec.obs.Enrollment.State == LinkEnrolled && h.enrolledCount("hub2") == 1
	})
	never(t, "hub one's prod-1 reads as enrolled because hub two's is", func() bool {
		rec, _ := h.observed("hub1", "prod-1")
		return rec.obs.Enrollment.State != LinkObserved || h.enrolledCount("hub1") != 0
	})

	// Hub one's prod-1 is another cluster, with another id and target.
	if out, err = h.enrollment.Present(h.ctx, bearerOne, kubernetesaddon.PresentRequest{ClusterName: "prod-1", CACert: oneCA, Token: oneToken}); err != nil || out.Cluster.ID != "hub1.prod-1" {
		t.Fatalf("Present under hub one's add-on: %+v, %v", out.Cluster, err)
	}
	if target, _ := h.target("k8s-hub1.prod-1"); target.Labels()[LabelHubClusterSet] != "hub1.default" || target.Labels()[clusterSetLabel] != "default" {
		t.Errorf("labels of hub one's prod-1 = %v: the set's own label is the same on both hubs, the hub's label tells them apart", target.Labels())
	}

	// Disconnecting one hub leaves the other whole.
	if err := h.svc.Disconnect(h.ctx, "hub1", "ops@example.test"); err != nil {
		t.Fatalf("Disconnect(hub1): %v", err)
	}
	if _, ok := h.observed("hub1", "prod-1"); ok {
		t.Errorf("hub one's observed cluster outlived its connection")
	}
	if rec, ok := h.observed("hub2", "prod-1"); !ok || rec.obs.Enrollment.State != LinkEnrolled {
		t.Errorf("hub two's record after hub one was disconnected = %+v, %v", rec.obs, ok)
	}
	if _, err := h.svc.GetHub(h.ctx, "hub2"); err != nil {
		t.Errorf("GetHub(hub2) after hub one was disconnected: %v", err)
	}
	for _, id := range []domain.TargetID{"k8s-hub1.prod-1", "k8s-hub2.prod-1"} {
		if _, ok := h.target(id); !ok {
			t.Errorf("target %s did not survive the disconnect of hub one", id)
		}
	}
	if _, err := h.enrollment.Present(h.ctx, bearerOne, kubernetesaddon.PresentRequest{ClusterName: "prod-1", CACert: oneCA, Token: oneToken}); !errors.Is(err, kubernetesaddon.ErrEnrollmentUnauthenticated) {
		t.Errorf("Present with the add-on of the disconnected hub: %v", err)
	}
	if _, err := h.enrollment.Present(h.ctx, bearerTwo, kubernetesaddon.PresentRequest{ClusterName: "prod-1", CACert: twoCA, Token: twoToken}); err != nil {
		t.Errorf("Present with the add-on of the hub that stayed: %v", err)
	}
}

// A hub that publishes its clusters' own identifier (the cluster claim
// "id.k8s.io") links a cluster at whatever address it records for it.
func TestAHubThatPublishesClusterIdentifiersLinksAtAnyAddress(t *testing.T) {
	h := newHarness(t)
	api, ca, token := h.addRealCluster("byhand")
	if _, err := h.enrollment.Enroll(h.ctx, kubernetesaddon.EnrollRequest{
		ClusterID: "byhand", APIServer: api, CACert: ca, Token: token, Actor: "ops",
	}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	// The hub knows the same cluster under its own name and at another
	// address: the address says nothing here.
	const byIP = "https://10.0.0.21:6443"
	h.clusterAlsoAt(byIP, api)
	h.createCluster(managedCluster("edge-7", withClientConfig(byIP, ca), withClaim(clusterIDClaim, clusterUIDOf("byhand"))))
	h.connect("hub1")

	wantLink := EnrollmentLink{State: LinkEnrolled, Cluster: "clusters/byhand", Target: "k8s-byhand", Basis: BasisClusterID}
	eventually(t, "linked by the cluster's identifier", func() bool {
		rec, ok := h.observed("hub1", "edge-7")
		return ok && rec.obs.Enrollment == wantLink
	})
	eventually(t, "hub facts on the hand-enrolled target", func() bool {
		target, _ := h.target("k8s-byhand")
		return target.Labels()[LabelHub] == "hub1" && target.Labels()[LabelManagedCluster] == "edge-7"
	})

	// The hub's add-on does not enroll it a second time under the hub's
	// name, although the hub records another address for it.
	_, err := h.enrollment.Present(h.ctx, h.addonBearer("hub1"), kubernetesaddon.PresentRequest{ClusterName: "edge-7", CACert: ca, Token: token})
	if !hasReason(err, kubernetesaddon.ErrEnrollmentConflict, "cluster-already-known") {
		t.Fatalf("second enrollment of one cluster at its other address: %v", err)
	}
	if _, ok := h.target("k8s-hub1.edge-7"); ok {
		t.Fatalf("a second target was made for the same cluster")
	}

	// A second hub that knows the cluster too, with no address on
	// record at all, reads it as enrolled. The facts on the target stay
	// those of the hub that labeled it first.
	hubTwo, hubTwoCA := h.addHubTwo()
	h.createClusterOn(hubTwo, managedCluster("site-b-7", withLabels(map[string]string{clusterSetLabel: "site-b"}),
		withClaim(clusterIDClaim, clusterUIDOf("byhand"))))
	h.connectHubTwo("hub2", hubTwoCA)
	eventually(t, "the second hub's record linked by the cluster's identifier", func() bool {
		rec, ok := h.observed("hub2", "site-b-7")
		return ok && rec.obs.Enrollment == wantLink && h.enrolledCount("hub2") == 1
	})
	never(t, "the second hub took over the facts on the target", func() bool {
		target, _ := h.target("k8s-byhand")
		return target.Labels()[LabelHub] != "hub1" || target.Labels()[clusterSetLabel] != "default"
	})
}

// A name alone links a hub's record to a cluster only while one hub has
// that name without an address. Two hubs cannot both mean the one
// cluster, and the name does not say which of them does.
func TestANameTwoHubsShareLinksNeither(t *testing.T) {
	h := newHarness(t)
	api, ca, token := h.addRealCluster("edge-9")
	if _, err := h.enrollment.Enroll(h.ctx, kubernetesaddon.EnrollRequest{
		ClusterID: "edge-9", APIServer: api, CACert: ca, Token: token, Actor: "ops",
	}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	h.createCluster(managedCluster("edge-9"))
	h.connect("hub1")
	byName := EnrollmentLink{State: LinkEnrolled, Cluster: "clusters/edge-9", Target: "k8s-edge-9", Basis: BasisName}
	eventually(t, "one hub: linked by name", func() bool {
		rec, ok := h.observed("hub1", "edge-9")
		return ok && rec.obs.Enrollment == byName && h.enrolledCount("hub1") == 1
	})

	hubTwo, hubTwoCA := h.addHubTwo()
	h.createClusterOn(hubTwo, managedCluster("edge-9"))
	h.connectHubTwo("hub2", hubTwoCA)
	for hub, other := range map[string]string{"hub1": "hub2", "hub2": "hub1"} {
		eventually(t, hub+"'s edge-9 linked to nothing, and saying why", func() bool {
			rec, ok := h.observed(hub, "edge-9")
			return ok && rec.obs.Enrollment == EnrollmentLink{State: LinkObserved} &&
				strings.Contains(rec.obs.Conflict, `hub "`+other+`"`) && h.enrolledCount(hub) == 0
		})
	}
	if target, _ := h.target("k8s-edge-9"); target.Labels()[LabelHub] != "" {
		t.Errorf("a hub's facts were put on a cluster linked by name: %v", target.Labels())
	}

	// The name is one hub's again.
	if err := hubTwo.client.Resource(managedClusterGVR).Delete(h.ctx, "edge-9", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete hub two's edge-9: %v", err)
	}
	eventually(t, "linked by name again", func() bool {
		rec, ok := h.observed("hub1", "edge-9")
		return ok && rec.obs.Enrollment == byName && rec.obs.Conflict == ""
	})
}

// hubFacts returns what an enrolled cluster's record says about the
// facts its hub supplied: the condition's status and its reason.
func (h *harness) hubFacts(clusterID string) (domain.ConditionStatus, string) {
	h.t.Helper()
	res, err := h.svc.readRecord(h.ctx, kubernetesaddon.ClusterResourceType, kubernetesaddon.ClusterRecordName(clusterID))
	if err != nil {
		h.t.Fatalf("read record of cluster %s: %v", clusterID, err)
	}
	for _, c := range res.Inventory().Conditions() {
		if c.Type() == ClusterConditionHubFacts {
			return c.Status(), c.Reason()
		}
	}
	return domain.ConditionUnknown, ""
}

// An enrolled cluster carries its hub's facts: labels, cluster set,
// claims. They select the cluster for deployments, so the cluster's
// record says whether somebody still keeps them, hub by hub.
func TestAClustersRecordSaysWhetherItsHubFactsAreCurrent(t *testing.T) {
	h := newHarness(t)
	hubTwo, hubTwoCA := h.addHubTwo()
	oneAPI, oneCA, oneToken := h.addRealCluster("prod-1")
	twoAPI, twoCA, twoToken := h.addRealCluster("prod-2")
	h.createCluster(managedCluster("prod-1", withClientConfig(oneAPI, oneCA)))
	h.createClusterOn(hubTwo, managedCluster("prod-2", withClientConfig(twoAPI, twoCA)))
	h.connect("hub1")
	h.connectHubTwo("hub2", hubTwoCA)
	if _, err := h.enrollment.Present(h.ctx, h.addonBearer("hub1"), kubernetesaddon.PresentRequest{ClusterName: "prod-1", CACert: oneCA, Token: oneToken}); err != nil {
		t.Fatalf("Present(prod-1): %v", err)
	}
	if _, err := h.enrollment.Present(h.ctx, h.addonBearer("hub2"), kubernetesaddon.PresentRequest{ClusterName: "prod-2", CACert: twoCA, Token: twoToken}); err != nil {
		t.Fatalf("Present(prod-2): %v", err)
	}
	says := func(clusterID string, status domain.ConditionStatus, reason string) func() bool {
		return func() bool {
			gotStatus, gotReason := h.hubFacts(clusterID)
			return gotStatus == status && gotReason == reason
		}
	}
	eventually(t, "hub one's cluster: facts current", says("hub1.prod-1", domain.ConditionTrue, HubFactsCurrent))
	eventually(t, "hub two's cluster: facts current", says("hub2.prod-2", domain.ConditionTrue, HubFactsCurrent))

	// Hub one stops answering. Its cluster keeps the labels and is
	// still delivered to by them; the record says they are old.
	h.hub.failing.Store(true)
	eventually(t, "hub one's cluster: hub unreachable", says("hub1.prod-1", domain.ConditionFalse, HubFactsHubUnreachable))
	if target, _ := h.target("k8s-hub1.prod-1"); target.Labels()[LabelHub] != "hub1" {
		t.Errorf("the labels of an unreachable hub's cluster were taken away: %v", target.Labels())
	}
	never(t, "hub two's cluster was marked for hub one's outage", func() bool {
		return !says("hub2.prod-2", domain.ConditionTrue, HubFactsCurrent)()
	})
	h.hub.failing.Store(false)
	eventually(t, "hub one's cluster: facts current again", says("hub1.prod-1", domain.ConditionTrue, HubFactsCurrent))

	// The cluster leaves hub two.
	if err := hubTwo.client.Resource(managedClusterGVR).Delete(h.ctx, "prod-2", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete hub two's prod-2: %v", err)
	}
	eventually(t, "hub two's cluster: no longer reported", says("hub2.prod-2", domain.ConditionFalse, HubFactsNotReportedByHub))
	h.createClusterOn(hubTwo, managedCluster("prod-2", withClientConfig(twoAPI, twoCA)))
	eventually(t, "hub two's cluster: reported again", says("hub2.prod-2", domain.ConditionTrue, HubFactsCurrent))

	// Hub one's connection is removed.
	if err := h.svc.Disconnect(h.ctx, "hub1", "ops"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if status, reason := h.hubFacts("hub1.prod-1"); status != domain.ConditionFalse || reason != HubFactsHubDisconnected {
		t.Errorf("after the hub's disconnect: %s %s", status, reason)
	}
	if !says("hub2.prod-2", domain.ConditionTrue, HubFactsCurrent)() {
		t.Errorf("hub two's cluster was marked for hub one's disconnect")
	}

	// Connected again under its id, the hub keeps its clusters' facts
	// again.
	h.connect("hub1")
	eventually(t, "hub one's cluster: facts current after the hub came back", says("hub1.prod-1", domain.ConditionTrue, HubFactsCurrent))
}

// A cluster that carries one hub's facts takes another hub's only by an
// explicit operation, and only from a hub that knows the cluster on
// more than its name.
func TestAdoptClusterMovesHubFactsToAnotherHub(t *testing.T) {
	h := newHarness(t)
	hubTwo, hubTwoCA := h.addHubTwo()
	api, ca, token := h.addRealCluster("prod-1")
	h.createCluster(managedCluster("prod-1", withLabels(map[string]string{clusterSetLabel: "default", "site": "a"}), withClientConfig(api, ca)))
	h.connect("hub1")
	if _, err := h.enrollment.Present(h.ctx, h.addonBearer("hub1"), kubernetesaddon.PresentRequest{ClusterName: "prod-1", CACert: ca, Token: token}); err != nil {
		t.Fatalf("Present: %v", err)
	}

	// The cluster is registered with hub two as well, under another
	// name and with other labels.
	h.createClusterOn(hubTwo, managedCluster("prod-1a", withLabels(map[string]string{clusterSetLabel: "default", "site": "b"}), withClientConfig(api, ca)))
	h.createClusterOn(hubTwo, managedCluster("other"))
	h.connectHubTwo("hub2", hubTwoCA)
	eventually(t, "hub two's record linked", func() bool {
		rec, ok := h.observed("hub2", "prod-1a")
		return ok && rec.obs.Enrollment.State == LinkEnrolled && rec.obs.Enrollment.Basis == BasisAddressAndCA
	})
	never(t, "hub two's facts reached the cluster by themselves", func() bool {
		target, _ := h.target("k8s-hub1.prod-1")
		return target.Labels()["site"] != "a" || target.Labels()[LabelHub] != "hub1"
	})

	refusals := map[string]struct {
		hub, cluster string
		reason       string
	}{
		"a cluster the hub does not know": {"hub2", "hub1.nope", "not-found"},
		"a hub that is not connected":     {"hub9", "hub1.prod-1", "not-found"},
	}
	for name, tc := range refusals {
		if _, err := h.svc.AdoptCluster(h.ctx, tc.hub, tc.cluster, "ops"); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("%s: %v, want %s", name, err, tc.reason)
		}
	}
	otherAPI, otherCA, otherToken := h.addRealCluster("elsewhere")
	if _, err := h.enrollment.Enroll(h.ctx, kubernetesaddon.EnrollRequest{ClusterID: "other", APIServer: otherAPI, CACert: otherCA, Token: otherToken, Actor: "ops"}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	eventually(t, "hub two synced", func() bool { return h.svc.hub("hub2").isSynced() })
	if _, err := h.svc.AdoptCluster(h.ctx, "hub2", "other", "ops"); reasonOf(err) != "weak-link" {
		t.Errorf("a cluster the hub knows by name alone: %v", err)
	}
	if _, err := h.svc.AdoptCluster(h.ctx, "hub1", "other", "ops"); reasonOf(err) != "cluster-not-on-hub" {
		t.Errorf("a cluster the hub has no record of: %v", err)
	}

	name, err := h.svc.AdoptCluster(h.ctx, "hub2", "hub1.prod-1", "ops")
	if err != nil || name != "prod-1a" {
		t.Fatalf("AdoptCluster: %q, %v", name, err)
	}
	target, _ := h.target("k8s-hub1.prod-1")
	if l := target.Labels(); l[LabelHub] != "hub2" || l[LabelManagedCluster] != "prod-1a" || l["site"] != "b" || l[LabelHubClusterSet] != "hub2.default" {
		t.Errorf("labels after adoption = %v", l)
	}
	// Hub two keeps them from now on; hub one leaves them alone.
	obj, err := hubTwo.client.Resource(managedClusterGVR).Get(h.ctx, "prod-1a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get prod-1a: %v", err)
	}
	obj = obj.DeepCopy()
	obj.SetLabels(map[string]string{clusterSetLabel: "default", "site": "c"})
	if _, err := hubTwo.client.Resource(managedClusterGVR).Update(h.ctx, obj, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update prod-1a: %v", err)
	}
	eventually(t, "the cluster follows hub two", func() bool {
		target, _ := h.target("k8s-hub1.prod-1")
		status, reason := h.hubFacts("hub1.prod-1")
		return target.Labels()["site"] == "c" && target.Labels()[LabelHub] == "hub2" && status == domain.ConditionTrue && reason == HubFactsCurrent
	})
	never(t, "hub one took the facts back", func() bool {
		target, _ := h.target("k8s-hub1.prod-1")
		return target.Labels()[LabelHub] != "hub2"
	})
}

// The hub's record says how the hub's enrollment add-on reaches its
// clusters and whether a valid enrollment secret exists for the hub.
// Each hub has its own answer.
func TestEachHubKeepsItsRolloutAndItsSecrets(t *testing.T) {
	h := newHarness(t)
	hubTwo, hubTwoCA := h.addHubTwo()
	h.connect("hub1")
	h.connectHubTwo("hub2", hubTwoCA)
	stored := func(hubID string) (*EnrollmentRollout, int) {
		obs, _, _ := h.hubRecordStored(hubID)
		return obs.EnrollmentRollout, obs.EnrollmentSecrets
	}
	addon := func(hubID string, rollout *EnrollmentRollout) EnrollmentAddonResult {
		t.Helper()
		res, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: hubID, Rollout: rollout, Endpoint: testEndpoint(), Actor: "ops"})
		if err != nil {
			t.Fatalf("EnrollmentAddon(%s): %v", hubID, err)
		}
		return res
	}
	everyCluster := EnrollmentRollout{Mode: RolloutEveryCluster, PlacementNamespace: RolloutNamespace, PlacementName: RolloutPlacementName}

	// Before any add-on: nothing to keep, and no secret.
	if rollout, secrets := stored("hub1"); rollout != nil || secrets != 0 {
		t.Fatalf("a new hub connection: rollout %+v, %d secrets", rollout, secrets)
	}

	addon("hub2", &EnrollmentRollout{Mode: RolloutEveryCluster})
	addon("hub1", nil)
	eventually(t, "each hub's record says its own rollout", func() bool {
		one, oneSecrets := stored("hub1")
		two, twoSecrets := stored("hub2")
		return one != nil && *one == EnrollmentRollout{Mode: RolloutManual} && oneSecrets == 1 &&
			two != nil && *two == everyCluster && twoSecrets == 1
	})

	// A manifest made again for hub two, with nothing said, is again
	// for every cluster; hub one's stays manual.
	if res := addon("hub2", nil); res.Addon.Rollout != everyCluster || res.Addon.Enable != nil {
		t.Errorf("hub two's second add-on: rollout %+v", res.Addon.Rollout)
	}
	if res := addon("hub1", nil); res.Addon.Rollout.Mode != RolloutManual {
		t.Errorf("hub one's second add-on: rollout %+v", res.Addon.Rollout)
	}
	eventually(t, "both secrets of each hub counted", func() bool {
		_, one := stored("hub1")
		_, two := stored("hub2")
		return one == 2 && two == 2
	})

	// A new credential for the same hub keeps what the record says.
	if rec := h.connectHubTwo("hub2", hubTwoCA); rec.Observation.EnrollmentRollout == nil || *rec.Observation.EnrollmentRollout != everyCluster {
		t.Errorf("connecting hub two again lost its rollout: %+v", rec.Observation.EnrollmentRollout)
	}

	// So does a restart.
	if err := h.svc.Close(h.ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.svc = h.newService()
	if rec, err := h.svc.GetHub(h.ctx, "hub2"); err != nil || rec.Observation.EnrollmentRollout == nil || *rec.Observation.EnrollmentRollout != everyCluster {
		t.Errorf("hub two after a restart: %+v, %v", rec.Observation.EnrollmentRollout, err)
	}

	// A disconnect revokes the hub's secrets. Connected again, the hub
	// reads as ready, and its record says that no valid secret exists:
	// the add-on on the hub carries one that is not accepted any more.
	// It also says how many clusters that leaves without repair: the
	// ones that were enrolled through the hub's add-on.
	twoAPI, twoCA, twoToken := h.addRealCluster("prod-2")
	h.createClusterOn(hubTwo, managedCluster("prod-2", withClientConfig(twoAPI, twoCA)))
	if _, err := h.enrollment.Present(h.ctx, h.addonBearer("hub2"), kubernetesaddon.PresentRequest{ClusterName: "prod-2", CACert: twoCA, Token: twoToken}); err != nil {
		t.Fatalf("Present(prod-2): %v", err)
	}
	byHandAPI, byHandCA, byHandToken := h.addRealCluster("by-hand")
	h.createClusterOn(hubTwo, managedCluster("by-hand", withClientConfig(byHandAPI, byHandCA)))
	if _, err := h.enrollment.Enroll(h.ctx, kubernetesaddon.EnrollRequest{ClusterID: "by-hand", APIServer: byHandAPI, CACert: byHandCA, Token: byHandToken, Actor: "ops"}); err != nil {
		t.Fatalf("Enroll(by-hand): %v", err)
	}
	eventually(t, "hub two counts both enrolled clusters, one of them through the hub", func() bool {
		obs, _, _ := h.hubRecordStored("hub2")
		return obs.EnrolledCount == 2 && obs.EnrolledThroughHubCount == 1
	})
	if err := h.svc.Disconnect(h.ctx, "hub2", "ops"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	h.connectHubTwo("hub2", hubTwoCA)
	eventually(t, "hub two ready again, with its enrolled clusters and no secret", func() bool {
		obs, conds, ok := h.hubRecordStored("hub2")
		return ok && conds[string(HubConditionReady)].Status() == domain.ConditionTrue &&
			obs.EnrolledCount == 2 && obs.EnrolledThroughHubCount == 1
	})
	if rollout, secrets := stored("hub2"); rollout != nil || secrets != 0 {
		t.Errorf("hub two after a disconnect and a new connection: rollout %+v, %d secrets, want none", rollout, secrets)
	}
	never(t, "hub one lost a secret to hub two's disconnect", func() bool {
		_, one := stored("hub1")
		return one != 2
	})

	// A secret that someone revokes by hand is no longer counted.
	bundles, err := h.enrollment.ListBundles(h.ctx)
	if err != nil {
		t.Fatalf("ListBundles: %v", err)
	}
	for _, b := range bundles {
		if b.Scope.VerifierScope == "hub1" {
			if err := h.enrollment.RevokeBundle(h.ctx, b.ID, "ops"); err != nil {
				t.Fatalf("RevokeBundle: %v", err)
			}
		}
	}
	eventually(t, "hub one's record says no valid secret is left", func() bool {
		rollout, secrets := stored("hub1")
		return secrets == 0 && rollout != nil
	})
}

// The resource manager starts while one of its hubs is down. What it
// recorded of that hub in an earlier run is the last state seen and
// must say so; the hub must read as not ready, not as starting.
func TestAHubThatIsDownAtStartIsShownStale(t *testing.T) {
	h := newHarness(t)
	hubTwo, hubTwoCA := h.addHubTwo()
	h.createCluster(managedCluster("a"))
	h.createClusterOn(hubTwo, managedCluster("b"))
	h.connect("hub1")
	h.connectHubTwo("hub2", hubTwoCA)
	eventually(t, "both hubs' clusters recorded", func() bool {
		_, a := h.observed("hub1", "a")
		_, b := h.observed("hub2", "b")
		return a && b
	})

	if err := h.svc.Close(h.ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.hub.failing.Store(true)
	h.svc = h.newService()

	eventually(t, "the record of the hub that is down is stale, and the hub not ready", func() bool {
		rec, ok := h.observed("hub1", "a")
		_, conds, hubOK := h.hubRecordStored("hub1")
		ready := conds[string(HubConditionReady)]
		return ok && rec.conds[string(ConditionStale)].Status() == domain.ConditionTrue &&
			hubOK && ready.Status() == domain.ConditionFalse && ready.Reason() == "Unreachable"
	})
	rec, _ := h.observed("hub1", "a")
	if rec.obs.Name != "a" || rec.obs.Available != "True" || rec.conds[conditionAvailable].Status() != domain.ConditionTrue {
		t.Errorf("the stale record lost the last state seen: %+v", rec.obs)
	}
	eventually(t, "the other hub is read as before", func() bool {
		rec, ok := h.observed("hub2", "b")
		_, conds, hubOK := h.hubRecordStored("hub2")
		_, stale := rec.conds[string(ConditionStale)]
		return ok && !stale && hubOK && conds[string(HubConditionReady)].Status() == domain.ConditionTrue
	})

	h.hub.failing.Store(false)
	eventually(t, "record current again and hub ready", func() bool {
		rec, ok := h.observed("hub1", "a")
		_, conds, hubOK := h.hubRecordStored("hub1")
		_, stale := rec.conds[string(ConditionStale)]
		return ok && !stale && hubOK && conds[string(HubConditionReady)].Status() == domain.ConditionTrue
	})
}

func TestAHubIsRecognizedAtAnotherAddress(t *testing.T) {
	h := newHarness(t)
	rec := h.connect("hub1")
	if rec.Observation.ClusterUID == "" {
		t.Fatalf("the hub record carries no identifier of the hub: %+v", rec.Observation)
	}

	// The same hub answers at a second address. The address differs, so
	// only the hub's own identifier can say that it is the same hub.
	const second = "https://10.0.0.7:6443"
	h.alsoAt(second, h.hub)
	_, err := h.connectHub("hub-again", second, h.hubCA, hubToken)
	var ee *kubernetesaddon.EnrollmentError
	if !errors.As(err, &ee) || ee.Reason != "hub-already-connected" || ee.Details["hub"] != "hub1" {
		t.Fatalf("the same hub at another address under a second id: %v", err)
	}
	if !strings.Contains(ee.Message, hubAPI) {
		t.Errorf("the refusal does not say where the hub is connected: %q", ee.Message)
	}
	if _, _, ok := h.hubRecordStored("hub-again"); ok {
		t.Fatalf("the refused connection left a record")
	}
}

func TestAHubThatMovedKeepsItsConnection(t *testing.T) {
	h := newHarness(t)
	h.createCluster(managedCluster("a"))
	first := h.connect("hub1")
	eventually(t, "a recorded", func() bool { _, ok := h.observed("hub1", "a"); return ok })

	// The hub is reachable at a new address. Connecting it under its own
	// id moves the connection: no disconnect, nothing lost.
	const moved = "https://hub-new.test:6443"
	h.alsoAt(moved, h.hub)
	rec, err := h.connectHub("hub1", moved, h.hubCA, hubToken)
	if err != nil {
		t.Fatalf("connect the hub at its new address under its id: %v", err)
	}
	if rec.Observation.APIServer != moved {
		t.Errorf("the record's address = %q, want %q", rec.Observation.APIServer, moved)
	}
	if rec.Observation.ClusterUID != first.Observation.ClusterUID {
		t.Errorf("the hub's identifier changed with its address: %q -> %q", first.Observation.ClusterUID, rec.Observation.ClusterUID)
	}
	if rec.Observation.ConnectedBy != first.Observation.ConnectedBy || !rec.Observation.ConnectedAt.Equal(*first.Observation.ConnectedAt) {
		t.Errorf("moving the hub changed who connected it or when: %+v", rec.Observation)
	}
	conn, err := h.svc.loadConnection(h.ctx, "hub1")
	if err != nil || conn.APIServer != moved {
		t.Fatalf("stored connection = %+v, %v", conn, err)
	}
	if _, ok := h.observed("hub1", "a"); !ok {
		t.Errorf("the hub's observed cluster was lost when the hub moved")
	}
}

func TestAnIDInUseIsNotGivenToAnotherHub(t *testing.T) {
	h := newHarness(t)
	h.connect("hub1")
	_, ca := h.addHubTwo()

	// Another hub: its identifier differs.
	if _, err := h.connectHub("hub1", hubTwoAPI, ca, hubTwoToken); reasonOf(err) != "hub-id-in-use" {
		t.Fatalf("another hub under an id in use: %v", err)
	}
	conn, err := h.svc.loadConnection(h.ctx, "hub1")
	if err != nil || conn.APIServer != hubAPI {
		t.Fatalf("the refused connect changed the stored connection: %+v, %v", conn, err)
	}
}

func TestWithoutAnIdentifierTheAddressDecides(t *testing.T) {
	h := newHarness(t)
	h.hub.withoutIdentifier()
	rec := h.connect("hub1")
	if rec.Observation.ClusterUID != "" {
		t.Fatalf("identifier = %q for an account that may not read it", rec.Observation.ClusterUID)
	}

	// The same address and CA are still recognized as the same hub.
	if _, err := h.connectHub("hub-again", hubAPI, h.hubCA, hubToken); reasonOf(err) != "hub-already-connected" {
		t.Fatalf("the same address under a second id: %v", err)
	}

	// Another address under the id in use cannot be shown to be the
	// same hub, and is refused with words that say so.
	const second = "https://10.0.0.7:6443"
	h.alsoAt(second, h.hub)
	_, err := h.connectHub("hub1", second, h.hubCA, hubToken)
	var ee *kubernetesaddon.EnrollmentError
	if !errors.As(err, &ee) || ee.Reason != "hub-id-in-use" {
		t.Fatalf("another address under an id in use, no identifier: %v", err)
	}
	if !strings.Contains(ee.Message, "hub access manifest") {
		t.Errorf("the refusal does not say how the hub can be recognized: %q", ee.Message)
	}
}
