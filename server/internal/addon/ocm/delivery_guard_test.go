package ocm

import (
	"strings"
	"testing"
	"time"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// addHubCluster makes the cluster a hub runs on exist for the
// enrollment prober, at the hub's own address and with its CA, and
// returns a token of its access account. The cluster reports the
// identifier the hub reports.
func (h *harness) addHubCluster(api string, ca []byte) string {
	h.t.Helper()
	now := time.Now()
	token := makeToken("access-of-"+api, now, now.Add(12*time.Hour))
	h.prober.mu.Lock()
	h.prober.clusters[api] = &fakeCluster{ca: ca, uid: "kube-system-of-" + api, tokens: map[string]struct{}{token: {}}}
	h.prober.mu.Unlock()
	return token
}

func (h *harness) guardOf(id domain.TargetID) kubernetesaddon.ObjectGuard {
	h.t.Helper()
	target, ok := h.target(id)
	if !ok {
		h.t.Fatalf("no target %s", id)
	}
	return h.svc.GuardTarget(h.ctx, target)
}

func guarded(group, kind, namespace, name string) kubernetesaddon.GuardedObject {
	return kubernetesaddon.GuardedObject{Group: group, Kind: kind, Namespace: namespace, Name: name}
}

type guardCase struct {
	obj kubernetesaddon.GuardedObject
	// why is a part of the reason; "" for an object in reach.
	why string
}

func checkGuard(t *testing.T, where string, guard kubernetesaddon.ObjectGuard, cases []guardCase) {
	t.Helper()
	if guard == nil {
		t.Fatalf("%s: no guard", where)
	}
	for _, tc := range cases {
		got := guard(tc.obj)
		switch {
		case tc.why == "" && got != "":
			t.Errorf("%s: %s is refused (%q), want it in reach", where, tc.obj, got)
		case tc.why != "" && !strings.Contains(got, tc.why):
			t.Errorf("%s: %s: reason %q, want one that says %q", where, tc.obj, got, tc.why)
		}
	}
}

// A hub's own cluster can be enrolled like any other cluster, and
// workloads can be delivered to it. What makes it a hub stays out of
// reach of deliveries; so does Open Cluster Management's agent on the
// clusters a hub manages. Two hubs are told apart.
func TestDeliveriesStayOutOfOpenClusterManagement(t *testing.T) {
	h := newHarness(t)
	hubTwo, hubTwoCA := h.addHubTwo()
	ownToken := h.addHubCluster(hubAPI, h.hubCA)
	ownTwoToken := h.addHubCluster(hubTwoAPI, hubTwoCA)
	extAPI, extCA, extToken := h.addRealCluster("ext")
	h.createCluster(managedCluster("local-cluster", withClientConfig(hubAPI, h.hubCA)))
	h.createCluster(managedCluster("ext", withClientConfig(extAPI, extCA)))
	h.createClusterOn(hubTwo, managedCluster("local-cluster", withClientConfig(hubTwoAPI, hubTwoCA)))
	h.createClusterOn(hubTwo, managedCluster("ext3"))
	h.connect("hub1")
	h.connectHubTwo("hub2", hubTwoCA)
	bearerOne, bearerTwo := h.addonBearer("hub1"), h.addonBearer("hub2")
	for _, p := range []struct {
		bearer, name string
		ca           []byte
		token        string
	}{
		{bearerOne, "local-cluster", h.hubCA, ownToken},
		{bearerOne, "ext", extCA, extToken},
		{bearerTwo, "local-cluster", hubTwoCA, ownTwoToken},
	} {
		if _, err := h.enrollment.Present(h.ctx, p.bearer, kubernetesaddon.PresentRequest{ClusterName: p.name, CACert: p.ca, Token: p.token}); err != nil {
			t.Fatalf("Present(%s): %v", p.name, err)
		}
	}

	const (
		ocms     = "belongs to Open Cluster Management"
		hubOnes  = `hub "hub1"`
		hubTwos  = `hub "hub2"`
		noWrites = "does not write to a hub"
	)
	checkGuard(t, "hub one's own cluster", h.guardOf("k8s-hub1.local-cluster"), []guardCase{
		{guarded("", "ConfigMap", "open-cluster-management-hub", "x"), ocms},
		{guarded("apps", "Deployment", "open-cluster-management", "cluster-manager"), ocms},
		{guarded("", "Namespace", "open-cluster-management-hub", "open-cluster-management-hub"), ocms},
		// The hub keeps a namespace for each of its clusters.
		{guarded("", "Secret", "ext", "x"), hubOnes},
		{guarded("", "ConfigMap", "local-cluster", "x"), hubOnes},
		{guarded("", "Namespace", "ext", "ext"), hubOnes},
		{guarded("", "ServiceAccount", HubReaderNamespace, "another"), hubOnes},
		// The hub's API is the hub.
		{guarded("work.open-cluster-management.io", "ManifestWork", "default", "x"), noWrites},
		{guarded("cluster.open-cluster-management.io", "ManagedCluster", "", "x"), noWrites},
		{guarded("addon.open-cluster-management.io", "ClusterManagementAddOn", "", "x"), noWrites},
		// Workloads are welcome.
		{guarded("apps", "Deployment", "default", "web"), ""},
		{guarded("", "Namespace", "team-a", "team-a"), ""},
		{guarded("", "ConfigMap", "team-a", "settings"), ""},
		// The name of the other hub's cluster means nothing here.
		{guarded("", "ConfigMap", "ext3", "x"), ""},
		{guarded("", "ConfigMap", "open-cluster-managementx", "x"), ""},
		{guarded("example.io", "Widget", "default", "x"), ""},
	})
	checkGuard(t, "hub two's own cluster", h.guardOf("k8s-hub2.local-cluster"), []guardCase{
		{guarded("", "ConfigMap", "open-cluster-management-hub", "x"), ocms},
		{guarded("", "ConfigMap", "ext3", "x"), hubTwos},
		{guarded("", "ConfigMap", "local-cluster", "x"), hubTwos},
		{guarded("work.open-cluster-management.io", "ManifestWork", "default", "x"), hubTwos},
		{guarded("", "ConfigMap", "ext", "x"), ""},
		{guarded("apps", "Deployment", "default", "web"), ""},
	})
	checkGuard(t, "a cluster hub one manages", h.guardOf("k8s-hub1.ext"), []guardCase{
		{guarded("", "ConfigMap", "open-cluster-management-agent", "x"), ocms},
		{guarded("", "Namespace", "open-cluster-management-agent-addon", "open-cluster-management-agent-addon"), ocms},
		// It is not a hub: these names mean nothing on it.
		{guarded("", "ConfigMap", "ext", "x"), ""},
		{guarded("", "ConfigMap", "local-cluster", "x"), ""},
		{guarded("", "ConfigMap", HubReaderNamespace, "x"), ""},
		{guarded("cluster.open-cluster-management.io", "ClusterClaim", "", "x"), ""},
		{guarded("apps", "Deployment", "default", "web"), ""},
	})

	// A cluster no hub has to do with is not this guard's business.
	api, ca, token := h.addRealCluster("alone")
	if _, err := h.enrollment.Enroll(h.ctx, kubernetesaddon.EnrollRequest{ClusterID: "alone", APIServer: api, CACert: ca, Token: token, Actor: "ops"}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if guard := h.guardOf("k8s-alone"); guard != nil {
		t.Errorf("a guard for a cluster no hub knows: it refuses an OCM namespace with %q", guard(guarded("", "ConfigMap", "open-cluster-management-agent", "x")))
	}

	// A cluster stays a cluster of Open Cluster Management after its
	// hub's connection is removed here: the agent is still on it.
	if err := h.svc.Disconnect(h.ctx, "hub1", "ops"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	checkGuard(t, "a cluster of a hub that was disconnected", h.guardOf("k8s-hub1.ext"), []guardCase{
		{guarded("", "ConfigMap", "open-cluster-management-agent", "x"), ocms},
		{guarded("apps", "Deployment", "default", "web"), ""},
	})
}

// The cluster a hub runs on is that hub's cluster at whatever address
// it was enrolled, and while the hub cannot be read its recorded
// clusters still say which namespaces are the hub's.
func TestAHubsClusterIsRecognizedByItsIdentifier(t *testing.T) {
	h := newHarness(t)
	token := h.addHubCluster(hubAPI, h.hubCA)
	const byIP = "https://10.0.0.5:6443"
	h.clusterAlsoAt(byIP, hubAPI)
	h.createCluster(managedCluster("ext"))
	h.connect("hub1")
	eventually(t, "ext recorded", func() bool { _, ok := h.observed("hub1", "ext"); return ok })

	// Enrolled by hand, under a name of its own and at another address:
	// no hub's record leads to it.
	if _, err := h.enrollment.Enroll(h.ctx, kubernetesaddon.EnrollRequest{ClusterID: "mgmt", APIServer: byIP, CACert: h.hubCA, Token: token, Actor: "ops"}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	want := []guardCase{
		{guarded("", "ConfigMap", "open-cluster-management-hub", "x"), "belongs to Open Cluster Management"},
		{guarded("", "ConfigMap", "ext", "x"), `hub "hub1"`},
		{guarded("cluster.open-cluster-management.io", "ManagedCluster", "", "ext"), "does not write to a hub"},
		{guarded("apps", "Deployment", "default", "web"), ""},
	}
	checkGuard(t, "the hub's cluster, enrolled at another address", h.guardOf("k8s-mgmt"), want)

	// The resource manager restarts while the hub is down.
	if err := h.svc.Close(h.ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.hub.failing.Store(true)
	h.svc = h.newService()
	if h.svc.hub("hub1").isSynced() {
		t.Fatalf("the hub was read although it does not answer")
	}
	checkGuard(t, "the hub's cluster while the hub cannot be read", h.guardOf("k8s-mgmt"), want)
}
