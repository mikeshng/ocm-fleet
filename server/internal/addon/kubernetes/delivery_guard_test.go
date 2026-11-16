package kubernetes

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/client-go/rest"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// keepOut is a guard that keeps one namespace out of reach on every
// target, and counts how often it was asked for a target.
type keepOut struct {
	namespace string
	asked     int
}

func (g *keepOut) GuardTarget(context.Context, domain.TargetInfo) ObjectGuard {
	g.asked++
	return func(obj GuardedObject) string {
		if obj.Namespace != g.namespace {
			return ""
		}
		return "namespace \"" + g.namespace + "\" is kept for the test"
	}
}

func rawObject(apiVersion, kind, namespace, name string) json.RawMessage {
	meta := map[string]any{"name": name}
	if namespace != "" {
		meta["namespace"] = namespace
	}
	raw, _ := json.Marshal(map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": meta})
	return raw
}

// unreachableCluster is a cluster that cannot be asked anything: a test
// that passes against it proves the guard answered before the cluster
// was asked.
func unreachableCluster() *rest.Config {
	return &rest.Config{Host: "https://127.0.0.1:1", BearerToken: "tok"}
}

func TestGuardedObject(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw  json.RawMessage
		want GuardedObject
		text string
	}{
		{rawObject("v1", "ConfigMap", "team-a", "settings"), GuardedObject{Kind: "ConfigMap", Namespace: "team-a", Name: "settings"}, "ConfigMap team-a/settings"},
		{rawObject("apps/v1", "Deployment", "", "web"), GuardedObject{Group: "apps", Kind: "Deployment", Name: "web"}, "Deployment web"},
		// Applying or deleting a Namespace object acts on the namespace.
		{rawObject("v1", "Namespace", "", "team-a"), GuardedObject{Kind: "Namespace", Namespace: "team-a", Name: "team-a"}, "Namespace team-a"},
		{rawObject("rbac.authorization.k8s.io/v1", "ClusterRole", "", "reader"), GuardedObject{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "reader"}, "ClusterRole reader"},
	}
	for _, tc := range cases {
		got, ok := guardedObject(tc.raw)
		if !ok || got != tc.want || got.String() != tc.text {
			t.Errorf("guardedObject(%s) = %+v (%q), %v; want %+v (%q)", tc.raw, got, got, ok, tc.want, tc.text)
		}
	}
	if _, ok := guardedObject(json.RawMessage(`{not-json`)); ok {
		t.Errorf("a manifest that cannot be read was judged")
	}
}

// A delivery that names an object out of reach fails before anything is
// applied: the object in reach that comes first is not applied either.
func TestDeliveryGuard_RefusesTheWholeDelivery(t *testing.T) {
	t.Parallel()
	reporter := &deliveryRecordingReporter{}
	a := NewDeliveryAgent(reporter)
	guard := &keepOut{namespace: "kept"}
	a.AddGuard(guard)
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: TargetType, Name: "t1"})
	manifests := []domain.Manifest{
		{ManifestType: ManifestManifestType, Raw: rawObject("v1", "ConfigMap", "default", "fine")},
		{ManifestType: ManifestManifestType, Raw: rawObject("v1", "ConfigMap", "kept", "not-fine")},
	}

	a.applyManifests(context.Background(), target, "d1", 1, unreachableCluster(), manifests, true)

	if reporter.result.State != domain.DeliveryStateFailed || reporter.result.Operation != domain.DeliveryOperationDeliver {
		t.Fatalf("result = %+v, want a failed delivery", reporter.result)
	}
	for _, want := range []string{"manifest 2", "ConfigMap kept/not-fine", "kept for the test", "nothing of this delivery was applied"} {
		if !strings.Contains(reporter.result.Message, want) {
			t.Errorf("message %q does not say %q", reporter.result.Message, want)
		}
	}
	if len(reporter.events) != 0 {
		t.Errorf("the delivery went on to apply: %d progress events", len(reporter.events))
	}
	if guard.asked != 1 {
		t.Errorf("the guard was asked %d times for one delivery, want once", guard.asked)
	}
}

func TestDeliveryGuard_NamespaceObjectsCount(t *testing.T) {
	t.Parallel()
	reporter := &deliveryRecordingReporter{}
	a := NewDeliveryAgent(reporter)
	a.AddGuard(&keepOut{namespace: "kept"})
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: TargetType, Name: "t1"})

	a.applyManifests(context.Background(), target, "d1", 1, unreachableCluster(),
		[]domain.Manifest{{ManifestType: ManifestManifestType, Raw: rawObject("v1", "Namespace", "", "kept")}}, true)

	if reporter.result.State != domain.DeliveryStateFailed || !strings.Contains(reporter.result.Message, "Namespace kept") {
		t.Fatalf("result = %+v, want the Namespace object refused", reporter.result)
	}
}

// A removal leaves an object out of reach as it is and names it; the
// cluster is not asked about it.
func TestDeliveryGuard_RemovalLeavesTheObject(t *testing.T) {
	t.Parallel()
	a := NewDeliveryAgent(nopReporterForInternal{})
	guard := (&keepOut{namespace: "kept"}).GuardTarget(context.Background(), domain.TargetInfo{})
	manifests := []domain.Manifest{{ManifestType: ManifestManifestType, Raw: rawObject("v1", "ConfigMap", "kept", "not-fine")}}

	for name, remove := range map[string]func() ([]string, error){
		"caller's credential": func() ([]string, error) {
			return a.deleteManifests(context.Background(), unreachableCluster(), manifests, guard)
		},
		"stored credential": func() ([]string, error) {
			return a.deleteManifestsWithStoredCredential(context.Background(), unreachableCluster(), manifests, guard)
		},
	} {
		left, err := remove()
		if err != nil {
			t.Fatalf("%s: the cluster was asked about an object out of reach: %v", name, err)
		}
		if len(left) != 1 || !strings.Contains(left[0], "ConfigMap kept/not-fine") || !strings.Contains(left[0], "kept for the test") {
			t.Fatalf("%s: left = %v", name, left)
		}
		if result := removalResult(left); result.State != domain.DeliveryStatePartial || !strings.Contains(result.Message, "kept for the test") {
			t.Errorf("%s: removal result = %+v, want Partial with the reason", name, result)
		}
	}
}

// The agent's own rule: on an enrolled cluster, the namespace that
// holds this resource manager's access is not a place for deliveries.
func TestDeliveryGuard_AccessNamespaceOfAnEnrolledCluster(t *testing.T) {
	t.Parallel()
	a := NewDeliveryAgent(nopReporterForInternal{})
	enrolled := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "k8s-ext", Type: TargetType, Name: "ext",
		Properties: map[string]string{PropOrigin: OriginEnrolled, PropServiceAccount: AccessNamespace + "/" + AccessServiceAccount},
	})
	guard := a.guardFor(context.Background(), enrolled)
	for _, obj := range []GuardedObject{
		{Kind: "ConfigMap", Namespace: AccessNamespace, Name: "x"},
		{Kind: "Namespace", Namespace: AccessNamespace, Name: AccessNamespace},
		{Group: "rbac.authorization.k8s.io", Kind: "RoleBinding", Namespace: AccessNamespace, Name: "more"},
	} {
		if why := guard(obj); !strings.Contains(why, "own access to the cluster") {
			t.Errorf("%s on an enrolled cluster: reason %q", obj, why)
		}
	}
	if why := guard(GuardedObject{Kind: "ConfigMap", Namespace: "default", Name: "x"}); why != "" {
		t.Errorf("an ordinary object was refused: %q", why)
	}

	// Access someone made by hand lives where they put it. Nothing says
	// that namespace is kept for it, so nothing is kept.
	byHand := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "k8s-byhand", Type: TargetType, Name: "byhand",
		Properties: map[string]string{PropOrigin: OriginEnrolled, PropServiceAccount: "default/deployer"},
	})
	guard = a.guardFor(context.Background(), byHand)
	for _, ns := range []string{"default", AccessNamespace} {
		if why := guard(GuardedObject{Kind: "ConfigMap", Namespace: ns, Name: "x"}); why != "" {
			t.Errorf("namespace %q on a cluster with hand-made access: %q", ns, why)
		}
	}

	// A cluster this resource manager created has no access namespace.
	created := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k8s-made", Type: TargetType, Name: "made"})
	if why := a.guardFor(context.Background(), created)(GuardedObject{Kind: "ConfigMap", Namespace: AccessNamespace, Name: "x"}); why != "" {
		t.Errorf("a cluster that was not enrolled: %q", why)
	}
}
