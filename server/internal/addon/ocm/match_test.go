package ocm

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func encodeBase64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func TestParseManagedCluster(t *testing.T) {
	ca := testCA(t, "c1")
	obj := managedCluster("c1",
		withLabels(map[string]string{clusterSetLabel: "prod", "region": "eu"}),
		withClientConfig("https://c1.test:6443/", ca),
		withClaim("id.k8s.io", "abc"))
	obj.Object["spec"].(map[string]any)["taints"] = []any{
		map[string]any{"key": "cluster.open-cluster-management.io/unreachable", "effect": "NoSelect"},
		map[string]any{"key": "gpu", "value": "true", "effect": "NoSelectIfNew"},
	}
	c := parseManagedCluster(obj)
	if c.name != "c1" || c.uid != "uid-c1" || !c.accepted || c.clusterSet != "prod" || c.version != "v1.37.0" {
		t.Errorf("parsed = %+v", c)
	}
	if len(c.configs) != 1 || c.configs[0].url != "https://c1.test:6443" || !kubernetesaddon.SameCA(c.configs[0].ca, ca) {
		t.Errorf("client configs = %+v", c.configs)
	}
	if c.claims["id.k8s.io"] != "abc" {
		t.Errorf("claims = %v", c.claims)
	}
	if strings.Join(c.taints, " ") != "cluster.open-cluster-management.io/unreachable:NoSelect gpu=true:NoSelectIfNew" {
		t.Errorf("taints = %v", c.taints)
	}
	if c.available != domain.ConditionTrue || len(c.conditions) != 2 {
		t.Errorf("conditions = %+v, available %s", c.conditions, c.available)
	}
	if got := c.conditions[1]; got.Type() != conditionAvailable || got.Reason() != "ManagedClusterAvailable" || got.LastTransitionTime().IsZero() {
		t.Errorf("availability condition = %+v", got)
	}
}

func TestParseManagedClusterToleratesOddShapes(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": clusterAPIGroup + "/v1",
		"kind":       "ManagedCluster",
		"metadata":   map[string]any{"name": "odd"},
		"spec": map[string]any{
			"hubAcceptsClient":            "yes",
			"managedClusterClientConfigs": []any{"text", map[string]any{"caBundle": "!!"}, map[string]any{"url": "https://odd.test", "caBundle": "!!"}},
			"taints":                      []any{int64(7), map[string]any{"effect": "NoSelect"}},
		},
		"status": map[string]any{
			"clusterClaims": []any{map[string]any{"value": "nameless"}},
			"conditions":    []any{map[string]any{"type": "X", "status": "Maybe"}, "text"},
		},
	}}
	c := parseManagedCluster(obj)
	if c.accepted || len(c.taints) != 0 || len(c.claims) != 0 || len(c.conditions) != 0 || c.available != domain.ConditionUnknown {
		t.Errorf("parsed = %+v", c)
	}
	if len(c.configs) != 1 || c.configs[0].url != "https://odd.test" || c.configs[0].ca != nil {
		t.Errorf("client configs = %+v", c.configs)
	}
	obs := c.observation("hub1", clusterLink{})
	if obs.Enrollment.State != LinkObserved || obs.Available != "Unknown" || len(obs.ClientConfigs) != 1 || obs.ClientConfigs[0].CAFingerprint != "" {
		t.Errorf("observation = %+v", obs)
	}
}

func kubeTarget(id, name, apiServer string, ca []byte, extra map[string]string) domain.TargetInfo {
	props := map[string]string{
		kubernetesaddon.PropAPIServer:           apiServer,
		kubernetesaddon.PropCACert:              string(ca),
		kubernetesaddon.PropClusterResourceName: "clusters/" + name,
	}
	for k, v := range extra {
		props[k] = v
	}
	return domain.NewTargetInfo(domain.TargetID(id), kubernetesaddon.TargetType, name, domain.TargetStateReady, nil, props, nil)
}

func enrolledVia(hubID, cluster string) map[string]string {
	src, _ := json.Marshal(kubernetesaddon.ClusterSource{Verifier: VerifierName, Scope: hubID, Name: cluster})
	return map[string]string{
		kubernetesaddon.PropOrigin:           kubernetesaddon.OriginEnrolled,
		kubernetesaddon.PropEnrollmentSource: string(src),
	}
}

func TestWithdrawnIndexMatch(t *testing.T) {
	caA, caB := testCA(t, "a"), testCA(t, "b")
	fingerprint := func(ca []byte) string {
		t.Helper()
		fps, err := kubernetesaddon.CAFingerprints(ca)
		if err != nil {
			t.Fatalf("CAFingerprints: %v", err)
		}
		return fps[0]
	}
	bundle := append(append([]byte{}, caA...), caB...)
	idx := newWithdrawnIndex([]withdrawnCluster{
		{id: "one", apiServer: "https://one.test:6443", caFingerprint: fingerprint(caA)},
		{id: "twin-1", apiServer: "https://twin.test:6443", caFingerprint: fingerprint(caA)},
		{id: "twin-2", apiServer: "https://twin.test:6443", caFingerprint: fingerprint(caA)},
		{id: "no-address", caFingerprint: fingerprint(caA)},
		{id: "no-fingerprint", apiServer: "https://nofp.test:6443"},
	})
	cluster := func(url string, ca []byte) observedCluster {
		return observedCluster{name: "c", configs: []clientConfig{{url: url, ca: ca}}}
	}
	cases := map[string]struct {
		c    observedCluster
		id   string
		want bool
	}{
		"one record with the address and CA":       {cluster("https://one.test:6443", caA), "one", true},
		"the CA is one certificate of a bundle":    {cluster("https://one.test:6443", bundle), "one", true},
		"another CA":                               {cluster("https://one.test:6443", caB), "", false},
		"several records share the address and CA": {cluster("https://twin.test:6443", caA), "", false},
		"a record with no address":                 {cluster("https://missing.test:6443", caA), "", false},
		"a record with no fingerprint":             {cluster("https://nofp.test:6443", caA), "", false},
		"the hub records no address":               {observedCluster{name: "c"}, "", false},
		"an unparseable CA":                        {cluster("https://one.test:6443", []byte("not a CA")), "", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			id, found := idx.match(tc.c)
			if id != tc.id || found != tc.want {
				t.Errorf("match = %q, %v; want %q, %v", id, found, tc.id, tc.want)
			}
		})
	}
}

func TestTargetIndexMatch(t *testing.T) {
	caA, caB, caC := testCA(t, "a"), testCA(t, "b"), testCA(t, "c")
	idx := newTargetIndex([]domain.TargetInfo{
		kubeTarget("k8s-hub1.a", "hub1.a", "https://a.test:6443", caA, enrolledVia("hub1", "a")),
		kubeTarget("k8s-b", "b", "https://b.test:6443", caB, nil),
		kubeTarget("k8s-hand", "hand", "https://c.test:6443", caC, map[string]string{kubernetesaddon.PropOrigin: kubernetesaddon.OriginEnrolled}),
		kubeTarget("k8s-twin-1", "twin", "https://twin.test:6443", caA, nil),
		kubeTarget("k8s-twin-2", "twin", "https://twin.test:6443", caA, nil),
		kubeTarget("k8s-nameonly", "nameonly", "https://nameonly.test:6443", caB, nil),
		domain.NewTargetInfo("kind-local", "kind", "b", domain.TargetStateReady, nil, nil, nil),
	})
	cluster := func(name, url string, ca []byte) observedCluster {
		c := observedCluster{name: name}
		if url != "" {
			c.configs = []clientConfig{{url: url, ca: ca}}
		}
		return c
	}
	cases := map[string]struct {
		hub      string
		c        observedCluster
		want     EnrollmentLink
		conflict string
	}{
		"enrolled through this hub": {
			"hub1", cluster("a", "https://moved.test:6443", caB),
			EnrollmentLink{State: LinkEnrolled, Cluster: "clusters/hub1.a", Target: "k8s-hub1.a", Basis: BasisEnrollment}, "",
		},
		"same name on another hub is another cluster": {
			"hub2", cluster("a", "https://elsewhere.test:6443", caB),
			EnrollmentLink{State: LinkObserved}, "",
		},
		"known to another hub by address": {
			"hub2", cluster("a-as-hub2-calls-it", "https://a.test:6443", caA),
			EnrollmentLink{State: LinkEnrolled, Cluster: "clusters/hub1.a", Target: "k8s-hub1.a", Basis: BasisAddressAndCA}, "",
		},
		"created by the resource manager": {
			"hub1", cluster("spoke", "https://b.test:6443", caB),
			EnrollmentLink{State: LinkCreated, Cluster: "clusters/b", Target: "k8s-b", Basis: BasisAddressAndCA}, "",
		},
		"enrolled by hand": {
			"hub1", cluster("c", "https://c.test:6443", caC),
			EnrollmentLink{State: LinkEnrolled, Cluster: "clusters/hand", Target: "k8s-hand", Basis: BasisAddressAndCA}, "",
		},
		"same address, another CA": {
			"hub1", cluster("b", "https://b.test:6443", caC),
			EnrollmentLink{State: LinkObserved}, "",
		},
		"several candidates": {
			"hub1", cluster("twin", "https://twin.test:6443", caA),
			EnrollmentLink{State: LinkObserved}, "k8s-twin-1, k8s-twin-2",
		},
		"name only, hub records no address": {
			"hub1", cluster("nameonly", "", nil),
			EnrollmentLink{State: LinkCreated, Cluster: "clusters/nameonly", Target: "k8s-nameonly", Basis: BasisName}, "",
		},
		"name only, several of that name": {
			"hub1", cluster("twin", "", nil),
			EnrollmentLink{State: LinkObserved}, "k8s-twin-1, k8s-twin-2",
		},
		"name agrees but the hub records another address": {
			"hub1", cluster("nameonly", "https://other.test:6443", caB),
			EnrollmentLink{State: LinkObserved}, "",
		},
		"a target of another type is never a match": {
			"hub1", cluster("kind-local", "", nil),
			EnrollmentLink{State: LinkObserved}, "",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := idx.match(tc.hub, tc.c)
			if got.link != tc.want {
				t.Errorf("link = %+v, want %+v", got.link, tc.want)
			}
			if (tc.conflict == "") != (got.conflict == "") || !strings.Contains(got.conflict, tc.conflict) {
				t.Errorf("conflict = %q, want it to name %q", got.conflict, tc.conflict)
			}
			if (got.target != "") != (tc.want.Target != "") {
				t.Errorf("target = %q for link %+v", got.target, got.link)
			}
		})
	}
}
