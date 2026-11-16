package ocm

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestHubAccessManifestGrantsReadOfClustersOnly(t *testing.T) {
	objs := HubAccessManifest()
	kinds := map[string]int{}
	for _, obj := range objs {
		kinds[obj["kind"].(string)]++
		if obj["kind"] == "Secret" {
			t.Errorf("the hub access manifest carries a Secret")
		}
	}
	want := map[string]int{"Namespace": 1, "ServiceAccount": 1, "ClusterRole": 1, "ClusterRoleBinding": 1, "Role": 1, "RoleBinding": 1}
	for kind, n := range want {
		if kinds[kind] != n {
			t.Errorf("%d objects of kind %s, want %d", kinds[kind], kind, n)
		}
	}
	for _, obj := range objs {
		if obj["kind"] != "ClusterRole" {
			continue
		}
		rules := obj["rules"].([]any)
		if len(rules) != 2 {
			t.Fatalf("cluster role has %d rules, want 2: the hub's clusters, and the namespace that identifies the hub", len(rules))
		}
		rule := rules[0].(map[string]any)
		for _, verb := range rule["verbs"].([]any) {
			if v := verb.(string); v != "get" && v != "list" && v != "watch" {
				t.Errorf("cluster role grants verb %q", v)
			}
		}
		if groups := rule["apiGroups"].([]any); len(groups) != 1 || groups[0] != clusterAPIGroup {
			t.Errorf("cluster role covers API groups %v", groups)
		}
		for _, res := range rule["resources"].([]any) {
			if r := res.(string); r != "managedclusters" && r != "managedclustersets" {
				t.Errorf("cluster role covers resource %q", r)
			}
		}
		// The second rule reads one named namespace and nothing else:
		// no list, no other name, no other resource.
		identity := rules[1].(map[string]any)
		if verbs := identity["verbs"].([]any); len(verbs) != 1 || verbs[0] != "get" {
			t.Errorf("the identity rule grants verbs %v, want get alone", verbs)
		}
		if res := identity["resources"].([]any); len(res) != 1 || res[0] != "namespaces" {
			t.Errorf("the identity rule covers resources %v", res)
		}
		if names := identity["resourceNames"].([]any); len(names) != 1 || names[0] != "kube-system" {
			t.Errorf("the identity rule covers names %v, want kube-system alone", names)
		}
	}
	yaml, err := kubernetesaddon.RenderManifestYAML(objs)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(string(yaml), "name: "+HubReaderServiceAccount) || strings.Count(string(yaml), "---") != len(objs)-1 {
		t.Errorf("rendered manifest:\n%s", yaml)
	}
	if len(HubAccessInstructions()) == 0 {
		t.Errorf("no instructions")
	}
}

// On a hub that is registered with itself, the hub's own cluster can be
// enrolled like any other. What connects the hub and what gives access
// to that cluster must then be two things: removing one, by the
// namespace or by the label the instructions name, leaves the other.
func TestHubConnectionAndClusterAccessAreSeparate(t *testing.T) {
	if HubReaderNamespace == kubernetesaddon.AccessNamespace {
		t.Fatalf("the hub reader lives in %q, the namespace of a cluster's access piece", HubReaderNamespace)
	}
	const partOf = "app.kubernetes.io/part-of"
	access := map[string]bool{}
	for _, obj := range buildAddon(t, false).Hub {
		manifests, _, _ := unstructured.NestedSlice(obj, "spec", "agentSpec", "workload", "manifests")
		for _, m := range manifests {
			if v, _, _ := unstructured.NestedString(m.(map[string]any), "metadata", "labels", partOf); v != "" {
				access[v] = true
			}
		}
	}
	if len(access) == 0 {
		t.Fatalf("the access piece carries no %s label", partOf)
	}
	for _, obj := range HubAccessManifest() {
		v, _, _ := unstructured.NestedString(obj, "metadata", "labels", partOf)
		if v == "" || access[v] {
			t.Errorf("%v %v is labeled %s=%q, a label the access piece carries too", obj["kind"], obj["metadata"].(map[string]any)["name"], partOf, v)
		}
		if ns, _, _ := unstructured.NestedString(obj, "metadata", "namespace"); ns == kubernetesaddon.AccessNamespace {
			t.Errorf("%v is in the access piece's namespace", obj["kind"])
		}
	}
	for _, line := range append(HubAccessInstructions(), EnrollmentAddonInstructions("hub1", false, EnrollmentRollout{Mode: RolloutManual})...) {
		if strings.Contains(line, "delete namespace "+kubernetesaddon.AccessNamespace) && strings.Contains(line, HubReaderServiceAccount) {
			t.Errorf("one instruction removes the access namespace and the hub reader together: %q", line)
		}
	}
}

func buildAddon(t *testing.T, removeAccess bool) EnrollmentAddon {
	t.Helper()
	return buildAddonFor(t, removeAccess, EnrollmentRollout{})
}

func buildAddonFor(t *testing.T, removeAccess bool, rollout EnrollmentRollout) EnrollmentAddon {
	t.Helper()
	addon, err := BuildEnrollmentAddon(EnrollmentAddonOptions{
		Profile:               kubernetesaddon.AccessProfileWorkloads,
		RemoveAccessWithAddon: removeAccess,
		Endpoint:              testEndpoint(),
		BundleToken:           "eb-000000000000.secret",
		TokenLifetime:         2 * time.Hour,
		Rollout:               rollout,
	})
	if err != nil {
		t.Fatalf("BuildEnrollmentAddon: %v", err)
	}
	return addon
}

// hubObject returns the one object of the kind among what is applied to
// the hub.
func hubObject(t *testing.T, addon EnrollmentAddon, kind string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, obj := range addon.Hub {
		if obj["kind"] == kind {
			if found != nil {
				t.Fatalf("several objects of kind %s", kind)
			}
			found = obj
		}
	}
	if found == nil {
		t.Fatalf("no object of kind %s among %d hub objects", kind, len(addon.Hub))
	}
	return found
}

// One choice enrolls every cluster of a hub: the manifest carries what
// makes the hub's own add-on manager do it, and nothing has to be done
// per cluster.
func TestEnrollmentAddonForEveryClusterOfAHub(t *testing.T) {
	addon := buildAddonFor(t, false, EnrollmentRollout{Mode: RolloutEveryCluster})
	kinds := make([]string, 0, len(addon.Hub))
	for _, obj := range addon.Hub {
		kinds = append(kinds, obj["kind"].(string))
	}
	// In the order a hub can take them: what the add-on follows first.
	if got := strings.Join(kinds, " "); got != "Namespace ManagedClusterSetBinding Placement AddOnTemplate ClusterManagementAddOn" {
		t.Fatalf("hub objects = %s", got)
	}
	want := EnrollmentRollout{Mode: RolloutEveryCluster, PlacementNamespace: RolloutNamespace, PlacementName: RolloutPlacementName}
	if addon.Rollout != want {
		t.Errorf("rollout = %+v, want %+v", addon.Rollout, want)
	}

	cma := hubObject(t, addon, "ClusterManagementAddOn")
	if v, _, _ := unstructured.NestedString(cma, "spec", "installStrategy", "type"); v != "Placements" {
		t.Errorf("install strategy = %q, want Placements", v)
	}
	placements, _, _ := unstructured.NestedSlice(cma, "spec", "installStrategy", "placements")
	if len(placements) != 1 || placements[0].(map[string]any)["namespace"] != RolloutNamespace || placements[0].(map[string]any)["name"] != RolloutPlacementName {
		t.Errorf("the add-on follows %v, want the generated Placement", placements)
	}

	binding := hubObject(t, addon, "ManagedClusterSetBinding")
	placement := hubObject(t, addon, "Placement")
	for _, obj := range []map[string]any{binding, placement} {
		if ns, _, _ := unstructured.NestedString(obj, "metadata", "namespace"); ns != RolloutNamespace {
			t.Errorf("%v is in namespace %q, want %q", obj["kind"], ns, RolloutNamespace)
		}
	}
	if name, _, _ := unstructured.NestedString(hubObject(t, addon, "Namespace"), "metadata", "name"); name != RolloutNamespace {
		t.Errorf("namespace = %q", name)
	}
	// The binding must carry the name of its cluster set: a hub
	// refuses any other.
	if name, _, _ := unstructured.NestedString(binding, "metadata", "name"); name != "global" {
		t.Errorf("binding name = %q", name)
	}
	if set, _, _ := unstructured.NestedString(binding, "spec", "clusterSet"); set != "global" {
		t.Errorf("binding binds %q, want the set every cluster of a hub is in", set)
	}
	if sets, _, _ := unstructured.NestedStringSlice(placement, "spec", "clusterSets"); len(sets) != 1 || sets[0] != "global" {
		t.Errorf("placement selects sets %v", sets)
	}
	if _, narrowed, _ := unstructured.NestedSlice(placement, "spec", "predicates"); narrowed {
		t.Errorf("the placement for every cluster carries predicates")
	}
	if _, limited, _ := unstructured.NestedFieldNoCopy(placement, "spec", "numberOfClusters"); limited {
		t.Errorf("the placement for every cluster limits the number of clusters")
	}
	// A cluster that stopped answering is still a cluster of the hub.
	tolerated := map[string]bool{}
	tolerations, _, _ := unstructured.NestedSlice(placement, "spec", "tolerations")
	for _, raw := range tolerations {
		m := raw.(map[string]any)
		if m["operator"] != "Exists" || m["effect"] != nil || m["tolerationSeconds"] != nil {
			t.Errorf("toleration %v does not tolerate the taint for good", m)
		}
		tolerated[m["key"].(string)] = true
	}
	for _, key := range []string{"cluster.open-cluster-management.io/unavailable", "cluster.open-cluster-management.io/unreachable"} {
		if !tolerated[key] {
			t.Errorf("the placement drops a cluster with taint %s", key)
		}
	}

	// Nothing is enabled by hand: the add-on manager would remove it
	// again from a cluster the Placement does not select.
	if addon.Enable != nil {
		t.Errorf("an example for one cluster comes with the add-on for every cluster")
	}
	// The template is the same whatever the rollout.
	if got, want := len(templateManifests(t, addon)), len(templateManifests(t, buildAddon(t, false))); got != want {
		t.Errorf("template carries %d manifests, %d with the manual rollout", got, want)
	}

	steps := strings.Join(EnrollmentAddonInstructions("hub1", false, addon.Rollout), "\n")
	for _, want := range []string{"every cluster", RolloutNamespace, "joins later", "its own cluster included"} {
		if !strings.Contains(steps, want) {
			t.Errorf("instructions do not say %q:\n%s", want, steps)
		}
	}
	if strings.Contains(steps, "enableExample") {
		t.Errorf("instructions point at an example that is not given:\n%s", steps)
	}
	if withdrawn := strings.Join(EnrollmentAddonInstructions("hub1", true, addon.Rollout), "\n"); !strings.Contains(withdrawn, "stops selecting") {
		t.Errorf("instructions do not say that a cluster the Placement drops is withdrawn:\n%s", withdrawn)
	}
}

func TestEnrollmentAddonFollowsANamedPlacement(t *testing.T) {
	rollout := EnrollmentRollout{Mode: RolloutPlacement, PlacementNamespace: "fleet-team", PlacementName: "prod-clusters"}
	addon := buildAddonFor(t, false, rollout)
	if len(addon.Hub) != 2 {
		t.Fatalf("%d hub objects: the Placement is the administrator's, nothing is made for it", len(addon.Hub))
	}
	placements, _, _ := unstructured.NestedSlice(hubObject(t, addon, "ClusterManagementAddOn"), "spec", "installStrategy", "placements")
	if len(placements) != 1 || placements[0].(map[string]any)["namespace"] != "fleet-team" || placements[0].(map[string]any)["name"] != "prod-clusters" {
		t.Errorf("the add-on follows %v", placements)
	}
	if addon.Enable != nil || addon.Rollout != rollout {
		t.Errorf("enable = %v, rollout = %+v", addon.Enable, addon.Rollout)
	}
	if steps := strings.Join(EnrollmentAddonInstructions("hub1", false, rollout), "\n"); !strings.Contains(steps, "prod-clusters") || !strings.Contains(steps, "fleet-team") || !strings.Contains(steps, "also where it was enabled by hand") {
		t.Errorf("instructions:\n%s", steps)
	}
}

func TestResolveRollout(t *testing.T) {
	everyCluster := EnrollmentRollout{Mode: RolloutEveryCluster, PlacementNamespace: RolloutNamespace, PlacementName: RolloutPlacementName}
	named := EnrollmentRollout{Mode: RolloutPlacement, PlacementNamespace: "team", PlacementName: "prod"}
	ok := []struct {
		name      string
		requested *EnrollmentRollout
		previous  *EnrollmentRollout
		want      EnrollmentRollout
	}{
		{"nothing asked, nothing before", nil, nil, EnrollmentRollout{Mode: RolloutManual}},
		{"an empty request, nothing before", &EnrollmentRollout{}, nil, EnrollmentRollout{Mode: RolloutManual}},
		// A manifest made again keeps what the hub was told before.
		{"nothing asked keeps every cluster", nil, &everyCluster, everyCluster},
		{"nothing asked keeps a named placement", &EnrollmentRollout{}, &named, named},
		// What is asked for decides.
		{"manual asked after every cluster", &EnrollmentRollout{Mode: RolloutManual}, &everyCluster, EnrollmentRollout{Mode: RolloutManual}},
		{"every cluster asked", &EnrollmentRollout{Mode: RolloutEveryCluster}, nil, everyCluster},
		{"every cluster, resolved before", &everyCluster, nil, everyCluster},
		{"a placement asked", &named, &everyCluster, named},
	}
	for _, tc := range ok {
		got, err := ResolveRollout(tc.requested, tc.previous)
		if err != nil || got != tc.want {
			t.Errorf("%s: %+v, %v; want %+v", tc.name, got, err, tc.want)
		}
	}
	refused := map[string]EnrollmentRollout{
		"unknown mode":                     {Mode: "all"},
		"placement without a name":         {Mode: RolloutPlacement, PlacementNamespace: "team"},
		"placement without a namespace":    {Mode: RolloutPlacement, PlacementName: "prod"},
		"placement with a bad namespace":   {Mode: RolloutPlacement, PlacementNamespace: "Team_A", PlacementName: "prod"},
		"a name with manual":               {Mode: RolloutManual, PlacementNamespace: "team", PlacementName: "prod"},
		"a name with every cluster":        {Mode: RolloutEveryCluster, PlacementNamespace: "team", PlacementName: "prod"},
		"a name and no mode":               {PlacementNamespace: "team", PlacementName: "prod"},
		"a namespace of 64 characters":     {Mode: RolloutPlacement, PlacementNamespace: strings.Repeat("a", 64), PlacementName: "prod"},
		"a name that could break out YAML": {Mode: RolloutPlacement, PlacementNamespace: "team", PlacementName: "prod\nkind: Secret"},
	}
	for name, requested := range refused {
		if got, err := ResolveRollout(&requested, nil); !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("%s: %+v, %v; want invalid argument", name, got, err)
		}
	}
}

func TestEnrollmentAddonTemplate(t *testing.T) {
	addon := buildAddon(t, false)
	if len(addon.Hub) != 2 {
		t.Fatalf("%d hub objects, want the template and the add-on", len(addon.Hub))
	}
	template, cma := addon.Hub[0], addon.Hub[1]
	if template["apiVersion"] != addonAPIVersion || template["kind"] != "AddOnTemplate" {
		t.Errorf("template = %v %v", template["apiVersion"], template["kind"])
	}
	if name, _, _ := unstructured.NestedString(template, "spec", "addonName"); name != EnrollmentAddonName {
		t.Errorf("template addonName = %q", name)
	}
	if cma["kind"] != "ClusterManagementAddOn" {
		t.Errorf("second object is %v", cma["kind"])
	}
	if v, _, _ := unstructured.NestedString(cma, "metadata", "annotations", addonLifecycleAnnotation); v != "addon-manager" {
		t.Errorf("the add-on is not handed to the hub's add-on manager: %q", v)
	}
	if v, _, _ := unstructured.NestedString(cma, "spec", "installStrategy", "type"); v != "Manual" {
		t.Errorf("install strategy = %q, want Manual: nothing is installed until someone enables it", v)
	}
	configs, _, _ := unstructured.NestedSlice(cma, "spec", "supportedConfigs")
	if len(configs) != 1 {
		t.Fatalf("supported configs = %v", configs)
	}
	if name, _, _ := unstructured.NestedString(configs[0].(map[string]any), "defaultConfig", "name"); name != EnrollmentAddonName {
		t.Errorf("default config names template %q", name)
	}

	var deployment, secret map[string]any
	for _, obj := range templateManifests(t, addon) {
		meta := obj["metadata"].(map[string]any)
		labels, _ := meta["labels"].(map[string]any)
		annotations, _ := meta["annotations"].(map[string]any)
		_, kept := annotations[addonOrphanAnnotation]
		switch labels["app.kubernetes.io/component"] {
		case "access":
			if !kept {
				t.Errorf("%s %s is not kept when the add-on is removed", obj["kind"], meta["name"])
			}
		case "enrollment-agent":
			if kept {
				t.Errorf("%s %s of the agent is kept when the add-on is removed", obj["kind"], meta["name"])
			}
		default:
			t.Errorf("%s %s has no component label", obj["kind"], meta["name"])
		}
		switch obj["kind"] {
		case "Deployment":
			deployment = obj
		case "Secret":
			secret = obj
		case "Job":
			t.Errorf("the add-on runs the agent as a Job; it must keep running")
		}
	}
	if deployment == nil || secret == nil {
		t.Fatalf("template lacks the agent's Deployment or Secret")
	}
	containers, _, _ := unstructured.NestedSlice(deployment, "spec", "template", "spec", "containers")
	container := containers[0].(map[string]any)
	if container["image"] != "registry.example/enrollment-agent:1" {
		t.Errorf("agent image = %v", container["image"])
	}
	args := map[string]bool{}
	for _, a := range container["args"].([]any) {
		args[a.(string)] = true
	}
	for _, want := range []string{
		"--server-url=https://fleet.example:8085",
		"--connect-address=fleet-internal:8085",
		"--cluster-name=" + addonClusterNameVariable,
		"--token-lifetime=2h0m0s",
		"--service-account=" + kubernetesaddon.AccessServiceAccount,
	} {
		if !args[want] {
			t.Errorf("agent arguments %v lack %q", container["args"], want)
		}
	}
	if args["--once"] {
		t.Errorf("the add-on's agent exits after one presentation")
	}
	if container["readinessProbe"] == nil {
		t.Errorf("the agent has no readiness probe: the hub could not tell whether the add-on works")
	}
	if token, _, _ := unstructured.NestedString(secret, "stringData", "token"); token != "eb-000000000000.secret" {
		t.Errorf("secret token = %q", token)
	}

	if addon.Enable["kind"] != "ManagedClusterAddOn" {
		t.Errorf("enable object kind = %v", addon.Enable["kind"])
	}
	if ns, _, _ := unstructured.NestedString(addon.Enable, "metadata", "namespace"); ns != enablePlaceholder {
		t.Errorf("enable object namespace = %q", ns)
	}

	yaml, err := kubernetesaddon.RenderManifestYAML(addon.Hub)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(string(yaml), "kind: AddOnTemplate") || !strings.Contains(string(yaml), "kind: ClusterManagementAddOn") {
		t.Errorf("rendered add-on lacks a kind")
	}
}

func TestEnrollmentAddonCanRemoveAccessWithTheAddon(t *testing.T) {
	addon := buildAddon(t, true)
	for _, obj := range templateManifests(t, addon) {
		meta := obj["metadata"].(map[string]any)
		if annotations, ok := meta["annotations"].(map[string]any); ok {
			if _, kept := annotations[addonOrphanAnnotation]; kept {
				t.Errorf("%s %s is kept although access is to be removed with the add-on", obj["kind"], meta["name"])
			}
		}
	}
	keep := strings.Join(EnrollmentAddonInstructions("hub1", false, EnrollmentRollout{Mode: RolloutManual}), "\n")
	remove := strings.Join(EnrollmentAddonInstructions("hub1", true, EnrollmentRollout{Mode: RolloutManual}), "\n")
	if !strings.Contains(keep, "stays enrolled") || !strings.Contains(remove, "is withdrawn") {
		t.Errorf("instructions do not say what disabling the add-on does")
	}
	if !strings.Contains(keep, "k8s-hub1.") {
		t.Errorf("instructions do not name the target clusters get")
	}
	// A hub that was disconnected and connected again needs the add-on
	// made again; the instructions say so before it happens.
	if !strings.Contains(keep, "disconnected and connected again") {
		t.Errorf("instructions do not say when a manifest has to be made again")
	}
}

func TestEnrollmentAddonNeedsAnImageAndAnAddress(t *testing.T) {
	for name, endpoint := range map[string]kubernetesaddon.EnrollmentEndpoint{
		"no image":   {ServerURL: "https://fleet.example:8085"},
		"no address": {AgentImage: "registry.example/agent:1"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := BuildEnrollmentAddon(EnrollmentAddonOptions{Endpoint: endpoint, BundleToken: "eb-000000000000.s"})
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("error = %v, want invalid argument", err)
			}
		})
	}
}

const tokenKubeconfig = `apiVersion: v1
kind: Config
current-context: hub
clusters:
- name: hub
  cluster:
    server: https://hub.test:6443
    certificate-authority-data: Q0EtUEVN
contexts:
- name: hub
  context: {cluster: hub, user: reader}
users:
- name: reader
  user: {token: reader-token}
`

func TestConnectionFromKubeconfig(t *testing.T) {
	conn, err := ConnectionFromKubeconfig([]byte(tokenKubeconfig))
	if err != nil {
		t.Fatalf("ConnectionFromKubeconfig: %v", err)
	}
	if conn != (HubConnection{APIServer: "https://hub.test:6443", CACert: "CA-PEM", Token: "reader-token"}) {
		t.Errorf("connection = %+v", conn)
	}

	// Without a current context, the only context is used.
	if _, err := ConnectionFromKubeconfig([]byte(strings.Replace(tokenKubeconfig, "current-context: hub\n", "", 1))); err != nil {
		t.Errorf("single context without current-context: %v", err)
	}

	refused := map[string]string{
		"client certificate": strings.Replace(tokenKubeconfig, "user: {token: reader-token}", "user: {client-certificate-data: QQ==, client-key-data: QQ==}", 1),
		"insecure":           strings.Replace(tokenKubeconfig, "certificate-authority-data: Q0EtUEVN", "insecure-skip-tls-verify: true", 1),
		"CA as a file":       strings.Replace(tokenKubeconfig, "certificate-authority-data: Q0EtUEVN", "certificate-authority: /etc/ca.crt", 1),
		"no such context":    strings.Replace(tokenKubeconfig, "current-context: hub", "current-context: elsewhere", 1),
		"not a kubeconfig":   "{{{",
		"exec plugin":        strings.Replace(tokenKubeconfig, "user: {token: reader-token}", "user: {exec: {apiVersion: client.authentication.k8s.io/v1, command: get-token}}", 1),
	}
	for name, doc := range refused {
		t.Run(name, func(t *testing.T) {
			if _, err := ConnectionFromKubeconfig([]byte(doc)); !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("error = %v, want invalid argument", err)
			}
		})
	}
}

func TestValidateHubID(t *testing.T) {
	for _, id := range []string{"hub1", "a", "east-1", strings.Repeat("a", 40)} {
		if err := ValidateHubID(id); err != nil {
			t.Errorf("ValidateHubID(%q) = %v", id, err)
		}
	}
	for _, id := range []string{"", "Hub", "a.b", "-a", "a-", "a_b", strings.Repeat("a", 41), "a/b"} {
		if err := ValidateHubID(id); !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("ValidateHubID(%q) = %v, want invalid argument", id, err)
		}
	}
	if got := ClusterIDFor("hub1", "ext"); got != "hub1.ext" || kubernetesaddon.ValidateClusterID(got) != nil {
		t.Errorf("ClusterIDFor = %q", got)
	}
	if got := ManagedClusterRecordName("hub1", "ext"); got != "hubs/hub1/managedClusters/ext" {
		t.Errorf("ManagedClusterRecordName = %q", got)
	}
}
