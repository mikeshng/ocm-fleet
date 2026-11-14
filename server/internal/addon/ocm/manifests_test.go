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
		if len(rules) != 1 {
			t.Fatalf("cluster role has %d rules, want 1", len(rules))
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

func buildAddon(t *testing.T, removeAccess bool) EnrollmentAddon {
	t.Helper()
	addon, err := BuildEnrollmentAddon(EnrollmentAddonOptions{
		Profile:               kubernetesaddon.AccessProfileWorkloads,
		RemoveAccessWithAddon: removeAccess,
		Endpoint:              testEndpoint(),
		BundleToken:           "eb-000000000000.secret",
		TokenLifetime:         2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("BuildEnrollmentAddon: %v", err)
	}
	return addon
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
	keep := strings.Join(EnrollmentAddonInstructions("hub1", false), "\n")
	remove := strings.Join(EnrollmentAddonInstructions("hub1", true), "\n")
	if !strings.Contains(keep, "stays enrolled") || !strings.Contains(remove, "is withdrawn") {
		t.Errorf("instructions do not say what disabling the add-on does")
	}
	if !strings.Contains(keep, "k8s-hub1.") {
		t.Errorf("instructions do not name the target clusters get")
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
