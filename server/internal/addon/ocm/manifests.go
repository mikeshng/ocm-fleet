package ocm

import (
	"fmt"
	"time"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// Names of what the integration asks a hub's administrator to apply.
const (
	// HubReaderNamespace and HubReaderServiceAccount name the account
	// whose token connects a hub. It can read clusters, nothing else.
	HubReaderNamespace      = kubernetesaddon.AccessNamespace
	HubReaderServiceAccount = "ocm-fleet-hub-reader"
	// hubReaderTokenDuration is the lifetime the instructions suggest
	// for the first token. The integration renews it by itself.
	hubReaderTokenDuration = 30 * 24 * time.Hour

	// EnrollmentAddonName names the add-on, its template, and the
	// per-cluster object that enables it.
	EnrollmentAddonName = "ocm-fleet-enrollment"

	addonAPIVersion = "addon.open-cluster-management.io/v1alpha1"
	// addonLifecycleAnnotation hands the add-on's installation to the
	// hub's own add-on manager.
	addonLifecycleAnnotation = "addon.open-cluster-management.io/lifecycle"
	// addonOrphanAnnotation marks an object that the hub leaves on the
	// cluster when the add-on is removed.
	addonOrphanAnnotation = "addon.open-cluster-management.io/deletion-orphan"
	// addonClusterNameVariable is replaced by the hub, per cluster,
	// with the cluster's name.
	addonClusterNameVariable = "{{CLUSTER_NAME}}"
)

func partOfLabels() map[string]any {
	return map[string]any{"app.kubernetes.io/part-of": "ocm-fleet"}
}

// HubAccessManifest returns what a hub's administrator applies before
// connecting the hub: a service account that may read the hub's
// clusters and cluster sets and may ask for its own token (so that the
// credential can be renewed), and nothing else.
func HubAccessManifest() []map[string]any {
	meta := func(name, namespace string) map[string]any {
		m := map[string]any{"name": name, "labels": partOfLabels()}
		if namespace != "" {
			m["namespace"] = namespace
		}
		return m
	}
	subject := []any{map[string]any{"kind": "ServiceAccount", "name": HubReaderServiceAccount, "namespace": HubReaderNamespace}}
	tokenRole := HubReaderServiceAccount + "-token"
	return []map[string]any{
		{"apiVersion": "v1", "kind": "Namespace", "metadata": meta(HubReaderNamespace, "")},
		{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": meta(HubReaderServiceAccount, HubReaderNamespace)},
		{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
			"metadata": meta(HubReaderServiceAccount, ""),
			"rules": []any{map[string]any{
				"apiGroups": []any{clusterAPIGroup},
				"resources": []any{"managedclusters", "managedclustersets"},
				"verbs":     []any{"get", "list", "watch"},
			}},
		},
		{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
			"metadata": meta(HubReaderServiceAccount, ""),
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": HubReaderServiceAccount},
			"subjects": subject,
		},
		{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
			"metadata": meta(tokenRole, HubReaderNamespace),
			"rules": []any{map[string]any{
				"apiGroups":     []any{""},
				"resources":     []any{"serviceaccounts/token"},
				"resourceNames": []any{HubReaderServiceAccount},
				"verbs":         []any{"create"},
			}},
		},
		{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
			"metadata": meta(tokenRole, HubReaderNamespace),
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": tokenRole},
			"subjects": subject,
		},
	}
}

// HubAccessInstructions are the steps that go with [HubAccessManifest].
func HubAccessInstructions() []string {
	return []string{
		"Apply the manifest to the hub as its administrator: kubectl apply -f <file>",
		fmt.Sprintf("Request a credential for the account: kubectl -n %s create token %s --duration=%s",
			HubReaderNamespace, HubReaderServiceAccount, hubReaderTokenDuration),
		"Connect the hub with its API address, its CA certificate, and that credential (POST " + APIPrefix + "/hubs:connect). " +
			"The resource manager renews the credential by itself and writes nothing to the hub.",
		fmt.Sprintf("To end the connection from the hub's side, delete namespace %s and the cluster role and binding named %s.",
			HubReaderNamespace, HubReaderServiceAccount),
	}
}

// EnrollmentAddonOptions describes the add-on to render.
type EnrollmentAddonOptions struct {
	// Profile and ClusterRole decide what the resource manager may do
	// on the clusters enrolled through the add-on.
	Profile     string
	ClusterRole string
	// RemoveAccessWithAddon makes disabling the add-on for a cluster
	// also remove the access piece, which withdraws the cluster. By
	// default the access piece stays, so that a cluster stays enrolled
	// when the add-on is removed and no agent keeps running for it.
	RemoveAccessWithAddon bool
	// Endpoint is where the agent presents; BundleToken what it shows.
	Endpoint      kubernetesaddon.EnrollmentEndpoint
	BundleToken   string
	TokenLifetime time.Duration
}

// EnrollmentAddon is the rendered add-on.
type EnrollmentAddon struct {
	// Hub is applied once to the hub: the add-on and its template.
	Hub []map[string]any
	// Enable is the object that enables the add-on for one cluster;
	// its namespace is a placeholder for the cluster's name.
	Enable map[string]any
}

// enablePlaceholder stands for a cluster's name in the example.
const enablePlaceholder = "<managed-cluster-name>"

// BuildEnrollmentAddon renders the enrollment add-on for a hub's own
// add-on mechanism: the hub installs the access piece and the agent on
// every cluster the add-on is enabled for, and the agent presents the
// cluster to the resource manager. The integration applies none of it.
func BuildEnrollmentAddon(opts EnrollmentAddonOptions) (EnrollmentAddon, error) {
	var keep map[string]string
	if !opts.RemoveAccessWithAddon {
		keep = map[string]string{addonOrphanAnnotation: ""}
	}
	piece, err := kubernetesaddon.BuildAccessPiece(kubernetesaddon.AccessPieceOptions{
		Profile:           opts.Profile,
		ClusterRole:       opts.ClusterRole,
		AccessAnnotations: keep,
		Mode:              kubernetesaddon.AgentModeDeployment,
		Image:             opts.Endpoint.AgentImage,
		ServerURL:         opts.Endpoint.ServerURL,
		ServerCA:          opts.Endpoint.ServerCA,
		ConnectAddress:    opts.Endpoint.ConnectAddress,
		BundleToken:       opts.BundleToken,
		ClusterName:       addonClusterNameVariable,
		TokenLifetime:     opts.TokenLifetime,
	})
	if err != nil {
		return EnrollmentAddon{}, err
	}
	manifests := make([]any, 0, len(piece.Access)+len(piece.Agent))
	for _, obj := range piece.All() {
		manifests = append(manifests, obj)
	}
	if len(manifests) == 0 {
		return EnrollmentAddon{}, fmt.Errorf("%w: the access piece is empty", domain.ErrInvalidArgument)
	}

	template := map[string]any{
		"apiVersion": addonAPIVersion,
		"kind":       "AddOnTemplate",
		"metadata":   map[string]any{"name": EnrollmentAddonName, "labels": partOfLabels()},
		"spec": map[string]any{
			"addonName": EnrollmentAddonName,
			"agentSpec": map[string]any{
				"workload": map[string]any{"manifests": manifests},
			},
		},
	}
	addon := map[string]any{
		"apiVersion": addonAPIVersion,
		"kind":       "ClusterManagementAddOn",
		"metadata": map[string]any{
			"name":        EnrollmentAddonName,
			"labels":      partOfLabels(),
			"annotations": map[string]any{addonLifecycleAnnotation: "addon-manager"},
		},
		"spec": map[string]any{
			"addOnMeta": map[string]any{
				"displayName": "OCM Fleet enrollment",
				"description": "Enrolls the cluster with an OCM Fleet resource manager: adds an access account and presents its credential.",
			},
			"supportedConfigs": []any{map[string]any{
				"group":         "addon.open-cluster-management.io",
				"resource":      "addontemplates",
				"defaultConfig": map[string]any{"name": EnrollmentAddonName},
			}},
			"installStrategy": map[string]any{"type": "Manual"},
		},
	}
	enable := map[string]any{
		"apiVersion": addonAPIVersion,
		"kind":       "ManagedClusterAddOn",
		"metadata":   map[string]any{"name": EnrollmentAddonName, "namespace": enablePlaceholder},
		"spec":       map[string]any{},
	}
	return EnrollmentAddon{Hub: []map[string]any{template, addon}, Enable: enable}, nil
}

// EnrollmentAddonInstructions are the steps that go with the add-on.
func EnrollmentAddonInstructions(hubID string, removeAccessWithAddon bool) []string {
	out := []string{
		"Apply the manifest to the hub as its administrator: kubectl apply -f <file>. It adds one AddOnTemplate and one ClusterManagementAddOn; nothing changes on any cluster yet.",
		fmt.Sprintf("Enable the add-on for a cluster by creating the ManagedClusterAddOn %q in that cluster's namespace on the hub (see enableExample), "+
			"or for many clusters at once by giving the ClusterManagementAddOn an install strategy of type Placements.", EnrollmentAddonName),
		fmt.Sprintf("Each cluster is then enrolled as clusters/%s with target %s, and carries the hub's labels for it.",
			ClusterIDFor(hubID, enablePlaceholder), kubernetesaddon.TargetIDForCluster(ClusterIDFor(hubID, enablePlaceholder))),
	}
	if removeAccessWithAddon {
		out = append(out, "Disabling the add-on for a cluster removes the access account with it: the cluster is withdrawn.")
	} else {
		out = append(out,
			"Disabling the add-on for a cluster removes the agent and leaves the access account: the cluster stays enrolled and the resource manager renews its credential by itself.",
			fmt.Sprintf("To withdraw a cluster from its own side, delete namespace %s and the cluster role bindings labeled app.kubernetes.io/part-of=ocm-fleet on it.",
				kubernetesaddon.AccessNamespace))
	}
	return append(out, "The manifest carries an enrollment secret. It lets only clusters that this hub has accepted, and whose CA matches the hub's record, present themselves; it is not a cluster credential. Keep it as you keep the hub's other secrets.")
}
