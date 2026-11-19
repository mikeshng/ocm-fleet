package ocm

import (
	"fmt"
	"regexp"
	"time"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// Names of what the integration asks a hub's administrator to apply.
const (
	// HubReaderNamespace and HubReaderServiceAccount name the account
	// whose token connects a hub. It can read clusters and the hub's
	// own identifier, nothing else. The namespace holds nothing but
	// that account: on a hub that is registered with itself, the access
	// piece of the hub's own cluster lives in another namespace, so
	// that removing either one leaves the other in place.
	HubReaderNamespace      = "ocm-fleet-hub-connection"
	HubReaderServiceAccount = "ocm-fleet-hub-reader"
	// hubIdentityNamespace is the namespace whose UID identifies the
	// cluster a hub runs on, whatever address the hub is reached at.
	hubIdentityNamespace = "kube-system"
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

	// RolloutNamespace and RolloutPlacementName name what the add-on
	// manifest adds to a hub for the rollout "every-cluster": a
	// namespace that holds a binding of the cluster set every cluster
	// of a hub is in, and the Placement that selects them all.
	RolloutNamespace     = "ocm-fleet-enrollment"
	RolloutPlacementName = "every-cluster"
	rolloutClusterSet    = "global"
)

// Taints a hub puts on a cluster that stops answering. The Placement
// for "every-cluster" tolerates them: a cluster that is away is still a
// cluster of the hub, and dropping it would disable the add-on for it.
var rolloutToleratedTaints = []string{
	"cluster.open-cluster-management.io/unavailable",
	"cluster.open-cluster-management.io/unreachable",
}

var (
	kubeNamespacePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	kubeNamePattern      = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)
)

// ResolveRollout returns the rollout of a new add-on: the requested
// one, or, when the request names none, the one of the add-on generated
// before for the same hub, or "manual". A manifest that is made again,
// to replace the secret for one, then keeps what the hub was told
// before.
func ResolveRollout(requested, previous *EnrollmentRollout) (EnrollmentRollout, error) {
	if requested == nil {
		requested = &EnrollmentRollout{}
	}
	// A rollout that was resolved before carries the names of the
	// generated Placement; it resolves to itself.
	named := requested.PlacementNamespace != "" || requested.PlacementName != ""
	generated := requested.Mode == RolloutEveryCluster &&
		requested.PlacementNamespace == RolloutNamespace && requested.PlacementName == RolloutPlacementName
	if requested.Mode != RolloutPlacement && named && !generated {
		return EnrollmentRollout{}, fmt.Errorf("%w: a Placement can only be named with rollout %q", domain.ErrInvalidArgument, RolloutPlacement)
	}
	switch requested.Mode {
	case "":
		if previous != nil {
			return *previous, nil
		}
		return EnrollmentRollout{Mode: RolloutManual}, nil
	case RolloutManual:
		return EnrollmentRollout{Mode: RolloutManual}, nil
	case RolloutEveryCluster:
		return EnrollmentRollout{Mode: RolloutEveryCluster, PlacementNamespace: RolloutNamespace, PlacementName: RolloutPlacementName}, nil
	case RolloutPlacement:
		if !kubeNamespacePattern.MatchString(requested.PlacementNamespace) || !kubeNamePattern.MatchString(requested.PlacementName) {
			return EnrollmentRollout{}, fmt.Errorf("%w: rollout %q needs the namespace and the name of a Placement on the hub", domain.ErrInvalidArgument, RolloutPlacement)
		}
		return *requested, nil
	default:
		return EnrollmentRollout{}, fmt.Errorf("%w: unknown rollout %q (use %s, %s, or %s)", domain.ErrInvalidArgument,
			requested.Mode, RolloutManual, RolloutEveryCluster, RolloutPlacement)
	}
}

func partOfLabels() map[string]any {
	return map[string]any{"app.kubernetes.io/part-of": "ocm-fleet"}
}

// hubReaderLabels mark what connects a hub. They differ from the labels
// of a cluster's access piece: an administrator who removes the access
// piece of a hub's own cluster by its label must not remove the hub's
// connection with it.
func hubReaderLabels() map[string]any {
	return map[string]any{"app.kubernetes.io/part-of": "ocm-fleet-hub-connection"}
}

// HubAccessManifest returns what a hub's administrator applies before
// connecting the hub: a service account that may read the hub's
// clusters and cluster sets, may read the one namespace whose UID
// identifies the hub, and may ask for its own token (so that the
// credential can be renewed), and nothing else.
func HubAccessManifest() []map[string]any {
	meta := func(name, namespace string) map[string]any {
		m := map[string]any{"name": name, "labels": hubReaderLabels()}
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
			"rules": []any{
				map[string]any{
					"apiGroups": []any{clusterAPIGroup},
					"resources": []any{"managedclusters", "managedclustersets"},
					"verbs":     []any{"get", "list", "watch"},
				},
				map[string]any{
					"apiGroups":     []any{""},
					"resources":     []any{"namespaces"},
					"resourceNames": []any{hubIdentityNamespace},
					"verbs":         []any{"get"},
				},
			},
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
	// Rollout says which clusters of the hub get the add-on; see
	// [ResolveRollout]. The zero value is "manual".
	Rollout EnrollmentRollout
}

// EnrollmentAddon is the rendered add-on.
type EnrollmentAddon struct {
	// Hub is applied once to the hub: the add-on and its template,
	// preceded for the rollout "every-cluster" by the namespace, the
	// cluster set binding, and the Placement the add-on follows.
	Hub []map[string]any
	// Enable is the object that enables the add-on for one cluster;
	// its namespace is a placeholder for the cluster's name. It is nil
	// unless the rollout is "manual": an add-on that follows a
	// Placement is removed again from a cluster that Placement does not
	// select.
	Enable map[string]any
	// Rollout is the rollout the add-on was rendered for.
	Rollout EnrollmentRollout
}

// enablePlaceholder stands for a cluster's name in the example.
const enablePlaceholder = "<managed-cluster-name>"

// BuildEnrollmentAddon renders the enrollment add-on for a hub's own
// add-on mechanism: the hub installs the access piece and the agent on
// every cluster the add-on is enabled for, and the agent presents the
// cluster to the resource manager. The integration applies none of it.
func BuildEnrollmentAddon(opts EnrollmentAddonOptions) (EnrollmentAddon, error) {
	rollout, err := ResolveRollout(&opts.Rollout, nil)
	if err != nil {
		return EnrollmentAddon{}, err
	}
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
	install := map[string]any{"type": "Manual"}
	if rollout.Mode != RolloutManual {
		install = map[string]any{
			"type":       "Placements",
			"placements": []any{map[string]any{"namespace": rollout.PlacementNamespace, "name": rollout.PlacementName}},
		}
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
			"installStrategy": install,
		},
	}
	out := EnrollmentAddon{Hub: []map[string]any{template, addon}, Rollout: rollout}
	switch rollout.Mode {
	case RolloutManual:
		out.Enable = map[string]any{
			"apiVersion": addonAPIVersion,
			"kind":       "ManagedClusterAddOn",
			"metadata":   map[string]any{"name": EnrollmentAddonName, "namespace": enablePlaceholder},
			"spec":       map[string]any{},
		}
	case RolloutEveryCluster:
		out.Hub = append(everyClusterObjects(), out.Hub...)
	}
	return out, nil
}

// everyClusterObjects are what makes a hub enable the add-on for every
// cluster it has: a namespace, in it a binding of the cluster set that
// holds all of the hub's clusters, and a Placement that selects that
// set and keeps a cluster that stopped answering.
func everyClusterObjects() []map[string]any {
	tolerations := make([]any, 0, len(rolloutToleratedTaints))
	for _, key := range rolloutToleratedTaints {
		tolerations = append(tolerations, map[string]any{"key": key, "operator": "Exists"})
	}
	return []map[string]any{
		{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": RolloutNamespace, "labels": partOfLabels()}},
		{
			"apiVersion": clusterAPIGroup + "/v1beta2", "kind": "ManagedClusterSetBinding",
			"metadata": map[string]any{"name": rolloutClusterSet, "namespace": RolloutNamespace, "labels": partOfLabels()},
			"spec":     map[string]any{"clusterSet": rolloutClusterSet},
		},
		{
			"apiVersion": clusterAPIGroup + "/v1beta1", "kind": "Placement",
			"metadata": map[string]any{"name": RolloutPlacementName, "namespace": RolloutNamespace, "labels": partOfLabels()},
			"spec": map[string]any{
				"clusterSets": []any{rolloutClusterSet},
				"tolerations": tolerations,
			},
		},
	}
}

// EnrollmentAddonInstructions are the steps that go with the add-on.
func EnrollmentAddonInstructions(hubID string, removeAccessWithAddon bool, rollout EnrollmentRollout) []string {
	var out []string
	switch rollout.Mode {
	case RolloutEveryCluster:
		out = []string{
			fmt.Sprintf("Apply the manifest to the hub as its administrator: kubectl apply -f <file>. It adds the add-on (one AddOnTemplate and one ClusterManagementAddOn) "+
				"and what makes the hub enable it for every cluster: namespace %s with a binding of the cluster set %s and the Placement %s.",
				RolloutNamespace, rolloutClusterSet, RolloutPlacementName),
			"The hub then enables the add-on for every cluster it has accepted, its own cluster included when that is registered with the hub, " +
				"and for every cluster that joins later. A cluster that stops answering stays selected.",
			fmt.Sprintf("To enroll only some of the hub's clusters, ask for the add-on with rollout %q and a Placement of your own, or with rollout %q.",
				RolloutPlacement, RolloutManual),
		}
	case RolloutPlacement:
		out = []string{
			"Apply the manifest to the hub as its administrator: kubectl apply -f <file>. It adds one AddOnTemplate and one ClusterManagementAddOn.",
			fmt.Sprintf("The add-on follows the Placement %s in namespace %s, which you keep on the hub together with a binding of its cluster sets in that namespace: "+
				"the hub enables the add-on for the clusters that Placement selects, and disables it for every other cluster, also where it was enabled by hand.",
				rollout.PlacementName, rollout.PlacementNamespace),
		}
	default:
		out = []string{
			"Apply the manifest to the hub as its administrator: kubectl apply -f <file>. It adds one AddOnTemplate and one ClusterManagementAddOn; nothing changes on any cluster yet.",
			fmt.Sprintf("Enable the add-on for a cluster by creating the ManagedClusterAddOn %q in that cluster's namespace on the hub (see enableExample). "+
				"To enroll every cluster of the hub at once, ask for the add-on with rollout %q.", EnrollmentAddonName, RolloutEveryCluster),
		}
	}
	out = append(out, fmt.Sprintf("Each cluster is then enrolled as clusters/%s with target %s, and carries the hub's labels for it.",
		ClusterIDFor(hubID, enablePlaceholder), kubernetesaddon.TargetIDForCluster(ClusterIDFor(hubID, enablePlaceholder))))
	if removeAccessWithAddon {
		out = append(out, "Disabling the add-on for a cluster removes the access account with it: the cluster is withdrawn.")
		if rollout.Mode != RolloutManual {
			out = append(out, "The hub disables the add-on for a cluster that the Placement stops selecting, and for a cluster that leaves the hub: such a cluster is withdrawn as well.")
		}
	} else {
		out = append(out,
			"Disabling the add-on for a cluster removes the agent and leaves the access account: the cluster stays enrolled and the resource manager renews its credential by itself.",
			fmt.Sprintf("To withdraw a cluster from its own side, delete namespace %s and the cluster role bindings labeled app.kubernetes.io/part-of=ocm-fleet on it.",
				kubernetesaddon.AccessNamespace))
	}
	return append(out,
		"The manifest carries an enrollment secret. It lets only clusters that this hub has accepted, and whose CA matches the hub's record, present themselves; it is not a cluster credential. Keep it as you keep the hub's other secrets.",
		"A manifest that is made again replaces that secret on every cluster the add-on is enabled for, and keeps this rollout unless another is asked for. One is needed after the hub was disconnected and connected again: the disconnect revokes the secret.")
}
