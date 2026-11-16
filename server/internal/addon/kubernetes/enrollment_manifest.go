package kubernetes

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// The access piece is everything an enrollment adds to a cluster: a
// namespace, the access service account, the bindings that decide what
// the resource manager may do, and a role that lets the account ask
// for its own token so that the credential can be renewed. Next to it
// an enrollment agent presents the account's first credential; the
// agent and its own objects can be removed once the cluster is
// enrolled. Every object is built from typed values, never from text.

// Names of the generated objects.
const (
	accessTokenRoleName    = AccessServiceAccount + "-token"
	accessClusterRoleName  = AccessServiceAccount + "-cluster"
	accessBindingName      = AccessServiceAccount
	enrollmentAgentName    = "ocm-fleet-enrollment-agent"
	enrollmentSecretName   = "ocm-fleet-enrollment"
	enrollmentJobName      = "ocm-fleet-enroll"
	enrollmentAgentPort    = 8081
	partOfLabel            = "app.kubernetes.io/part-of"
	partOfValue            = "ocm-fleet"
	componentLabel         = "app.kubernetes.io/component"
	componentAccess        = "access"
	componentEnrollment    = "enrollment-agent"
	enrollmentConfigVolume = "enrollment"
	enrollmentConfigPath   = "/etc/ocm-fleet-enrollment"
)

// AgentMode is how the enrollment agent runs.
type AgentMode string

const (
	// AgentModeJob presents once and exits. Used when someone applies
	// the manifest by hand.
	AgentModeJob AgentMode = "job"
	// AgentModeDeployment keeps presenting at a third of the token
	// lifetime, so a lapsed credential repairs itself. Used when an
	// installer keeps the piece in place.
	AgentModeDeployment AgentMode = "deployment"
)

// AccessPieceOptions describes the access piece to generate.
type AccessPieceOptions struct {
	// Namespace of the piece; empty means [AccessNamespace].
	Namespace string
	// Profile and ClusterRole decide the bindings.
	Profile     string
	ClusterRole string
	// AccessAnnotations are put on the objects that must outlive the
	// agent (namespace, account, roles, bindings). An installer uses
	// them to keep those objects when it removes the rest.
	AccessAnnotations map[string]string
	// AccessOnly leaves the agent out: for an enrollment by hand, where
	// a person requests the account's token and presents it.
	AccessOnly bool

	// Agent settings.
	Mode  AgentMode
	Image string
	// ServerURL, ServerCA, and ConnectAddress say where the agent
	// presents and how it verifies and reaches that endpoint.
	ServerURL      string
	ServerCA       []byte
	ConnectAddress string
	// BundleToken is the bearer value the agent presents.
	BundleToken string
	// ClusterName is the cluster's name within a verifier's scope.
	// It may be a placeholder an installer substitutes per cluster.
	ClusterName string
	// TokenLifetime of the credential the agent requests.
	TokenLifetime time.Duration
}

// AccessPiece is the generated set of objects, in apply order.
type AccessPiece struct {
	// Access outlives the agent: namespace, account, roles, bindings.
	Access []map[string]any
	// Agent can be removed once the cluster is enrolled.
	Agent []map[string]any
}

// All returns every object in apply order.
func (p AccessPiece) All() []map[string]any {
	return append(append([]map[string]any{}, p.Access...), p.Agent...)
}

// clusterReadRules is what the generated cluster role lets the access
// account read at cluster scope when it is not a cluster administrator.
// Secrets are deliberately absent.
func clusterReadRules() []rbacv1.PolicyRule {
	read := []string{"get", "list", "watch"}
	return []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"namespaces", "nodes", "persistentvolumes"}, Verbs: read},
		{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"storageclasses", "volumeattachments", "csidrivers", "csinodes"}, Verbs: read},
		{APIGroups: []string{"apiextensions.k8s.io"}, Resources: []string{"customresourcedefinitions"}, Verbs: read},
		{APIGroups: []string{"apiregistration.k8s.io"}, Resources: []string{"apiservices"}, Verbs: read},
		{APIGroups: []string{"scheduling.k8s.io"}, Resources: []string{"priorityclasses"}, Verbs: read},
		{APIGroups: []string{"node.k8s.io"}, Resources: []string{"runtimeclasses"}, Verbs: read},
		{APIGroups: []string{"networking.k8s.io"}, Resources: []string{"ingressclasses"}, Verbs: read},
		{APIGroups: []string{"rbac.authorization.k8s.io"}, Resources: []string{"clusterroles", "clusterrolebindings", "roles", "rolebindings"}, Verbs: read},
		{APIGroups: []string{"admissionregistration.k8s.io"}, Resources: []string{"*"}, Verbs: read},
		{APIGroups: []string{"flowcontrol.apiserver.k8s.io"}, Resources: []string{"*"}, Verbs: read},
		{APIGroups: []string{"certificates.k8s.io"}, Resources: []string{"certificatesigningrequests"}, Verbs: read},
	}
}

// BuildAccessPiece generates the access piece and its agent.
func BuildAccessPiece(opts AccessPieceOptions) (AccessPiece, error) {
	ns := opts.Namespace
	if ns == "" {
		ns = AccessNamespace
	}
	profile, clusterRole, err := NormalizeAccessProfile(opts.Profile, opts.ClusterRole, AccessProfileWorkloads)
	if err != nil {
		return AccessPiece{}, err
	}
	if !opts.AccessOnly {
		if opts.Image == "" {
			return AccessPiece{}, fmt.Errorf("%w: no enrollment agent image is configured on this resource manager", domain.ErrInvalidArgument)
		}
		if opts.ServerURL == "" {
			return AccessPiece{}, fmt.Errorf("%w: no enrollment server address is configured on this resource manager", domain.ErrInvalidArgument)
		}
		if opts.BundleToken == "" {
			return AccessPiece{}, fmt.Errorf("%w: bundle token is required", domain.ErrInvalidArgument)
		}
	}
	mode := opts.Mode
	if mode == "" {
		mode = AgentModeJob
	}
	lifetime := opts.TokenLifetime
	if lifetime == 0 {
		lifetime = DefaultTokenLifetime
	}

	accessMeta := func(name, namespace string) metav1.ObjectMeta {
		meta := metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{partOfLabel: partOfValue, componentLabel: componentAccess},
		}
		if len(opts.AccessAnnotations) > 0 {
			meta.Annotations = map[string]string{}
			for k, v := range opts.AccessAnnotations {
				meta.Annotations[k] = v
			}
		}
		return meta
	}
	agentMeta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{partOfLabel: partOfValue, componentLabel: componentEnrollment},
		}
	}
	accessSubject := []rbacv1.Subject{{Kind: "ServiceAccount", Name: AccessServiceAccount, Namespace: ns}}
	clusterBinding := func(name, role string) *rbacv1.ClusterRoleBinding {
		return &rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: accessMeta(name, ""),
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
			Subjects:   accessSubject,
		}
	}

	access := []runtime.Object{
		&corev1.Namespace{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
			ObjectMeta: accessMeta(ns, ""),
		},
		&corev1.ServiceAccount{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
			ObjectMeta: accessMeta(AccessServiceAccount, ns),
		},
		// The account may ask for its own token: this is what lets the
		// resource manager renew the credential with nothing running
		// on the cluster.
		&rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
			ObjectMeta: accessMeta(accessTokenRoleName, ns),
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"},
				ResourceNames: []string{AccessServiceAccount}, Verbs: []string{"create"},
			}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: accessMeta(accessTokenRoleName, ns),
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: accessTokenRoleName},
			Subjects:   accessSubject,
		},
	}
	clusterRules := func(extra ...rbacv1.PolicyRule) *rbacv1.ClusterRole {
		return &rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: accessMeta(accessClusterRoleName, ""),
			Rules:      append(clusterReadRules(), extra...),
		}
	}
	switch profile {
	case AccessProfileClusterAdmin:
		access = append(access, clusterBinding(accessBindingName+"-cluster-admin", "cluster-admin"))
	case AccessProfileWorkloads:
		// A deployment may bring its namespace, and a removal deletes
		// what the deployment applied: the right to create namespaces
		// comes with the right to delete them.
		access = append(access,
			clusterBinding(accessBindingName+"-edit", "edit"),
			clusterRules(rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"create", "update", "patch", "delete"}}),
			clusterBinding(accessClusterRoleName, accessClusterRoleName),
		)
	case AccessProfileReadOnly:
		access = append(access,
			clusterBinding(accessBindingName+"-view", "view"),
			clusterRules(),
			clusterBinding(accessClusterRoleName, accessClusterRoleName),
		)
	case AccessProfileCustom:
		access = append(access, clusterBinding(accessBindingName+"-custom", clusterRole))
	}

	var piece AccessPiece
	if piece.Access, err = toManifestObjects(access); err != nil {
		return AccessPiece{}, err
	}
	if opts.AccessOnly {
		return piece, nil
	}

	args := []string{
		"--server-url=" + opts.ServerURL,
		"--bundle-token-file=" + enrollmentConfigPath + "/token",
		"--namespace=$(POD_NAMESPACE)",
		"--service-account=" + AccessServiceAccount,
		"--token-lifetime=" + lifetime.String(),
		"--health-addr=:" + strconv.Itoa(enrollmentAgentPort),
	}
	secretData := map[string]string{"token": opts.BundleToken}
	if len(opts.ServerCA) > 0 {
		secretData["ca.crt"] = string(opts.ServerCA)
		args = append(args, "--server-ca-file="+enrollmentConfigPath+"/ca.crt")
	}
	if opts.ConnectAddress != "" {
		args = append(args, "--connect-address="+opts.ConnectAddress)
	}
	if opts.ClusterName != "" {
		args = append(args, "--cluster-name="+opts.ClusterName)
	}
	if mode == AgentModeJob {
		args = append(args, "--once")
	}

	container := corev1.Container{
		Name:            "enrollment-agent",
		Image:           opts.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args:            args,
		Env: []corev1.EnvVar{{
			Name:      "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
		}},
		VolumeMounts: []corev1.VolumeMount{{Name: enrollmentConfigVolume, MountPath: enrollmentConfigPath, ReadOnly: true}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("16Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			RunAsNonRoot:             ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}
	podSpec := corev1.PodSpec{
		ServiceAccountName: enrollmentAgentName,
		Containers:         []corev1.Container{container},
		Volumes: []corev1.Volume{{
			Name:         enrollmentConfigVolume,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: enrollmentSecretName}},
		}},
	}
	podLabels := map[string]string{partOfLabel: partOfValue, componentLabel: componentEnrollment}

	agent := []runtime.Object{
		&corev1.ServiceAccount{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
			ObjectMeta: agentMeta(enrollmentAgentName),
		},
		// The agent may request a token for the access account and
		// nothing else.
		&rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
			ObjectMeta: agentMeta(enrollmentAgentName),
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"},
				ResourceNames: []string{AccessServiceAccount}, Verbs: []string{"create"},
			}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: agentMeta(enrollmentAgentName),
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: enrollmentAgentName},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: enrollmentAgentName, Namespace: ns}},
		},
		&corev1.Secret{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: agentMeta(enrollmentSecretName),
			Type:       corev1.SecretTypeOpaque,
			StringData: secretData,
		},
	}
	switch mode {
	case AgentModeJob:
		podSpec.RestartPolicy = corev1.RestartPolicyOnFailure
		agent = append(agent, &batchv1.Job{
			TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
			ObjectMeta: agentMeta(enrollmentJobName),
			Spec: batchv1.JobSpec{
				BackoffLimit: ptr.To(int32(10)),
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					Spec:       podSpec,
				},
			},
		})
	case AgentModeDeployment:
		probe := &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromInt32(enrollmentAgentPort)}},
			PeriodSeconds:    5,
			FailureThreshold: 3,
		}
		podSpec.Containers[0].ReadinessProbe = probe
		podSpec.Containers[0].Ports = []corev1.ContainerPort{{Name: "health", ContainerPort: enrollmentAgentPort}}
		agent = append(agent, &appsv1.Deployment{
			TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
			ObjectMeta: agentMeta(enrollmentAgentName),
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(1)),
				Selector: &metav1.LabelSelector{MatchLabels: podLabels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
					Spec:       podSpec,
				},
			},
		})
	default:
		return AccessPiece{}, fmt.Errorf("%w: unknown agent mode %q", domain.ErrInvalidArgument, mode)
	}

	if piece.Agent, err = toManifestObjects(agent); err != nil {
		return AccessPiece{}, err
	}
	return piece, nil
}

// toManifestObjects converts typed objects to the plain maps a
// manifest is written from, without the empty fields typed objects
// carry (creation time, status).
func toManifestObjects(objs []runtime.Object) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(objs))
	for _, obj := range objs {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return nil, fmt.Errorf("convert %T: %w", obj, err)
		}
		pruneManifestObject(m)
		out = append(out, m)
	}
	return out, nil
}

func pruneManifestObject(m map[string]any) {
	delete(m, "status")
	if meta, ok := m["metadata"].(map[string]any); ok {
		delete(meta, "creationTimestamp")
	}
	if spec, ok := m["spec"].(map[string]any); ok {
		if tmpl, ok := spec["template"].(map[string]any); ok {
			if meta, ok := tmpl["metadata"].(map[string]any); ok {
				delete(meta, "creationTimestamp")
			}
		}
		if strategy, ok := spec["strategy"].(map[string]any); ok && len(strategy) == 0 {
			delete(spec, "strategy")
		}
		if len(spec) == 0 {
			delete(m, "spec")
		}
	}
}

// RenderManifestYAML writes objects as one multi-document YAML stream.
func RenderManifestYAML(objs []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	for i, obj := range objs {
		if i > 0 {
			buf.WriteString("---\n")
		}
		data, err := yaml.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("render manifest: %w", err)
		}
		buf.Write(data)
	}
	return buf.Bytes(), nil
}
