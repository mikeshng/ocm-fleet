package kubernetes

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// clusterProbeTimeout bounds every request a probe makes to a cluster.
const clusterProbeTimeout = 15 * time.Second

// clusterIdentityNamespace is the namespace whose UID identifies a
// cluster apart from the address it is reached at.
const clusterIdentityNamespace = "kube-system"

// serviceAccountUserPrefix is the user name prefix Kubernetes gives a
// service account: system:serviceaccount:<namespace>:<name>.
const serviceAccountUserPrefix = "system:serviceaccount:"

// ErrClusterRejectedCredential means the cluster answered a probe or a
// renewal with 401: it does not recognize the credential (any more).
// It is distinct from a cluster that cannot be reached.
var ErrClusterRejectedCredential = errors.New("cluster rejected the credential")

// ErrClusterDeniedRequest means the cluster recognized the credential
// but refused the request (403).
var ErrClusterDeniedRequest = errors.New("cluster denied the request")

// ClusterConnection is what is needed to talk to an existing cluster:
// its API address, the CA that signs its serving certificate, and a
// bearer token the cluster issued. The CA is the only trust root used.
type ClusterConnection struct {
	APIServer string
	CACert    []byte
	Token     string
}

// ServiceAccountRef names a service account on a cluster.
type ServiceAccountRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// String returns "<namespace>/<name>", or "" for the zero value.
func (r ServiceAccountRef) String() string {
	if r.Namespace == "" && r.Name == "" {
		return ""
	}
	return r.Namespace + "/" + r.Name
}

// ParseServiceAccountRef parses "<namespace>/<name>".
func ParseServiceAccountRef(s string) (ServiceAccountRef, bool) {
	ns, name, ok := strings.Cut(s, "/")
	if !ok || ns == "" || name == "" || strings.Contains(name, "/") {
		return ServiceAccountRef{}, false
	}
	return ServiceAccountRef{Namespace: ns, Name: name}, true
}

// ClusterRights is what a credential may do on its cluster, as the
// cluster itself reports it. It is recorded so that a person can see
// what an enrollment granted without reading the cluster's RBAC.
type ClusterRights struct {
	// ClusterAdmin: every verb on every resource.
	ClusterAdmin bool `json:"clusterAdmin"`
	// Workloads: may create Deployments in a namespace.
	Workloads bool `json:"workloads"`
	// Namespaces: may create namespaces.
	Namespaces bool `json:"namespaces"`
	// ReadCluster: may list namespaces and nodes.
	ReadCluster bool `json:"readCluster"`
	// CanRenew: may request a new token for its own service account.
	CanRenew bool `json:"canRenew"`
}

// Profile names the closest declared access profile for the rights.
func (r ClusterRights) Profile() string {
	switch {
	case r.ClusterAdmin:
		return AccessProfileClusterAdmin
	case r.Workloads:
		return AccessProfileWorkloads
	case r.ReadCluster:
		return AccessProfileReadOnly
	default:
		return "none"
	}
}

// ClusterProbeResult is what a cluster says about a credential.
type ClusterProbeResult struct {
	// Username is the identity the cluster maps the credential to.
	Username string
	// ServiceAccount is set when Username is a service account.
	ServiceAccount ServiceAccountRef
	// KubernetesVersion is the cluster's reported git version.
	KubernetesVersion string
	// ClusterUID identifies the cluster itself: the UID of its
	// kube-system namespace. It is the same at every address the
	// cluster is reached at. Empty when the credential may not read it.
	ClusterUID string
	// Rights is what the credential may do.
	Rights ClusterRights
	// TokenIssuedAt and TokenExpiresAt come from the token's own
	// claims; both are zero when the token is not a JWT.
	TokenIssuedAt  time.Time
	TokenExpiresAt time.Time
}

// ClusterProber checks a credential against its cluster and renews it.
// The production implementation is [ClientGoClusterProber]; tests use a
// fake.
type ClusterProber interface {
	// Probe asks the cluster who the credential is, which version it
	// runs, and what the credential may do.
	Probe(ctx context.Context, conn ClusterConnection) (ClusterProbeResult, error)
	// Identity asks only who the credential is. It is the cheap check
	// used to notice that a cluster stopped recognizing a credential.
	Identity(ctx context.Context, conn ClusterConnection) (string, error)
	// RenewToken asks the cluster for a new token for the service
	// account, authenticating with the token in conn.
	RenewToken(ctx context.Context, conn ClusterConnection, sa ServiceAccountRef, lifetime time.Duration) (token string, expiresAt time.Time, err error)
}

// ClientGoClusterProber is the [ClusterProber] that talks to real
// clusters.
type ClientGoClusterProber struct {
	// Egress decides which addresses the prober may dial. The zero
	// value applies the default refusals; see [EgressPolicy].
	Egress EgressPolicy
}

var _ ClusterProber = ClientGoClusterProber{}

func (p ClientGoClusterProber) client(conn ClusterConnection) (kubeclient.Interface, error) {
	if conn.APIServer == "" {
		return nil, fmt.Errorf("%w: cluster API address is required", domain.ErrInvalidArgument)
	}
	if len(conn.CACert) == 0 {
		return nil, fmt.Errorf("%w: cluster CA certificate is required", domain.ErrInvalidArgument)
	}
	if conn.Token == "" {
		return nil, fmt.Errorf("%w: cluster credential is required", domain.ErrInvalidArgument)
	}
	cfg := &rest.Config{
		Host:            conn.APIServer,
		BearerToken:     conn.Token,
		TLSClientConfig: rest.TLSClientConfig{CAData: conn.CACert},
		Timeout:         clusterProbeTimeout,
	}
	p.Egress.DialConfig(cfg)
	client, err := kubeclient.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build cluster client: %w", err)
	}
	return client, nil
}

// Identity implements [ClusterProber].
func (p ClientGoClusterProber) Identity(ctx context.Context, conn ClusterConnection) (string, error) {
	client, err := p.client(conn)
	if err != nil {
		return "", err
	}
	return selfIdentity(ctx, client)
}

func selfIdentity(ctx context.Context, client kubeclient.Interface) (string, error) {
	review, err := client.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return "", classifyClusterError("ask the cluster who the credential is", err)
	}
	if review.Status.UserInfo.Username == "" {
		return "", fmt.Errorf("cluster returned no identity for the credential")
	}
	return review.Status.UserInfo.Username, nil
}

// Probe implements [ClusterProber].
func (p ClientGoClusterProber) Probe(ctx context.Context, conn ClusterConnection) (ClusterProbeResult, error) {
	client, err := p.client(conn)
	if err != nil {
		return ClusterProbeResult{}, err
	}
	username, err := selfIdentity(ctx, client)
	if err != nil {
		return ClusterProbeResult{}, err
	}
	out := ClusterProbeResult{Username: username}
	out.ServiceAccount, _ = ServiceAccountFromUsername(username)
	out.TokenIssuedAt, out.TokenExpiresAt = TokenLifetime(conn.Token)

	version, err := client.Discovery().ServerVersion()
	if err != nil {
		return ClusterProbeResult{}, classifyClusterError("read the cluster version", err)
	}
	out.KubernetesVersion = version.GitVersion

	// What identifies the cluster apart from its address. A credential
	// that may not read it leaves the cluster without one.
	ns, err := client.CoreV1().Namespaces().Get(ctx, clusterIdentityNamespace, metav1.GetOptions{})
	switch {
	case err == nil:
		out.ClusterUID = string(ns.UID)
	case apierrors.IsForbidden(err), apierrors.IsNotFound(err):
	default:
		return ClusterProbeResult{}, classifyClusterError("read the cluster's identifier", err)
	}

	can := func(attrs authorizationv1.ResourceAttributes) (bool, error) {
		review, err := client.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
			Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &attrs},
		}, metav1.CreateOptions{})
		if err != nil {
			return false, classifyClusterError("ask the cluster what the credential may do", err)
		}
		return review.Status.Allowed, nil
	}
	if out.Rights.ClusterAdmin, err = can(authorizationv1.ResourceAttributes{Verb: "*", Group: "*", Resource: "*"}); err != nil {
		return ClusterProbeResult{}, err
	}
	if out.Rights.Workloads, err = can(authorizationv1.ResourceAttributes{Namespace: "default", Verb: "create", Group: "apps", Resource: "deployments"}); err != nil {
		return ClusterProbeResult{}, err
	}
	if out.Rights.Namespaces, err = can(authorizationv1.ResourceAttributes{Verb: "create", Resource: "namespaces"}); err != nil {
		return ClusterProbeResult{}, err
	}
	listNamespaces, err := can(authorizationv1.ResourceAttributes{Verb: "list", Resource: "namespaces"})
	if err != nil {
		return ClusterProbeResult{}, err
	}
	listNodes, err := can(authorizationv1.ResourceAttributes{Verb: "list", Resource: "nodes"})
	if err != nil {
		return ClusterProbeResult{}, err
	}
	out.Rights.ReadCluster = listNamespaces && listNodes
	if sa := out.ServiceAccount; sa.Name != "" {
		if out.Rights.CanRenew, err = can(authorizationv1.ResourceAttributes{
			Namespace: sa.Namespace, Verb: "create", Resource: "serviceaccounts", Subresource: "token", Name: sa.Name,
		}); err != nil {
			return ClusterProbeResult{}, err
		}
	}
	return out, nil
}

// RenewToken implements [ClusterProber].
func (p ClientGoClusterProber) RenewToken(ctx context.Context, conn ClusterConnection, sa ServiceAccountRef, lifetime time.Duration) (string, time.Time, error) {
	if sa.Namespace == "" || sa.Name == "" {
		return "", time.Time{}, fmt.Errorf("%w: service account is required to renew a token", domain.ErrInvalidArgument)
	}
	client, err := p.client(conn)
	if err != nil {
		return "", time.Time{}, err
	}
	req := &authenticationv1.TokenRequest{}
	if lifetime > 0 {
		req.Spec.ExpirationSeconds = ptr.To(int64(lifetime / time.Second))
	}
	resp, err := client.CoreV1().ServiceAccounts(sa.Namespace).CreateToken(ctx, sa.Name, req, metav1.CreateOptions{})
	if err != nil {
		return "", time.Time{}, classifyClusterError("request a new token", err)
	}
	if resp.Status.Token == "" {
		return "", time.Time{}, fmt.Errorf("cluster returned an empty token for %s", sa)
	}
	return resp.Status.Token, resp.Status.ExpirationTimestamp.Time, nil
}

// classifyClusterError maps the cluster's 401 and 403 onto the two
// sentinel errors callers act on and leaves everything else (network,
// TLS, server errors) as it is.
func classifyClusterError(action string, err error) error {
	switch {
	case apierrors.IsUnauthorized(err):
		return fmt.Errorf("%s: %w: %v", action, ErrClusterRejectedCredential, err)
	case apierrors.IsForbidden(err):
		return fmt.Errorf("%s: %w: %v", action, ErrClusterDeniedRequest, err)
	default:
		return fmt.Errorf("%s: %w", action, err)
	}
}

// ServiceAccountFromUsername parses a Kubernetes service account user
// name (system:serviceaccount:<namespace>:<name>).
func ServiceAccountFromUsername(username string) (ServiceAccountRef, bool) {
	rest, ok := strings.CutPrefix(username, serviceAccountUserPrefix)
	if !ok {
		return ServiceAccountRef{}, false
	}
	ns, name, ok := strings.Cut(rest, ":")
	if !ok || ns == "" || name == "" || strings.Contains(name, ":") {
		return ServiceAccountRef{}, false
	}
	return ServiceAccountRef{Namespace: ns, Name: name}, true
}

// TokenLifetime reads the issued-at and expiry claims of a JWT without
// verifying it. The cluster is the verifier; this only tells the
// resource manager when to renew. Both results are zero for a token
// that is not a JWT or carries no expiry.
func TokenLifetime(token string) (issuedAt, expiresAt time.Time) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, time.Time{}
	}
	var claims struct {
		Iat *float64 `json:"iat"`
		Exp *float64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == nil {
		return time.Time{}, time.Time{}
	}
	expiresAt = time.Unix(int64(*claims.Exp), 0).UTC()
	if claims.Iat != nil {
		issuedAt = time.Unix(int64(*claims.Iat), 0).UTC()
	}
	return issuedAt, expiresAt
}
