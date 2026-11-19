package ocm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// hubRequestTimeout bounds every single request the integration makes
// to a hub outside its watch.
const hubRequestTimeout = 15 * time.Second

const clusterAPIGroup = "cluster.open-cluster-management.io"

// managedClusterGVR is the one hub resource the integration watches.
var managedClusterGVR = schema.GroupVersionResource{Group: clusterAPIGroup, Version: "v1", Resource: "managedclusters"}

// HubProbe is what a hub says about a credential.
type HubProbe struct {
	// Username is the identity the hub maps the credential to;
	// ServiceAccount is set when that is a service account.
	Username       string
	ServiceAccount kubernetesaddon.ServiceAccountRef
	// ClusterUID identifies the cluster the hub runs on: the UID of its
	// kube-system namespace. It is the same at every address the hub is
	// reached at. Empty when the credential may not read it, as with an
	// account made before the integration asked for that right.
	ClusterUID string
	// CanRead: may list and watch ManagedCluster objects.
	CanRead bool
	// CanWrite: may change clusters or create work on the hub. The
	// integration needs neither.
	CanWrite bool
	// CanRenew: may request a new token for its own service account.
	CanRenew bool
	// TokenIssuedAt and TokenExpiresAt come from the token's claims.
	TokenIssuedAt  time.Time
	TokenExpiresAt time.Time
}

// HubClients is how the integration reaches hubs. The production
// implementation is [ClientGoHubClients]; tests substitute a fake.
type HubClients interface {
	// Probe asks the hub who the credential is and what it may do, and
	// checks that the hub serves the ManagedCluster API.
	Probe(ctx context.Context, conn HubConnection) (HubProbe, error)
	// Dynamic returns a client for the hub that authenticates each
	// request with the token the function returns at that moment, so
	// that a renewed credential takes effect without a new client.
	Dynamic(conn HubConnection, token func() string) (dynamic.Interface, error)
	// RenewToken asks the hub for a new token for the service account.
	RenewToken(ctx context.Context, conn HubConnection, sa kubernetesaddon.ServiceAccountRef, lifetime time.Duration) (string, time.Time, error)
}

// ClientGoHubClients is the [HubClients] that talks to real hubs.
type ClientGoHubClients struct {
	// Egress decides which addresses the integration may dial. The zero
	// value applies the default refusals; see
	// [kubernetesaddon.EgressPolicy].
	Egress kubernetesaddon.EgressPolicy
}

var _ HubClients = ClientGoHubClients{}

func (c ClientGoHubClients) restConfig(conn HubConnection) (*rest.Config, error) {
	if conn.APIServer == "" || conn.CACert == "" {
		return nil, fmt.Errorf("%w: hub API address and CA certificate are required", domain.ErrInvalidArgument)
	}
	cfg := &rest.Config{
		Host:            conn.APIServer,
		TLSClientConfig: rest.TLSClientConfig{CAData: []byte(conn.CACert)},
	}
	c.Egress.DialConfig(cfg)
	return cfg, nil
}

// Probe implements [HubClients].
func (c ClientGoHubClients) Probe(ctx context.Context, conn HubConnection) (HubProbe, error) {
	prober := kubernetesaddon.ClientGoClusterProber{Egress: c.Egress}
	username, err := prober.Identity(ctx, conn.clusterConnection())
	if err != nil {
		return HubProbe{}, err
	}
	out := HubProbe{Username: username}
	out.ServiceAccount, _ = kubernetesaddon.ServiceAccountFromUsername(username)
	out.TokenIssuedAt, out.TokenExpiresAt = kubernetesaddon.TokenLifetime(conn.Token)

	cfg, err := c.restConfig(conn)
	if err != nil {
		return HubProbe{}, err
	}
	cfg.BearerToken = conn.Token
	cfg.Timeout = hubRequestTimeout
	client, err := kubeclient.NewForConfig(cfg)
	if err != nil {
		return HubProbe{}, fmt.Errorf("build hub client: %w", err)
	}
	can := func(attrs authorizationv1.ResourceAttributes) (bool, error) {
		review, err := client.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
			Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &attrs},
		}, metav1.CreateOptions{})
		if err != nil {
			return false, fmt.Errorf("ask the hub what the credential may do: %w", err)
		}
		return review.Status.Allowed, nil
	}
	list, err := can(authorizationv1.ResourceAttributes{Verb: "list", Group: clusterAPIGroup, Resource: "managedclusters"})
	if err != nil {
		return HubProbe{}, err
	}
	watch, err := can(authorizationv1.ResourceAttributes{Verb: "watch", Group: clusterAPIGroup, Resource: "managedclusters"})
	if err != nil {
		return HubProbe{}, err
	}
	out.CanRead = list && watch
	for _, attrs := range []authorizationv1.ResourceAttributes{
		{Verb: "update", Group: clusterAPIGroup, Resource: "managedclusters"},
		{Verb: "delete", Group: clusterAPIGroup, Resource: "managedclusters"},
		{Verb: "create", Group: "work.open-cluster-management.io", Resource: "manifestworks"},
	} {
		allowed, err := can(attrs)
		if err != nil {
			return HubProbe{}, err
		}
		out.CanWrite = out.CanWrite || allowed
	}
	if sa := out.ServiceAccount; sa.Name != "" {
		if out.CanRenew, err = can(authorizationv1.ResourceAttributes{
			Namespace: sa.Namespace, Verb: "create", Resource: "serviceaccounts", Subresource: "token", Name: sa.Name,
		}); err != nil {
			return HubProbe{}, err
		}
	}

	// The API must exist: a cluster without it is not a hub.
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return HubProbe{}, fmt.Errorf("build hub client: %w", err)
	}
	if out.CanRead {
		if _, err := dyn.Resource(managedClusterGVR).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
			if apierrors.IsNotFound(err) {
				return HubProbe{}, errNotAHub
			}
			return HubProbe{}, fmt.Errorf("list ManagedCluster objects: %w", err)
		}
	}

	// What identifies the hub apart from its address. A credential that
	// may not read it leaves the hub without one.
	ns, err := client.CoreV1().Namespaces().Get(ctx, hubIdentityNamespace, metav1.GetOptions{})
	switch {
	case err == nil:
		out.ClusterUID = string(ns.UID)
	case apierrors.IsForbidden(err), apierrors.IsNotFound(err):
	default:
		return HubProbe{}, fmt.Errorf("read the hub's identifier: %w", err)
	}
	return out, nil
}

// errNotAHub: the cluster does not serve the ManagedCluster API.
var errNotAHub = errors.New("the cluster does not serve the ManagedCluster API")

// Dynamic implements [HubClients].
func (c ClientGoHubClients) Dynamic(conn HubConnection, token func() string) (dynamic.Interface, error) {
	cfg, err := c.restConfig(conn)
	if err != nil {
		return nil, err
	}
	cfg.WrapTransport = func(next http.RoundTripper) http.RoundTripper {
		return &bearerTransport{next: next, token: token}
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build hub client: %w", err)
	}
	return client, nil
}

// RenewToken implements [HubClients].
func (c ClientGoHubClients) RenewToken(ctx context.Context, conn HubConnection, sa kubernetesaddon.ServiceAccountRef, lifetime time.Duration) (string, time.Time, error) {
	return kubernetesaddon.ClientGoClusterProber{Egress: c.Egress}.RenewToken(ctx, conn.clusterConnection(), sa, lifetime)
}

// bearerTransport adds the current bearer token to each request.
type bearerTransport struct {
	next  http.RoundTripper
	token func() string
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", "Bearer "+t.token())
	return t.next.RoundTrip(out)
}

// ConnectionFromKubeconfig reads a hub connection out of a kubeconfig:
// the current context's server, its embedded CA, and its bearer token.
// Only a token is accepted as the credential. A client certificate or
// an exec plugin is refused: neither can be renewed by the resource
// manager, and an administrator's certificate is far more than the
// read access the integration needs.
func ConnectionFromKubeconfig(data []byte) (HubConnection, error) {
	invalid := func(format string, args ...any) (HubConnection, error) {
		return HubConnection{}, fmt.Errorf("%w: kubeconfig: %s", domain.ErrInvalidArgument, fmt.Sprintf(format, args...))
	}
	cfg, err := clientcmd.Load(data)
	if err != nil {
		return invalid("%v", err)
	}
	name := cfg.CurrentContext
	if name == "" && len(cfg.Contexts) == 1 {
		for only := range cfg.Contexts {
			name = only
		}
	}
	kctx, ok := cfg.Contexts[name]
	if !ok || kctx == nil {
		return invalid("no current context")
	}
	cluster, ok := cfg.Clusters[kctx.Cluster]
	if !ok || cluster == nil {
		return invalid("context %q names no cluster", name)
	}
	if cluster.InsecureSkipTLSVerify {
		return invalid("skipping TLS verification is not accepted; embed the hub's CA certificate")
	}
	if len(cluster.CertificateAuthorityData) == 0 {
		return invalid("the hub's CA certificate must be embedded (certificate-authority-data)")
	}
	user, ok := cfg.AuthInfos[kctx.AuthInfo]
	if !ok || user == nil || user.Token == "" {
		return invalid("the user must carry a bearer token of a service account; client certificates, token files, and exec plugins are not accepted")
	}
	return HubConnection{
		APIServer: cluster.Server,
		CACert:    string(cluster.CertificateAuthorityData),
		Token:     user.Token,
	}, nil
}
