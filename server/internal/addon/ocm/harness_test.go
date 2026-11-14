package ocm

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

// The tests run the integration against a real SQLite store and vault,
// the real enrollment service, a fake hub (client-go's fake dynamic
// client), and a fake cluster prober. Intervals are shortened so that
// a test waits milliseconds where production waits seconds.
func TestMain(m *testing.M) {
	hubTickInterval = 10 * time.Millisecond
	hubProbeInterval = 40 * time.Millisecond
	hubLinkInterval = 40 * time.Millisecond
	hubRenewRetryInterval = 40 * time.Millisecond
	os.Exit(m.Run())
}

// recordStore adapts the application's inventory report service, as
// server composition does.
type recordStore struct {
	reports *application.InventoryReportService
}

func (b *recordStore) ReplaceBatch(ctx context.Context, rt domain.ResourceType, reports []kubernetesaddon.InventoryObjectReport) error {
	in := application.InventoryReplacementBatchInput{Reports: make([]application.InventoryReplacementInput, len(reports))}
	for i, r := range reports {
		name := r.Name
		in.Reports[i] = application.InventoryReplacementInput{
			ResourceType: rt, Name: &name, IsDelete: r.IsDelete,
			Labels: r.Labels, Observation: r.Observation, Conditions: r.Conditions, ObservedAt: r.ObservedAt,
		}
	}
	return b.reports.ReplaceBatch(ctx, in)
}

func (b *recordStore) ListCollections(ctx context.Context, rt domain.ResourceType) ([]domain.CollectionName, error) {
	return b.reports.ListCollections(ctx, rt)
}

func (b *recordStore) PurgeCollectionPrefix(ctx context.Context, rt domain.ResourceType, prefix string) (int64, error) {
	return b.reports.PurgeCollectionPrefix(ctx, rt, prefix)
}

// fakeHub is one hub as the fake clients know it.
type fakeHub struct {
	ca      []byte
	client  *dynamicfake.FakeDynamicClient
	failing atomic.Bool

	mu      sync.Mutex
	tokens  map[string]HubProbe
	denyNew bool
	minted  int
}

type fakeHubClients struct {
	mu   sync.Mutex
	hubs map[string]*fakeHub
}

func (f *fakeHubClients) lookup(conn HubConnection) (*fakeHub, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	hub, ok := f.hubs[conn.APIServer]
	if !ok {
		return nil, fmt.Errorf("dial %s: no such host", conn.APIServer)
	}
	if !kubernetesaddon.SameCA([]byte(conn.CACert), hub.ca) {
		return nil, errors.New("x509: certificate signed by unknown authority")
	}
	return hub, nil
}

func (f *fakeHubClients) Probe(_ context.Context, conn HubConnection) (HubProbe, error) {
	hub, err := f.lookup(conn)
	if err != nil {
		return HubProbe{}, err
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	probe, ok := hub.tokens[conn.Token]
	if !ok {
		return HubProbe{}, fmt.Errorf("probe: %w", kubernetesaddon.ErrClusterRejectedCredential)
	}
	probe.TokenIssuedAt, probe.TokenExpiresAt = kubernetesaddon.TokenLifetime(conn.Token)
	return probe, nil
}

func (f *fakeHubClients) Dynamic(conn HubConnection, _ func() string) (dynamic.Interface, error) {
	hub, err := f.lookup(conn)
	if err != nil {
		return nil, err
	}
	return hub.client, nil
}

func (f *fakeHubClients) RenewToken(_ context.Context, conn HubConnection, _ kubernetesaddon.ServiceAccountRef, lifetime time.Duration) (string, time.Time, error) {
	hub, err := f.lookup(conn)
	if err != nil {
		return "", time.Time{}, err
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	probe, ok := hub.tokens[conn.Token]
	if !ok {
		return "", time.Time{}, fmt.Errorf("renew: %w", kubernetesaddon.ErrClusterRejectedCredential)
	}
	if hub.denyNew {
		return "", time.Time{}, fmt.Errorf("renew: %w", kubernetesaddon.ErrClusterDeniedRequest)
	}
	hub.minted++
	now := time.Now()
	token := makeToken(fmt.Sprintf("hub-renewed-%d", hub.minted), now, now.Add(lifetime))
	hub.tokens[token] = probe
	return token, now.Add(lifetime), nil
}

// fakeProber is the cluster side of enrollment: which credential each
// cluster recognizes.
type fakeProber struct {
	mu       sync.Mutex
	clusters map[string]*fakeCluster
}

type fakeCluster struct {
	ca     []byte
	tokens map[string]struct{}
}

func (p *fakeProber) check(conn kubernetesaddon.ClusterConnection) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.clusters[conn.APIServer]
	if !ok {
		return fmt.Errorf("dial %s: no such host", conn.APIServer)
	}
	if !kubernetesaddon.SameCA(conn.CACert, c.ca) {
		return errors.New("x509: certificate signed by unknown authority")
	}
	if _, ok := c.tokens[conn.Token]; !ok {
		return fmt.Errorf("probe: %w", kubernetesaddon.ErrClusterRejectedCredential)
	}
	return nil
}

var accessAccount = kubernetesaddon.ServiceAccountRef{
	Namespace: kubernetesaddon.AccessNamespace, Name: kubernetesaddon.AccessServiceAccount,
}

const accessUser = "system:serviceaccount:" + kubernetesaddon.AccessNamespace + ":" + kubernetesaddon.AccessServiceAccount

func (p *fakeProber) Probe(_ context.Context, conn kubernetesaddon.ClusterConnection) (kubernetesaddon.ClusterProbeResult, error) {
	if err := p.check(conn); err != nil {
		return kubernetesaddon.ClusterProbeResult{}, err
	}
	out := kubernetesaddon.ClusterProbeResult{
		Username:          accessUser,
		ServiceAccount:    accessAccount,
		KubernetesVersion: "v1.37.0",
		Rights:            kubernetesaddon.ClusterRights{Workloads: true, Namespaces: true, ReadCluster: true, CanRenew: true},
	}
	out.TokenIssuedAt, out.TokenExpiresAt = kubernetesaddon.TokenLifetime(conn.Token)
	return out, nil
}

func (p *fakeProber) Identity(_ context.Context, conn kubernetesaddon.ClusterConnection) (string, error) {
	if err := p.check(conn); err != nil {
		return "", err
	}
	return accessUser, nil
}

func (p *fakeProber) RenewToken(context.Context, kubernetesaddon.ClusterConnection, kubernetesaddon.ServiceAccountRef, time.Duration) (string, time.Time, error) {
	return "", time.Time{}, errors.New("not used in these tests")
}

// makeToken builds an unsigned JWT-shaped token with the given times.
func makeToken(id string, issuedAt, expiresAt time.Time) string {
	enc := func(v any) string {
		raw, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return enc(map[string]string{"alg": "none"}) + "." +
		enc(map[string]any{"jti": id, "iat": issuedAt.Unix(), "exp": expiresAt.Unix()}) + ".sig"
}

// testCA returns a self-signed CA certificate in PEM.
func testCA(t *testing.T, name string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

const (
	hubAPI   = "https://hub.test:6443"
	hubToken = "hub-reader-token"
)

var hubReader = kubernetesaddon.ServiceAccountRef{Namespace: HubReaderNamespace, Name: HubReaderServiceAccount}

type harness struct {
	t          *testing.T
	ctx        context.Context
	store      *sqlite.Store
	vault      *sqlite.VaultStore
	records    *recordStore
	clients    *fakeHubClients
	prober     *fakeProber
	enrollment *kubernetesaddon.EnrollmentService
	svc        *Service
	hub        *fakeHub
	hubCA      []byte
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	ctx, cancel := context.WithCancel(context.Background())

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for _, sch := range append(kubernetesaddon.Schemas(), Schemas()...) {
		def := domain.NewExtensionResourceType(sch.ResourceType, domain.APIVersion(sch.Version),
			domain.CollectionID(sch.CollectionID), time.Now(), domain.WithInventory())
		if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
			tx.RollbackOnExit()
			t.Fatalf("CreateType(%s): %v", sch.ResourceType, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	h := &harness{
		t:       t,
		ctx:     ctx,
		store:   store,
		vault:   &sqlite.VaultStore{DB: db},
		records: &recordStore{reports: application.NewInventoryReportService(store)},
		clients: &fakeHubClients{hubs: map[string]*fakeHub{}},
		prober:  &fakeProber{clusters: map[string]*fakeCluster{}},
		hubCA:   testCA(t, "hub"),
	}
	h.hub = h.addHub(hubAPI, h.hubCA, hubToken)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h.enrollment = kubernetesaddon.NewEnrollmentService(kubernetesaddon.EnrollmentDeps{
		Store: store, Vault: h.vault, Records: h.records, Prober: h.prober, Logger: logger,
	})
	h.svc = h.newService()
	t.Cleanup(func() {
		_ = h.svc.Close(context.Background())
		cancel()
	})
	return h
}

// newService builds a started service over the harness's store, as a
// restarted resource manager would.
func (h *harness) newService() *Service {
	h.t.Helper()
	svc := NewService(Deps{
		Store: h.store, Vault: h.vault, Records: h.records, Enrollment: h.enrollment, Clients: h.clients,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := svc.Start(h.ctx); err != nil {
		h.t.Fatalf("Start: %v", err)
	}
	return svc
}

func (h *harness) addHub(api string, ca []byte, token string) *fakeHub {
	hub := &fakeHub{
		ca: ca,
		client: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
			map[schema.GroupVersionResource]string{managedClusterGVR: "ManagedClusterList"}),
		tokens: map[string]HubProbe{token: {
			Username:       "system:serviceaccount:" + HubReaderNamespace + ":" + HubReaderServiceAccount,
			ServiceAccount: hubReader,
			CanRead:        true,
			CanRenew:       true,
		}},
	}
	hub.client.PrependReactor("list", "managedclusters", func(clienttesting.Action) (bool, runtime.Object, error) {
		if hub.failing.Load() {
			return true, nil, errors.New("dial tcp: connection refused")
		}
		return false, nil, nil
	})
	h.clients.mu.Lock()
	h.clients.hubs[api] = hub
	h.clients.mu.Unlock()
	return hub
}

func (h *harness) connect(hubID string) HubRecord {
	h.t.Helper()
	rec, err := h.svc.Connect(h.ctx, ConnectRequest{
		HubID:      hubID,
		Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken},
		Actor:      "ops@example.test",
	})
	if err != nil {
		h.t.Fatalf("Connect(%s): %v", hubID, err)
	}
	return rec
}

// clusterOption changes a ManagedCluster object before it is created.
type clusterOption func(obj map[string]any)

func withLabels(labels map[string]string) clusterOption {
	return func(obj map[string]any) {
		m := map[string]any{}
		for k, v := range labels {
			m[k] = v
		}
		obj["metadata"].(map[string]any)["labels"] = m
	}
}

func withClientConfig(url string, ca []byte) clusterOption {
	return func(obj map[string]any) {
		obj["spec"].(map[string]any)["managedClusterClientConfigs"] = []any{map[string]any{
			"url": url, "caBundle": base64.StdEncoding.EncodeToString(ca),
		}}
	}
}

func notAccepted() clusterOption {
	return func(obj map[string]any) { obj["spec"].(map[string]any)["hubAcceptsClient"] = false }
}

func withClaim(name, value string) clusterOption {
	return func(obj map[string]any) {
		status := obj["status"].(map[string]any)
		claims, _ := status["clusterClaims"].([]any)
		status["clusterClaims"] = append(claims, map[string]any{"name": name, "value": value})
	}
}

func managedCluster(name string, opts ...clusterOption) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": clusterAPIGroup + "/v1",
		"kind":       "ManagedCluster",
		"metadata": map[string]any{
			"name":   name,
			"uid":    "uid-" + name,
			"labels": map[string]any{clusterSetLabel: "default"},
		},
		"spec": map[string]any{"hubAcceptsClient": true},
		"status": map[string]any{
			"version": map[string]any{"kubernetes": "v1.37.0"},
			"conditions": []any{
				map[string]any{"type": "HubAcceptedManagedCluster", "status": "True", "reason": "HubClusterAdminAccepted", "message": "Accepted", "lastTransitionTime": "2026-01-01T00:00:00Z"},
				map[string]any{"type": conditionAvailable, "status": "True", "reason": "ManagedClusterAvailable", "message": "Available", "lastTransitionTime": "2026-01-01T00:01:00Z"},
			},
		},
	}
	for _, opt := range opts {
		opt(obj)
	}
	return &unstructured.Unstructured{Object: obj}
}

func (h *harness) createCluster(obj *unstructured.Unstructured) {
	h.t.Helper()
	if _, err := h.hub.client.Resource(managedClusterGVR).Create(h.ctx, obj, metav1.CreateOptions{}); err != nil {
		h.t.Fatalf("create ManagedCluster %s: %v", obj.GetName(), err)
	}
}

func (h *harness) updateCluster(name string, change func(obj *unstructured.Unstructured)) {
	h.t.Helper()
	res := h.hub.client.Resource(managedClusterGVR)
	obj, err := res.Get(h.ctx, name, metav1.GetOptions{})
	if err != nil {
		h.t.Fatalf("get ManagedCluster %s: %v", name, err)
	}
	obj = obj.DeepCopy()
	change(obj)
	if _, err := res.Update(h.ctx, obj, metav1.UpdateOptions{}); err != nil {
		h.t.Fatalf("update ManagedCluster %s: %v", name, err)
	}
}

func (h *harness) deleteCluster(name string) {
	h.t.Helper()
	if err := h.hub.client.Resource(managedClusterGVR).Delete(h.ctx, name, metav1.DeleteOptions{}); err != nil {
		h.t.Fatalf("delete ManagedCluster %s: %v", name, err)
	}
}

// observedRecord is an observed cluster's record as stored.
type observedRecord struct {
	labels map[string]string
	obs    ManagedClusterObservation
	conds  map[string]domain.Condition
}

func (h *harness) observed(hubID, name string) (observedRecord, bool) {
	h.t.Helper()
	res, err := h.svc.readRecord(h.ctx, ManagedClusterResourceType, ManagedClusterRecordName(hubID, name))
	if errors.Is(err, domain.ErrNotFound) {
		return observedRecord{}, false
	}
	if err != nil {
		h.t.Fatalf("read record of %s/%s: %v", hubID, name, err)
	}
	out := observedRecord{labels: map[string]string{}, conds: map[string]domain.Condition{}}
	if err := decodeObservation(res, &out.obs); err != nil {
		h.t.Fatalf("decode record of %s/%s: %v", hubID, name, err)
	}
	for k, v := range res.Inventory().Labels() {
		out.labels[k] = v
	}
	for _, c := range res.Inventory().Conditions() {
		out.conds[string(c.Type())] = c
	}
	return out, true
}

func (h *harness) hubRecordStored(hubID string) (HubObservation, map[string]domain.Condition, bool) {
	h.t.Helper()
	res, err := h.svc.readRecord(h.ctx, HubResourceType, HubRecordName(hubID))
	if errors.Is(err, domain.ErrNotFound) {
		return HubObservation{}, nil, false
	}
	if err != nil {
		h.t.Fatalf("read record of hub %s: %v", hubID, err)
	}
	var obs HubObservation
	if err := decodeObservation(res, &obs); err != nil {
		h.t.Fatalf("decode record of hub %s: %v", hubID, err)
	}
	conds := map[string]domain.Condition{}
	for _, c := range res.Inventory().Conditions() {
		conds[string(c.Type())] = c
	}
	return obs, conds, true
}

func (h *harness) target(id domain.TargetID) (domain.TargetInfo, bool) {
	h.t.Helper()
	targets, err := h.svc.listTargets(h.ctx)
	if err != nil {
		h.t.Fatalf("list targets: %v", err)
	}
	for _, t := range targets {
		if t.ID() == id {
			return t, true
		}
	}
	return domain.TargetInfo{}, false
}

// addCreatedTarget registers a kubernetes target the way a
// provisioner's delivery output does: a cluster the resource manager
// created itself.
func (h *harness) addCreatedTarget(name, apiServer string, ca []byte) domain.TargetID {
	h.t.Helper()
	id := domain.TargetID("k8s-" + name)
	target := domain.NewTargetInfo(id, kubernetesaddon.TargetType, name, domain.TargetStateReady, nil, map[string]string{
		kubernetesaddon.PropAPIServer:           apiServer,
		kubernetesaddon.PropCACert:              string(ca),
		kubernetesaddon.PropClusterResourceName: "clusters/" + name,
	}, nil)
	tx, err := h.store.Begin(h.ctx)
	if err != nil {
		h.t.Fatalf("begin: %v", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Inventory().CreateOrUpdate(h.ctx, domain.NewInventoryItem(
		target.InventoryItemID(), domain.InventoryItemType(target.Type()), target.Name(), []byte("{}"), nil, nil, time.Now(),
	)); err != nil {
		h.t.Fatalf("create inventory item: %v", err)
	}
	if err := tx.Targets().CreateOrUpdate(h.ctx, target); err != nil {
		h.t.Fatalf("create target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		h.t.Fatalf("commit: %v", err)
	}
	h.svc.markLinksDirty()
	return id
}

// addRealCluster makes a cluster exist for the enrollment prober and
// returns its API address, its CA, and a token of its access account.
func (h *harness) addRealCluster(name string) (apiServer string, ca []byte, token string) {
	h.t.Helper()
	apiServer = "https://" + name + ".test:6443"
	ca = testCA(h.t, name)
	now := time.Now()
	token = makeToken(name+"-access", now, now.Add(12*time.Hour))
	h.prober.mu.Lock()
	h.prober.clusters[apiServer] = &fakeCluster{ca: ca, tokens: map[string]struct{}{token: {}}}
	h.prober.mu.Unlock()
	return apiServer, ca, token
}

// eventually polls cond until it holds or the test fails.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// never checks that cond does not come to hold for a short while.
func never(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("happened although it must not: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
