package kubernetes

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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

// The enrollment tests run the service against a real SQLite store and
// vault, with a fake cluster prober and a recording indexing runtime.

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeCluster is one cluster as the fake prober knows it.
type fakeCluster struct {
	ca          []byte
	version     string
	tokens      map[string]ClusterProbeResult
	unreachable error
	denyRenew   bool
}

type fakeProber struct {
	mu       sync.Mutex
	clock    *fakeClock
	clusters map[string]*fakeCluster
	minted   int
}

func (p *fakeProber) lookup(conn ClusterConnection) (*fakeCluster, ClusterProbeResult, error) {
	c, ok := p.clusters[conn.APIServer]
	if !ok {
		return nil, ClusterProbeResult{}, fmt.Errorf("dial %s: no such host", conn.APIServer)
	}
	if c.unreachable != nil {
		return nil, ClusterProbeResult{}, c.unreachable
	}
	if !SameCA(conn.CACert, c.ca) {
		return nil, ClusterProbeResult{}, errors.New("x509: certificate signed by unknown authority")
	}
	res, ok := c.tokens[conn.Token]
	if !ok {
		return nil, ClusterProbeResult{}, fmt.Errorf("probe: %w", ErrClusterRejectedCredential)
	}
	if _, exp := TokenLifetime(conn.Token); !exp.IsZero() && !p.clock.Now().Before(exp) {
		return nil, ClusterProbeResult{}, fmt.Errorf("probe: %w", ErrClusterRejectedCredential)
	}
	return c, res, nil
}

func (p *fakeProber) Probe(_ context.Context, conn ClusterConnection) (ClusterProbeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, res, err := p.lookup(conn)
	if err != nil {
		return ClusterProbeResult{}, err
	}
	res.KubernetesVersion = c.version
	res.TokenIssuedAt, res.TokenExpiresAt = TokenLifetime(conn.Token)
	return res, nil
}

func (p *fakeProber) Identity(_ context.Context, conn ClusterConnection) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, res, err := p.lookup(conn)
	if err != nil {
		return "", err
	}
	return res.Username, nil
}

func (p *fakeProber) RenewToken(_ context.Context, conn ClusterConnection, sa ServiceAccountRef, lifetime time.Duration) (string, time.Time, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, res, err := p.lookup(conn)
	if err != nil {
		return "", time.Time{}, err
	}
	if c.denyRenew {
		return "", time.Time{}, fmt.Errorf("renew: %w", ErrClusterDeniedRequest)
	}
	if res.ServiceAccount != sa {
		return "", time.Time{}, fmt.Errorf("renew: %w", ErrClusterDeniedRequest)
	}
	p.minted++
	now := p.clock.Now()
	token := makeToken(fmt.Sprintf("renewed-%d", p.minted), now, now.Add(lifetime))
	c.tokens[token] = res
	return token, now.Add(lifetime), nil
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

// fakeIndexing records EnsureIndexer and StopIndexer calls.
type fakeIndexing struct {
	mu        sync.Mutex
	running   map[domain.TargetID]IndexRuntimeInput
	ensures   []IndexRuntimeInput
	stops     []domain.TargetID
	ensureErr error
}

func newFakeIndexing() *fakeIndexing {
	return &fakeIndexing{running: map[domain.TargetID]IndexRuntimeInput{}}
}

func (f *fakeIndexing) EnsureIndexer(_ context.Context, input IndexRuntimeInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensures = append(f.ensures, input)
	if f.ensureErr != nil {
		return f.ensureErr
	}
	f.running[input.TargetID] = input
	return nil
}

func (f *fakeIndexing) StopIndexer(_ context.Context, id domain.TargetID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, id)
	delete(f.running, id)
	return nil
}

func (f *fakeIndexing) StopAll(context.Context) error { return nil }

func (f *fakeIndexing) IndexedClusters() []domain.ResourceName {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.ResourceName, 0, len(f.running))
	for _, in := range f.running {
		out = append(out, in.ClusterResourceName)
	}
	return out
}

func (f *fakeIndexing) credentialOf(id domain.TargetID) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	in, ok := f.running[id]
	return string(in.Credential), ok
}

// storeRecordStore adapts the application's inventory report service
// to [InventoryRecordStore], as server composition does.
type storeRecordStore struct {
	storeReportBackend
}

func (b *storeRecordStore) ListCollections(ctx context.Context, rt domain.ResourceType) ([]domain.CollectionName, error) {
	return b.reports.ListCollections(ctx, rt)
}

func (b *storeRecordStore) PurgeCollectionPrefix(ctx context.Context, rt domain.ResourceType, prefix string) (int64, error) {
	return b.reports.PurgeCollectionPrefix(ctx, rt, prefix)
}

type enrollHarness struct {
	t        *testing.T
	store    *sqlite.Store
	vault    *sqlite.VaultStore
	records  *storeRecordStore
	prober   *fakeProber
	indexing *fakeIndexing
	clock    *fakeClock
	svc      *EnrollmentService
}

const testAccessUser = "system:serviceaccount:" + AccessNamespace + ":" + AccessServiceAccount

func newEnrollHarness(t *testing.T) *enrollHarness {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	ctx := context.Background()

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for _, sch := range Schemas() {
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

	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	h := &enrollHarness{
		t:        t,
		store:    store,
		vault:    &sqlite.VaultStore{DB: db},
		records:  &storeRecordStore{storeReportBackend{reports: application.NewInventoryReportService(store)}},
		prober:   &fakeProber{clock: clock, clusters: map[string]*fakeCluster{}},
		indexing: newFakeIndexing(),
		clock:    clock,
	}
	h.svc = NewEnrollmentService(EnrollmentDeps{
		Store:    store,
		Vault:    h.vault,
		Records:  h.records,
		Indexing: h.indexing,
		Prober:   h.prober,
		Trust: func(context.Context) ([]domain.TrustBundleEntry, error) {
			return []domain.TrustBundleEntry{{IssuerURL: "https://issuer.test", JWKSURI: "https://issuer.test/keys"}}, nil
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:    clock.Now,
	})
	return h
}

// addCluster registers a cluster with the fake prober and returns its
// API address, its CA, and a token of the access account that is valid
// for lifetime.
func (h *enrollHarness) addCluster(name string, lifetime time.Duration) (apiServer string, ca []byte, token string) {
	h.t.Helper()
	apiServer = "https://" + name + ":6443"
	ca = testCA(h.t, name)
	now := h.clock.Now()
	token = makeToken(name+"-first", now, now.Add(lifetime))
	sa := ServiceAccountRef{Namespace: AccessNamespace, Name: AccessServiceAccount}
	h.prober.mu.Lock()
	h.prober.clusters[apiServer] = &fakeCluster{
		ca:      ca,
		version: "v1.37.0",
		tokens: map[string]ClusterProbeResult{token: {
			Username:       testAccessUser,
			ServiceAccount: sa,
			Rights:         ClusterRights{Workloads: true, Namespaces: true, ReadCluster: true, CanRenew: true},
		}},
	}
	h.prober.mu.Unlock()
	return apiServer, ca, token
}

// mintToken adds another valid token of the access account to a cluster.
func (h *enrollHarness) mintToken(apiServer, id string, lifetime time.Duration) string {
	h.t.Helper()
	now := h.clock.Now()
	token := makeToken(id, now, now.Add(lifetime))
	h.prober.mu.Lock()
	defer h.prober.mu.Unlock()
	h.prober.clusters[apiServer].tokens[token] = ClusterProbeResult{
		Username:       testAccessUser,
		ServiceAccount: ServiceAccountRef{Namespace: AccessNamespace, Name: AccessServiceAccount},
		Rights:         ClusterRights{Workloads: true, Namespaces: true, ReadCluster: true, CanRenew: true},
	}
	return token
}

func (h *enrollHarness) cluster(apiServer string) *fakeCluster {
	h.prober.mu.Lock()
	defer h.prober.mu.Unlock()
	return h.prober.clusters[apiServer]
}

func (h *enrollHarness) target(id string) (domain.TargetInfo, bool) {
	h.t.Helper()
	target, found, err := h.svc.getTarget(context.Background(), TargetIDForCluster(id))
	if err != nil {
		h.t.Fatalf("getTarget(%s): %v", id, err)
	}
	return target, found
}

func (h *enrollHarness) vaultToken(id string) (string, bool) {
	h.t.Helper()
	val, err := h.vault.Get(context.Background(), domain.SecretRef(fmt.Sprintf("targets/%s/sa-token", TargetIDForCluster(id))))
	if errors.Is(err, domain.ErrNotFound) {
		return "", false
	}
	if err != nil {
		h.t.Fatalf("vault get: %v", err)
	}
	return string(val), true
}

func (h *enrollHarness) conditions(id string) map[domain.ConditionType]domain.Condition {
	h.t.Helper()
	return h.svc.loadClusterState(context.Background(), id).conds
}

// seedObjects reports indexed objects under a cluster id.
func (h *enrollHarness) seedObjects(clusterID string, n int) {
	h.t.Helper()
	reports := make([]InventoryObjectReport, 0, n)
	for i := 0; i < n; i++ {
		obs := json.RawMessage(`{"kind":"Pod"}`)
		reports = append(reports, InventoryObjectReport{
			Name:        domain.ResourceName(fmt.Sprintf("clusters/%s/namespaces/default/apiResources/pods/objects/uid-%d", clusterID, i)),
			Labels:      map[string]string{},
			Observation: &obs,
			ObservedAt:  h.clock.Now(),
		})
	}
	if err := h.records.ReplaceBatch(context.Background(), ObjectResourceType, reports); err != nil {
		h.t.Fatalf("seed objects: %v", err)
	}
}

// hasObjects reports whether any indexed object remains under a cluster id.
func (h *enrollHarness) hasObjects(clusterID string) bool {
	h.t.Helper()
	names, err := h.records.ListCollections(context.Background(), ObjectResourceType)
	if err != nil {
		h.t.Fatalf("ListCollections: %v", err)
	}
	for _, name := range names {
		if strings.HasPrefix(string(name), "clusters/"+clusterID+"/") {
			return true
		}
	}
	return false
}

// seedDeployment stores a deployment whose placement uses the target.
func (h *enrollHarness) seedDeployment(name string, placement domain.PlacementStrategySpec, resolved ...domain.TargetID) {
	h.t.Helper()
	ctx := context.Background()
	now := h.clock.Now()
	fid := domain.FulfillmentID("f-" + name)
	f := domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID: fid, State: domain.FulfillmentStateActive, ResolvedTargets: resolved, CreatedAt: now, UpdatedAt: now,
	})
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
	}, now)
	f.AdvancePlacementStrategy(placement, now)
	tx, err := h.store.Begin(ctx)
	if err != nil {
		h.t.Fatalf("begin: %v", err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Fulfillments().Create(ctx, f); err != nil {
		h.t.Fatalf("create fulfillment: %v", err)
	}
	if err := tx.Deployments().Create(ctx, domain.NewDeployment(domain.ResourceName("deployments/"+name), domain.NewDeploymentUID(), fid, now)); err != nil {
		h.t.Fatalf("create deployment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		h.t.Fatalf("commit: %v", err)
	}
}

// enrollErr asserts err is an enrollment error of the kind and reason.
func enrollErr(t *testing.T, err error, kind error, reason string) *EnrollmentError {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("error = %v, want kind %v", err, kind)
	}
	var ee *EnrollmentError
	if !errors.As(err, &ee) {
		t.Fatalf("error %v is not an EnrollmentError", err)
	}
	if ee.Reason != reason {
		t.Fatalf("reason = %q, want %q (error: %v)", ee.Reason, reason, err)
	}
	return ee
}
