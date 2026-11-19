package ocm

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestConnectObservesTheHubsClusters(t *testing.T) {
	h := newHarness(t)
	extCA := testCA(t, "ext")
	h.createCluster(managedCluster("ext",
		withLabels(map[string]string{clusterSetLabel: "default", "region": "eu"}),
		withClientConfig("https://ext.test:6443/", extCA),
		withClaim("id.k8s.io", "abc-123")))
	h.createCluster(managedCluster("pending", notAccepted()))

	rec := h.connect("hub1")
	if rec.ID != "hub1" || rec.Name != "hubs/hub1" || rec.Observation.APIServer != hubAPI {
		t.Fatalf("hub record = %+v", rec)
	}
	if rec.Observation.ConnectedBy != "ops@example.test" || rec.Observation.ServiceAccount != hubReader.String() {
		t.Errorf("hub record audit fields = %+v", rec.Observation)
	}
	if rec.Observation.WriteAccessFound {
		t.Errorf("WriteAccessFound = true for a read-only credential")
	}

	eventually(t, "both clusters recorded", func() bool {
		_, a := h.observed("hub1", "ext")
		_, b := h.observed("hub1", "pending")
		return a && b
	})
	ext, _ := h.observed("hub1", "ext")
	if ext.labels["region"] != "eu" || ext.labels[clusterSetLabel] != "default" {
		t.Errorf("labels = %v", ext.labels)
	}
	obs := ext.obs
	if obs.Hub != "hub1" || obs.Name != "ext" || !obs.Accepted || obs.Available != "True" || obs.ClusterSet != "default" || obs.KubernetesVersion != "v1.37.0" {
		t.Errorf("observation = %+v", obs)
	}
	if obs.Claims["id.k8s.io"] != "abc-123" {
		t.Errorf("claims = %v", obs.Claims)
	}
	if len(obs.ClientConfigs) != 1 || obs.ClientConfigs[0].URL != "https://ext.test:6443" || !strings.HasPrefix(obs.ClientConfigs[0].CAFingerprint, "sha256:") {
		t.Errorf("client configs = %+v", obs.ClientConfigs)
	}
	if obs.Enrollment.State != LinkObserved || obs.Enrollment.Target != "" {
		t.Errorf("enrollment link = %+v, want observed", obs.Enrollment)
	}
	for _, term := range []string{"ext", "hub1", "default", "eu", "region=eu"} {
		if !contains(obs.SearchTerms, term) {
			t.Errorf("search terms %v lack %q", obs.SearchTerms, term)
		}
	}
	if c, ok := ext.conds[conditionAvailable]; !ok || c.Status() != domain.ConditionTrue || c.Reason() != "ManagedClusterAvailable" {
		t.Errorf("availability condition = %+v", c)
	}
	if _, stale := ext.conds[string(ConditionStale)]; stale {
		t.Errorf("a fresh record carries the Stale condition")
	}
	pending, _ := h.observed("hub1", "pending")
	if pending.obs.Accepted {
		t.Errorf("pending cluster recorded as accepted")
	}

	eventually(t, "hub record shows counts and readiness", func() bool {
		obs, conds, ok := h.hubRecordStored("hub1")
		return ok && obs.ClusterCount == 2 && obs.AcceptedCount == 1 && obs.AvailableCount == 2 && obs.EnrolledCount == 0 &&
			conds[string(HubConditionReady)].Status() == domain.ConditionTrue &&
			conds[string(HubConditionSynced)].Status() == domain.ConditionTrue
	})
}

func TestObservedRecordsFollowTheHub(t *testing.T) {
	h := newHarness(t)
	h.createCluster(managedCluster("a"))
	h.connect("hub1")
	eventually(t, "a recorded", func() bool { _, ok := h.observed("hub1", "a"); return ok })

	// A cluster joins.
	h.createCluster(managedCluster("b"))
	eventually(t, "b recorded", func() bool { _, ok := h.observed("hub1", "b"); return ok })

	// A label changes.
	h.updateCluster("a", func(obj *unstructured.Unstructured) {
		labels := obj.GetLabels()
		labels["tier"] = "gold"
		obj.SetLabels(labels)
	})
	eventually(t, "label change recorded", func() bool {
		rec, ok := h.observed("hub1", "a")
		return ok && rec.labels["tier"] == "gold" && contains(rec.obs.SearchTerms, "gold")
	})

	// A cluster leaves.
	h.deleteCluster("b")
	eventually(t, "b's record removed", func() bool { _, ok := h.observed("hub1", "b"); return !ok })
	if _, ok := h.observed("hub1", "a"); !ok {
		t.Errorf("a's record was removed with b's")
	}
}

func TestConnectRefusals(t *testing.T) {
	h := newHarness(t)
	otherCA := testCA(t, "other")
	good := HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken}

	noRead := "no-read-token"
	h.hub.mu.Lock()
	h.hub.tokens[noRead] = HubProbe{
		Username:       "system:serviceaccount:default:nobody",
		ServiceAccount: kubernetesaddon.ServiceAccountRef{Namespace: "default", Name: "nobody"},
	}
	userToken := "user-token"
	h.hub.tokens[userToken] = HubProbe{Username: "ops@example.test", CanRead: true}
	h.hub.mu.Unlock()

	cases := map[string]struct {
		req    ConnectRequest
		kind   error
		reason string
	}{
		"bad hub id":       {ConnectRequest{HubID: "Hub_1", Connection: good}, domain.ErrInvalidArgument, ""},
		"hub id with dot":  {ConnectRequest{HubID: "a.b", Connection: good}, domain.ErrInvalidArgument, ""},
		"cleartext":        {ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: "http://hub.test:6443", CACert: string(h.hubCA), Token: hubToken}}, domain.ErrInvalidArgument, ""},
		"no CA":            {ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, Token: hubToken}}, domain.ErrInvalidArgument, ""},
		"no token":         {ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA)}}, domain.ErrInvalidArgument, ""},
		"unknown token":    {ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: "nope"}}, kubernetesaddon.ErrEnrollmentUnverified, "credential-rejected"},
		"wrong CA":         {ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, CACert: string(otherCA), Token: hubToken}}, kubernetesaddon.ErrEnrollmentUnverified, "hub-unreachable"},
		"unknown address":  {ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: "https://nowhere.test:6443", CACert: string(h.hubCA), Token: hubToken}}, kubernetesaddon.ErrEnrollmentUnverified, "hub-unreachable"},
		"may not read":     {ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: noRead}}, kubernetesaddon.ErrEnrollmentUnverified, "access-denied"},
		"not an account":   {ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: userToken}}, kubernetesaddon.ErrEnrollmentUnverified, "not-a-service-account"},
		"long displayName": {ConnectRequest{HubID: "hub1", DisplayName: strings.Repeat("x", 121), Connection: good}, domain.ErrInvalidArgument, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := h.svc.Connect(h.ctx, tc.req)
			if !errors.Is(err, tc.kind) {
				t.Fatalf("Connect error = %v, want %v", err, tc.kind)
			}
			var ee *kubernetesaddon.EnrollmentError
			if tc.reason != "" && (!errors.As(err, &ee) || ee.Reason != tc.reason) {
				t.Fatalf("Connect error = %v, want reason %q", err, tc.reason)
			}
		})
	}
	if _, _, ok := h.hubRecordStored("hub1"); ok {
		t.Fatalf("a refused connection left a hub record")
	}
	if _, err := h.vault.Get(h.ctx, hubConnectionRef("hub1")); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("a refused connection left a stored credential: %v", err)
	}
}

func TestOneHubOneConnection(t *testing.T) {
	h := newHarness(t)
	h.connect("hub1")

	// The same hub under another id.
	_, err := h.svc.Connect(h.ctx, ConnectRequest{HubID: "hub2", Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken}})
	var ee *kubernetesaddon.EnrollmentError
	if !errors.As(err, &ee) || ee.Reason != "hub-already-connected" || ee.Details["hub"] != "hub1" {
		t.Fatalf("second id for one hub: %v", err)
	}

	// Another hub under the same id.
	otherCA := testCA(t, "other-hub")
	h.addHub("https://other.test:6443", otherCA, "other-token")
	_, err = h.svc.Connect(h.ctx, ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: "https://other.test:6443", CACert: string(otherCA), Token: "other-token"}})
	if !errors.As(err, &ee) || ee.Reason != "hub-id-in-use" {
		t.Fatalf("second hub for one id: %v", err)
	}

	// The same hub under the same id with a new credential replaces it.
	h.hub.mu.Lock()
	h.hub.tokens["second-token"] = h.hub.tokens[hubToken]
	h.hub.mu.Unlock()
	rec, err := h.svc.Connect(h.ctx, ConnectRequest{HubID: "hub1", Actor: "someone-else",
		Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: "second-token"}})
	if err != nil {
		t.Fatalf("reconnect with a new credential: %v", err)
	}
	if rec.Observation.ConnectedBy != "ops@example.test" {
		t.Errorf("reconnect changed who connected the hub: %q", rec.Observation.ConnectedBy)
	}
	conn, err := h.svc.loadConnection(h.ctx, "hub1")
	if err != nil || conn.Token != "second-token" {
		t.Fatalf("stored connection = %+v, %v", conn, err)
	}
}

func TestDisconnectRemovesWhatWasObserved(t *testing.T) {
	h := newHarness(t)
	h.createCluster(managedCluster("a"))
	h.createCluster(managedCluster("b"))
	h.connect("hub1")
	eventually(t, "clusters recorded", func() bool {
		_, a := h.observed("hub1", "a")
		_, b := h.observed("hub1", "b")
		return a && b
	})
	res, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1", Endpoint: testEndpoint(), Actor: "ops"})
	if err != nil {
		t.Fatalf("EnrollmentAddon: %v", err)
	}

	if err := h.svc.Disconnect(h.ctx, "hub1", "ops"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		if _, ok := h.observed("hub1", name); ok {
			t.Errorf("record of %s survived the disconnect", name)
		}
	}
	if _, _, ok := h.hubRecordStored("hub1"); ok {
		t.Errorf("hub record survived the disconnect")
	}
	if _, err := h.vault.Get(h.ctx, hubConnectionRef("hub1")); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("stored credential survived the disconnect: %v", err)
	}
	if _, err := h.enrollment.GetBundle(h.ctx, res.Bundle.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("the hub's enrollment bundle survived the disconnect: %v", err)
	}
	if _, err := h.svc.GetHub(h.ctx, "hub1"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetHub after disconnect: %v", err)
	}
	// Nothing is written after the watch has stopped.
	never(t, "records reappear after disconnect", func() bool { _, ok := h.observed("hub1", "a"); return ok })

	if err := h.svc.Disconnect(h.ctx, "hub1", "ops"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("second Disconnect = %v, want not found", err)
	}
}

func TestRestartWatchesAgainAndDropsClustersThatLeft(t *testing.T) {
	h := newHarness(t)
	h.createCluster(managedCluster("stays"))
	h.createCluster(managedCluster("leaves"))
	h.connect("hub1")
	eventually(t, "clusters recorded", func() bool {
		_, a := h.observed("hub1", "stays")
		_, b := h.observed("hub1", "leaves")
		return a && b
	})

	// The resource manager stops; meanwhile a cluster leaves the hub.
	if err := h.svc.Close(h.ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.deleteCluster("leaves")
	h.createCluster(managedCluster("joined-meanwhile"))

	h.svc = h.newService()
	eventually(t, "restart reconciles the records", func() bool {
		_, stays := h.observed("hub1", "stays")
		_, leaves := h.observed("hub1", "leaves")
		_, joined := h.observed("hub1", "joined-meanwhile")
		return stays && !leaves && joined
	})
	rec, err := h.svc.GetHub(h.ctx, "hub1")
	if err != nil {
		t.Fatalf("GetHub after restart: %v", err)
	}
	if rec.Observation.ConnectedBy != "ops@example.test" || rec.Observation.APIServer != hubAPI {
		t.Errorf("hub record after restart = %+v", rec.Observation)
	}
}

func TestUnreachableHubMarksItsClustersStale(t *testing.T) {
	h := newHarness(t)
	h.createCluster(managedCluster("a"))
	h.connect("hub1")
	eventually(t, "a recorded", func() bool { _, ok := h.observed("hub1", "a"); return ok })

	h.hub.failing.Store(true)
	eventually(t, "record marked stale and hub not ready", func() bool {
		rec, ok := h.observed("hub1", "a")
		_, conds, hubOK := h.hubRecordStored("hub1")
		return ok && rec.conds[string(ConditionStale)].Status() == domain.ConditionTrue && hubOK &&
			conds[string(HubConditionConnected)].Status() == domain.ConditionFalse &&
			conds[string(HubConditionReady)].Status() == domain.ConditionFalse
	})
	rec, _ := h.observed("hub1", "a")
	if rec.obs.Available != "True" || rec.conds[conditionAvailable].Status() != domain.ConditionTrue {
		t.Errorf("a stale record lost the last state seen: %+v", rec.obs)
	}

	h.hub.failing.Store(false)
	eventually(t, "record current again and hub ready", func() bool {
		rec, ok := h.observed("hub1", "a")
		_, conds, hubOK := h.hubRecordStored("hub1")
		_, stale := rec.conds[string(ConditionStale)]
		return ok && !stale && hubOK && conds[string(HubConditionReady)].Status() == domain.ConditionTrue
	})
}

func TestHubCredentialIsRenewed(t *testing.T) {
	h := newHarness(t)
	now := time.Now()
	// Two thirds of the lifetime are over.
	short := makeToken("hub-first", now.Add(-50*time.Minute), now.Add(10*time.Minute))
	h.hub.mu.Lock()
	h.hub.tokens[short] = h.hub.tokens[hubToken]
	h.hub.mu.Unlock()
	if _, err := h.svc.Connect(h.ctx, ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: short}}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	eventually(t, "stored credential replaced by a renewed one", func() bool {
		conn, err := h.svc.loadConnection(h.ctx, "hub1")
		return err == nil && conn.Token != short
	})
	conn, _ := h.svc.loadConnection(h.ctx, "hub1")
	if _, exp := kubernetesaddon.TokenLifetime(conn.Token); !exp.After(now.Add(30 * time.Minute)) {
		t.Errorf("renewed credential expires at %s, want about an hour from now", exp)
	}
	if h.svc.hub("hub1").token() != conn.Token {
		t.Errorf("the watch does not use the renewed credential")
	}
	eventually(t, "hub record shows the new expiry", func() bool {
		obs, conds, ok := h.hubRecordStored("hub1")
		return ok && obs.CredentialExpiresAt != nil && obs.CredentialExpiresAt.After(now.Add(30*time.Minute)) &&
			conds[string(HubConditionCredentialValid)].Status() == domain.ConditionTrue
	})
}

func TestHubCredentialThatCannotBeRenewedIsReported(t *testing.T) {
	h := newHarness(t)
	now := time.Now()
	short := makeToken("hub-first", now.Add(-50*time.Minute), now.Add(10*time.Minute))
	h.hub.mu.Lock()
	h.hub.tokens[short] = h.hub.tokens[hubToken]
	h.hub.denyNew = true
	h.hub.mu.Unlock()
	if _, err := h.svc.Connect(h.ctx, ConnectRequest{HubID: "hub1", Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: short}}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	eventually(t, "hub record says the credential cannot be renewed", func() bool {
		_, conds, ok := h.hubRecordStored("hub1")
		c := conds[string(HubConditionCredentialValid)]
		return ok && c.Status() == domain.ConditionTrue && c.Reason() == "CannotRenew" && strings.Contains(c.Message(), "connect the hub again")
	})
	if conn, _ := h.svc.loadConnection(h.ctx, "hub1"); conn.Token != short {
		t.Errorf("stored credential changed although renewal was denied")
	}
}

func TestLinkToAClusterTheResourceManagerCreated(t *testing.T) {
	h := newHarness(t)
	spokeCA := testCA(t, "spoke")
	h.createCluster(managedCluster("spoke", withClientConfig("https://spoke.test:6443", spokeCA)))
	h.createCluster(managedCluster("other", withClientConfig("https://other.test:6443", testCA(t, "other"))))
	h.connect("hub1")
	eventually(t, "clusters recorded", func() bool { _, ok := h.observed("hub1", "spoke"); return ok })

	h.addCreatedTarget("spoke", "https://spoke.test:6443", spokeCA)
	// Same name, another cluster: the hub records another address.
	h.addCreatedTarget("other", "https://elsewhere.test:6443", testCA(t, "elsewhere"))

	eventually(t, "spoke linked by address and CA", func() bool {
		rec, ok := h.observed("hub1", "spoke")
		return ok && rec.obs.Enrollment == EnrollmentLink{State: LinkCreated, Cluster: "clusters/spoke", Target: "k8s-spoke", Basis: BasisAddressAndCA}
	})
	never(t, "a cluster is linked by name although the hub records another address", func() bool {
		rec, _ := h.observed("hub1", "other")
		return rec.obs.Enrollment.State != LinkObserved
	})
	// Labels of a cluster the resource manager created are not touched.
	if target, _ := h.target("k8s-spoke"); len(target.Labels()) != 0 {
		t.Errorf("labels were put on a target the integration does not own: %v", target.Labels())
	}
}

func TestExplainUnknownTarget(t *testing.T) {
	h := newHarness(t)
	spokeCA := testCA(t, "spoke")
	h.createCluster(managedCluster("ext"))
	h.createCluster(managedCluster("spoke", withClientConfig("https://spoke.test:6443", spokeCA)))
	h.connect("hub1")
	h.addCreatedTarget("spoke", "https://spoke.test:6443", spokeCA)
	eventually(t, "spoke linked", func() bool {
		rec, ok := h.observed("hub1", "spoke")
		return ok && rec.obs.Enrollment.State == LinkCreated
	})

	for _, id := range []domain.TargetID{"k8s-ext", "k8s-hub1.ext", "ext"} {
		got := h.svc.ExplainUnknownTarget(h.ctx, id)
		if !strings.Contains(got, "observed, not enrolled") || !strings.Contains(got, `"k8s-hub1.ext"`) {
			t.Errorf("ExplainUnknownTarget(%s) = %q", id, got)
		}
	}
	if got := h.svc.ExplainUnknownTarget(h.ctx, "k8s-hub1.spoke"); !strings.Contains(got, `known to the resource manager as target "k8s-spoke"`) {
		t.Errorf("ExplainUnknownTarget(k8s-hub1.spoke) = %q", got)
	}
	if got := h.svc.ExplainUnknownTarget(h.ctx, "k8s-unknown"); got != "" {
		t.Errorf("ExplainUnknownTarget(k8s-unknown) = %q, want empty", got)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func testEndpoint() kubernetesaddon.EnrollmentEndpoint {
	return kubernetesaddon.EnrollmentEndpoint{
		ServerURL:      "https://fleet.example:8085",
		ServerCA:       []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"),
		ConnectAddress: "fleet-internal:8085",
		AgentImage:     "registry.example/enrollment-agent:1",
	}
}

// The console reads the hub record: it must never carry the stored
// credential, only what describes the hub.
func TestHubRecordDoesNotCarryTheStoredCredential(t *testing.T) {
	h := newHarness(t)
	rec := h.connect("hub1")
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal the hub record: %v", err)
	}
	if strings.Contains(string(raw), hubToken) {
		t.Error("the hub record carries the stored credential")
	}
	if !strings.Contains(string(raw), hubAPI) {
		t.Error("the hub record does not carry the hub address")
	}

	obs, _, ok := h.hubRecordStored("hub1")
	if !ok {
		t.Fatal("the hub's record was not stored")
	}
	raw, err = json.Marshal(obs)
	if err != nil {
		t.Fatalf("marshal the observation: %v", err)
	}
	if strings.Contains(string(raw), hubToken) {
		t.Error("the stored observation carries the stored credential")
	}
	if !strings.Contains(string(raw), hubAPI) {
		t.Error("the stored observation does not carry the hub address")
	}
}

func TestTargetLabelsSkipEntriesThatAreNotKubernetesLabels(t *testing.T) {
	c := observedCluster{name: "spoke", labels: map[string]string{
		"env":            "prod",
		"bad key":        "v",
		"a/b/c":          "v",
		"long-key":       strings.Repeat("k", 64),
		"long-value-key": strings.Repeat("v", 64),
	}}
	got := c.targetLabels("hub1")
	if got["env"] != "prod" {
		t.Errorf("target labels = %v, want env=prod kept", got)
	}
	if got[LabelHub] != "hub1" || got[LabelManagedCluster] != "spoke" {
		t.Errorf("target labels = %v, want the hub and cluster labels", got)
	}
	for _, key := range []string{"bad key", "a/b/c", "long-key", "long-value-key"} {
		if _, ok := got[key]; ok {
			t.Errorf("target carries %q, which is not a Kubernetes label", key)
		}
	}
}

// A disconnect can find the hub record already gone while observed
// clusters, the credential, or bundles remain. A later disconnect with
// the record already gone finishes what remains; with nothing left at
// all the hub is not found.
func TestDisconnectFinishesWhenTheRecordIsAlreadyGone(t *testing.T) {
	h := newHarness(t)
	h.createCluster(managedCluster("a"))
	h.createCluster(managedCluster("b"))
	h.connect("hub1")
	eventually(t, "clusters recorded", func() bool {
		_, a := h.observed("hub1", "a")
		_, b := h.observed("hub1", "b")
		return a && b
	})
	res, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1", Endpoint: testEndpoint(), Actor: "ops"})
	if err != nil {
		t.Fatalf("EnrollmentAddon: %v", err)
	}

	// The service stops, and the hub record is gone while the rest stays.
	if err := h.svc.Close(h.ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.svc.records.ReplaceBatch(h.ctx, HubResourceType, []kubernetesaddon.InventoryObjectReport{{
		Name: HubRecordName("hub1"), IsDelete: true,
	}}); err != nil {
		t.Fatalf("delete the hub record: %v", err)
	}

	if err := h.svc.Disconnect(h.ctx, "hub1", "ops"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		if _, ok := h.observed("hub1", name); ok {
			t.Errorf("record of %s survived the disconnect", name)
		}
	}
	if _, err := h.vault.Get(h.ctx, hubConnectionRef("hub1")); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("stored credential survived the disconnect: %v", err)
	}
	if _, err := h.enrollment.GetBundle(h.ctx, res.Bundle.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("the hub's enrollment bundle survived the disconnect: %v", err)
	}
	if err := h.svc.Disconnect(h.ctx, "hub1", "ops"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("second Disconnect = %v, want not found", err)
	}
}

// EnrollmentAddon is part of the hub lifecycle: it must not pass its
// hub check while a connect or disconnect is in progress, or a bundle
// could be written just after a disconnect's sweep listed the bundles
// to revoke.
func TestEnrollmentAddonWaitsForTheLifecycleLock(t *testing.T) {
	h := newHarness(t)
	h.svc.opMu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := h.svc.EnrollmentAddon(h.ctx, EnrollmentAddonRequest{HubID: "hub1", Endpoint: testEndpoint(), Actor: "ops"})
		done <- err
	}()
	// Once the attempt is recorded the call can only proceed past the lock,
	// which this test holds, so a non-blocking look at done is conclusive.
	h.attempts.waitAttempt(t, LifecycleEnrollmentAddon, 1)
	select {
	case err := <-done:
		h.svc.opMu.Unlock()
		t.Fatalf("EnrollmentAddon answered while the lifecycle lock was held: %v", err)
	default:
	}
	h.svc.opMu.Unlock()
	select {
	case err := <-done:
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("EnrollmentAddon = %v, want the hub-not-connected refusal", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("EnrollmentAddon did not finish after the lifecycle lock was released")
	}
}

func TestConnectAndDisconnectSerialize(t *testing.T) {
	h := newHarness(t)
	entered, release := h.blockProbes()

	connectDone := make(chan error, 1)
	go func() {
		_, err := h.svc.Connect(h.ctx, ConnectRequest{
			HubID:      "hub1",
			Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken},
			Actor:      "ops@example.test",
		})
		connectDone <- err
	}()
	<-entered

	disconnectDone := make(chan error, 1)
	go func() { disconnectDone <- h.svc.Disconnect(h.ctx, "hub1", "ops@example.test") }()

	// A disconnect that ran beside the probe would find nothing to remove
	// and return before the connect stored its credential. The disconnect
	// has reached the lifecycle lock the connect holds, so it cannot have
	// removed anything yet.
	h.attempts.waitAttempt(t, LifecycleDisconnect, 1)
	select {
	case err := <-disconnectDone:
		t.Fatalf("Disconnect finished while Connect was probing: %v", err)
	default:
	}

	release()
	if err := <-connectDone; err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := <-disconnectDone; err != nil {
		t.Fatalf("Disconnect after the connect: %v", err)
	}

	if _, _, ok := h.hubRecordStored("hub1"); ok {
		t.Error("hub record survived the disconnect")
	}
	if _, err := h.vault.Get(h.ctx, hubConnectionRef("hub1")); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("stored credential survived the disconnect: %v", err)
	}
	if hw := h.svc.hub("hub1"); hw != nil {
		t.Error("the disconnected hub is still watched")
	}
	never(t, "the disconnected hub is watched again", func() bool { return h.svc.hub("hub1") != nil })
}

func TestTwoConnectsLeaveOneConnection(t *testing.T) {
	h := newHarness(t)
	const secondToken = "hub-reader-token-2"
	h.hub.mu.Lock()
	h.hub.tokens[secondToken] = h.hub.tokens[hubToken]
	h.hub.mu.Unlock()

	entered, release := h.blockProbes()
	firstDone := make(chan error, 1)
	go func() {
		_, err := h.svc.Connect(h.ctx, ConnectRequest{
			HubID:      "hub1",
			Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken},
			Actor:      "ops@example.test",
		})
		firstDone <- err
	}()
	<-entered

	secondDone := make(chan error, 1)
	go func() {
		_, err := h.svc.Connect(h.ctx, ConnectRequest{
			HubID:      "hub1",
			Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: secondToken},
			Actor:      "ops@example.test",
		})
		secondDone <- err
	}()
	// The second connect has reached the lifecycle lock the first holds, so
	// it cannot replace the credential until the first has finished.
	h.attempts.waitAttempt(t, LifecycleConnect, 2)
	select {
	case err := <-secondDone:
		t.Fatalf("second Connect finished while the first was probing: %v", err)
	default:
	}

	release()
	if err := <-firstDone; err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Connect: %v", err)
	}

	raw, err := h.vault.Get(h.ctx, hubConnectionRef("hub1"))
	if err != nil {
		t.Fatalf("stored credential: %v", err)
	}
	var stored HubConnection
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("decode stored credential: %v", err)
	}
	if stored.Token != secondToken {
		t.Errorf("stored token is the first connection's; the second connect must replace it")
	}
	if _, _, ok := h.hubRecordStored("hub1"); !ok {
		t.Error("hub record missing after the connects")
	}
	if h.svc.hub("hub1") == nil {
		t.Error("hub is not watched after the connects")
	}
}

// A renewal under way when a new credential arrives must not overwrite it:
// Connect stops and joins the running watch before storing the connection,
// so the store is the last write of the two.
func TestConnectReplacesTheCredentialAfterAnInFlightRenewal(t *testing.T) {
	h := newHarness(t)
	now := time.Now()
	short := makeToken("connect-short", now.Add(-9*time.Minute), now.Add(time.Minute))
	long := makeToken("connect-long", now, now.Add(time.Hour))
	h.hub.mu.Lock()
	h.hub.tokens[short] = h.hub.tokens[hubToken]
	h.hub.tokens[long] = h.hub.tokens[hubToken]
	h.hub.mu.Unlock()

	// The first connect stores the short credential (the write that
	// passes); its watch then renews the credential and that write is
	// held in flight.
	entered, release := h.gated.holdNextPut(1)
	if _, err := h.svc.Connect(h.ctx, ConnectRequest{
		HubID:      "hub1",
		Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: short},
		Actor:      "ops@example.test",
	}); err != nil {
		t.Fatalf("Connect with the short credential: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("the watch did not write a renewed credential")
	}

	// A second connect arrives while that write is in flight. The new
	// credential must not be stored until the held write has landed.
	connectDone := make(chan error, 1)
	go func() {
		_, err := h.svc.Connect(h.ctx, ConnectRequest{
			HubID:      "hub1",
			Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: long},
			Actor:      "ops@example.test",
		})
		connectDone <- err
	}()
	deadline := time.Now().Add(300 * time.Millisecond)
	for {
		conn, err := h.svc.loadConnection(h.ctx, "hub1")
		if err != nil {
			release()
			t.Fatalf("load connection: %v", err)
		}
		if conn.Token == long {
			release()
			t.Fatal("the new credential was stored while the old watch's renewal write was in flight")
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	release()
	select {
	case err := <-connectDone:
		if err != nil {
			t.Fatalf("Connect with the long credential: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not finish after the renewal write landed")
	}

	conn, err := h.svc.loadConnection(h.ctx, "hub1")
	if err != nil {
		t.Fatalf("load connection: %v", err)
	}
	if conn.Token != long {
		t.Fatalf("stored credential = %q, want the credential the second connect supplied", conn.Token)
	}
	hw := h.svc.hub("hub1")
	if hw == nil {
		t.Fatal("hub is not watched after the connects")
	}
	if hw.connection().Token != long {
		t.Fatalf("the watch runs with the old credential, want the new one")
	}
}

// Connect writes the hub's record before it stores the credential and
// before it starts the watch. A process that stops between the steps
// then leaves a record without a credential, which a later Connect,
// Disconnect or Start finds; a credential without a record would be
// unreachable and could never be removed.
func TestConnectWritesTheHubRecordBeforeTheCredentialAndTheWatch(t *testing.T) {
	h := newHarness(t)
	entered, release := h.gated.holdNextPut(0)
	defer release()

	done := make(chan error, 1)
	go func() {
		_, err := h.svc.Connect(h.ctx, ConnectRequest{
			HubID:       "hub1",
			DisplayName: "primary",
			Connection:  HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken},
			Actor:       "ops@example.test",
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not reach the credential store")
	}

	obs, _, ok := h.hubRecordStored("hub1")
	if !ok {
		t.Fatal("the hub record is not stored while the credential write is held")
	}
	if obs.APIServer != hubAPI || obs.DisplayName != "primary" {
		t.Fatalf("record stored before the credential = %+v", obs)
	}
	if h.svc.hub("hub1") != nil {
		t.Fatal("the watch started before the credential was stored")
	}

	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not finish after the credential write landed")
	}
	if h.svc.hub("hub1") == nil {
		t.Fatal("hub is not watched after Connect")
	}
}

// A Connect that cannot write the hub's record reports failure before
// it stores the credential, so there is nothing to undo.
func TestConnectLeavesNoTraceWhenTheRecordCannotBeWritten(t *testing.T) {
	h := newHarness(t)
	h.records.failWrites()
	defer h.records.allowWrites()

	if _, err := h.svc.Connect(h.ctx, ConnectRequest{
		HubID:      "hub1",
		Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken},
		Actor:      "ops@example.test",
	}); err == nil {
		t.Fatal("Connect succeeded although the hub record could not be written")
	}
	assertHubLeftNoTrace(t, h)
}

// A Connect that cannot store the credential removes the record it
// wrote before reporting failure.
func TestConnectLeavesNoTraceWhenTheCredentialCannotBeStored(t *testing.T) {
	h := newHarness(t)
	h.gated.failNextPut()

	if _, err := h.svc.Connect(h.ctx, ConnectRequest{
		HubID:      "hub1",
		Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken},
		Actor:      "ops@example.test",
	}); err == nil {
		t.Fatal("Connect succeeded although the credential could not be stored")
	}
	assertHubLeftNoTrace(t, h)
}

// A Connect whose watch cannot start removes the record and the
// credential it stored before reporting failure.
func TestConnectLeavesNoTraceWhenTheWatchCannotStart(t *testing.T) {
	h := newHarness(t)
	h.failDynamicFor(hubToken)

	if _, err := h.svc.Connect(h.ctx, ConnectRequest{
		HubID:      "hub1",
		Connection: HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken},
		Actor:      "ops@example.test",
	}); err == nil {
		t.Fatal("Connect succeeded although the watch could not start")
	}
	assertHubLeftNoTrace(t, h)
}

// A reconnect that cannot write the hub's record keeps the previous
// credential and the previous watch: the running connection is not
// disturbed by a replacement that never began.
func TestFailedReconnectKeepsThePreviousConnectionWhenTheRecordCannotBeWritten(t *testing.T) {
	h := newHarness(t)
	const replacementToken = "hub-reader-token-replacement"
	h.hub.mu.Lock()
	h.hub.tokens[replacementToken] = h.hub.tokens[hubToken]
	h.hub.mu.Unlock()
	h.connectAs("original", hubToken)

	h.records.failWrites()
	_, err := h.svc.Connect(h.ctx, ConnectRequest{
		HubID:       "hub1",
		DisplayName: "replacement",
		Connection:  HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: replacementToken},
		Actor:       "ops@example.test",
	})
	h.records.allowWrites()
	if err == nil {
		t.Fatal("reconnect succeeded although the hub record could not be written")
	}

	conn, err := h.svc.loadConnection(h.ctx, "hub1")
	if err != nil {
		t.Fatalf("load stored credential: %v", err)
	}
	if conn.Token != hubToken {
		t.Errorf("stored credential = %q, want the previous connection's", conn.Token)
	}
	obs, _, ok := h.hubRecordStored("hub1")
	if !ok {
		t.Fatal("hub record missing after a failed reconnect")
	}
	if obs.DisplayName != "original" {
		t.Errorf("record display name = %q, want the previous connection's", obs.DisplayName)
	}
	hw := h.svc.hub("hub1")
	if hw == nil {
		t.Fatal("hub is not watched after a failed reconnect")
	}
	if hw.connection().Token != hubToken {
		t.Errorf("the watch runs with the replacement credential, want the previous one")
	}
}

// A reconnect whose watch cannot start puts back the previous record,
// credential and watch before reporting failure.
func TestFailedReconnectRestoresThePreviousConnectionWhenTheWatchCannotStart(t *testing.T) {
	h := newHarness(t)
	const replacementToken = "hub-reader-token-replacement"
	h.hub.mu.Lock()
	h.hub.tokens[replacementToken] = h.hub.tokens[hubToken]
	h.hub.mu.Unlock()
	h.connectAs("original", hubToken)
	h.failDynamicFor(replacementToken)

	_, err := h.svc.Connect(h.ctx, ConnectRequest{
		HubID:       "hub1",
		DisplayName: "replacement",
		Connection:  HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: replacementToken},
		Actor:       "ops@example.test",
	})
	if err == nil {
		t.Fatal("reconnect succeeded although the new watch could not start")
	}

	conn, err := h.svc.loadConnection(h.ctx, "hub1")
	if err != nil {
		t.Fatalf("load stored credential: %v", err)
	}
	if conn.Token != hubToken {
		t.Errorf("stored credential = %q, want the previous connection's", conn.Token)
	}
	obs, _, ok := h.hubRecordStored("hub1")
	if !ok {
		t.Fatal("hub record missing after a failed reconnect")
	}
	if obs.DisplayName != "original" {
		t.Errorf("record display name = %q, want the previous connection's", obs.DisplayName)
	}
	hw := h.svc.hub("hub1")
	if hw == nil {
		t.Fatal("hub is not watched after a failed reconnect")
	}
	if hw.connection().Token != hubToken {
		t.Errorf("the watch runs with the replacement credential, want the previous one")
	}
}

// connectAs connects hub1 and waits until its watch reports the hub
// connected, so that a replacement starts from a settled watch.
func (h *harness) connectAs(displayName, token string) {
	h.t.Helper()
	if _, err := h.svc.Connect(h.ctx, ConnectRequest{
		HubID:       "hub1",
		DisplayName: displayName,
		Connection:  HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: token},
		Actor:       "ops@example.test",
	}); err != nil {
		h.t.Fatalf("Connect(%s): %v", displayName, err)
	}
	eventually(h.t, "the hub is connected", func() bool {
		_, conds, ok := h.hubRecordStored("hub1")
		c, found := conds[string(HubConditionReady)]
		return ok && found && c.Status() == domain.ConditionTrue
	})
}

// assertHubLeftNoTrace checks that hub1 has no record, no stored
// credential and no watch.
func assertHubLeftNoTrace(t *testing.T, h *harness) {
	t.Helper()
	if _, _, ok := h.hubRecordStored("hub1"); ok {
		t.Error("hub record stored although Connect reported failure")
	}
	if _, err := h.vault.Get(h.ctx, hubConnectionRef("hub1")); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("credential stored although Connect reported failure: %v", err)
	}
	if h.svc.hub("hub1") != nil {
		t.Error("hub is watched although Connect reported failure")
	}
}

func TestReplacedWatchStopsWriting(t *testing.T) {
	h := newHarness(t)
	conn := HubConnection{APIServer: hubAPI, CACert: string(h.hubCA), Token: hubToken}
	obs := HubObservation{ID: "hub1"}
	if err := h.svc.startWatch("hub1", conn, obs); err != nil {
		t.Fatalf("startWatch: %v", err)
	}
	stale := h.svc.hub("hub1")
	if err := h.svc.startWatch("hub1", conn, obs); err != nil {
		t.Fatalf("replacing startWatch: %v", err)
	}
	current := h.svc.hub("hub1")
	if current == nil || current == stale {
		t.Fatal("startWatch did not replace the registered watch")
	}
	// Remove the replacement and stop it, then clear the record it may
	// have written: only a write by the replaced watch could bring the
	// record back.
	h.svc.mu.Lock()
	delete(h.svc.hubs, "hub1")
	h.svc.mu.Unlock()
	current.stop()
	if err := h.svc.records.ReplaceBatch(h.ctx, HubResourceType, []kubernetesaddon.InventoryObjectReport{{
		Name: HubRecordName("hub1"), IsDelete: true,
	}}); err != nil {
		t.Fatalf("delete the hub record: %v", err)
	}

	if err := stale.writeHubRecord(h.ctx, h.svc.now().UTC(), true); err != nil {
		t.Fatalf("stale writeHubRecord: %v", err)
	}
	if _, _, ok := h.hubRecordStored("hub1"); ok {
		t.Error("a replaced watch wrote the hub record")
	}
}
