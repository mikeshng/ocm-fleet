package kubernetes

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func enrolledHarness(t *testing.T, lifetime time.Duration) (*enrollHarness, string) {
	t.Helper()
	h := newEnrollHarness(t)
	api, ca, token := h.addCluster("ext", lifetime)
	if _, err := h.svc.Enroll(context.Background(), EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	return h, api
}

func TestCheckCluster_RenewsWhenAThirdOfTheLifetimeIsLeft(t *testing.T) {
	h, _ := enrolledHarness(t, time.Hour)
	ctx := context.Background()
	first, _ := h.vaultToken("ext")

	h.clock.Advance(30 * time.Minute)
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	if got, _ := h.vaultToken("ext"); got != first {
		t.Fatal("the credential was renewed with half its lifetime left")
	}

	h.clock.Advance(11 * time.Minute) // 19 minutes left of 60
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	renewed, _ := h.vaultToken("ext")
	if renewed == first {
		t.Fatal("the credential was not renewed with less than a third of its lifetime left")
	}
	if cred, _ := h.indexing.credentialOf("k8s-ext"); cred != renewed {
		t.Error("the indexer does not run with the renewed credential")
	}
	rec, _ := h.svc.GetCluster(ctx, "ext")
	if rec.Observation.CredentialExpiresAt == nil || !rec.Observation.CredentialExpiresAt.Equal(h.clock.Now().Add(time.Hour)) {
		t.Errorf("credentialExpiresAt = %v", rec.Observation.CredentialExpiresAt)
	}

	// A delivery after the first credential's expiry still has a valid one.
	h.clock.Advance(25 * time.Minute)
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	if target, _ := h.target("ext"); target.State() != domain.TargetStateReady {
		t.Errorf("target state = %q after the first credential expired, want ready", target.State())
	}
}

func TestCheckCluster_CannotRenewThenLapsesThenRecovers(t *testing.T) {
	h, api := enrolledHarness(t, time.Hour)
	ctx := context.Background()
	h.cluster(api).denyRenew = true

	h.clock.Advance(45 * time.Minute)
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	conds := h.conditions("ext")
	if c := conds[ClusterConditionCredentialValid]; c.Status() != domain.ConditionTrue || c.Reason() != "CannotRenew" {
		t.Errorf("CredentialValid = %q/%q, want True/CannotRenew", c.Status(), c.Reason())
	}
	rec, _ := h.svc.GetCluster(ctx, "ext")
	if rec.Observation.Rights == nil || rec.Observation.Rights.CanRenew {
		t.Errorf("rights = %+v, want canRenew false", rec.Observation.Rights)
	}
	if target, _ := h.target("ext"); target.State() != domain.TargetStateReady {
		t.Errorf("target state = %q while the credential is still valid", target.State())
	}

	h.clock.Advance(16 * time.Minute) // past expiry
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	target, found := h.target("ext")
	if !found {
		t.Fatal("a lapsed credential removed the target")
	}
	if target.State() != domain.TargetStateInitializing {
		t.Errorf("target state = %q, want initializing (out of placement)", target.State())
	}
	if pool := domain.PlacementTargets([]domain.TargetInfo{target}); len(pool) != 0 {
		t.Error("a target with a lapsed credential is still offered to placement")
	}
	rec, _ = h.svc.GetCluster(ctx, "ext")
	if rec.Observation.State != ClusterStateNeedsEnrollment {
		t.Errorf("state = %q, want needs-enrollment", rec.Observation.State)
	}
	conds = h.conditions("ext")
	if c := conds[ClusterConditionCredentialValid]; c.Status() != domain.ConditionFalse || c.Reason() != "Expired" {
		t.Errorf("CredentialValid = %q/%q, want False/Expired", c.Status(), c.Reason())
	}
	if conds[ClusterConditionReady].Status() != domain.ConditionFalse {
		t.Error("Ready is not False for a lapsed credential")
	}
	if _, running := h.indexing.credentialOf("k8s-ext"); running {
		t.Error("the indexer still runs with an expired credential")
	}

	// The cluster's side presents a new credential: back in placement.
	_, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{Scope: BundleScope{ClusterID: "ext", APIServer: api}, Actor: "ops"})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	ca := h.cluster(api).ca
	if _, err := h.svc.Present(ctx, bearer, PresentRequest{CACert: ca, Token: h.mintToken(api, "fresh", time.Hour)}); err != nil {
		t.Fatalf("Present: %v", err)
	}
	target, _ = h.target("ext")
	if target.State() != domain.TargetStateReady {
		t.Errorf("target state = %q after a new credential, want ready", target.State())
	}
	rec, _ = h.svc.GetCluster(ctx, "ext")
	if rec.Observation.State != ClusterStateEnrolled {
		t.Errorf("state = %q after a new credential", rec.Observation.State)
	}
}

func TestCheckCluster_ClusterSideWithdrawal(t *testing.T) {
	h, api := enrolledHarness(t, 12*time.Hour)
	ctx := context.Background()
	h.seedObjects("ext", 2)
	stored, _ := h.vaultToken("ext")

	// The cluster's side removes the access account: the credential,
	// which has not expired, is no longer recognized.
	delete(h.cluster(api).tokens, stored)

	check := func() {
		t.Helper()
		if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
			t.Fatalf("CheckCluster: %v", err)
		}
	}
	check()
	h.clock.Advance(10 * time.Second)
	check()
	if _, found := h.target("ext"); !found {
		t.Fatal("two refusals were taken for a withdrawal")
	}
	if c := h.conditions("ext")[ClusterConditionCredentialValid]; c.Status() != domain.ConditionFalse || c.Reason() != "Rejected" {
		t.Errorf("CredentialValid = %q/%q, want False/Rejected", c.Status(), c.Reason())
	}
	h.clock.Advance(10 * time.Second)
	check() // third refusal, but only 20 s after the first
	if _, found := h.target("ext"); !found {
		t.Fatal("refusals within the window were taken for a withdrawal")
	}
	h.clock.Advance(15 * time.Second)
	check()

	if _, found := h.target("ext"); found {
		t.Fatal("the target still exists after the cluster withdrew")
	}
	if _, ok := h.vaultToken("ext"); ok {
		t.Error("the credential is still stored")
	}
	if h.hasObjects("ext") {
		t.Error("the cluster's indexed objects were not removed")
	}
	rec, err := h.svc.GetCluster(ctx, "ext")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if rec.Observation.State != ClusterStateWithdrawn || rec.Observation.WithdrawnBy != "cluster" {
		t.Errorf("observation = %+v, want withdrawn by the cluster", rec.Observation)
	}
	// Withdrawn, not failing: a later check has nothing to do.
	check()
}

func TestCheckCluster_OneRefusalIsForgottenWhenTheClusterAnswersAgain(t *testing.T) {
	h, api := enrolledHarness(t, 12*time.Hour)
	ctx := context.Background()
	stored, _ := h.vaultToken("ext")
	saved := h.cluster(api).tokens[stored]

	delete(h.cluster(api).tokens, stored)
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	h.cluster(api).tokens[stored] = saved
	for i := 0; i < 5; i++ {
		h.clock.Advance(time.Minute)
		if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
			t.Fatalf("CheckCluster: %v", err)
		}
	}
	if _, found := h.target("ext"); !found {
		t.Fatal("the cluster was withdrawn although it answers again")
	}
	if c := h.conditions("ext")[ClusterConditionCredentialValid]; c.Status() != domain.ConditionTrue {
		t.Errorf("CredentialValid = %q, want True", c.Status())
	}
}

func TestCheckCluster_UnreachableIsNotWithdrawn(t *testing.T) {
	h, api := enrolledHarness(t, 12*time.Hour)
	ctx := context.Background()
	h.cluster(api).unreachable = errors.New("dial tcp: connection refused")
	for i := 0; i < 6; i++ {
		h.clock.Advance(time.Minute)
		if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
			t.Fatalf("CheckCluster: %v", err)
		}
	}
	target, found := h.target("ext")
	if !found || target.State() != domain.TargetStateReady {
		t.Fatalf("an unreachable cluster lost its target (found %v, state %q)", found, target.State())
	}
	conds := h.conditions("ext")
	if conds[ClusterConditionReachable].Status() != domain.ConditionFalse || conds[ClusterConditionReady].Status() != domain.ConditionFalse {
		t.Errorf("Reachable %q Ready %q, want both False", conds[ClusterConditionReachable].Status(), conds[ClusterConditionReady].Status())
	}
	rec, _ := h.svc.GetCluster(ctx, "ext")
	if rec.Observation.State != ClusterStateEnrolled {
		t.Errorf("state = %q, want enrolled", rec.Observation.State)
	}

	h.cluster(api).unreachable = nil
	h.clock.Advance(time.Minute)
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	if c := h.conditions("ext")[ClusterConditionReady]; c.Status() != domain.ConditionTrue {
		t.Errorf("Ready = %q after the cluster came back", c.Status())
	}
}

func TestCheckCluster_UnchangedRecordIsNotRewritten(t *testing.T) {
	h, _ := enrolledHarness(t, 12*time.Hour)
	ctx := context.Background()
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	before, err := h.svc.readRecord(ctx, ClusterResourceType, ClusterRecordName("ext"))
	if err != nil {
		t.Fatalf("readRecord: %v", err)
	}
	h.clock.Advance(time.Minute)
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	after, err := h.svc.readRecord(ctx, ClusterResourceType, ClusterRecordName("ext"))
	if err != nil {
		t.Fatalf("readRecord: %v", err)
	}
	if !after.Inventory().ObservedAt().Equal(before.Inventory().ObservedAt()) {
		t.Error("a check that changed nothing rewrote the record")
	}
}

func TestCheckCluster_RestartsAnIndexerThatIsNotRunning(t *testing.T) {
	h, _ := enrolledHarness(t, 12*time.Hour)
	ctx := context.Background()
	if err := h.indexing.StopIndexer(ctx, "k8s-ext"); err != nil {
		t.Fatalf("StopIndexer: %v", err)
	}
	h.clock.Advance(indexerEnsureInterval + time.Second)
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster: %v", err)
	}
	if _, running := h.indexing.credentialOf("k8s-ext"); !running {
		t.Error("the indexer was not started again")
	}
}

func TestWorker_SweepRemovesObjectsOfClustersThatAreGone(t *testing.T) {
	h, _ := enrolledHarness(t, 12*time.Hour)
	ctx := context.Background()
	h.seedObjects("ext", 2)      // enrolled: has a target
	h.seedObjects("gone", 3)     // no target, no indexer
	h.seedObjects("starting", 1) // no target yet, but an indexer runs
	h.seedObjects("gone.too", 1) // dotted id
	created := domain.NewTargetInfo("k8s-spoke", TargetType, "spoke", "", nil, map[string]string{
		PropAPIServer: "https://spoke:6443", PropClusterResourceName: "clusters/spoke",
	}, nil)
	if err := h.svc.putTarget(ctx, created, h.clock.Now()); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	h.seedObjects("spoke", 2) // created by the resource manager: has a target
	in, err := NewIndexRuntimeInput("k8s-starting", "clusters/starting", "https://starting:6443", "", []byte("tok"), "", 1, DefaultIndexConfig())
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := h.indexing.EnsureIndexer(ctx, in); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}

	orphans, err := h.svc.OrphanedObjectClusters(ctx, h.indexing)
	if err != nil {
		t.Fatalf("OrphanedObjectClusters: %v", err)
	}
	if len(orphans) != 2 || orphans[0] != "gone" || orphans[1] != "gone.too" {
		t.Fatalf("orphans = %v, want [gone gone.too]", orphans)
	}

	w := NewEnrollmentWorker(h.svc, h.indexing)
	w.Sweep(ctx)
	if !h.hasObjects("gone") {
		t.Fatal("objects were removed on the first sighting")
	}
	w.Sweep(ctx)
	for _, id := range []string{"gone", "gone.too"} {
		if h.hasObjects(id) {
			t.Errorf("objects of %q were not removed on the second sighting", id)
		}
	}
	for _, id := range []string{"ext", "spoke", "starting"} {
		if !h.hasObjects(id) {
			t.Errorf("objects of %q were removed", id)
		}
	}
}

func TestWorker_SweepForgetsAClusterThatGotATarget(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	h.seedObjects("late", 1)
	w := NewEnrollmentWorker(h.svc, h.indexing)
	w.Sweep(ctx) // first sighting

	late := domain.NewTargetInfo("k8s-late", TargetType, "late", "", nil, map[string]string{
		PropAPIServer: "https://late:6443", PropClusterResourceName: "clusters/late",
	}, nil)
	if err := h.svc.putTarget(ctx, late, h.clock.Now()); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	w.Sweep(ctx)
	if err := h.svc.deleteTarget(ctx, "k8s-late"); err != nil {
		t.Fatalf("deleteTarget: %v", err)
	}
	w.Sweep(ctx) // orphaned again: this is a first sighting, not a second
	if !h.hasObjects("late") {
		t.Fatal("sightings that were not consecutive removed the objects")
	}
	w.Sweep(ctx)
	if h.hasObjects("late") {
		t.Fatal("objects were not removed after two consecutive sightings")
	}
}

func TestWorker_StartAndClose(t *testing.T) {
	h, _ := enrolledHarness(t, time.Hour)
	w := NewEnrollmentWorker(h.svc, h.indexing)
	w.checkInterval = 5 * time.Millisecond
	w.sweepDelay = 5 * time.Millisecond
	w.sweepInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	w.Start(ctx) // idempotent
	time.Sleep(30 * time.Millisecond)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := w.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := (&EnrollmentWorker{}).Close(closeCtx); err != nil {
		t.Fatalf("Close of a worker that never started: %v", err)
	}
}
