package kubernetes

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestPlacementReevaluation_FirstEnrollmentAndPlainRefresh(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)

	req := EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token,
		Labels: map[string]string{"env": "prod"}}
	if _, err := h.svc.Enroll(ctx, req); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if got := h.placement.count(); got != 1 {
		t.Fatalf("placement re-evaluations after first enrollment = %d, want 1", got)
	}

	// The same cluster presents again with the same target: a credential
	// refresh, not a fleet change.
	if _, err := h.svc.Enroll(ctx, req); err != nil {
		t.Fatalf("Enroll again: %v", err)
	}
	if got := h.placement.count(); got != 1 {
		t.Fatalf("placement re-evaluations after a plain refresh = %d, want 1", got)
	}

	// Changed labels move the target in and out of selector placement.
	req.Labels = map[string]string{"env": "staging"}
	if _, err := h.svc.Enroll(ctx, req); err != nil {
		t.Fatalf("Enroll with new labels: %v", err)
	}
	if got := h.placement.count(); got != 2 {
		t.Fatalf("placement re-evaluations after a label change = %d, want 2", got)
	}
}

func TestPlacementReevaluation_SetExternalLabelsOnlyOnChange(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	before := h.placement.count()

	changed, err := h.svc.SetExternalLabels(ctx, "ext", map[string]string{"hub": "hub1"})
	if err != nil {
		t.Fatalf("SetExternalLabels: %v", err)
	}
	if !changed {
		t.Fatal("SetExternalLabels reported no change")
	}
	if got := h.placement.count(); got != before+1 {
		t.Fatalf("placement re-evaluations after a label change = %d, want %d", got, before+1)
	}

	// Writing the same labels again is not a fleet change.
	changed, err = h.svc.SetExternalLabels(ctx, "ext", map[string]string{"hub": "hub1"})
	if err != nil {
		t.Fatalf("SetExternalLabels again: %v", err)
	}
	if changed {
		t.Fatal("SetExternalLabels reported a change for identical labels")
	}
	if got := h.placement.count(); got != before+1 {
		t.Fatalf("placement re-evaluations after an unchanged write = %d, want %d", got, before+1)
	}
}

func TestPlacementReevaluation_LapseAndRecovery(t *testing.T) {
	h, api := enrolledHarness(t, time.Hour)
	ctx := context.Background()
	if got := h.placement.count(); got != 1 {
		t.Fatalf("placement re-evaluations after enrollment = %d, want 1", got)
	}

	h.cluster(api).denyRenew = true
	h.clock.Advance(45 * time.Minute)
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster (cannot renew): %v", err)
	}
	if got := h.placement.count(); got != 1 {
		t.Fatalf("a still-valid credential re-ran placement (%d calls)", got)
	}

	h.clock.Advance(16 * time.Minute) // past expiry: the target takes no new placement
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster (lapsed): %v", err)
	}
	if got := h.placement.count(); got != 2 {
		t.Fatalf("placement re-evaluations after a lapse = %d, want 2", got)
	}

	// The cluster presents a fresh credential: it takes new placement again.
	_, bearer, err := h.svc.CreateBundle(ctx, CreateBundleRequest{
		Scope: BundleScope{ClusterID: "ext", APIServer: api}, Actor: "ops",
	})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	if _, err := h.svc.Present(ctx, bearer, PresentRequest{
		CACert: h.cluster(api).ca, Token: h.mintToken(api, "fresh", time.Hour),
	}); err != nil {
		t.Fatalf("Present: %v", err)
	}
	if got := h.placement.count(); got != 3 {
		t.Fatalf("placement re-evaluations after recovery = %d, want 3", got)
	}

	// A target that is not ready while its stored credential still
	// works is made ready on the next check, without a presentation to
	// carry the change.
	target, found := h.target("ext")
	if !found {
		t.Fatal("target not found after recovery")
	}
	waiting := domain.NewTargetInfo(target.ID(), target.Type(), target.Name(), domain.TargetStateInitializing,
		target.Labels(), target.Properties(), target.AcceptedManifestTypes())
	if err := h.svc.putTarget(ctx, waiting, h.clock.Now()); err != nil {
		t.Fatalf("putTarget: %v", err)
	}
	if err := h.svc.CheckCluster(ctx, "ext"); err != nil {
		t.Fatalf("CheckCluster (rejoined): %v", err)
	}
	if got := h.placement.count(); got != 4 {
		t.Fatalf("placement re-evaluations after a check-driven recovery = %d, want 4", got)
	}
}

func TestPlacementReevaluation_Withdrawal(t *testing.T) {
	h := newEnrollHarness(t)
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)
	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	before := h.placement.count()

	if _, err := h.svc.Withdraw(ctx, "ext", WithdrawRequest{Actor: "ops"}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if got := h.placement.count(); got != before+1 {
		t.Fatalf("placement re-evaluations after withdrawal = %d, want %d", got, before+1)
	}
}

func TestPlacementReevaluation_FailureDoesNotFailEnrollment(t *testing.T) {
	h := newEnrollHarness(t)
	h.placement.err = errors.New("workflow backend unavailable")
	ctx := context.Background()
	api, ca, token := h.addCluster("ext", time.Hour)

	if _, err := h.svc.Enroll(ctx, EnrollRequest{ClusterID: "ext", APIServer: api, CACert: ca, Token: token}); err != nil {
		t.Fatalf("Enroll with a failing placement re-run: %v", err)
	}
	target, found := h.target("ext")
	if !found || target.State() != domain.TargetStateReady {
		t.Fatal("a failed placement re-run undid the enrollment")
	}
	if got := h.placement.count(); got != 1 {
		t.Fatalf("placement re-evaluations = %d, want 1", got)
	}
}
