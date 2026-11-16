// Package deploymentrepotest provides contract tests for
// [domain.DeploymentRepository] implementations.
package deploymentrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// Factory creates a fresh [domain.Tx] for each test so callers can use
// [domain.Tx.Fulfillments] before creating deployments (foreign key)
// and [domain.Tx.Deployments] for the repository under test.
type Factory func(t *testing.T) domain.Tx

// Run exercises the [domain.DeploymentRepository] contract.
func Run(t *testing.T, factory Factory) {
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	sampleFulfillment := func(id domain.FulfillmentID) *domain.Fulfillment {
		f := domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
			ID:        id,
			State:     domain.FulfillmentStateCreating,
			CreatedAt: fixedTime,
			UpdatedAt: fixedTime,
		})
		f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
		}, fixedTime)
		f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1", "t2"},
		}, fixedTime)
		return f
	}

	sampleThinDeployment := func(depName domain.ResourceName, fid domain.FulfillmentID) domain.Deployment {
		return domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
			Name:          depName,
			UID:           domain.NewDeploymentUID(),
			FulfillmentID: fid,
			CreatedAt:     fixedTime,
			UpdatedAt:     fixedTime,
		})
	}

	sampleDelivery := func(fid domain.FulfillmentID, tid domain.TargetID, state domain.DeliveryState, message string, attempts int) domain.Delivery {
		return domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
			ID:            domain.DeliveryID(string(fid) + ":" + string(tid)),
			FulfillmentID: fid,
			TargetID:      tid,
			Manifests:     []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
			Generation:    1,
			State:         state,
			Operation:     domain.DeliveryOperationDeliver,
			Message:       message,
			Attempts:      attempts,
			CreatedAt:     fixedTime,
			UpdatedAt:     fixedTime,
		})
	}

	// checkDeliveries asserts that got matches want, including the
	// target-ID ordering the view promises.
	checkDeliveries := func(t *testing.T, prefix string, got, want []domain.Delivery) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: Deliveries len = %d, want %d", prefix, len(got), len(want))
		}
		for i := range want {
			if got[i].TargetID() != want[i].TargetID() {
				t.Errorf("%s: Deliveries[%d].TargetID = %q, want %q", prefix, i, got[i].TargetID(), want[i].TargetID())
			}
			if got[i].State() != want[i].State() {
				t.Errorf("%s: Deliveries[%d].State = %q, want %q", prefix, i, got[i].State(), want[i].State())
			}
			if got[i].Message() != want[i].Message() {
				t.Errorf("%s: Deliveries[%d].Message = %q, want %q", prefix, i, got[i].Message(), want[i].Message())
			}
			if got[i].Attempts() != want[i].Attempts() {
				t.Errorf("%s: Deliveries[%d].Attempts = %d, want %d", prefix, i, got[i].Attempts(), want[i].Attempts())
			}
		}
	}

	t.Run("CreateAndGet", func(t *testing.T) {
		tx := factory(t)
		defer tx.RollbackOnExit()

		fid := domain.FulfillmentID("f-create-get")
		if err := tx.Fulfillments().Create(ctx, sampleFulfillment(fid)); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}
		d := sampleThinDeployment("deployments/d-create-get", fid)
		repo := tx.Deployments()

		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "deployments/d-create-get")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.FulfillmentID() != fid {
			t.Errorf("FulfillmentID = %q, want %q", got.FulfillmentID(), fid)
		}
		if got.UID().IsZero() {
			t.Error("UID is zero, want non-zero")
		}
		if !got.CreatedAt().Equal(fixedTime) {
			t.Errorf("CreatedAt = %v, want %v", got.CreatedAt(), fixedTime)
		}
		if !got.UpdatedAt().Equal(fixedTime) {
			t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt(), fixedTime)
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		tx := factory(t)
		defer tx.RollbackOnExit()

		fid := domain.FulfillmentID("f-dup")
		if err := tx.Fulfillments().Create(ctx, sampleFulfillment(fid)); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}
		d := sampleThinDeployment("deployments/d-dup", fid)
		repo := tx.Deployments()
		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		err := repo.Create(ctx, d)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second Create: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.RollbackOnExit()

		_, err := tx.Deployments().Get(ctx, "deployments/nonexistent-deployment")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get: got %v, want ErrNotFound", err)
		}
	})

	t.Run("GetView", func(t *testing.T) {
		tx := factory(t)
		defer tx.RollbackOnExit()

		f := sampleFulfillment("f-view")
		f.AdvanceRolloutStrategy(&domain.RolloutStrategySpec{
			Type:                  domain.RolloutStrategyImmediate,
			VersionConflictPolicy: domain.VersionConflictCompleteAll,
		}, fixedTime)

		if err := tx.Fulfillments().Create(ctx, f); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}

		d := sampleThinDeployment("deployments/d-view", f.ID())
		repo := tx.Deployments()
		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create deployment: %v", err)
		}

		// Deliveries of the fulfillment are part of the view: the API
		// reports why a target failed from here. Seed them out of target
		// order, plus one belonging to an unrelated fulfillment that
		// must not leak into this view.
		const reason = `apply object 1: conflict with "work-agent": .data.owner`
		wantDeliveries := []domain.Delivery{
			sampleDelivery(f.ID(), "t1", domain.DeliveryStateFailed, reason, 2),
			sampleDelivery(f.ID(), "t2", domain.DeliveryStateDelivered, "", 0),
		}
		for _, dl := range []domain.Delivery{
			sampleDelivery(f.ID(), "t2", domain.DeliveryStateDelivered, "", 0),
			sampleDelivery("f-view-other", "t9", domain.DeliveryStateFailed, "other fulfillment", 1),
			sampleDelivery(f.ID(), "t1", domain.DeliveryStateFailed, reason, 2),
		} {
			if err := tx.Deliveries().Put(ctx, dl); err != nil {
				t.Fatalf("Put delivery %s: %v", dl.ID(), err)
			}
		}

		v, err := repo.GetView(ctx, "deployments/d-view")
		if err != nil {
			t.Fatalf("GetView: %v", err)
		}
		if v.Deployment.Name() != "deployments/d-view" {
			t.Errorf("Deployment.Name = %q, want %q", v.Deployment.Name(), "deployments/d-view")
		}
		if v.Deployment.FulfillmentID() != f.ID() {
			t.Errorf("Deployment.FulfillmentID = %q, want %q", v.Deployment.FulfillmentID(), f.ID())
		}
		if v.Fulfillment.ID() != f.ID() {
			t.Errorf("Fulfillment.ID = %q, want %q", v.Fulfillment.ID(), f.ID())
		}
		if v.Fulfillment.ManifestStrategy().Type != domain.ManifestStrategyInline {
			t.Errorf("Fulfillment.ManifestStrategy.Type = %q, want %q", v.Fulfillment.ManifestStrategy().Type, domain.ManifestStrategyInline)
		}
		if len(v.Fulfillment.PlacementStrategy().Targets) != 2 {
			t.Errorf("Fulfillment.PlacementStrategy.Targets len = %d, want 2", len(v.Fulfillment.PlacementStrategy().Targets))
		}
		if v.Fulfillment.State() != domain.FulfillmentStateCreating {
			t.Errorf("Fulfillment.State = %q, want %q", v.Fulfillment.State(), domain.FulfillmentStateCreating)
		}
		if v.Fulfillment.RolloutStrategy() == nil {
			t.Fatal("Fulfillment.RolloutStrategy is nil after GetView")
		}
		if v.Fulfillment.RolloutStrategy().Type != domain.RolloutStrategyImmediate {
			t.Errorf("RolloutStrategy.Type = %q, want %q", v.Fulfillment.RolloutStrategy().Type, domain.RolloutStrategyImmediate)
		}
		checkDeliveries(t, "GetView", v.Deliveries, wantDeliveries)
	})

	t.Run("ListView", func(t *testing.T) {
		tx := factory(t)
		defer tx.RollbackOnExit()

		// The names differ in ways a locale collation orders differently
		// from byte-wise comparison: an upper-case letter and a
		// punctuation character before the lower-case names. ListView
		// promises byte order, because the page token walk seeks with
		// Go's comparison.
		wantOrder := []domain.ResourceName{
			"deployments/D-list",
			"deployments/_list",
			"deployments/d-list-a",
			"deployments/d-list-b",
		}
		for _, pair := range []struct {
			depName domain.ResourceName
			fid     domain.FulfillmentID
		}{
			{"deployments/d-list-a", "f-list-a"},
			{"deployments/d-list-b", "f-list-b"},
			{"deployments/D-list", "f-list-upper"},
			{"deployments/_list", "f-list-punct"},
		} {
			f := sampleFulfillment(pair.fid)
			if err := tx.Fulfillments().Create(ctx, f); err != nil {
				t.Fatalf("Create fulfillment %q: %v", pair.fid, err)
			}
			d := sampleThinDeployment(pair.depName, pair.fid)
			if err := tx.Deployments().Create(ctx, d); err != nil {
				t.Fatalf("Create deployment %q: %v", pair.depName, err)
			}
		}

		// Each view carries the deliveries of its own fulfillment.
		wantDeliveries := map[domain.ResourceName][]domain.Delivery{
			"deployments/d-list-a": {
				sampleDelivery("f-list-a", "t1", domain.DeliveryStateDelivered, "", 0),
			},
			"deployments/d-list-b": {
				sampleDelivery("f-list-b", "t1", domain.DeliveryStateFailed, "apply failed", 3),
				sampleDelivery("f-list-b", "t2", domain.DeliveryStateProgressing, "", 0),
			},
		}
		for _, dl := range []domain.Delivery{
			sampleDelivery("f-list-a", "t1", domain.DeliveryStateDelivered, "", 0),
			sampleDelivery("f-list-b", "t2", domain.DeliveryStateProgressing, "", 0),
			sampleDelivery("f-list-b", "t1", domain.DeliveryStateFailed, "apply failed", 3),
		} {
			if err := tx.Deliveries().Put(ctx, dl); err != nil {
				t.Fatalf("Put delivery %s: %v", dl.ID(), err)
			}
		}

		views, err := tx.Deployments().ListView(ctx)
		if err != nil {
			t.Fatalf("ListView: %v", err)
		}
		if len(views) != len(wantOrder) {
			t.Fatalf("ListView len = %d, want %d", len(views), len(wantOrder))
		}
		for i, v := range views {
			if got := v.Deployment.Name(); got != wantOrder[i] {
				t.Errorf("ListView[%d].Name = %q, want %q (byte order)", i, got, wantOrder[i])
			}
			if v.Deployment.FulfillmentID() != v.Fulfillment.ID() {
				t.Errorf("deployment %s: FulfillmentID %q != Fulfillment.ID %q", v.Deployment.Name(), v.Deployment.FulfillmentID(), v.Fulfillment.ID())
			}
			checkDeliveries(t, string(v.Deployment.Name()), v.Deliveries, wantDeliveries[v.Deployment.Name()])
		}
	})

	t.Run("ListViewPage", func(t *testing.T) {
		tx := factory(t)
		defer tx.RollbackOnExit()

		// The same byte-order trap as ListView, on a collection small
		// enough to walk one view at a time.
		wantOrder := []domain.ResourceName{
			"deployments/D-page",
			"deployments/_page",
			"deployments/d-page",
		}
		for _, pair := range []struct {
			depName domain.ResourceName
			fid     domain.FulfillmentID
		}{
			{"deployments/d-page", "f-page-lower"},
			{"deployments/D-page", "f-page-upper"},
			{"deployments/_page", "f-page-punct"},
		} {
			if err := tx.Fulfillments().Create(ctx, sampleFulfillment(pair.fid)); err != nil {
				t.Fatalf("Create fulfillment %q: %v", pair.fid, err)
			}
			d := sampleThinDeployment(pair.depName, pair.fid)
			if err := tx.Deployments().Create(ctx, d); err != nil {
				t.Fatalf("Create deployment %q: %v", pair.depName, err)
			}
		}

		// Each page carries the deliveries of the fulfillments it holds
		// and no others. The middle deployment's fulfillment has none,
		// which keeps a page that loads nothing from looking correct.
		wantDeliveries := map[domain.ResourceName][]domain.Delivery{
			"deployments/D-page": {
				sampleDelivery("f-page-upper", "t2", domain.DeliveryStateDelivered, "", 0),
			},
			"deployments/d-page": {
				sampleDelivery("f-page-lower", "t1", domain.DeliveryStateFailed, "apply failed", 2),
			},
		}
		for _, dl := range []domain.Delivery{
			sampleDelivery("f-page-lower", "t1", domain.DeliveryStateFailed, "apply failed", 2),
			sampleDelivery("f-page-upper", "t2", domain.DeliveryStateDelivered, "", 0),
		} {
			if err := tx.Deliveries().Put(ctx, dl); err != nil {
				t.Fatalf("Put delivery %s: %v", dl.ID(), err)
			}
		}

		repo := tx.Deployments()
		var visited []domain.ResourceName
		after := domain.ResourceName("")
		for {
			if len(visited) > len(wantOrder) {
				t.Fatalf("ListViewPage walk visited %d views, want %d", len(visited), len(wantOrder))
			}
			page, err := repo.ListViewPage(ctx, after, 1)
			if err != nil {
				t.Fatalf("ListViewPage(after %q): %v", after, err)
			}
			if len(page.Views) != 1 {
				t.Fatalf("ListViewPage(after %q) len = %d, want 1", after, len(page.Views))
			}
			v := page.Views[0]
			name := v.Deployment.Name()
			visited = append(visited, name)
			checkDeliveries(t, string(name), v.Deliveries, wantDeliveries[name])
			if !page.More {
				break
			}
			after = name
		}
		if len(visited) != len(wantOrder) {
			t.Fatalf("ListViewPage walk visited %d views, want %d", len(visited), len(wantOrder))
		}
		for i, got := range visited {
			if got != wantOrder[i] {
				t.Errorf("ListViewPage walk[%d] = %q, want %q (byte order)", i, got, wantOrder[i])
			}
		}

		// A page that exactly fills the limit reports no more.
		full, err := repo.ListViewPage(ctx, "", len(wantOrder))
		if err != nil {
			t.Fatalf("ListViewPage(full): %v", err)
		}
		if len(full.Views) != len(wantOrder) || full.More {
			t.Fatalf("full page = (%d views, more %t), want (%d, false)", len(full.Views), full.More, len(wantOrder))
		}

		// The predicate is strictly after the token name: a name that is
		// stored nowhere between two views resumes at the next view,
		// and the last stored name ends the walk.
		gap, err := repo.ListViewPage(ctx, "deployments/C", len(wantOrder))
		if err != nil {
			t.Fatalf("ListViewPage(gap): %v", err)
		}
		if len(gap.Views) != len(wantOrder) {
			t.Fatalf("ListViewPage(gap) len = %d, want %d", len(gap.Views), len(wantOrder))
		}
		if got := gap.Views[0].Deployment.Name(); got != wantOrder[0] {
			t.Errorf("ListViewPage(gap) starts at %q, want %q", got, wantOrder[0])
		}
		past, err := repo.ListViewPage(ctx, wantOrder[len(wantOrder)-1], len(wantOrder))
		if err != nil {
			t.Fatalf("ListViewPage(past): %v", err)
		}
		if len(past.Views) != 0 || past.More {
			t.Fatalf("past page = (%d views, more %t), want (0, false)", len(past.Views), past.More)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tx := factory(t)
		defer tx.RollbackOnExit()

		fid := domain.FulfillmentID("f-del")
		if err := tx.Fulfillments().Create(ctx, sampleFulfillment(fid)); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}
		d := sampleThinDeployment("deployments/d-del", fid)
		repo := tx.Deployments()
		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Delete(ctx, "deployments/d-del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := repo.Get(ctx, "deployments/d-del")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.RollbackOnExit()

		err := tx.Deployments().Delete(ctx, "deployments/nonexistent-deployment")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Delete: got %v, want ErrNotFound", err)
		}
	})
}
