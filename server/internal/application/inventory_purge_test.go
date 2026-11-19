package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

const purgeTestObjectType domain.ResourceType = "inv.open-cluster-management.io/Object"

// seedPurgeObjectType registers an inventory-only type whose resources
// nest under a parent, the shape the Kubernetes object inventory uses.
func seedPurgeObjectType(t *testing.T, store domain.Store) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.RollbackOnExit()
	def := domain.NewExtensionResourceType(purgeTestObjectType, "v1", "objects", time.Now(), domain.WithInventory())
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func reportPurgeObjects(t *testing.T, svc *application.InventoryReportService, names ...domain.ResourceName) {
	t.Helper()
	in := application.InventoryReplacementBatchInput{}
	for i := range names {
		in.Reports = append(in.Reports, application.InventoryReplacementInput{
			ResourceType: purgeTestObjectType,
			Name:         &names[i],
			ObservedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		})
	}
	if err := svc.ReplaceBatch(context.Background(), in); err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}
}

func TestInventoryReportService_PurgeCollectionPrefix_RemovesOneParentOnly(t *testing.T) {
	store := newStore(t)
	seedPurgeObjectType(t, store)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	reportPurgeObjects(t, svc,
		"clusters/gone/apiResources/pods/objects/u1",
		"clusters/gone/namespaces/ns/apiResources/pods/objects/u2",
		"clusters/gone-too/apiResources/pods/objects/u3",
		"clusters/kept/apiResources/pods/objects/u4",
	)

	before, err := svc.ListCollections(ctx, purgeTestObjectType)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(before) != 4 {
		t.Fatalf("ListCollections before purge = %v, want 4 collections", before)
	}

	n, err := svc.PurgeCollectionPrefix(ctx, purgeTestObjectType, "clusters/gone/")
	if err != nil {
		t.Fatalf("PurgeCollectionPrefix: %v", err)
	}
	if n != 2 {
		t.Errorf("purged = %d, want 2", n)
	}

	after, err := svc.ListCollections(ctx, purgeTestObjectType)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	want := map[domain.CollectionName]bool{
		"clusters/gone-too/apiResources/pods/objects": true,
		"clusters/kept/apiResources/pods/objects":     true,
	}
	if len(after) != len(want) {
		t.Fatalf("ListCollections after purge = %v, want %v", after, want)
	}
	for _, name := range after {
		if !want[name] {
			t.Errorf("unexpected collection %q left after purge", name)
		}
	}

	again, err := svc.PurgeCollectionPrefix(ctx, purgeTestObjectType, "clusters/gone/")
	if err != nil {
		t.Fatalf("PurgeCollectionPrefix (repeat): %v", err)
	}
	if again != 0 {
		t.Errorf("purged on repeat = %d, want 0", again)
	}
}

func TestInventoryReportService_PurgeCollectionPrefix_Rejections(t *testing.T) {
	store := newStore(t)
	seedPurgeObjectType(t, store)
	const managedPlus domain.ResourceType = "widgets.open-cluster-management.io/Widget"
	seedManagedPlusInventoryType(t, store, managedPlus)
	svc := application.NewInventoryReportService(store)
	ctx := context.Background()

	if _, err := svc.PurgeCollectionPrefix(ctx, purgeTestObjectType, ""); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("empty prefix: got %v, want ErrInvalidArgument", err)
	}
	// A type that is also managed keeps its rows: they are not the
	// reporter's to remove.
	if _, err := svc.PurgeCollectionPrefix(ctx, managedPlus, "widgets"); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("managed-plus-inventory type: got %v, want ErrInvalidArgument", err)
	}
	if _, err := svc.PurgeCollectionPrefix(ctx, "unknown.open-cluster-management.io/Thing", "things"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("unknown type: got %v, want ErrNotFound", err)
	}
	if _, err := svc.ListCollections(ctx, "unknown.open-cluster-management.io/Thing"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("ListCollections unknown type: got %v, want ErrNotFound", err)
	}

	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := svc.PurgeStaleCollectionPrefix(ctx, purgeTestObjectType, "", since); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("stale purge, empty prefix: got %v, want ErrInvalidArgument", err)
	}
	if _, err := svc.PurgeStaleCollectionPrefix(ctx, purgeTestObjectType, "clusters/a/", time.Time{}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("stale purge, no time: got %v, want ErrInvalidArgument", err)
	}
	if _, err := svc.PurgeStaleCollectionPrefix(ctx, managedPlus, "widgets", since); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("stale purge, managed-plus-inventory type: got %v, want ErrInvalidArgument", err)
	}
	if _, err := svc.PurgeStaleCollectionPrefix(ctx, "unknown.open-cluster-management.io/Thing", "things", since); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("stale purge, unknown type: got %v, want ErrNotFound", err)
	}
}

// A reporter that starts again reports everything that exists; what it
// does not report again is removed, and only under its own parent.
func TestInventoryReportService_PurgeStaleCollectionPrefix_RemovesWhatWasNotReportedAgain(t *testing.T) {
	store := newStore(t)
	seedPurgeObjectType(t, store)
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	svc := application.NewInventoryReportService(store, application.WithInventoryReportClock(func() time.Time { return now }))
	ctx := context.Background()

	// An earlier run of the reporter.
	reportPurgeObjects(t, svc,
		"clusters/c/apiResources/pods/objects/still-there",
		"clusters/c/apiResources/pods/objects/deleted-meanwhile",
		"clusters/c/namespaces/ns/apiResources/pods/objects/deleted-too",
		"clusters/other/apiResources/pods/objects/untouched",
	)

	// The reporter starts again an hour later and reports what exists now.
	restarted := now.Add(time.Hour)
	now = restarted.Add(3 * time.Second)
	reportPurgeObjects(t, svc,
		"clusters/c/apiResources/pods/objects/still-there",
		"clusters/c/apiResources/pods/objects/new",
	)

	n, err := svc.PurgeStaleCollectionPrefix(ctx, purgeTestObjectType, "clusters/c/", restarted)
	if err != nil {
		t.Fatalf("PurgeStaleCollectionPrefix: %v", err)
	}
	if n != 2 {
		t.Errorf("purged = %d, want 2", n)
	}
	after, err := svc.ListCollections(ctx, purgeTestObjectType)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	want := map[domain.CollectionName]bool{
		"clusters/c/apiResources/pods/objects":     true,
		"clusters/other/apiResources/pods/objects": true,
	}
	if len(after) != len(want) {
		t.Fatalf("collections after purge = %v, want %v", after, want)
	}
	for _, name := range after {
		if !want[name] {
			t.Errorf("unexpected collection %q left after purge", name)
		}
	}
	if again, err := svc.PurgeStaleCollectionPrefix(ctx, purgeTestObjectType, "clusters/c/", restarted); err != nil || again != 0 {
		t.Errorf("repeat purge = %d, %v; want 0, nil", again, err)
	}
}
