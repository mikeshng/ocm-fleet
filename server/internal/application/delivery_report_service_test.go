package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

func setupDeliveryReportService(t *testing.T) (*application.DeliveryReportService, domain.Store, *recordingSignal, *testDeliveryObserver) {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	sig := &recordingSignal{}
	obs := &testDeliveryObserver{}
	svc := application.NewDeliveryReportService(store, sig, application.WithDeliveryObserver(obs))
	return svc, store, sig, obs
}

// setupDeliveryReportServiceWithVault wires a vault that shares the
// store's database, matching production where both may live in the
// same file. The service must order its vault writes around the store
// transaction for this to work.
func setupDeliveryReportServiceWithVault(t *testing.T) (*application.DeliveryReportService, domain.Store, *sqlite.VaultStore, *recordingSignal) {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	vault := &sqlite.VaultStore{DB: db}
	sig := &recordingSignal{}
	svc := application.NewDeliveryReportService(store, sig, application.WithDeliveryReportVault(vault))
	return svc, store, vault, sig
}

func getInventoryItem(t *testing.T, store domain.Store, id domain.InventoryItemID) domain.InventoryItem {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	item, err := tx.Inventory().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get inventory item %q: %v", id, err)
	}
	return item
}

func inventoryItemExists(t *testing.T, store domain.Store, id domain.InventoryItemID) bool {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	_, err = tx.Inventory().Get(context.Background(), id)
	if errors.Is(err, domain.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("get inventory item %q: %v", id, err)
	}
	return true
}

func getTarget(t *testing.T, store domain.Store, id domain.TargetID) domain.TargetInfo {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	target, err := tx.Targets().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get target %q: %v", id, err)
	}
	return target
}

func targetExists(t *testing.T, store domain.Store, id domain.TargetID) bool {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	_, err = tx.Targets().Get(context.Background(), id)
	if errors.Is(err, domain.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("get target %q: %v", id, err)
	}
	return true
}

func seedDeliveryRecord(t *testing.T, store domain.Store, d domain.Delivery) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func seedTarget(t *testing.T, store domain.Store, target domain.TargetInfo) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Targets().Create(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func getDeliveryState(t *testing.T, store domain.Store, id domain.DeliveryID) domain.DeliveryState {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	d, err := tx.Deliveries().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get delivery %q: %v", id, err)
	}
	return d.State()
}

// recordingSignal implements [domain.FulfillmentSignaler] and captures
// fulfillment events for test assertions.
type recordingSignal struct {
	mu     sync.Mutex
	events []signalRecord
}

type signalRecord struct {
	FulfillmentID domain.FulfillmentID
	Event         domain.FulfillmentEvent
}

func (r *recordingSignal) SignalFulfillmentEvent(_ context.Context, fID domain.FulfillmentID, event domain.FulfillmentEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, signalRecord{FulfillmentID: fID, Event: event})
	return nil
}

func (r *recordingSignal) snapshot() []signalRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]signalRecord, len(r.events))
	copy(out, r.events)
	return out
}

// testDeliveryObserver captures observer calls.
type testDeliveryObserver struct {
	domain.NoOpDeliveryObserver
	mu      sync.Mutex
	events  []testReportEventRecord
	results []testReportResultRecord
}

type testReportEventRecord struct {
	DeliveryID domain.DeliveryID
	Generation domain.Generation
}

type testReportResultRecord struct {
	DeliveryID domain.DeliveryID
	Generation domain.Generation
}

func (o *testDeliveryObserver) ReportEventStarted(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, _ domain.DeliveryEvent) (context.Context, domain.ReportEventProbe) {
	o.mu.Lock()
	o.events = append(o.events, testReportEventRecord{DeliveryID: deliveryID, Generation: generation})
	o.mu.Unlock()
	return ctx, domain.NoOpReportEventProbe{}
}

func (o *testDeliveryObserver) ReportResultStarted(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, _ domain.DeliveryResult) (context.Context, domain.ReportResultProbe) {
	o.mu.Lock()
	o.results = append(o.results, testReportResultRecord{DeliveryID: deliveryID, Generation: generation})
	o.mu.Unlock()
	return ctx, domain.NoOpReportResultProbe{}
}

func (o *testDeliveryObserver) snapshotEvents() []testReportEventRecord {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]testReportEventRecord, len(o.events))
	copy(out, o.events)
	return out
}

func (o *testDeliveryObserver) snapshotResults() []testReportResultRecord {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]testReportResultRecord, len(o.results))
	copy(out, o.results)
	return out
}

// ---------------------------------------------------------------------------
// ReportEvent tests
// ---------------------------------------------------------------------------

func TestDeliveryReportService_ReportEvent_TransitionsToProgressing(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	}))

	err := svc.ReportEvent(ctx, "del-1", 0, domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "step 1",
	})
	if err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}

	state := getDeliveryState(t, store, "del-1")
	if state != domain.DeliveryStateProgressing {
		t.Errorf("state = %q, want %q", state, domain.DeliveryStateProgressing)
	}
}

func TestDeliveryReportService_ReportEvent_AlreadyProgressing_NoStateChange(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	err := svc.ReportEvent(ctx, "del-1", 0, domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "step 2",
	})
	if err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}

	state := getDeliveryState(t, store, "del-1")
	if state != domain.DeliveryStateProgressing {
		t.Errorf("state = %q, want %q", state, domain.DeliveryStateProgressing)
	}
}

// fencedReadStore counts delivery reads performed through
// [domain.DeliveryRepository.GetForUpdate]. A report transaction must
// read its delivery that way: SQLite serializes writers, so a lost
// update cannot be reproduced there, but the Postgres backend is only
// safe when the read takes the row's write fence.
type fencedReadStore struct {
	domain.Store
	mu     sync.Mutex
	fenced int
}

func (s *fencedReadStore) Begin(ctx context.Context) (domain.Tx, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &fencedReadTx{Tx: tx, store: s}, nil
}

func (s *fencedReadStore) fencedReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fenced
}

type fencedReadTx struct {
	domain.Tx
	store *fencedReadStore
}

func (t *fencedReadTx) Deliveries() domain.DeliveryRepository {
	return &fencedReadDeliveries{DeliveryRepository: t.Tx.Deliveries(), store: t.store}
}

type fencedReadDeliveries struct {
	domain.DeliveryRepository
	store *fencedReadStore
}

func (r *fencedReadDeliveries) GetForUpdate(ctx context.Context, id domain.DeliveryID) (domain.Delivery, error) {
	r.store.mu.Lock()
	r.store.fenced++
	r.store.mu.Unlock()
	return r.DeliveryRepository.GetForUpdate(ctx, id)
}

func TestDeliveryReportService_ReadsTheReportedDeliveryUnderTheWriteFence(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &fencedReadStore{Store: &sqlite.Store{DB: db}}
	svc := application.NewDeliveryReportService(store, &recordingSignal{})
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-event", FulfillmentID: "f1", TargetID: "t1",
		Operation: domain.DeliveryOperationDeliver,
		State:     domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	}))
	before := store.fencedReads()
	if err := svc.ReportEvent(ctx, "del-event", 0, domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "step 1",
	}); err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}
	if got := store.fencedReads() - before; got == 0 {
		t.Error("ReportEvent read the delivery without the write fence; a stale progress report could overwrite a terminal one")
	}

	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-result", FulfillmentID: "f1", TargetID: "t2",
		Operation: domain.DeliveryOperationDeliver,
		State:     domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	}))
	before = store.fencedReads()
	if err := svc.ReportResult(ctx, "del-result", 0, domain.DeliveryResult{
		Operation: domain.DeliveryOperationDeliver, State: domain.DeliveryStateDelivered,
	}); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}
	if got := store.fencedReads() - before; got == 0 {
		t.Error("ReportResult read the delivery without the write fence; a stale progress report could overwrite a terminal one")
	}
}

func TestDeliveryReportService_ReportEvent_CallsObserver(t *testing.T) {
	svc, store, _, obs := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	}))

	_ = svc.ReportEvent(ctx, "del-1", 0, domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "applying",
	})

	events := obs.snapshotEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].DeliveryID != "del-1" {
		t.Errorf("delivery ID = %q, want %q", events[0].DeliveryID, "del-1")
	}
}

// TestDeliveryReportService_ReportEvent_AcksRemoveOperation verifies the
// progress-event ack of a removal cycle carries the record's operation:
// the workflow compares it to the cycle it is dispatching and discards a
// mismatch.
func TestDeliveryReportService_ReportEvent_AcksRemoveOperation(t *testing.T) {
	svc, store, sig, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State:     domain.DeliveryStatePending,
		Operation: domain.DeliveryOperationRemove,
		CreatedAt: now, UpdatedAt: now,
	}))

	err := svc.ReportEvent(ctx, "del-1", 0, domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "removing",
	})
	if err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}

	signals := sig.snapshot()
	if len(signals) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(signals))
	}
	acked := signals[0].Event.DeliveryAcked
	if acked == nil {
		t.Fatal("expected DeliveryAcked for the first progress event of a removal cycle")
	}
	if acked.Operation != domain.DeliveryOperationRemove {
		t.Errorf("ack operation = %q, want %q", acked.Operation, domain.DeliveryOperationRemove)
	}
	if acked.DeliveryID != "del-1" || acked.Generation != 0 {
		t.Errorf("ack = (%q, generation %d), want (del-1, generation 0)", acked.DeliveryID, acked.Generation)
	}
}

// ---------------------------------------------------------------------------
// ReportResult tests
// ---------------------------------------------------------------------------

func TestDeliveryReportService_ReportResult_UpdatesStateAndSignals(t *testing.T) {
	svc, store, sig, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	err := svc.ReportResult(ctx, "del-1", 0, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
	})
	if err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	state := getDeliveryState(t, store, "del-1")
	if state != domain.DeliveryStateDelivered {
		t.Errorf("state = %q, want %q", state, domain.DeliveryStateDelivered)
	}

	signals := sig.snapshot()
	if len(signals) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(signals))
	}
	if signals[0].FulfillmentID != "f1" {
		t.Errorf("fulfillment ID = %q, want %q", signals[0].FulfillmentID, "f1")
	}
	completed := signals[0].Event.DeliveryCompleted
	if completed == nil {
		t.Fatal("expected DeliveryCompleted event")
	}
	if completed.DeliveryID != "del-1" {
		t.Errorf("delivery ID = %q, want %q", completed.DeliveryID, "del-1")
	}
	if completed.Result.State != domain.DeliveryStateDelivered {
		t.Errorf("result state = %q, want %q", completed.Result.State, domain.DeliveryStateDelivered)
	}
	if completed.Operation != domain.DeliveryOperationDeliver {
		t.Errorf("completion operation = %q, want %q: the workflow discards a completion stamped with another operation", completed.Operation, domain.DeliveryOperationDeliver)
	}
}

func TestDeliveryReportService_ReportResult_CallsObserver(t *testing.T) {
	svc, store, _, obs := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	_ = svc.ReportResult(ctx, "del-1", 0, domain.DeliveryResult{
		State: domain.DeliveryStateFailed, Message: "connection refused",
		Operation: domain.DeliveryOperationDeliver,
	})

	results := obs.snapshotResults()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].DeliveryID != "del-1" {
		t.Errorf("delivery ID = %q, want %q", results[0].DeliveryID, "del-1")
	}
}

func TestDeliveryReportService_ReportResult_StaleGeneration_Discarded(t *testing.T) {
	secretRef := domain.SecretRef("targets/cluster-a/kubeconfig")
	cases := []struct {
		name            string
		producedSecrets []domain.ProducedSecret
	}{
		{name: "without outputs"},
		{
			name: "with produced secrets",
			producedSecrets: []domain.ProducedSecret{{
				Ref: secretRef, Value: []byte("kubeconfig-data"),
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, store, vault, sig := setupDeliveryReportServiceWithVault(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)

			seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
			seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
				Generation: 5,
				State:      domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
			}))

			err := svc.ReportResult(ctx, "del-1", 3, domain.DeliveryResult{
				State:           domain.DeliveryStateDelivered,
				Operation:       domain.DeliveryOperationDeliver,
				ProducedSecrets: tc.producedSecrets,
			})
			if err != nil {
				t.Fatalf("ReportResult: %v", err)
			}

			state := getDeliveryState(t, store, "del-1")
			if state != domain.DeliveryStatePending {
				t.Errorf("state = %q, want %q (stale report should be discarded)", state, domain.DeliveryStatePending)
			}
			if len(sig.snapshot()) != 0 {
				t.Error("expected no signals for stale report")
			}
			if _, err := vault.Get(ctx, secretRef); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("vault Get error = %v, want ErrNotFound: a stale-generation report must not store its secrets", err)
			}
		})
	}
}

// TestDeliveryReportService_ReportResult_StaleOperation_IsDiscarded
// covers the operation fence: a target's put and removal share a
// delivery ID, so a report of the other operation at the same
// generation must not change the record, emit signals, or persist its
// outputs.
func TestDeliveryReportService_ReportResult_StaleOperation_IsDiscarded(t *testing.T) {
	cases := []struct {
		name     string
		reportOp domain.DeliveryOperation
		recordOp domain.DeliveryOperation
	}{
		{name: "other operation", reportOp: domain.DeliveryOperationDeliver, recordOp: domain.DeliveryOperationRemove},
		{name: "unset operation", reportOp: "", recordOp: domain.DeliveryOperationDeliver},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, store, vault, sig := setupDeliveryReportServiceWithVault(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)

			seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
			seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID: "del-1", FulfillmentID: "f1", TargetID: "t1", Generation: 3,
				State: domain.DeliveryStateProgressing, Operation: tc.recordOp,
				CreatedAt: now, UpdatedAt: now,
			}))

			secretRef := domain.SecretRef("targets/cluster-a/kubeconfig")
			err := svc.ReportResult(ctx, "del-1", 3, domain.DeliveryResult{
				State:     domain.DeliveryStateDelivered,
				Operation: tc.reportOp,
				ProvisionedTargets: []domain.ProvisionedTarget{{
					ID: "cluster-a", Type: "kubernetes", Name: "cluster-a",
				}},
				ProducedSecrets: []domain.ProducedSecret{{
					Ref: secretRef, Value: []byte("kubeconfig-data"),
				}},
			})
			if err != nil {
				t.Fatalf("ReportResult: %v", err)
			}

			if state := getDeliveryState(t, store, "del-1"); state != domain.DeliveryStateProgressing {
				t.Errorf("state = %q, want %q (stale operation should be discarded)", state, domain.DeliveryStateProgressing)
			}
			if signals := sig.snapshot(); len(signals) != 0 {
				t.Errorf("expected no signals for a stale operation, got %d", len(signals))
			}
			if _, err := vault.Get(ctx, secretRef); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("vault Get error = %v, want ErrNotFound: a stale report must not store its secrets", err)
			}
			if inventoryItemExists(t, store, domain.InventoryItemID("target:cluster-a")) {
				t.Error("inventory item registered from a stale report")
			}
			if targetExists(t, store, "cluster-a") {
				t.Error("target registered from a stale report")
			}
		})
	}
}

// TestDeliveryReportService_ReportResult_TerminalOutputsArePersisted
// verifies that a terminal report commits its structured outputs with
// the delivery state: the secret is in the vault and the inventory and
// target registrations are durable before the completion signal fires.
func TestDeliveryReportService_ReportResult_TerminalOutputsArePersisted(t *testing.T) {
	svc, store, vault, sig := setupDeliveryReportServiceWithVault(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	secretRef := domain.SecretRef("targets/cluster-a/kubeconfig")
	err := svc.ReportResult(ctx, "del-1", 0, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
		ProvisionedTargets: []domain.ProvisionedTarget{{
			ID:   "cluster-a",
			Type: "kubernetes",
			Name: "cluster-a",
			Properties: map[string]string{
				"kubeconfig_ref": string(secretRef),
			},
			AcceptedManifestTypes: []domain.ManifestType{"kubernetes.manifest"},
		}},
		ProducedSecrets: []domain.ProducedSecret{{
			Ref:   secretRef,
			Value: []byte("kubeconfig-data"),
		}},
	})
	if err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	value, err := vault.Get(ctx, secretRef)
	if err != nil {
		t.Fatalf("vault Get: %v", err)
	}
	if string(value) != "kubeconfig-data" {
		t.Errorf("vault secret = %q, want %q", value, "kubeconfig-data")
	}

	item := getInventoryItem(t, store, domain.InventoryItemID("target:cluster-a"))
	if item.Type() != domain.InventoryItemType("kubernetes") {
		t.Errorf("inventory type = %q, want kubernetes", item.Type())
	}
	if item.SourceDeliveryID() == nil || *item.SourceDeliveryID() != domain.DeliveryID("del-1") {
		t.Errorf("SourceDeliveryID = %v, want del-1", item.SourceDeliveryID())
	}
	var props map[string]string
	if err := json.Unmarshal(item.Properties(), &props); err != nil {
		t.Fatalf("unmarshal inventory properties: %v", err)
	}
	if props["kubeconfig_ref"] != string(secretRef) {
		t.Errorf("inventory kubeconfig_ref = %q, want %q", props["kubeconfig_ref"], secretRef)
	}

	target := getTarget(t, store, "cluster-a")
	if target.Type() != "kubernetes" || target.Name() != "cluster-a" {
		t.Errorf("target = (%q, %q), want (kubernetes, cluster-a)", target.Type(), target.Name())
	}
	if target.Properties()["kubeconfig_ref"] != string(secretRef) {
		t.Errorf("target kubeconfig_ref = %q, want %q", target.Properties()["kubeconfig_ref"], secretRef)
	}
	if len(target.AcceptedManifestTypes()) != 1 || target.AcceptedManifestTypes()[0] != "kubernetes.manifest" {
		t.Errorf("AcceptedManifestTypes = %v, want [kubernetes.manifest]", target.AcceptedManifestTypes())
	}

	if state := getDeliveryState(t, store, "del-1"); state != domain.DeliveryStateDelivered {
		t.Errorf("state = %q, want %q", state, domain.DeliveryStateDelivered)
	}
	signals := sig.snapshot()
	if len(signals) != 1 || signals[0].Event.DeliveryCompleted == nil {
		t.Fatalf("expected one completion signal after the durable report, got %d", len(signals))
	}
}

// TestDeliveryReportService_ReportResult_NonTerminalOutputsAreIgnored
// verifies that only terminal results carry outputs; a progress report
// with outputs must not register anything.
func TestDeliveryReportService_ReportResult_NonTerminalOutputsAreIgnored(t *testing.T) {
	svc, store, vault, _ := setupDeliveryReportServiceWithVault(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	}))

	secretRef := domain.SecretRef("targets/cluster-a/kubeconfig")
	err := svc.ReportResult(ctx, "del-1", 0, domain.DeliveryResult{
		State:     domain.DeliveryStateProgressing,
		Operation: domain.DeliveryOperationDeliver,
		ProvisionedTargets: []domain.ProvisionedTarget{{
			ID: "cluster-a", Type: "kubernetes", Name: "cluster-a",
		}},
		ProducedSecrets: []domain.ProducedSecret{{
			Ref: secretRef, Value: []byte("kubeconfig-data"),
		}},
	})
	if err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	if state := getDeliveryState(t, store, "del-1"); state != domain.DeliveryStateProgressing {
		t.Errorf("state = %q, want %q", state, domain.DeliveryStateProgressing)
	}
	if _, err := vault.Get(ctx, secretRef); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("vault Get error = %v, want ErrNotFound: a non-terminal report must not store secrets", err)
	}
	if inventoryItemExists(t, store, domain.InventoryItemID("target:cluster-a")) {
		t.Error("inventory item registered from a non-terminal report")
	}
	if targetExists(t, store, "cluster-a") {
		t.Error("target registered from a non-terminal report")
	}
}

// TestDeliveryReportService_ReportResult_NonTerminalRemoveAckKeepsOperation
// verifies that the first report of a removal cycle acknowledges the
// remove operation, not deliver: the workflow discards an ack whose
// operation does not match the cycle it is dispatching, so a mis-stamped
// ack would leave the removal unacknowledged until the retry timeout.
func TestDeliveryReportService_ReportResult_NonTerminalRemoveAckKeepsOperation(t *testing.T) {
	svc, store, sig, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State:     domain.DeliveryStatePending,
		Operation: domain.DeliveryOperationRemove,
		CreatedAt: now, UpdatedAt: now,
	}))

	err := svc.ReportResult(ctx, "del-1", 0, domain.DeliveryResult{
		State:     domain.DeliveryStateProgressing,
		Operation: domain.DeliveryOperationRemove,
	})
	if err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	signals := sig.snapshot()
	if len(signals) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(signals))
	}
	acked := signals[0].Event.DeliveryAcked
	if acked == nil {
		t.Fatal("expected DeliveryAcked for the first non-terminal result of a removal cycle")
	}
	if acked.Operation != domain.DeliveryOperationRemove {
		t.Errorf("ack operation = %q, want %q", acked.Operation, domain.DeliveryOperationRemove)
	}
	if acked.DeliveryID != "del-1" || acked.Generation != 0 {
		t.Errorf("ack = (%q, generation %d), want (del-1, generation 0)", acked.DeliveryID, acked.Generation)
	}
}

// TestDeliveryReportService_ReportResult_NilVault_RegistersTargets
// verifies the documented fallback without a vault: produced secrets
// are dropped, but the provisioned targets are still registered.
func TestDeliveryReportService_ReportResult_NilVault_RegistersTargets(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	err := svc.ReportResult(ctx, "del-1", 0, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
		ProvisionedTargets: []domain.ProvisionedTarget{{
			ID: "cluster-a", Type: "kubernetes", Name: "cluster-a",
		}},
		ProducedSecrets: []domain.ProducedSecret{{
			Ref: "targets/cluster-a/kubeconfig", Value: []byte("kubeconfig-data"),
		}},
	})
	if err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	if !inventoryItemExists(t, store, domain.InventoryItemID("target:cluster-a")) {
		t.Error("inventory item missing: targets must be registered even without a vault")
	}
	if !targetExists(t, store, "cluster-a") {
		t.Error("target missing: targets must be registered even without a vault")
	}
}

func TestDeliveryReportService_ReportEvent_StaleGeneration_Discarded(t *testing.T) {
	svc, store, sig, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		Generation: 5,
		State:      domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	}))

	err := svc.ReportEvent(ctx, "del-1", 3, domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "stale event",
	})
	if err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}

	state := getDeliveryState(t, store, "del-1")
	if state != domain.DeliveryStatePending {
		t.Errorf("state = %q, want %q (stale event should be discarded)", state, domain.DeliveryStatePending)
	}
	if len(sig.snapshot()) != 0 {
		t.Error("expected no signals for stale event")
	}
}

// ---------------------------------------------------------------------------
// ListActiveDeliveries tests
// ---------------------------------------------------------------------------

func seedFulfillment(t *testing.T, store domain.Store, f *domain.Fulfillment) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.RollbackOnExit()
	if err := tx.Fulfillments().Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func newFulfillment(id domain.FulfillmentID, gen domain.Generation, auth domain.DeliveryAuth, now time.Time) *domain.Fulfillment {
	f := domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID:        id,
		Auth:      auth,
		CreatedAt: now,
		UpdatedAt: now,
	})
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type: domain.ManifestStrategyInline,
	}, now)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type: domain.PlacementStrategyStatic,
	}, now)
	f.AdvanceRolloutStrategy(nil, now)
	snap := f.Snapshot()
	snap.Generation = gen
	return domain.FulfillmentFromSnapshot(snap)
}

func TestDeliveryReportService_ListActiveDeliveries(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	auth1 := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://idp.test"}},
	}
	auth2 := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-2", Issuer: "https://idp.test"}},
	}

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test", Name: "target-1"}))
	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Type: "test", Name: "target-2"}))
	seedFulfillment(t, store, newFulfillment("f1", 3, auth1, now))
	seedFulfillment(t, store, newFulfillment("f2", 3, auth2, now))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-active-1", FulfillmentID: "f1", TargetID: "t1",
		Generation: 3,
		State:      domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-active-2", FulfillmentID: "f2", TargetID: "t2",
		Generation: 3,
		State:      domain.DeliveryStateAccepted, CreatedAt: now, UpdatedAt: now,
	}))

	t.Run("nil targets returns all active", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, nil)
		if err != nil {
			t.Fatalf("ListActiveDeliveries(nil): %v", err)
		}
		if len(active) != 2 {
			t.Fatalf("expected 2 active deliveries, got %d", len(active))
		}
	})

	t.Run("filter by single target", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, []domain.TargetID{"t1"})
		if err != nil {
			t.Fatalf("ListActiveDeliveries([t1]): %v", err)
		}
		if len(active) != 1 {
			t.Fatalf("expected 1 active delivery, got %d", len(active))
		}
		if active[0].Delivery.ID() != "del-active-1" {
			t.Errorf("ID = %q, want %q", active[0].Delivery.ID(), "del-active-1")
		}
	})

	t.Run("filter by multiple targets", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, []domain.TargetID{"t1", "t2"})
		if err != nil {
			t.Fatalf("ListActiveDeliveries([t1,t2]): %v", err)
		}
		if len(active) != 2 {
			t.Fatalf("expected 2 active deliveries, got %d", len(active))
		}
	})

	t.Run("filter by unknown target returns empty", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, []domain.TargetID{"t-unknown"})
		if err != nil {
			t.Fatalf("ListActiveDeliveries([t-unknown]): %v", err)
		}
		if len(active) != 0 {
			t.Errorf("expected 0 active deliveries, got %d", len(active))
		}
	})

	t.Run("enriches target and auth", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, []domain.TargetID{"t1"})
		if err != nil {
			t.Fatalf("ListActiveDeliveries: %v", err)
		}
		if len(active) != 1 {
			t.Fatalf("expected 1, got %d", len(active))
		}
		ad := active[0]
		if ad.Target.ID() != "t1" {
			t.Errorf("Target.ID = %q, want %q", ad.Target.ID(), "t1")
		}
		if ad.Target.Name() != "target-1" {
			t.Errorf("Target.Name = %q, want %q", ad.Target.Name(), "target-1")
		}
		if ad.Auth.Caller == nil || ad.Auth.Caller.Subject != "user-1" {
			t.Errorf("Auth.Caller.Subject = %v, want user-1", ad.Auth.Caller)
		}
		if ad.Attestation != nil {
			t.Errorf("expected nil Attestation for unsigned fulfillment, got %v", ad.Attestation)
		}
	})
}

func TestDeliveryReportService_ListActiveDeliveries_Empty(t *testing.T) {
	svc, _, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()

	active, err := svc.ListActiveDeliveries(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveDeliveries: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active deliveries, got %d", len(active))
	}
}

func TestDeliveryReportService_ListActiveDeliveries_SkipsStale(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	auth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-a", Issuer: "https://idp.test"}},
	}

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test", Name: "target-1"}))
	seedFulfillment(t, store, newFulfillment("f-stale", 5, auth, now))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-stale", FulfillmentID: "f-stale", TargetID: "t1",
		Generation: 3, // older than fulfillment gen 5
		State:      domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	active, err := svc.ListActiveDeliveries(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveDeliveries: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected stale delivery to be filtered, got %d", len(active))
	}
}

func TestDeliveryReportService_ListActiveDeliveries_SkipsMissingFulfillment(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-orphan", FulfillmentID: "f-missing", TargetID: "t1",
		Generation: 1,
		State:      domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	active, err := svc.ListActiveDeliveries(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveDeliveries: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected delivery with missing fulfillment to be filtered, got %d", len(active))
	}
}

func TestDeliveryReportService_ListActiveDeliveries_SkipsMissingTarget(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	auth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-a", Issuer: "https://idp.test"}},
	}

	seedFulfillment(t, store, newFulfillment("f1", 3, auth, now))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-no-target", FulfillmentID: "f1", TargetID: "t-gone",
		Generation: 3,
		State:      domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	active, err := svc.ListActiveDeliveries(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveDeliveries: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected delivery with missing target to be filtered, got %d", len(active))
	}
}

// ---------------------------------------------------------------------------
// NoOp default observer tests
// ---------------------------------------------------------------------------

func TestDeliveryReportService_DefaultObserver_DoesNotPanic(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	sig := &recordingSignal{}
	svc := application.NewDeliveryReportService(store, sig)

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	}))

	if err := svc.ReportEvent(ctx, "del-1", 0, domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "ok",
	}); err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}

	if err := svc.ReportResult(ctx, "del-1", 0, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
	}); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}
}

func TestDeliveryReportService_ReportResult_FailurePersistsReasonAndAttempts(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	getDelivery := func() domain.Delivery {
		t.Helper()
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.RollbackOnExit()
		d, err := tx.Deliveries().Get(ctx, "del-1")
		if err != nil {
			t.Fatalf("get delivery: %v", err)
		}
		return d
	}
	resetForRetry := func() {
		t.Helper()
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.RollbackOnExit()
		d, err := tx.Deliveries().Get(ctx, "del-1")
		if err != nil {
			t.Fatalf("get delivery: %v", err)
		}
		if err := d.ResetForRetry(now); err != nil {
			t.Fatalf("ResetForRetry: %v", err)
		}
		if err := tx.Deliveries().Put(ctx, d); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1", Generation: 1,
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	}))

	const firstReason = `apply object 1: conflict with "work-agent": .data.owner`
	if err := svc.ReportResult(ctx, "del-1", 1, domain.DeliveryResult{
		State: domain.DeliveryStateFailed, Message: firstReason,
		Operation: domain.DeliveryOperationDeliver,
	}); err != nil {
		t.Fatalf("first ReportResult: %v", err)
	}
	d := getDelivery()
	if d.State() != domain.DeliveryStateFailed {
		t.Fatalf("state = %q, want failed", d.State())
	}
	if d.Message() != firstReason {
		t.Errorf("Message = %q, want %q", d.Message(), firstReason)
	}
	if d.Attempts() != 1 {
		t.Errorf("Attempts = %d, want 1", d.Attempts())
	}

	resetForRetry()
	const secondReason = `apply object 1: conflict with "work-agent": .data.owner (retry)`
	if err := svc.ReportResult(ctx, "del-1", 1, domain.DeliveryResult{
		State: domain.DeliveryStateFailed, Message: secondReason,
		Operation: domain.DeliveryOperationDeliver,
	}); err != nil {
		t.Fatalf("second ReportResult: %v", err)
	}
	d = getDelivery()
	if d.Message() != secondReason {
		t.Errorf("Message = %q, want the latest reason %q", d.Message(), secondReason)
	}
	if d.Attempts() != 2 {
		t.Errorf("Attempts = %d, want 2", d.Attempts())
	}

	resetForRetry()
	if err := svc.ReportResult(ctx, "del-1", 1, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
	}); err != nil {
		t.Fatalf("delivery ReportResult: %v", err)
	}
	d = getDelivery()
	if d.Message() != "" {
		t.Errorf("Message = %q, want empty after delivery", d.Message())
	}
	if d.Attempts() != 2 {
		t.Errorf("Attempts = %d, want 2 (kept for the record)", d.Attempts())
	}
}

// failingVault fails every write, so ReportResult's vault-first path
// can be exercised.
type failingVault struct{}

func (failingVault) Put(context.Context, domain.SecretRef, []byte) error {
	return errors.New("vault unavailable")
}
func (failingVault) Get(context.Context, domain.SecretRef) ([]byte, error) {
	return nil, domain.ErrNotFound
}
func (failingVault) Delete(context.Context, domain.SecretRef) error { return nil }

// beginHookStore runs a hook before the first write transaction opens,
// standing in for a cycle change that lands after ReportResult's
// read-only check.
type beginHookStore struct {
	domain.Store
	once    sync.Once
	hook    func() error
	hookErr error
}

func (s *beginHookStore) Begin(ctx context.Context) (domain.Tx, error) {
	s.once.Do(func() { s.hookErr = s.hook() })
	if s.hookErr != nil {
		return nil, s.hookErr
	}
	return s.Store.Begin(ctx)
}

// commitFailStore fails every commit, standing in for a store failure
// after the vault write succeeded.
type commitFailStore struct{ domain.Store }

func (s commitFailStore) Begin(ctx context.Context) (domain.Tx, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return commitFailTx{Tx: tx}, nil
}

type commitFailTx struct{ domain.Tx }

func (commitFailTx) Commit() error { return errors.New("commit failed") }

func TestDeliveryReportService_ReportResult_VaultFailureLeavesTheDeliveryUnreported(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	sig := &recordingSignal{}
	svc := application.NewDeliveryReportService(store, sig, application.WithDeliveryReportVault(failingVault{}))
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1", Generation: 3,
		State: domain.DeliveryStateProgressing, Operation: domain.DeliveryOperationDeliver,
		CreatedAt: now, UpdatedAt: now,
	}))

	err := svc.ReportResult(ctx, "del-1", 3, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
		ProvisionedTargets: []domain.ProvisionedTarget{{
			ID: "cluster-a", Type: "kubernetes", Name: "cluster-a",
		}},
		ProducedSecrets: []domain.ProducedSecret{{
			Ref: "targets/cluster-a/sa-token", Value: []byte("token"),
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "store secret") {
		t.Fatalf("ReportResult error = %v, want a store secret error", err)
	}

	if state := getDeliveryState(t, store, "del-1"); state != domain.DeliveryStateProgressing {
		t.Errorf("state = %q, want %q: a report whose secrets cannot be stored is not applied", state, domain.DeliveryStateProgressing)
	}
	if signals := sig.snapshot(); len(signals) != 0 {
		t.Errorf("expected no signals, got %d", len(signals))
	}
	if inventoryItemExists(t, store, domain.InventoryItemID("target:cluster-a")) {
		t.Error("inventory item registered although the report failed")
	}
	if targetExists(t, store, "cluster-a") {
		t.Error("target registered although the report failed")
	}
}

func TestDeliveryReportService_ReportResult_CycleChangeAfterTheCheckDiscardsTheReport(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	base := &sqlite.Store{DB: db}
	vault := &sqlite.VaultStore{DB: db}
	sig := &recordingSignal{}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, base, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, base, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1", Generation: 3,
		State: domain.DeliveryStateProgressing, Operation: domain.DeliveryOperationDeliver,
		CreatedAt: now, UpdatedAt: now,
	}))

	// The report matches the delivery when it is checked, and a newer
	// cycle replaces the record before the report's transaction opens.
	store := &beginHookStore{
		Store: base,
		hook: func() error {
			tx, err := base.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.RollbackOnExit()
			if err := tx.Deliveries().Put(ctx, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID: "del-1", FulfillmentID: "f1", TargetID: "t1", Generation: 5,
				State: domain.DeliveryStatePending, Operation: domain.DeliveryOperationDeliver,
				CreatedAt: now, UpdatedAt: now.Add(time.Minute),
			})); err != nil {
				return err
			}
			return tx.Commit()
		},
	}
	svc := application.NewDeliveryReportService(store, sig, application.WithDeliveryReportVault(vault))

	err := svc.ReportResult(ctx, "del-1", 3, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
		ProvisionedTargets: []domain.ProvisionedTarget{{
			ID: "cluster-a", Type: "kubernetes", Name: "cluster-a",
		}},
		ProducedSecrets: []domain.ProducedSecret{{
			Ref: "targets/cluster-a/sa-token", Value: []byte("token"),
		}},
	})
	if store.hookErr != nil {
		t.Fatalf("prepare the newer cycle: %v", store.hookErr)
	}
	if err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	if state := getDeliveryState(t, base, "del-1"); state != domain.DeliveryStatePending {
		t.Errorf("state = %q, want %q: the newer cycle's record must stand", state, domain.DeliveryStatePending)
	}
	if signals := sig.snapshot(); len(signals) != 0 {
		t.Errorf("expected no signals for a discarded report, got %d", len(signals))
	}
	if inventoryItemExists(t, base, domain.InventoryItemID("target:cluster-a")) {
		t.Error("inventory item registered from a discarded report")
	}
	if targetExists(t, base, "cluster-a") {
		t.Error("target registered from a discarded report")
	}
}

func TestDeliveryReportService_ReportResult_CommitFailureDoesNotReport(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	base := &sqlite.Store{DB: db}
	vault := &sqlite.VaultStore{DB: db}
	sig := &recordingSignal{}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, base, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Type: "test"}))
	seedDeliveryRecord(t, base, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1", Generation: 3,
		State: domain.DeliveryStateProgressing, Operation: domain.DeliveryOperationDeliver,
		CreatedAt: now, UpdatedAt: now,
	}))

	svc := application.NewDeliveryReportService(commitFailStore{Store: base}, sig,
		application.WithDeliveryReportVault(vault))
	err := svc.ReportResult(ctx, "del-1", 3, domain.DeliveryResult{
		State:     domain.DeliveryStateDelivered,
		Operation: domain.DeliveryOperationDeliver,
		ProducedSecrets: []domain.ProducedSecret{{
			Ref: "targets/cluster-a/sa-token", Value: []byte("token"),
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "commit") {
		t.Fatalf("ReportResult error = %v, want a commit error", err)
	}

	if state := getDeliveryState(t, base, "del-1"); state != domain.DeliveryStateProgressing {
		t.Errorf("state = %q, want %q: a failed commit applies nothing", state, domain.DeliveryStateProgressing)
	}
	if signals := sig.snapshot(); len(signals) != 0 {
		t.Errorf("expected no signals after a failed commit, got %d", len(signals))
	}
}
