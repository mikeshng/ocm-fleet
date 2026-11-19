package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kindaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kind"
	"github.com/stolostron/ocm-fleet/server/internal/addon/scripted"
	"github.com/stolostron/ocm-fleet/server/internal/application"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

// recoveryReporter answers ListActiveDeliveries with the active
// deliveries it was given, scoped to the requested targets as the real
// service does, and records every terminal result an agent reports.
type recoveryReporter struct {
	mu     sync.Mutex
	active []domain.ActiveDelivery
	asked  [][]domain.TargetID
	done   chan domain.DeliveryResult
}

func (r *recoveryReporter) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}

func (r *recoveryReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.done <- result
	return nil
}

func (r *recoveryReporter) ListActiveDeliveries(_ context.Context, targetIDs []domain.TargetID) ([]domain.ActiveDelivery, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asked = append(r.asked, append([]domain.TargetID(nil), targetIDs...))
	wanted := map[domain.TargetID]bool{}
	for _, id := range targetIDs {
		wanted[id] = true
	}
	var out []domain.ActiveDelivery
	for _, d := range r.active {
		if wanted[d.Target.ID()] {
			out = append(out, d)
		}
	}
	return out, nil
}

// recoveryAgent records the work it is dispatched and reports each
// delivery delivered, as an add-on agent does after it completes.
type recoveryAgent struct {
	reporter *recoveryReporter
	mu       sync.Mutex
	delivers []domain.DeliveryID
	removes  []domain.DeliveryID
}

func (a *recoveryAgent) Deliver(_ context.Context, _ domain.TargetInfo, id domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	a.mu.Lock()
	a.delivers = append(a.delivers, id)
	a.mu.Unlock()
	return a.reporter.ReportResult(context.Background(), id, 0, domain.DeliveryResult{State: domain.DeliveryStateDelivered, Operation: domain.DeliveryOperationDeliver})
}

func (a *recoveryAgent) Remove(_ context.Context, _ domain.TargetInfo, id domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	a.mu.Lock()
	a.removes = append(a.removes, id)
	a.mu.Unlock()
	return a.reporter.ReportResult(context.Background(), id, 0, domain.DeliveryResult{State: domain.DeliveryStateDelivered, Operation: domain.DeliveryOperationRemove})
}

func seedRecoveryTargets(t *testing.T) domain.Store {
	t.Helper()
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.RollbackOnExit()
	for _, target := range []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "kind-local", Type: kindaddon.TargetType, Name: "kind"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "scripted-local", Type: scripted.TargetType, Name: "scripted"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "other", Type: "kubernetes", Name: "other"}),
	} {
		if err := tx.Targets().Create(ctx, target); err != nil {
			t.Fatalf("create target %q: %v", target.ID(), err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return store
}

// An acknowledged delivery left in a non-terminal state when the
// process stopped is re-dispatched by the post-connect recovery hook of
// the add-on that owns its target, and its completion is reported. The
// recovery asks only for the add-on's own target type, so kind and
// scripted deliveries recover through their own agents and not through
// another add-on's.
func TestRecoverDeliveryAgent_RedispatchesOwnTargetsAndReportsCompletion(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name       string
		targetType domain.TargetType
		targetID   domain.TargetID
		otherID    domain.TargetID
		manifests  []domain.Manifest
	}{
		{
			name:       "kind deliver",
			targetType: kindaddon.TargetType,
			targetID:   "kind-local",
			otherID:    "scripted-local",
			manifests:  []domain.Manifest{{ManifestType: kindaddon.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		},
		{
			name:       "scripted remove",
			targetType: scripted.TargetType,
			targetID:   "scripted-local",
			otherID:    "kind-local",
			manifests:  []domain.Manifest{{ManifestType: scripted.ManagedManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := seedRecoveryTargets(t)
			operation := domain.DeliveryOperationDeliver
			if tt.name == "scripted remove" {
				operation = domain.DeliveryOperationRemove
			}
			delivery := domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID:            domain.DeliveryID("del-" + tt.name),
				FulfillmentID: "f1",
				TargetID:      tt.targetID,
				Manifests:     tt.manifests,
				Generation:    domain.Generation(i + 1),
				State:         domain.DeliveryStateProgressing,
				Operation:     operation,
				CreatedAt:     now,
				UpdatedAt:     now,
			})
			reporter := &recoveryReporter{
				done: make(chan domain.DeliveryResult, 2),
				active: []domain.ActiveDelivery{
					{Delivery: delivery, Target: domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: tt.targetID, Type: tt.targetType})},
					{Delivery: domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
						ID:            domain.DeliveryID("del-other-" + tt.name),
						FulfillmentID: "f2",
						TargetID:      tt.otherID,
						Generation:    1,
						State:         domain.DeliveryStateProgressing,
						Operation:     domain.DeliveryOperationDeliver,
						CreatedAt:     now,
						UpdatedAt:     now,
					}), Target: domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: tt.otherID, Type: "kubernetes"})},
				},
			}
			agent := &recoveryAgent{reporter: reporter}

			if err := recoverDeliveryAgent(context.Background(), reporter, storeTargetLister{store: store}, agent, tt.targetType); err != nil {
				t.Fatalf("recoverDeliveryAgent: %v", err)
			}

			reporter.mu.Lock()
			asked := reporter.asked
			reporter.mu.Unlock()
			if len(asked) != 1 || len(asked[0]) != 1 || asked[0][0] != tt.targetID {
				t.Fatalf("ListActiveDeliveries targets = %v, want [[%s]]", asked, tt.targetID)
			}
			agent.mu.Lock()
			delivers, removes := agent.delivers, agent.removes
			agent.mu.Unlock()
			wantDelivers, wantRemoves := 1, 0
			if operation == domain.DeliveryOperationRemove {
				wantDelivers, wantRemoves = 0, 1
			}
			if len(delivers) != wantDelivers || len(removes) != wantRemoves {
				t.Fatalf("dispatched delivers=%v removes=%v, want %d deliver and %d remove", delivers, removes, wantDelivers, wantRemoves)
			}
			select {
			case result := <-reporter.done:
				if result.State != domain.DeliveryStateDelivered {
					t.Fatalf("recovered result = %q, want Delivered", result.State)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the recovered delivery reported no completion")
			}
			// Dispatch is synchronous and done is buffered, so by the time
			// recoverDeliveryAgent returns every report it made is already
			// in the channel: a non-blocking look sees all of them.
			select {
			case extra := <-reporter.done:
				t.Fatalf("a delivery of another add-on's target was also dispatched: %+v", extra)
			default:
			}
		})
	}
}

// A recovery that cannot read the targets or the active deliveries
// reports the failure rather than silently recovering nothing.
func TestRecoverDeliveryAgent_StoreFailuresAreReported(t *testing.T) {
	reporter := &recoveryReporter{done: make(chan domain.DeliveryResult, 1)}
	agent := &recoveryAgent{reporter: reporter}
	store := seedRecoveryTargets(t)
	if err := recoverDeliveryAgent(context.Background(), reporter, storeTargetLister{store: failingTargetStore{Store: store}}, agent, kindaddon.TargetType); err == nil {
		t.Fatal("a failing target read was not reported")
	}

	listErr := &failingListReporter{recoveryReporter: reporter}
	if err := recoverDeliveryAgent(context.Background(), listErr, storeTargetLister{store: store}, agent, kindaddon.TargetType); err == nil {
		t.Fatal("a failing active-delivery read was not reported")
	}
}

type failingTargetStore struct {
	domain.Store
}

func (f failingTargetStore) BeginReadOnly(context.Context) (domain.Tx, error) {
	return nil, errors.New("store unavailable")
}

type failingListReporter struct {
	*recoveryReporter
}

func (f *failingListReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, errors.New("list active deliveries unavailable")
}

// enableAndConnectAddons is the path production takes at startup: it
// enables and connects each add-on, then runs that add-on's post-connect
// hook. The hook is where delivery recovery is registered, so this test
// drives the real recovery function through that path against a seeded
// store and asserts the dispatch reaches the agent.
func TestEnableAndConnectAddons_RunsTheRecoveryHook(t *testing.T) {
	store := seedRecoveryTargets(t)
	now := time.Now().UTC()
	delivery := domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            "del-hook",
		FulfillmentID: "f1",
		TargetID:      "kind-local",
		Manifests:     []domain.Manifest{{ManifestType: kindaddon.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		Generation:    1,
		State:         domain.DeliveryStateProgressing,
		Operation:     domain.DeliveryOperationDeliver,
		CreatedAt:     now,
		UpdatedAt:     now,
	})
	reporter := &recoveryReporter{
		done: make(chan domain.DeliveryResult, 1),
		active: []domain.ActiveDelivery{{
			Delivery: delivery,
			Target:   domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "kind-local", Type: kindaddon.TargetType}),
		}},
	}
	agent := &recoveryAgent{reporter: reporter}
	spec := AddonSpec{Descriptor: domain.AddonDescriptor{ID: "recovery-hook", Name: "recovery hook"}}
	spec.AfterConnectBestEffort = func(ctx context.Context) error {
		return recoverDeliveryAgent(ctx, reporter, storeTargetLister{store: store}, agent, kindaddon.TargetType)
	}

	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		TypeSvc: application.NewExtensionResourceTypeService(store),
	})
	if _, err := enableAndConnectAddons(context.Background(), mgr, []AddonSpec{spec}, logger); err != nil {
		t.Fatalf("enableAndConnectAddons: %v", err)
	}

	agent.mu.Lock()
	delivers := append([]domain.DeliveryID(nil), agent.delivers...)
	agent.mu.Unlock()
	if len(delivers) != 1 || delivers[0] != "del-hook" {
		t.Fatalf("the post-connect hook dispatched %v, want [del-hook]", delivers)
	}
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("recovered result = %q, want Delivered", result.State)
		}
	default:
		t.Fatal("the recovered delivery reported no completion")
	}
}

// A post-connect hook that fails is logged and startup continues: the
// next add-on is still connected and its hook still runs, and the close
// hooks of the add-ons already connected are still returned so the
// caller can unwind them.
func TestEnableAndConnectAddons_AFailingBestEffortHookIsLoggedAndDoesNotStopStartup(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	failing := AddonSpec{
		Descriptor: domain.AddonDescriptor{ID: "broken-recovery", Name: "broken recovery"},
		Close:      func(context.Context) error { return nil },
	}
	failing.AfterConnectBestEffort = func(context.Context) error { return errors.New("store unavailable") }
	var ran []domain.AddonID
	next := AddonSpec{Descriptor: domain.AddonDescriptor{ID: "next-addon", Name: "next addon"}}
	next.AfterConnectBestEffort = func(context.Context) error {
		ran = append(ran, "next-addon")
		return nil
	}

	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		TypeSvc: application.NewExtensionResourceTypeService(store),
	})
	hooks, err := enableAndConnectAddons(context.Background(), mgr, []AddonSpec{failing, next}, logger)
	if err != nil {
		t.Fatalf("a failing best-effort hook failed startup: %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("close hooks = %d, want the connected add-on's hook still returned", len(hooks))
	}
	if len(ran) != 1 || ran[0] != "next-addon" {
		t.Fatalf("hooks that ran = %v, want the next add-on's hook after the failure", ran)
	}
	logged := logBuf.String()
	for _, want := range []string{"addon post-connect best-effort failed", "broken-recovery", "store unavailable"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("the log does not name %q: %s", want, logged)
		}
	}
}

// failingDispatchAgent fails the dispatch of the deliveries it is given,
// with the error recorded for each, and records what it was asked to do.
type failingDispatchAgent struct {
	mu      sync.Mutex
	errs    map[domain.DeliveryID]error
	tried   []string
	byError map[error][]domain.DeliveryID
}

func (a *failingDispatchAgent) record(kind string, id domain.DeliveryID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tried = append(a.tried, kind+" "+string(id))
	err := a.errs[id]
	a.byError[err] = append(a.byError[err], id)
	return err
}

func (a *failingDispatchAgent) Deliver(_ context.Context, _ domain.TargetInfo, id domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	return a.record("deliver", id)
}

func (a *failingDispatchAgent) Remove(_ context.Context, _ domain.TargetInfo, id domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ domain.Generation) error {
	return a.record("remove", id)
}

// Every dispatch failure of a recovery is reported, not just the first:
// a delivery that could not be delivered and another that could not be
// removed both stay visible in the returned error.
func TestRecoverDeliveryAgent_JoinsEveryDispatchFailure(t *testing.T) {
	store := seedRecoveryTargets(t)
	now := time.Now().UTC()
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "kind-local", Type: kindaddon.TargetType})
	active := func(id domain.DeliveryID, op domain.DeliveryOperation) domain.ActiveDelivery {
		return domain.ActiveDelivery{
			Delivery: domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID:            id,
				FulfillmentID: "f1",
				TargetID:      "kind-local",
				Manifests:     []domain.Manifest{{ManifestType: kindaddon.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
				Generation:    1,
				State:         domain.DeliveryStateProgressing,
				Operation:     op,
				CreatedAt:     now,
				UpdatedAt:     now,
			}),
			Target: target,
		}
	}
	reporter := &recoveryReporter{
		done: make(chan domain.DeliveryResult, 1),
		active: []domain.ActiveDelivery{
			active("del-deliver", domain.DeliveryOperationDeliver),
			active("del-remove", domain.DeliveryOperationRemove),
		},
	}
	deliverErr := errors.New("apply refused")
	removeErr := errors.New("remove refused")
	agent := &failingDispatchAgent{
		errs:    map[domain.DeliveryID]error{"del-deliver": deliverErr, "del-remove": removeErr},
		byError: map[error][]domain.DeliveryID{},
	}

	err := recoverDeliveryAgent(context.Background(), reporter, storeTargetLister{store: store}, agent, kindaddon.TargetType)
	if err == nil {
		t.Fatal("dispatch failures were not reported")
	}
	if !errors.Is(err, deliverErr) || !errors.Is(err, removeErr) {
		t.Fatalf("recovery error %v does not join both dispatch failures", err)
	}
	for _, want := range []string{"recover delivery del-deliver", "recover delivery del-remove"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("recovery error %v does not name %q", err, want)
		}
	}
	agent.mu.Lock()
	tried := append([]string(nil), agent.tried...)
	agent.mu.Unlock()
	wantTried := []string{"deliver del-deliver", "remove del-remove"}
	if len(tried) != len(wantTried) || tried[0] != wantTried[0] || tried[1] != wantTried[1] {
		t.Fatalf("dispatched %v, want %v", tried, wantTried)
	}
}
