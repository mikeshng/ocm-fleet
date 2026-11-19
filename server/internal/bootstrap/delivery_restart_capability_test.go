package bootstrap

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	kindaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kind"
	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/addon/scripted"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/sqlite"
)

type nopInventoryReporter struct{}

func (nopInventoryReporter) ApplyDeltaBatch(context.Context, domain.InventoryDeltaBatch) error {
	return nil
}

// The assembled production add-ons expose the post-connect recovery hook
// for every agent that reports deliveries, and the scripted hook
// re-dispatches a non-terminal delivery through the real scripted agent,
// which reports its completion. A delivery acknowledged before a restart
// otherwise has no process left to report its outcome, so this is what
// lets a fulfillment finish after the resource manager comes back.
func TestAssembledAddonsRecoverNonTerminalDeliveries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg, err := NewConfig(ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   filepath.Join(t.TempDir(), "resource-manager.db"),
		Addons:   "kind,kubernetes,scripted",
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	store := seedRecoveryTargets(t)
	uid := domain.ExtensionResourceUID(uuid.New())
	raw, err := domain.WrapManagedResourceSpec("restart-recovery", uid, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("WrapManagedResourceSpec: %v", err)
	}
	now := time.Now().UTC()
	reporter := &recoveryReporter{
		done: make(chan domain.DeliveryResult, 2),
		active: []domain.ActiveDelivery{{
			Delivery: domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
				ID:            "del-restart-recovery",
				FulfillmentID: "f-restart-recovery",
				TargetID:      scripted.TargetID,
				Manifests:     []domain.Manifest{{ManifestType: scripted.ManagedManifestType, Raw: raw}},
				Generation:    1,
				State:         domain.DeliveryStateProgressing,
				Operation:     domain.DeliveryOperationDeliver,
				CreatedAt:     now,
				UpdatedAt:     now,
			}),
			Target: domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
				ID: scripted.TargetID, Type: scripted.TargetType,
			}),
		}},
	}

	specs, err := assembleProductionAddons(AddonDeps{
		Config:            cfg,
		Logger:            testLogger(),
		Store:             store,
		DeliveryReporter:  reporter,
		InventoryReporter: nopInventoryReporter{},
		AppCtx:            ctx,
	}, nil, nil)
	if err != nil {
		t.Fatalf("assembleProductionAddons: %v", err)
	}

	byID := make(map[domain.AddonID]AddonSpec, len(specs))
	for _, spec := range specs {
		byID[spec.Descriptor.ID] = spec
	}
	for _, want := range []struct {
		name string
		id   domain.AddonID
	}{
		{"kind", kindaddon.Descriptor().ID},
		{"kubernetes", kubernetesaddon.Descriptor().ID},
		{"scripted", scripted.Descriptor().ID},
	} {
		spec, ok := byID[want.id]
		if !ok {
			t.Fatalf("the %s add-on was not assembled", want.name)
		}
		if spec.AfterConnectBestEffort == nil {
			t.Fatalf("the %s add-on exposes no post-connect delivery recovery", want.name)
		}
	}

	scriptedSpec := byID[scripted.Descriptor().ID]
	if err := scriptedSpec.AfterConnectBestEffort(ctx); err != nil {
		t.Fatalf("scripted delivery recovery: %v", err)
	}
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("recovered delivery reported %q, want %q", result.State, domain.DeliveryStateDelivered)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the recovered delivery reported no completion")
	}
}

// signalRecorder forwards to the wrapped registry and records the
// fulfillment IDs whose completion events were signalled. The relay
// signals the event, so a recorded ID proves the relay ran with the
// registry the server uses.
type signalRecorder struct {
	domain.Registry
	mu      sync.Mutex
	signals []domain.FulfillmentID
}

func (r *signalRecorder) SignalFulfillmentEvent(ctx context.Context, id domain.FulfillmentID, event domain.FulfillmentEvent) error {
	r.mu.Lock()
	r.signals = append(r.signals, id)
	r.mu.Unlock()
	return r.Registry.SignalFulfillmentEvent(ctx, id, event)
}

func (r *signalRecorder) recorded() []domain.FulfillmentID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]domain.FulfillmentID(nil), r.signals...)
}

// A completion recorded before the process started is signalled and
// removed by the relay that Start runs. Only the started relay can clear
// the record, so this fails when the relay is not part of the server.
func TestStartRunsDeliveryCompletionRelay(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "resource-manager.db")
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open seed database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(
		`INSERT INTO delivery_completion_outbox
		     (delivery_id, fulfillment_id, generation, operation, state, message, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"del-restart-completion", "f-restart-completion", 1,
		string(domain.DeliveryOperationDeliver), string(domain.DeliveryStateDelivered),
		"", time.Now().UTC().Format(time.RFC3339),
	); err != nil {
		t.Fatalf("record the outstanding completion: %v", err)
	}

	registry := &signalRecorder{Registry: NewMemWorkflowRegistry()}
	startTestServerWithConfig(t, ConfigInput{DBPath: dbPath}, WithWorkflowRegistry(registry))

	deadline := time.Now().Add(5 * time.Second)
	for {
		var outstanding int
		if err := db.QueryRow(`SELECT count(*) FROM delivery_completion_outbox`).Scan(&outstanding); err != nil {
			t.Fatalf("read the completion outbox: %v", err)
		}
		if outstanding == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the recorded completion is still outstanding %v after Start; the relay is not running", 5*time.Second)
		}
		time.Sleep(20 * time.Millisecond)
	}

	signalled := false
	for _, id := range registry.recorded() {
		if id == "f-restart-completion" {
			signalled = true
			break
		}
	}
	if !signalled {
		t.Fatalf("the relay removed the completion without signalling it; signalled fulfillments = %v", registry.recorded())
	}
}
