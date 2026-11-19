package observability_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/observability"
)

func TestFulfillmentObserver_RunStarted_LogsAndReturnsProbe(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	ctx, probe := obs.RunStarted(context.Background(), "ful-1")
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 log record from RunStarted, got %d", len(records))
	}
	if records[0].Message != "fulfillment run started" {
		t.Errorf("message = %q, want %q", records[0].Message, "fulfillment run started")
	}

	probe.End()
}

func TestFulfillmentRunProbe_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-2")

	probe.StateChanged(domain.FulfillmentStateActive)
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	want := []string{
		"fulfillment run started",
		"fulfillment state changed",
		"fulfillment run completed",
	}
	if len(messages) != len(want) {
		t.Fatalf("got %d records %v, want %d %v", len(messages), messages, len(want), want)
	}
	for i, w := range want {
		if messages[i] != w {
			t.Errorf("record[%d] message = %q, want %q", i, messages[i], w)
		}
	}
}

func TestFulfillmentRunProbe_ManifestsFiltered_AllDroppedLogsWarning(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-filter")

	probe.ManifestsFiltered(domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k8s-1", Type: "kubernetes"}), 2, 0)
	probe.End()

	records := handler.Records()
	var filterRecord *slog.Record
	for i := range records {
		if records[i].Message == "all manifests filtered for target" {
			filterRecord = &records[i]
			break
		}
	}
	if filterRecord == nil {
		t.Fatal("expected 'all manifests filtered for target' log record")
	}
	if filterRecord.Level != slog.LevelWarn {
		t.Errorf("level = %v, want %v", filterRecord.Level, slog.LevelWarn)
	}
}

func TestFulfillmentRunProbe_ManifestsFiltered_PartialLogsDebug(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-partial")

	probe.ManifestsFiltered(domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "kind-1", Type: "kind"}), 3, 2)
	probe.End()

	records := handler.Records()
	var filterRecord *slog.Record
	for i := range records {
		if records[i].Message == "manifests filtered for target" {
			filterRecord = &records[i]
			break
		}
	}
	if filterRecord == nil {
		t.Fatal("expected 'manifests filtered for target' log record")
	}
	if filterRecord.Level != slog.LevelDebug {
		t.Errorf("level = %v, want %v", filterRecord.Level, slog.LevelDebug)
	}
}

func TestFulfillmentRunProbe_ErrorLogsAtErrorLevel(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-3")

	probe.Error(domain.ErrNotFound)
	probe.End()

	records := handler.Records()
	var endRecord *slog.Record
	for i := range records {
		if records[i].Message == "fulfillment run failed" {
			endRecord = &records[i]
			break
		}
	}
	if endRecord == nil {
		t.Fatal("expected 'deployment run failed' log record")
	}
	if endRecord.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", endRecord.Level, slog.LevelError)
	}
}

func TestFulfillmentRunProbe_DispatchCycle_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-dispatch")

	dprobe := probe.DispatchCycleStarted(2, 1)
	dprobe.Dispatched("d1:t1", false)
	dprobe.AckReceived("d1:t1")
	dprobe.Completed("d1:t1", domain.DeliveryStateDelivered)
	dprobe.End()
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	wantContains := []string{
		"dispatch cycle started",
		"delivery dispatched",
		"delivery ack received",
		"delivery completed",
		"dispatch cycle completed",
	}
	for _, want := range wantContains {
		found := false
		for _, msg := range messages {
			if msg == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected log message %q, got messages: %v", want, messages)
		}
	}
}

func TestFulfillmentRunProbe_DispatchCycle_AckTimeout(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-timeout")

	dprobe := probe.DispatchCycleStarted(1, 1)
	dprobe.Dispatched("d1:t1", false)
	dprobe.AckTimeout(1)
	dprobe.End()
	probe.End()

	records := handler.Records()
	var timeoutRecord *slog.Record
	for i := range records {
		if records[i].Message == "delivery ack timeout" {
			timeoutRecord = &records[i]
			break
		}
	}
	if timeoutRecord == nil {
		t.Fatal("expected 'delivery ack timeout' log record")
	}
	if timeoutRecord.Level != slog.LevelWarn {
		t.Errorf("level = %v, want %v", timeoutRecord.Level, slog.LevelWarn)
	}
}

func TestAcquireLockProbe_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.AcquireLockStarted(context.Background(), "ful-lock")

	probe.LockAcquired(false)
	probe.PoolLoaded(3)
	probe.EvidenceResolved(true)
	probe.End()

	records := handler.Records()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}

	wantContains := []string{
		"acquire lock started",
		"orchestration lock acquired",
		"target pool loaded",
		"attestation evidence resolved",
		"acquire lock completed",
	}
	for _, want := range wantContains {
		found := false
		for _, msg := range messages {
			if msg == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected log message %q, got messages: %v", want, messages)
		}
	}
}

func TestDeliverProbe_NewDelivery(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.DeliverStarted(context.Background(), domain.DeliverInput{
		FulfillmentID: "ful-1",
		DeliveryID:    "d1:t1",
		Target:        domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1"}),
	})

	probe.NewDelivery()
	probe.End()

	records := handler.Records()
	found := false
	for _, r := range records {
		if r.Message == "new delivery created" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'new delivery created' log record")
	}
}

func TestRemoveProbe_Withdrawn(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RemoveStarted(context.Background(), domain.RemoveInput{
		FulfillmentID: "ful-1",
		DeliveryID:    "d1:t1",
		Target:        domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1"}),
	})

	probe.Withdrawn()
	probe.End()

	records := handler.Records()
	found := false
	for _, r := range records {
		if r.Message == "delivery withdrawn" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'delivery withdrawn' log record")
	}
}

// recordAttr returns the value of key among a record's attributes.
func recordAttr(r slog.Record, key string) (slog.Value, bool) {
	var value slog.Value
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			value = a.Value
			found = true
			return false
		}
		return true
	})
	return value, found
}

func TestFulfillmentRunProbe_RetryDelayInterrupted(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RunStarted(context.Background(), "ful-retry")

	probe.RetryDelayInterrupted(3, 4)
	probe.End()

	records := handler.Records()
	var interrupted *slog.Record
	for i := range records {
		if records[i].Message == "retry delay interrupted by a new generation" {
			interrupted = &records[i]
			break
		}
	}
	if interrupted == nil {
		t.Fatal("expected 'retry delay interrupted by a new generation' log record")
	}
	if interrupted.Level != slog.LevelInfo {
		t.Errorf("level = %v, want %v", interrupted.Level, slog.LevelInfo)
	}
	if v, ok := recordAttr(*interrupted, "start_generation"); !ok || v.Int64() != 3 {
		t.Errorf("start_generation = %v (found %v), want 3", v, ok)
	}
	if v, ok := recordAttr(*interrupted, "current_generation"); !ok || v.Int64() != 4 {
		t.Errorf("current_generation = %v (found %v), want 4", v, ok)
	}
}

func TestDeliverProbe_SkippedTerminal(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.DeliverStarted(context.Background(), domain.DeliverInput{
		FulfillmentID: "ful-1",
		DeliveryID:    "d1:t1",
		Target:        domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1"}),
	})

	probe.SkippedTerminal(domain.DeliveryStateDelivered)
	probe.End()

	records := handler.Records()
	var skipped *slog.Record
	for i := range records {
		if records[i].Message == "delivery skipped: terminal for the current cycle" {
			skipped = &records[i]
			break
		}
	}
	if skipped == nil {
		t.Fatal("expected 'delivery skipped: terminal for the current cycle' log record")
	}
	if skipped.Level != slog.LevelDebug {
		t.Errorf("level = %v, want %v", skipped.Level, slog.LevelDebug)
	}
	if v, ok := recordAttr(*skipped, "state"); !ok || v.String() != string(domain.DeliveryStateDelivered) {
		t.Errorf("state = %v (found %v), want %q", v, ok, domain.DeliveryStateDelivered)
	}
}

func TestDeliverProbe_SkippedStale(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.DeliverStarted(context.Background(), domain.DeliverInput{
		FulfillmentID: "ful-1",
		DeliveryID:    "d1:t1",
		Target:        domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1"}),
	})

	probe.SkippedStale(5)
	probe.End()

	records := handler.Records()
	var skipped *slog.Record
	for i := range records {
		if records[i].Message == "delivery skipped: record generation is ahead of the dispatch cycle" {
			skipped = &records[i]
			break
		}
	}
	if skipped == nil {
		t.Fatal("expected 'delivery skipped: record generation is ahead of the dispatch cycle' log record")
	}
	if skipped.Level != slog.LevelWarn {
		t.Errorf("level = %v, want %v", skipped.Level, slog.LevelWarn)
	}
	if v, ok := recordAttr(*skipped, "record_generation"); !ok || v.Int64() != 5 {
		t.Errorf("record_generation = %v (found %v), want 5", v, ok)
	}
}

func TestRemoveProbe_SkippedTerminal(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.RemoveStarted(context.Background(), domain.RemoveInput{
		FulfillmentID: "ful-1",
		DeliveryID:    "d1:t1",
		Target:        domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1"}),
	})

	probe.SkippedTerminal(domain.DeliveryStateFailed)
	probe.End()

	records := handler.Records()
	var skipped *slog.Record
	for i := range records {
		if records[i].Message == "remove skipped: terminal" {
			skipped = &records[i]
			break
		}
	}
	if skipped == nil {
		t.Fatal("expected 'remove skipped: terminal' log record")
	}
	if skipped.Level != slog.LevelDebug {
		t.Errorf("level = %v, want %v", skipped.Level, slog.LevelDebug)
	}
	if v, ok := recordAttr(*skipped, "state"); !ok || v.String() != string(domain.DeliveryStateFailed) {
		t.Errorf("state = %v (found %v), want %q", v, ok, domain.DeliveryStateFailed)
	}
}

func TestPersistReconciliationProbe_FullLifecycle(t *testing.T) {
	h := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := newRecordingHandler(h)
	logger := slog.New(handler)

	obs := observability.NewFulfillmentObserver(logger)
	_, probe := obs.PersistReconciliationStarted(context.Background(), "ful-persist")

	probe.Persisted(domain.FulfillmentStateActive, false)
	probe.End()

	records := handler.Records()
	found := false
	for _, r := range records {
		if r.Message == "reconciliation persisted" {
			found = true
			if r.Level != slog.LevelInfo {
				t.Errorf("level = %v, want %v", r.Level, slog.LevelInfo)
			}
			break
		}
	}
	if !found {
		t.Error("expected 'reconciliation persisted' log record")
	}
}
