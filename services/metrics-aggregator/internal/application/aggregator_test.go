package application

import (
	"context"
	"testing"
	"time"

	"github.com/eventpulse/events"
	"github.com/eventpulse/metrics-aggregator/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestHandleCountsEventsBySourceAndType(t *testing.T) {
	m := metrics.New()
	agg := New(m)

	ev := events.Event{ID: "evt-1", Type: "order.created", Source: "checkout-api"}

	if err := agg.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if err := agg.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	got := testutil.ToFloat64(m.EventsProcessedTotal.WithLabelValues("checkout-api", "order.created"))
	if got != 2 {
		t.Fatalf("events_processed_total{source=checkout-api,event_type=order.created} = %v, want 2", got)
	}

	// Other label combinations stay untouched.
	other := testutil.ToFloat64(m.EventsProcessedTotal.WithLabelValues("docs-api", "document_uploaded"))
	if other != 0 {
		t.Fatalf("unexpected counter for unprocessed labels: %v", other)
	}
}

func TestHandleRecordsEndToEndLatency(t *testing.T) {
	m := metrics.New()
	now := time.Unix(1_700_000_000, 0)

	agg := New(m)
	agg.now = func() time.Time { return now }

	// The event was emitted 2s before it is aggregated.
	ev := events.Event{
		ID:        "evt-1",
		Type:      "document_uploaded",
		Source:    "docs-api",
		Timestamp: now.Add(-2 * time.Second).UnixMilli(),
	}

	if err := agg.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	count, sum := histogramSample(t, m.DocumentProcessingLatencySeconds)
	if count != 1 {
		t.Fatalf("histogram sample count = %d, want 1", count)
	}
	if sum < 1.9 || sum > 2.1 {
		t.Fatalf("histogram sum = %v, want ~2.0 (the 2s end-to-end lag)", sum)
	}
}

func TestHandleFallsBackToLocalDurationWithoutTimestamp(t *testing.T) {
	m := metrics.New()
	now := time.Unix(1_700_000_000, 0)

	agg := New(m)
	agg.now = func() time.Time { return now }

	// No timestamp: the local processing duration (0 with a frozen clock) is
	// recorded instead of the end-to-end lag.
	ev := events.Event{ID: "evt-1", Type: "heartbeat", Source: "ingestion-gateway"}

	if err := agg.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	count, sum := histogramSample(t, m.DocumentProcessingLatencySeconds)
	if count != 1 {
		t.Fatalf("histogram sample count = %d, want 1", count)
	}
	if sum != 0 {
		t.Fatalf("histogram sum = %v, want 0", sum)
	}
}

func TestSnapshotReturnsSortedCumulativeCounts(t *testing.T) {
	m := metrics.New()
	agg := New(m)

	events := []events.Event{
		{ID: "e1", Type: "document_uploaded", Source: "docs-api"},
		{ID: "e2", Type: "order.created", Source: "checkout-api"},
		{ID: "e3", Type: "document_uploaded", Source: "docs-api"},
	}

	for _, ev := range events {
		if err := agg.Handle(context.Background(), ev); err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
	}

	samples := agg.Snapshot()
	want := []Sample{
		{Source: "checkout-api", EventType: "order.created", Count: 1},
		{Source: "docs-api", EventType: "document_uploaded", Count: 2},
	}
	if len(samples) != len(want) {
		t.Fatalf("Snapshot() = %+v, want %+v", samples, want)
	}
	for i := range want {
		if samples[i] != want[i] {
			t.Fatalf("Snapshot()[%d] = %+v, want %+v", i, samples[i], want[i])
		}
	}
}

// histogramSample extracts the sample count and cumulative sum of a histogram.
func histogramSample(t *testing.T, h interface{ Write(*dto.Metric) error }) (uint64, float64) {
	t.Helper()

	pb := &dto.Metric{}
	if err := h.Write(pb); err != nil {
		t.Fatalf("write histogram: %v", err)
	}

	return pb.GetHistogram().GetSampleCount(), pb.GetHistogram().GetSampleSum()
}
