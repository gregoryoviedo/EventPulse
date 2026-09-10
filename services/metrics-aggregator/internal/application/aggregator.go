// Package application implements the domain ports: it turns raw events into
// aggregated Prometheus metrics.
package application

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/eventpulse/events"
	"github.com/eventpulse/metrics-aggregator/internal/domain"
	"github.com/eventpulse/metrics-aggregator/internal/metrics"
)

// Aggregator implements domain.EventAggregator on top of the Prometheus
// collectors. The `now` field is injectable so tests can control the latency
// computation.
type Aggregator struct {
	metrics *metrics.Metrics
	now     func() time.Time

	// counts keeps a cumulative tally per (source, event type), mirroring the
	// Prometheus counters, so the metrics.ticks producer can publish a snapshot
	// without walking the collector internals.
	mu     sync.Mutex
	counts map[string]int64
}

// Sample is one row of a metrics.ticks snapshot: the cumulative count of a
// (source, event type) pair.
type Sample struct {
	Source    string `json:"source"`
	EventType string `json:"event_type"`
	Count     int64  `json:"count"`
}

// Ensure the implementation satisfies the port at compile time.
var _ domain.EventAggregator = (*Aggregator)(nil)

// New builds an Aggregator over the given collectors.
func New(m *metrics.Metrics) *Aggregator {
	return &Aggregator{
		metrics: m,
		now:     time.Now,
		counts:  make(map[string]int64),
	}
}

// Handle counts the event and records its processing latency.
//
// Latency is measured end-to-end: from the timestamp carried by the event
// itself (when it was emitted) to the moment it is aggregated. This surfaces
// pipeline backlog in the histogram. When the event has no usable timestamp,
// the local processing duration is recorded instead.
func (a *Aggregator) Handle(_ context.Context, event events.Event) error {
	start := a.now()

	a.metrics.EventsProcessedTotal.WithLabelValues(event.Source, event.Type).Inc()
	a.tally(event.Source, event.Type)

	elapsed := a.now().Sub(start).Seconds()
	if event.Timestamp > 0 {
		if lag := a.now().Sub(time.UnixMilli(event.Timestamp)).Seconds(); lag >= 0 {
			elapsed = lag
		}
	}

	a.metrics.DocumentProcessingLatencySeconds.Observe(elapsed)

	return nil
}

// Snapshot returns the cumulative counts per (source, event type), sorted for
// a deterministic output, for the metrics.ticks producer.
func (a *Aggregator) Snapshot() []Sample {
	a.mu.Lock()
	defer a.mu.Unlock()

	samples := make([]Sample, 0, len(a.counts))
	for key, count := range a.counts {
		source, eventType, _ := strings.Cut(key, "\x00")
		samples = append(samples, Sample{Source: source, EventType: eventType, Count: count})
	}

	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Source != samples[j].Source {
			return samples[i].Source < samples[j].Source
		}

		return samples[i].EventType < samples[j].EventType
	})

	return samples
}

// tally increments the cumulative counter for a (source, event type) pair.
func (a *Aggregator) tally(source, eventType string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// \x00 cannot appear in either label value, so it is a safe key separator.
	a.counts[source+"\x00"+eventType]++
}
