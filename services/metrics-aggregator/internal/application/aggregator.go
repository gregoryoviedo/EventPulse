// Package application implements the domain ports: it turns raw events into
// aggregated Prometheus metrics.
package application

import (
	"context"
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
}

// Ensure the implementation satisfies the port at compile time.
var _ domain.EventAggregator = (*Aggregator)(nil)

// New builds an Aggregator over the given collectors.
func New(m *metrics.Metrics) *Aggregator {
	return &Aggregator{metrics: m, now: time.Now}
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

	elapsed := a.now().Sub(start).Seconds()
	if event.Timestamp > 0 {
		if lag := a.now().Sub(time.UnixMilli(event.Timestamp)).Seconds(); lag >= 0 {
			elapsed = lag
		}
	}

	a.metrics.DocumentProcessingLatencySeconds.Observe(elapsed)

	return nil
}
