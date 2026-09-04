// Package metrics owns the Prometheus collectors exposed by the service and
// the registry they are registered on.
//
// A dedicated registry is used instead of the default one so that /metrics
// serves exactly this service's collectors and tests can build fresh instances
// without polluting a global state.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles the collectors the service exposes on /metrics.
type Metrics struct {
	registry *prometheus.Registry

	// EventsProcessedTotal counts every event consumed from the raw topic,
	// broken down by source and event type.
	EventsProcessedTotal *prometheus.CounterVec
	// DocumentProcessingLatencySeconds measures the end-to-end latency of an
	// event: the time between its own timestamp and the moment it is folded
	// into the aggregates.
	DocumentProcessingLatencySeconds prometheus.Histogram
	// ActiveKafkaConsumers reports the number of consumers running in this
	// process (1 while the loop is up, 0 after it stops).
	ActiveKafkaConsumers prometheus.Gauge
}

// New builds all collectors and registers them on a fresh registry.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		EventsProcessedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "events_processed_total",
				Help: "Total number of events processed, labelled by source and event type.",
			},
			[]string{"source", "event_type"},
		),
		DocumentProcessingLatencySeconds: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "document_processing_latency_seconds",
				Help:    "End-to-end latency of event processing in seconds.",
				Buckets: prometheus.DefBuckets,
			},
		),
		ActiveKafkaConsumers: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "active_kafka_consumers",
				Help: "Number of active Kafka consumers in this process.",
			},
		),
	}

	m.registry.MustRegister(
		m.EventsProcessedTotal,
		m.DocumentProcessingLatencySeconds,
		m.ActiveKafkaConsumers,
	)

	return m
}

// Handler returns the HTTP handler serving the registry in the Prometheus
// text exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
