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

// EventProcessingDurationSeconds measures how long the consumer takes to
// process a single incoming Kafka message, broken down by source and event
// type. It is registered on the default Prometheus registry in init().
var EventProcessingDurationSeconds = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "event_processing_duration_seconds",
		Help:    "Duration of processing a single Kafka message in seconds, labelled by source and event type.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"source", "event_type"},
)

// EventsDLQTotal counts every event forwarded to the dead letter queue,
// labelled by the reason it was rejected (e.g. invalid_json, missing_fields).
// It is registered on the default Prometheus registry in init().
var EventsDLQTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "events_dlq_total",
		Help: "Total number of events forwarded to the dead letter queue, labelled by rejection reason.",
	},
	[]string{"reason"},
)

func init() {
	prometheus.MustRegister(EventProcessingDurationSeconds)
	prometheus.MustRegister(EventsDLQTotal)
}

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
	// EventProcessingDurationSeconds measures the wall-clock time spent
	// processing a single Kafka message, broken down by source and event type.
	EventProcessingDurationSeconds *prometheus.HistogramVec
	// EventsDLQTotal counts every event forwarded to the dead letter queue,
	// labelled by the rejection reason.
	EventsDLQTotal *prometheus.CounterVec
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
		EventProcessingDurationSeconds: EventProcessingDurationSeconds,
		EventsDLQTotal:                EventsDLQTotal,
	}

	m.registry.MustRegister(
		m.EventsProcessedTotal,
		m.DocumentProcessingLatencySeconds,
		m.ActiveKafkaConsumers,
		m.EventProcessingDurationSeconds,
		m.EventsDLQTotal,
	)

	return m
}

// Handler returns the HTTP handler serving the registry in the Prometheus
// text exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
