// Package http is the delivery layer: it exposes the Prometheus scrape
// endpoint and the liveness/readiness probe on the standard library mux.
package http

import (
	"net/http"

	"github.com/eventpulse/metrics-aggregator/internal/metrics"
)

// NewRouter builds the service routing table.
func NewRouter(m *metrics.Metrics) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /health", health)

	return mux
}

// health answers a liveness/readiness probe: the process is up as long as the
// HTTP server responds.
func health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
