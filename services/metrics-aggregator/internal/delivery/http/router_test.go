package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eventpulse/metrics-aggregator/internal/metrics"
)

func TestHealthReturnsOK(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	NewRouter(metrics.New()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ok") {
		t.Fatalf("body = %q, want to contain ok", rec.Body.String())
	}
}

func TestMetricsExposesCollectors(t *testing.T) {
	m := metrics.New()
	// Touch a counter so the exposition contains a sample.
	m.EventsProcessedTotal.WithLabelValues("docs-api", "document_uploaded").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	NewRouter(m).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	for _, want := range []string{
		"events_processed_total",
		"document_processing_latency_seconds",
		"active_kafka_consumers",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("metrics body missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestMetricsRejectsNonGET(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	rec := httptest.NewRecorder()

	NewRouter(metrics.New()).ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
