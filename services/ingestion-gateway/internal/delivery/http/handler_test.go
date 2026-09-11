package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eventpulse/ingestion-gateway/internal/domain"
)

// fakeProducer records published events and can be told to fail.
type fakeProducer struct {
	produceErr error
	called     bool
	event      domain.Event
}

func (f *fakeProducer) Produce(_ context.Context, e domain.Event) error {
	f.called = true
	f.event = e
	return f.produceErr
}

// newTestHandler wires the router with a fake producer.
func newTestHandler(t *testing.T, p domain.EventProducer) *httptest.Server {
	t.Helper()
	handler := NewEventHandler(p, slog.New(slog.DiscardHandler))
	return httptest.NewServer(NewRouter(handler))
}

func doRequest(t *testing.T, server *httptest.Server, method, path, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, server.URL+path, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	server.Config.Handler.ServeHTTP(rec, req)
	return rec
}

func TestCreateAccepted(t *testing.T) {
	p := &fakeProducer{}
	server := newTestHandler(t, p)
	defer server.Close()

	rec := doRequest(t, server, http.MethodPost, "/api/v1/events", "application/json",
		`{"source":"checkout","event_type":"order.created","payload":{"order_id":"42"}}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if !p.called {
		t.Fatalf("producer was not called")
	}
	if p.event.Source != "checkout" || p.event.EventType != "order.created" {
		t.Fatalf("published event = %+v", p.event)
	}

	var resp eventResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "accepted" || resp.ID == "" {
		t.Fatalf("response = %+v", resp)
	}
}

func TestCreateValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "missing source", body: `{"event_type":"t","payload":{"k":"v"}}`, want: "source is required"},
		{name: "missing event type", body: `{"source":"s","payload":{"k":"v"}}`, want: "event_type is required"},
		{name: "empty payload", body: `{"source":"s","event_type":"t","payload":{}}`, want: "payload must not be empty"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newTestHandler(t, &fakeProducer{})
			defer server.Close()

			rec := doRequest(t, server, http.MethodPost, "/api/v1/events", "application/json", tc.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
			if !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("body = %q, want it to contain %q", rec.Body.String(), tc.want)
			}
		})
	}
}

func TestCreateRejectsMalformedJSON(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "not json", body: "hello"},
		{name: "unknown field", body: `{"source":"s","event_type":"t","payload":{"k":"v"},"bogus":1}`},
		{name: "trailing content", body: `{"source":"s","event_type":"t","payload":{"k":"v"}}{"x":1}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newTestHandler(t, &fakeProducer{})
			defer server.Close()

			rec := doRequest(t, server, http.MethodPost, "/api/v1/events", "application/json", tc.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestCreateRejectsWrongContentType(t *testing.T) {
	server := newTestHandler(t, &fakeProducer{})
	defer server.Close()

	rec := doRequest(t, server, http.MethodPost, "/api/v1/events", "text/plain",
		`{"source":"s","event_type":"t","payload":{"k":"v"}}`)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnsupportedMediaType)
	}
}

func TestCreateProducerFailureReturns503(t *testing.T) {
	p := &fakeProducer{produceErr: errors.New("broker down")}
	server := newTestHandler(t, p)
	defer server.Close()

	rec := doRequest(t, server, http.MethodPost, "/api/v1/events", "application/json",
		`{"source":"s","event_type":"t","payload":{"k":"v"}}`)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !p.called {
		t.Fatalf("producer should have been called")
	}
}

func TestHealth(t *testing.T) {
	server := newTestHandler(t, &fakeProducer{})
	defer server.Close()

	rec := doRequest(t, server, http.MethodGet, "/healthz", "", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("body = %q, want status ok", rec.Body.String())
	}
}

func TestRouterRejectsWrongMethod(t *testing.T) {
	server := newTestHandler(t, &fakeProducer{})
	defer server.Close()

	rec := doRequest(t, server, http.MethodGet, "/api/v1/events", "", "")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}