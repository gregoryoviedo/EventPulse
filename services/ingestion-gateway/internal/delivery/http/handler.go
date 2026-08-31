// Package http is the delivery layer: it adapts HTTP requests into domain
// entities and delegates them to the domain ports. It owns the wire format
// (JSON DTOs, status codes) and nothing else.
package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/eventpulse/ingestion-gateway/internal/domain"
)

// maxBodyBytes caps the size of an accepted event body to keep a single
// request from exhausting the server's memory.
const maxBodyBytes = 1 << 20 // 1 MiB

// EventHandler serves the event ingestion endpoints. It depends only on the
// domain.EventProducer port, never on a concrete broker implementation.
type EventHandler struct {
	producer domain.EventProducer
	logger   *slog.Logger
}

// NewEventHandler wires a handler to the producer it should publish through.
// A nil logger falls back to the slog default so the handler is always usable.
func NewEventHandler(producer domain.EventProducer, logger *slog.Logger) *EventHandler {
	if logger == nil {
		logger = slog.Default()
	}

	return &EventHandler{producer: producer, logger: logger}
}

// eventRequest is the JSON contract of POST /api/v1/events.
//
// id and timestamp are optional: the gateway generates an id and stamps the
// current time when they are omitted.
type eventRequest struct {
	ID        string         `json:"id"`
	Source    string         `json:"source"`
	EventType string         `json:"event_type"`
	Payload   map[string]any `json:"payload"`
	Timestamp *time.Time     `json:"timestamp"`
}

// eventResponse is returned when an event has been accepted for publishing.
type eventResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// errorResponse is the JSON body returned for every non-2xx outcome.
type errorResponse struct {
	Error string `json:"error"`
}

// Create handles POST /api/v1/events: it parses and validates the incoming
// JSON, then publishes the resulting event.
//
// Publishing is asynchronous from the client's point of view, so a successful
// call answers 202 Accepted with the id assigned to the event.
func (h *EventHandler) Create(w http.ResponseWriter, r *http.Request) {
	if !hasJSONContentType(r) {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")

		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req eventRequest
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))

		return
	}

	// Reject trailing content so that "{...}{...}" is not silently accepted.
	if decoder.More() {
		writeError(w, http.StatusBadRequest, "body must contain a single JSON object")

		return
	}

	var ts time.Time
	if req.Timestamp != nil {
		ts = *req.Timestamp
	}

	event, err := domain.NewEvent(req.ID, req.Source, req.EventType, req.Payload, ts)
	if err != nil {
		if isValidationError(err) {
			writeError(w, http.StatusBadRequest, err.Error())

			return
		}

		h.logger.ErrorContext(r.Context(), "could not build event", slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, "internal error")

		return
	}

	if err := h.producer.Produce(r.Context(), event); err != nil {
		h.logger.ErrorContext(r.Context(), "could not publish event",
			slog.String("event_id", event.ID),
			slog.String("source", event.Source),
			slog.Any("error", err),
		)
		writeError(w, http.StatusServiceUnavailable, "event stream unavailable, retry later")

		return
	}

	h.logger.InfoContext(r.Context(), "event accepted",
		slog.String("event_id", event.ID),
		slog.String("source", event.Source),
		slog.String("event_type", event.EventType),
	)

	writeJSON(w, http.StatusAccepted, eventResponse{ID: event.ID, Status: "accepted"})
}

// Health handles GET /healthz and reports that the process is serving.
func (h *EventHandler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// isValidationError reports whether err is a domain rule violation, which
// maps to 400 rather than 500.
func isValidationError(err error) bool {
	return errors.Is(err, domain.ErrMissingSource) ||
		errors.Is(err, domain.ErrMissingEventType) ||
		errors.Is(err, domain.ErrMissingPayload)
}

// hasJSONContentType accepts a missing header for convenience but rejects any
// media type that is not application/json.
func hasJSONContentType(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return true
	}

	// Strip parameters such as "; charset=utf-8".
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}

	return strings.EqualFold(strings.TrimSpace(ct), "application/json")
}

// writeJSON serialises v as the response body with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already flushed; logging is all that is left.
		slog.Error("could not encode response body", slog.Any("error", err))
	}
}

// writeError writes a JSON error body with the given status code.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
