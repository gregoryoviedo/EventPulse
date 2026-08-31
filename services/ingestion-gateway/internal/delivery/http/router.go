package http

import "net/http"

// NewRouter builds the service routing table on the standard library's
// ServeMux, using the method-aware patterns available since Go 1.22. No
// third-party router is needed.
//
// A request to /api/v1/events with the wrong method is answered by the mux
// itself with 405 Method Not Allowed.
func NewRouter(h *EventHandler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/events", h.Create)
	mux.HandleFunc("GET /healthz", h.Health)

	return mux
}
