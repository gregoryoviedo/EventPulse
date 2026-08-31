package main

import (
	"log"
	"net/http"

	"github.com/eventpulse/telemetry"
	"github.com/eventpulse/events"
)

func main() {
	if err := telemetry.Init(telemetry.Config{ServiceName: "ingestion-gateway"}); err != nil {
		log.Fatalf("telemetry init: %v", err)
	}
	_ = events.TopicRawEvents

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	log.Println("ingestion-gateway listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
