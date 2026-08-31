package main

import (
	"log"

	"github.com/eventpulse/telemetry"
	"github.com/eventpulse/events"
)

func main() {
	if err := telemetry.Init(telemetry.Config{ServiceName: "metrics-aggregator"}); err != nil {
		log.Fatalf("telemetry init: %v", err)
	}
	_ = events.TopicMetricsTicks

	// TODO: consume events from Kafka and aggregate metrics.
	log.Println("metrics-aggregator started (scaffold)")
	select {}
}
