// Package config loads the runtime configuration of the metrics aggregator
// from the environment, applying the same conventions as the other eventpulse
// services (KAFKA_BROKERS, KAFKA_TOPIC, HTTP_ADDR, ...).
package config

import (
	"os"
	"strings"

	"github.com/eventpulse/events"
)

// Config holds every tunable of the service.
type Config struct {
	// KafkaBrokers is the bootstrap broker list, e.g. ["kafka:9092"].
	KafkaBrokers []string
	// KafkaTopic is the topic the consumer reads from.
	KafkaTopic string
	// KafkaDLQTopic is the dead letter queue topic unprocessable messages are
	// forwarded to.
	KafkaDLQTopic string
	// KafkaGroupID is the consumer group the service joins.
	KafkaGroupID string
	// HTTPAddr is the listen address for the metrics and health endpoints.
	HTTPAddr string
}

// Load reads the configuration from the environment, applying a default for
// every unset variable so the service runs out of the box.
func Load() Config {
	return Config{
		KafkaBrokers: kafkaBrokers(getenv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:   getenv("KAFKA_TOPIC", events.TopicRawEvents),
		KafkaDLQTopic: getenv("KAFKA_DLQ_TOPIC", events.TopicRawEventsDLQ),
		KafkaGroupID: getenv("KAFKA_GROUP_ID", "metrics-aggregator"),
		HTTPAddr:     getenv("HTTP_ADDR", ":9090"),
	}
}

// kafkaBrokers parses the comma-separated KAFKA_BROKERS variable, ignoring
// empty segments.
func kafkaBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))

	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}

	if len(brokers) == 0 {
		return []string{"localhost:9092"}
	}

	return brokers
}

// getenv returns the environment variable value or fallback when unset/empty.
func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}

	return fallback
}
