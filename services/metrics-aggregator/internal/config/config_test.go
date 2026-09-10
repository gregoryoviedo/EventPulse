package config

import (
	"reflect"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_DLQ_TOPIC", "")
	t.Setenv("KAFKA_GROUP_ID", "")
	t.Setenv("KAFKA_METRICS_TICKS_TOPIC", "")
	t.Setenv("KAFKA_METRICS_TICK_INTERVAL", "")
	t.Setenv("HTTP_ADDR", "")

	cfg := Load()

	if want := []string{"localhost:9092"}; !reflect.DeepEqual(cfg.KafkaBrokers, want) {
		t.Fatalf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, want)
	}
	if cfg.KafkaTopic != "raw.events" {
		t.Fatalf("KafkaTopic = %q, want raw.events", cfg.KafkaTopic)
	}
	if cfg.KafkaDLQTopic != "raw.events.dlq" {
		t.Fatalf("KafkaDLQTopic = %q, want raw.events.dlq", cfg.KafkaDLQTopic)
	}
	if cfg.KafkaGroupID != "metrics-aggregator" {
		t.Fatalf("KafkaGroupID = %q, want metrics-aggregator", cfg.KafkaGroupID)
	}
	if cfg.KafkaMetricsTicksTopic != "metrics.ticks" {
		t.Fatalf("KafkaMetricsTicksTopic = %q, want metrics.ticks", cfg.KafkaMetricsTicksTopic)
	}
	if cfg.KafkaMetricsTicksInterval != 15*time.Second {
		t.Fatalf("KafkaMetricsTicksInterval = %v, want 15s", cfg.KafkaMetricsTicksInterval)
	}
	if cfg.HTTPAddr != ":9090" {
		t.Fatalf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " kafka:9092 , localhost:29092 ")
	t.Setenv("KAFKA_TOPIC", "metrics.ticks")
	t.Setenv("KAFKA_DLQ_TOPIC", "raw.events.dlq.prod")
	t.Setenv("KAFKA_GROUP_ID", "my-group")
	t.Setenv("KAFKA_METRICS_TICKS_TOPIC", "metrics.ticks.prod")
	t.Setenv("KAFKA_METRICS_TICK_INTERVAL", "30s")
	t.Setenv("HTTP_ADDR", ":8080")

	cfg := Load()

	wantBrokers := []string{"kafka:9092", "localhost:29092"}
	if !reflect.DeepEqual(cfg.KafkaBrokers, wantBrokers) {
		t.Fatalf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, wantBrokers)
	}
	if cfg.KafkaTopic != "metrics.ticks" {
		t.Fatalf("KafkaTopic = %q, want metrics.ticks", cfg.KafkaTopic)
	}
	if cfg.KafkaDLQTopic != "raw.events.dlq.prod" {
		t.Fatalf("KafkaDLQTopic = %q, want raw.events.dlq.prod", cfg.KafkaDLQTopic)
	}
	if cfg.KafkaGroupID != "my-group" {
		t.Fatalf("KafkaGroupID = %q, want my-group", cfg.KafkaGroupID)
	}
	if cfg.KafkaMetricsTicksTopic != "metrics.ticks.prod" {
		t.Fatalf("KafkaMetricsTicksTopic = %q, want metrics.ticks.prod", cfg.KafkaMetricsTicksTopic)
	}
	if cfg.KafkaMetricsTicksInterval != 30*time.Second {
		t.Fatalf("KafkaMetricsTicksInterval = %v, want 30s", cfg.KafkaMetricsTicksInterval)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
}
