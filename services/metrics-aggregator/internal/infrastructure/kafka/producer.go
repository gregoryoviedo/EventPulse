// Package kafka holds the infrastructure adapters of the metrics aggregator:
// the raw.events consumer in consumer.go and this producer, which publishes
// aggregated metric samples to the metrics.ticks topic for downstream
// consumers.
package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eventpulse/events"
	"github.com/eventpulse/metrics-aggregator/internal/application"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	// defaultWriteTimeout bounds a single publish attempt.
	defaultWriteTimeout = 10 * time.Second
)

// Producer publishes metric tick samples to the metrics.ticks topic.
type Producer struct {
	writer       *kafkago.Writer
	writeTimeout time.Duration
}

// ProducerConfig holds the connection settings for the producer.
type ProducerConfig struct {
	// Brokers is the bootstrap broker list, e.g. ["kafka:9092"].
	Brokers []string
	// Topic is the destination topic. Defaults to events.TopicMetricsTicks.
	Topic string
	// WriteTimeout bounds a single publish attempt. Defaults to 10s.
	WriteTimeout time.Duration
}

// NewProducer builds a Producer for the given configuration. The writer is
// lazy: no connection is opened until the first publish.
func NewProducer(cfg ProducerConfig) *Producer {
	if cfg.Topic == "" {
		cfg.Topic = events.TopicMetricsTicks
	}

	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}

	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		Async:                  false,
		WriteTimeout:           cfg.WriteTimeout,
		AllowAutoTopicCreation: true,
	}

	return &Producer{writer: writer, writeTimeout: cfg.WriteTimeout}
}

// PublishTick publishes one aggregated sample as a metrics.ticks event.
func (p *Producer) PublishTick(ctx context.Context, sample application.Sample) error {
	envelope := events.Event{
		ID:        newEventID(),
		Type:      "metrics_tick",
		Source:    sample.Source,
		Timestamp: time.Now().UnixMilli(),
		Payload: map[string]interface{}{
			"event_type": sample.EventType,
			"count":      sample.Count,
		},
	}

	value, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal metrics tick: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, p.writeTimeout)
	defer cancel()

	msg := kafkago.Message{
		// Keyed by the aggregated source so every tick of a given source lands
		// on the same partition and keeps its relative order.
		Key:   []byte(sample.Source),
		Value: value,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish metrics tick for %s/%s: %w", sample.Source, sample.EventType, err)
	}

	return nil
}

// Close releases the broker connections, flushing anything buffered.
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close metrics tick writer: %w", err)
	}

	return nil
}

// newEventID returns a random hex id for the envelope.
func newEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("tick-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b[:])
}