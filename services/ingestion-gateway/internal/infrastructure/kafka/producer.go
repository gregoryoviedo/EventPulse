// Package kafka is the infrastructure adapter that implements the
// domain.EventProducer port on top of Apache Kafka.
//
// It is the only package in the service that knows about the broker client,
// and the only place where the internal domain.Event is translated into the
// cross-service wire envelope defined in pkg/events.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/eventpulse/events"
	"github.com/eventpulse/ingestion-gateway/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	// defaultWriteTimeout bounds a single publish attempt when the caller does
	// not configure one.
	defaultWriteTimeout = 10 * time.Second
	// defaultPartitions is the partition count used when provisioning the topic.
	defaultPartitions = 1
	// defaultReplicationFactor suits the single-broker development cluster.
	defaultReplicationFactor = 1
)

// Producer publishes domain events to a Kafka topic.
type Producer struct {
	writer *kafkago.Writer
	// writeTimeout bounds each Produce call on top of the caller's context.
	writeTimeout      time.Duration
	partitions        int
	replicationFactor int
}

// Config holds the connection settings for the Kafka producer.
type Config struct {
	// Brokers is the bootstrap broker list, e.g. ["kafka:9092"].
	Brokers []string
	// Topic is the destination topic. Defaults to events.TopicRawEvents.
	Topic string
	// WriteTimeout bounds a single publish attempt. Defaults to 10s.
	WriteTimeout time.Duration
	// Partitions is the partition count used by EnsureTopic. Defaults to 1.
	Partitions int
	// ReplicationFactor is used by EnsureTopic. Defaults to 1.
	ReplicationFactor int
}

// Ensure the adapter satisfies the port at compile time.
var _ domain.EventProducer = (*Producer)(nil)

// NewProducer builds a Producer for the given configuration.
//
// The writer is lazy: no connection is opened until the first publish, so this
// never fails and the service can start before the broker is ready.
func NewProducer(cfg Config) *Producer {
	if cfg.Topic == "" {
		cfg.Topic = events.TopicRawEvents
	}

	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}

	if cfg.Partitions <= 0 {
		cfg.Partitions = defaultPartitions
	}

	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = defaultReplicationFactor
	}

	writer := &kafkago.Writer{
		Addr:  kafkago.TCP(cfg.Brokers...),
		Topic: cfg.Topic,
		// Hash on the message key so every event from a given source lands on
		// the same partition and keeps its relative order.
		Balancer: &kafkago.Hash{},
		// Wait for all in-sync replicas: an HTTP 202 must mean durably stored.
		RequiredAcks: kafkago.RequireAll,
		// Synchronous writes, so Produce surfaces broker errors to the caller.
		Async:                  false,
		WriteTimeout:           cfg.WriteTimeout,
		AllowAutoTopicCreation: true,
	}

	return &Producer{
		writer:            writer,
		writeTimeout:      cfg.WriteTimeout,
		partitions:        cfg.Partitions,
		replicationFactor: cfg.ReplicationFactor,
	}
}

// EnsureTopic creates the destination topic unless it already exists.
//
// Broker-side auto-creation alone is not enough: the first publish races the
// creation and fails with UnknownTopicOrPartition, which would surface as a
// spurious 503 on the very first request. Provisioning up front avoids that.
// An already-existing topic is not an error.
func (p *Producer) EnsureTopic(ctx context.Context) error {
	client := &kafkago.Client{Addr: p.writer.Addr, Timeout: p.writeTimeout}

	resp, err := client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{
		Addr: p.writer.Addr,
		Topics: []kafkago.TopicConfig{{
			Topic:             p.writer.Topic,
			NumPartitions:     p.partitions,
			ReplicationFactor: p.replicationFactor,
		}},
	})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", p.writer.Topic, err)
	}

	for topic, topicErr := range resp.Errors {
		if topicErr != nil && !errors.Is(topicErr, kafkago.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", topic, topicErr)
		}
	}

	return nil
}

// Produce maps the event onto the shared envelope and publishes it.
func (p *Producer) Produce(ctx context.Context, event domain.Event) error {
	envelope := events.Event{
		ID:     event.ID,
		Type:   event.EventType,
		Source: event.Source,
		// Unix milliseconds keeps sub-second precision in the int64 field.
		Timestamp: event.Timestamp.UnixMilli(),
		Payload:   event.Payload,
	}

	value, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", event.ID, err)
	}

	ctx, cancel := context.WithTimeout(ctx, p.writeTimeout)
	defer cancel()

	msg := kafkago.Message{
		Key:   []byte(event.Source),
		Value: value,
		Time:  event.Timestamp,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish event %s to %s: %w", event.ID, p.writer.Topic, err)
	}

	return nil
}

// Topic reports the destination topic, useful for start-up logging.
func (p *Producer) Topic() string {
	return p.writer.Topic
}

// Close flushes any pending message and releases the broker connections. It is
// safe to call more than once.
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close kafka writer: %w", err)
	}

	return nil
}
