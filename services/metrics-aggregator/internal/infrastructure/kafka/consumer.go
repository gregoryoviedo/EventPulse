// Package kafka is the infrastructure adapter that reads the raw.events topic
// and hands each message to the domain.EventAggregator port.
//
// Offsets are committed by hand, after the event has been aggregated, so the
// service keeps at-least-once semantics: a crash re-delivers the message and
// the counters simply count it again.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/eventpulse/events"
	"github.com/eventpulse/metrics-aggregator/internal/domain"
	"github.com/eventpulse/metrics-aggregator/internal/metrics"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	// defaultMaxBytes caps a single fetched message to keep memory bounded.
	defaultMaxBytes = 1 << 20
)

// Config holds the connection settings for the Kafka consumer.
type Config struct {
	// Brokers is the bootstrap broker list, e.g. ["kafka:9092"].
	Brokers []string
	// Topic is the topic to consume. Defaults to events.TopicRawEvents.
	Topic string
	// GroupID is the consumer group the service joins. Defaults to
	// "metrics-aggregator".
	GroupID string
}

// Consumer reads raw events from Kafka and folds them into the aggregator.
type Consumer struct {
	reader     messageFetcher
	aggregator domain.EventAggregator
	metrics    *metrics.Metrics
	logger     *slog.Logger
	topic      string
	groupID    string
}

// messageFetcher is the subset of *kafkago.Reader the consumer relies on, so
// tests can substitute a fake.
type messageFetcher interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// Ensure the real reader satisfies the abstraction at compile time.
var _ messageFetcher = (*kafkago.Reader)(nil)

// NewConsumer builds a Consumer for the given configuration. The reader is
// lazy: no connection is opened until Run is called, so the service can start
// before the broker is ready.
func NewConsumer(cfg Config, aggregator domain.EventAggregator, m *metrics.Metrics, logger *slog.Logger) *Consumer {
	if cfg.Topic == "" {
		cfg.Topic = events.TopicRawEvents
	}

	if cfg.GroupID == "" {
		cfg.GroupID = "metrics-aggregator"
	}

	if logger == nil {
		logger = slog.Default()
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,
		MaxBytes: defaultMaxBytes,
	})

	return &Consumer{
		reader:     reader,
		aggregator: aggregator,
		metrics:    m,
		logger:     logger,
		topic:      cfg.Topic,
		groupID:    cfg.GroupID,
	}
}

// Run polls the topic until ctx is cancelled, aggregating and committing each
// message. It returns nil on a clean shutdown.
func (c *Consumer) Run(ctx context.Context) error {
	// One consumer per process: the gauge flips between 1 (running) and 0
	// (stopped), which is what the requirement asks for.
	c.metrics.ActiveKafkaConsumers.Inc()
	defer c.metrics.ActiveKafkaConsumers.Dec()

	c.logger.Info("kafka consumer started",
		slog.String("topic", c.topic),
		slog.String("group_id", c.groupID),
	)
	defer c.logger.Info("kafka consumer stopped")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return fmt.Errorf("fetch message: %w", err)
		}

		c.process(ctx, msg)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return fmt.Errorf("commit message: %w", err)
		}
	}
}

// process decodes the wire envelope and aggregates it. Undecodable messages
// are logged and skipped: they can never succeed, so committing keeps the
// partition moving instead of blocking on a poison message.
func (c *Consumer) process(ctx context.Context, msg kafkago.Message) {
	start := time.Now()

	var envelope events.Event
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		c.logger.Warn("dropping unprocessable message",
			slog.String("topic", msg.Topic),
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.Any("error", err),
		)

		return
	}

	if err := c.aggregator.Handle(ctx, envelope); err != nil {
		c.logger.Error("event aggregation failed",
			slog.String("event_id", envelope.ID),
			slog.String("source", envelope.Source),
			slog.String("event_type", envelope.Type),
			slog.Any("error", err),
		)
	}

	duration := time.Since(start).Seconds()
	c.metrics.EventProcessingDurationSeconds.WithLabelValues(envelope.Source, envelope.Type).Observe(duration)
}

// Close releases the broker connection and leaves the consumer group.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("close kafka reader: %w", err)
	}

	return nil
}
