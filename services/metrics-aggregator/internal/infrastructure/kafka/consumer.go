// Package kafka is the infrastructure adapter that reads the raw.events topic
// and hands each message to the domain.EventAggregator port. Messages that
// fail to deserialize or miss required fields are forwarded to the dead letter
// queue (raw.events.dlq) instead of being aggregated.
//
// Offsets are committed by hand, after the event has been aggregated or
// forwarded to the DLQ, so the service keeps at-least-once semantics: a crash
// re-delivers the message and the counters simply count it again.
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

	// defaultTopicSetupTimeout bounds provisioning the DLQ topic at start-up.
	defaultTopicSetupTimeout = 10 * time.Second
	// defaultDLQPartitions is the partition count used by EnsureTopic.
	defaultDLQPartitions = 1
	// defaultDLQReplicationFactor suits the single-broker development cluster.
	defaultDLQReplicationFactor = 1

	// dlqReasonInvalidJSON marks a message whose body is not valid JSON.
	dlqReasonInvalidJSON = "invalid_json"
	// dlqReasonMissingFields marks a message missing the required source or
	// event type fields.
	dlqReasonMissingFields = "missing_fields"
	// dlqReasonHeader is the Kafka header key carrying the rejection reason on
	// messages forwarded to the DLQ.
	dlqReasonHeader = "dlq_reason"
)

// Config holds the connection settings for the Kafka consumer.
type Config struct {
	// Brokers is the bootstrap broker list, e.g. ["kafka:9092"].
	Brokers []string
	// Topic is the topic to consume. Defaults to events.TopicRawEvents.
	Topic string
	// DLQTopic is the dead letter queue topic unprocessable messages are
	// forwarded to. Defaults to events.TopicRawEventsDLQ.
	DLQTopic string
	// GroupID is the consumer group the service joins. Defaults to
	// "metrics-aggregator".
	GroupID string
}

// Consumer reads raw events from Kafka and folds them into the aggregator.
type Consumer struct {
	reader     messageFetcher
	dlqWriter  messageWriter
	aggregator domain.EventAggregator
	metrics    *metrics.Metrics
	logger     *slog.Logger
	brokers    []string
	topic      string
	dlqTopic   string
	groupID    string
}

// messageFetcher is the subset of *kafkago.Reader the consumer relies on, so
// tests can substitute a fake.
type messageFetcher interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// messageWriter is the subset of *kafkago.Writer the consumer relies on to
// forward rejected messages to the dead letter queue.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// Ensure the real reader and writer satisfy the abstractions at compile time.
var (
	_ messageFetcher = (*kafkago.Reader)(nil)
	_ messageWriter  = (*kafkago.Writer)(nil)
)

// NewConsumer builds a Consumer for the given configuration. The reader and
// writer are lazy: no connection is opened until Run is called, so the service
// can start before the broker is ready.
func NewConsumer(cfg Config, aggregator domain.EventAggregator, m *metrics.Metrics, logger *slog.Logger) *Consumer {
	if cfg.Topic == "" {
		cfg.Topic = events.TopicRawEvents
	}

	if cfg.DLQTopic == "" {
		cfg.DLQTopic = events.TopicRawEventsDLQ
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

	// The destination topic is set per message in forwardToDLQ, so it must not
	// also be fixed on the writer: kafka-go rejects specifying it in both.
	dlqWriter := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.Brokers...),
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}

	return &Consumer{
		reader:     reader,
		dlqWriter:  dlqWriter,
		aggregator: aggregator,
		metrics:    m,
		logger:     logger,
		brokers:    cfg.Brokers,
		topic:      cfg.Topic,
		dlqTopic:   cfg.DLQTopic,
		groupID:    cfg.GroupID,
	}
}

// EnsureTopic provisions the dead letter queue topic unless it already exists.
//
// The DLQ writer resolves its destination topic per message, so broker-side
// auto-creation can race the very first poison message and surface
// UnknownTopicOrPartition. Provisioning up front removes that race.
// An already-existing topic is not an error.
func (c *Consumer) EnsureTopic(ctx context.Context) error {
	addr := kafkago.TCP(c.brokers...)
	client := &kafkago.Client{Addr: addr, Timeout: defaultTopicSetupTimeout}

	resp, err := client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{
		Addr: addr,
		Topics: []kafkago.TopicConfig{{
			Topic:             c.dlqTopic,
			NumPartitions:     defaultDLQPartitions,
			ReplicationFactor: defaultDLQReplicationFactor,
		}},
	})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", c.dlqTopic, err)
	}

	for topic, topicErr := range resp.Errors {
		if topicErr != nil && !errors.Is(topicErr, kafkago.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", topic, topicErr)
		}
	}

	return nil
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

// process decodes the wire envelope and aggregates it. Messages that fail to
// deserialize or miss required fields are forwarded to the dead letter queue
// with the reason attached as a header; the caller still commits the offset so
// the partition keeps moving instead of blocking on a poison message.
func (c *Consumer) process(ctx context.Context, msg kafkago.Message) {
	start := time.Now()

	var envelope events.Event
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		c.forwardToDLQ(ctx, msg, dlqReasonInvalidJSON)
		return
	}

	if envelope.Source == "" || envelope.Type == "" {
		c.forwardToDLQ(ctx, msg, dlqReasonMissingFields)
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

// forwardToDLQ republishes the full message to the dead letter queue, tagging
// it with the rejection reason in its headers, and records the events_dlq_total
// counter. A failed forward is logged but does not stop the caller from
// committing the offset: a poison message can never succeed on retry.
func (c *Consumer) forwardToDLQ(ctx context.Context, msg kafkago.Message, reason string) {
	dlqMsg := msg
	dlqMsg.Topic = c.dlqTopic
	dlqMsg.Headers = append(append([]kafkago.Header{}, msg.Headers...), kafkago.Header{
		Key:   dlqReasonHeader,
		Value: []byte(reason),
	})

	if err := c.dlqWriter.WriteMessages(ctx, dlqMsg); err != nil {
		c.logger.Error("failed to forward message to DLQ",
			slog.String("reason", reason),
			slog.String("source_topic", msg.Topic),
			slog.String("dlq_topic", c.dlqTopic),
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.Any("error", err),
		)

		return
	}

	c.metrics.EventsDLQTotal.WithLabelValues(reason).Inc()

	c.logger.Warn("message forwarded to DLQ",
		slog.String("reason", reason),
		slog.String("source_topic", msg.Topic),
		slog.String("dlq_topic", c.dlqTopic),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
	)
}

// Close releases the broker connections and leaves the consumer group.
func (c *Consumer) Close() error {
	var errs []error

	if err := c.reader.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close kafka reader: %w", err))
	}

	if err := c.dlqWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close kafka DLQ writer: %w", err))
	}

	return errors.Join(errs...)
}
