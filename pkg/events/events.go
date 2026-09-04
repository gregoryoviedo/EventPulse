// Package events defines the shared Kafka topic names and event envelope used
// across the eventpulse platform.
package events

// Topic names used across the eventpulse platform.
const (
	// TopicRawEvents carries unprocessed source events from the ingestion
	// gateway into the document processor.
	TopicRawEvents = "raw.events"
	// TopicDocsEmbedded carries documents that have been chunked and embedded
	// (vectorized) and are ready to be persisted into pgvector.
	TopicDocsEmbedded = "docs.embedded"
	// TopicMetricsTicks carries aggregated metric samples.
	TopicMetricsTicks = "metrics.ticks"
	// TopicRawEventsDLQ carries messages that failed to be processed and were
	// rejected by a consumer, for later inspection and replay.
	TopicRawEventsDLQ = "raw.events.dlq"
)

// Event is the base envelope for all platform events.
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Source    string                 `json:"source"`
	Timestamp int64                  `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}
