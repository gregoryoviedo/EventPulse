package kafka

import (
	"testing"
	"time"

	"github.com/eventpulse/events"
)

func TestNewProducerDefaults(t *testing.T) {
	p := NewProducer(Config{Brokers: []string{"kafka:9092"}})

	if got := p.Topic(); got != events.TopicRawEvents {
		t.Fatalf("Topic = %q, want %q", got, events.TopicRawEvents)
	}
	if p.writeTimeout != defaultWriteTimeout {
		t.Fatalf("writeTimeout = %v, want %v", p.writeTimeout, defaultWriteTimeout)
	}
	if p.partitions != defaultPartitions {
		t.Fatalf("partitions = %d, want %d", p.partitions, defaultPartitions)
	}
	if p.replicationFactor != defaultReplicationFactor {
		t.Fatalf("replicationFactor = %d, want %d", p.replicationFactor, defaultReplicationFactor)
	}
	if p.writer.Addr == nil {
		t.Fatalf("writer Addr = nil, want broker list")
	}
}

func TestNewProducerKeepsConfiguredValues(t *testing.T) {
	p := NewProducer(Config{
		Brokers:           []string{"kafka:9092"},
		Topic:             "custom.topic",
		WriteTimeout:      3 * time.Second,
		Partitions:        3,
		ReplicationFactor: 2,
	})

	if got := p.Topic(); got != "custom.topic" {
		t.Fatalf("Topic = %q, want custom.topic", got)
	}
	if p.writeTimeout != 3*time.Second {
		t.Fatalf("writeTimeout = %v, want 3s", p.writeTimeout)
	}
	if p.partitions != 3 {
		t.Fatalf("partitions = %d, want 3", p.partitions)
	}
	if p.replicationFactor != 2 {
		t.Fatalf("replicationFactor = %d, want 2", p.replicationFactor)
	}
}

func TestProducerImplementsPort(t *testing.T) {
	p := NewProducer(Config{Brokers: []string{"kafka:9092"}})
	var _ interface{ Close() error } = p
}