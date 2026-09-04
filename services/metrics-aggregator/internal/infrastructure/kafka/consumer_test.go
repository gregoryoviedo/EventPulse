package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/eventpulse/events"
	"github.com/eventpulse/metrics-aggregator/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	kafkago "github.com/segmentio/kafka-go"
)

// fakeFetcher substitutes the real kafka-go reader in tests. After its message
// queue runs dry it blocks until the context is cancelled, mirroring the real
// reader's behaviour on shutdown. Fields touched by the Run goroutine are
// guarded by a mutex.
type fakeFetcher struct {
	mu        sync.Mutex
	messages  []kafkago.Message
	err       error
	committed []kafkago.Message
	closed    bool
}

func (f *fakeFetcher) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	f.mu.Lock()
	err := f.err
	if err != nil {
		f.mu.Unlock()

		return kafkago.Message{}, err
	}

	if len(f.messages) == 0 {
		// Unlock while blocking on the context so the test can still read the
		// committed state.
		f.mu.Unlock()
		<-ctx.Done()

		return kafkago.Message{}, ctx.Err()
	}

	msg := f.messages[0]
	f.messages = f.messages[1:]
	f.mu.Unlock()

	return msg, nil
}

func (f *fakeFetcher) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.committed = append(f.committed, msgs...)

	return nil
}

func (f *fakeFetcher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true

	return nil
}

func (f *fakeFetcher) committedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.committed)
}

func (f *fakeFetcher) committedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	offsets := make([]int64, len(f.committed))
	for i, msg := range f.committed {
		offsets[i] = msg.Offset
	}

	return offsets
}

func (f *fakeFetcher) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.closed
}

// spyAggregator records every event the consumer hands over.
type spyAggregator struct {
	mu     sync.Mutex
	events []events.Event
	err    error
}

func (s *spyAggregator) Handle(_ context.Context, event events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, event)

	return s.err
}

func (s *spyAggregator) snapshot() []events.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]events.Event, len(s.events))
	copy(out, s.events)

	return out
}

func TestConsumerAggregatesAndCommitsValidMessage(t *testing.T) {
	m := metrics.New()
	spy := &spyAggregator{}
	fake := &fakeFetcher{
		messages: []kafkago.Message{{
			Topic:     "raw.events",
			Partition: 0,
			Offset:    42,
			Value:     []byte(`{"id":"evt-1","type":"document_uploaded","source":"docs-api","timestamp":1700000000000,"payload":{"content":"x"}}`),
		}},
	}

	c := NewConsumer(Config{
		Brokers: []string{"kafka:9092"},
		Topic:   "raw.events",
		GroupID: "metrics-aggregator",
	}, spy, m, nil)
	c.reader = fake

	if err := runAndWait(c, fake, func() bool {
		return fake.committedCount() == 1 && len(spy.snapshot()) == 1
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	got := spy.snapshot()
	if len(got) != 1 {
		t.Fatalf("aggregated events = %d, want 1", len(got))
	}
	if got[0].ID != "evt-1" || got[0].Source != "docs-api" || got[0].Type != "document_uploaded" {
		t.Fatalf("aggregated event = %+v, want evt-1/docs-api/document_uploaded", got[0])
	}

	if offsets := fake.committedOffsets(); len(offsets) != 1 || offsets[0] != 42 {
		t.Fatalf("committed offsets = %v, want [42]", offsets)
	}

	// The gauge is flipped back to 0 once the loop stops.
	if got := testutil.ToFloat64(m.ActiveKafkaConsumers); got != 0 {
		t.Fatalf("active_kafka_consumers = %v, want 0 after shutdown", got)
	}
}

func TestConsumerSkipsInvalidJSONButKeepsMoving(t *testing.T) {
	m := metrics.New()
	spy := &spyAggregator{}
	fake := &fakeFetcher{
		messages: []kafkago.Message{{
			Topic:  "raw.events",
			Offset: 7,
			Value:  []byte("this is not json"),
		}},
	}

	c := NewConsumer(Config{
		Brokers: []string{"kafka:9092"},
	}, spy, m, nil)
	c.reader = fake

	if err := runAndWait(c, fake, func() bool { return fake.committedCount() == 1 }); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := spy.snapshot(); len(got) != 0 {
		t.Fatalf("aggregated events = %d, want 0 for a poison message", len(got))
	}
	if offsets := fake.committedOffsets(); len(offsets) != 1 || offsets[0] != 7 {
		t.Fatalf("committed offsets = %v, want [7]", offsets)
	}
}

func TestConsumerReturnsNilOnContextCancellation(t *testing.T) {
	m := metrics.New()
	spy := &spyAggregator{}
	fake := &fakeFetcher{}

	c := NewConsumer(Config{
		Brokers: []string{"kafka:9092"},
	}, spy, m, nil)
	c.reader = fake

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run() on cancelled context = %v, want nil", err)
	}

	if got := spy.snapshot(); len(got) != 0 {
		t.Fatalf("aggregated events = %d, want 0", len(got))
	}
	if got := testutil.ToFloat64(m.ActiveKafkaConsumers); got != 0 {
		t.Fatalf("active_kafka_consumers = %v, want 0 after shutdown", got)
	}
}

func TestConsumerSurfacesFetchErrors(t *testing.T) {
	m := metrics.New()
	spy := &spyAggregator{}
	fake := &fakeFetcher{err: errors.New("broker unreachable")}

	c := NewConsumer(Config{
		Brokers: []string{"kafka:9092"},
	}, spy, m, nil)
	c.reader = fake

	if err := c.Run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want a fetch error")
	}
}

func TestConsumerCloseClosesReader(t *testing.T) {
	m := metrics.New()
	fake := &fakeFetcher{}

	c := NewConsumer(Config{Brokers: []string{"kafka:9092"}}, &spyAggregator{}, m, nil)
	c.reader = fake

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !fake.isClosed() {
		t.Fatal("reader was not closed")
	}
}

// runAndWait runs the consumer until predicate holds, then cancels the context
// and returns the Run result. This avoids racing the commit: the predicate
// only fires once the message has been fully processed and committed.
func runAndWait(c *Consumer, fake *fakeFetcher, predicate func() bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			cancel()

			return <-done
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancel()

	return errors.New("timed out waiting for the consumer")
}
