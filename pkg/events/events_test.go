package events

import (
	"encoding/json"
	"testing"
)

func TestTopicConstants(t *testing.T) {
	cases := map[string]string{
		TopicRawEvents:    "raw.events",
		TopicDocsEmbedded: "docs.embedded",
		TopicMetricsTicks: "metrics.ticks",
		TopicRawEventsDLQ: "raw.events.dlq",
	}

	for constant, want := range cases {
		if constant != want {
			t.Fatalf("topic constant = %q, want %q", constant, want)
		}
	}
}

func TestEventJSONRoundTrip(t *testing.T) {
	in := Event{
		ID:        "evt-1",
		Type:      "order.created",
		Source:    "checkout-api",
		Timestamp: 1789000000123,
		Payload: map[string]interface{}{
			"order_id": "o-42",
			"amount":   1000,
		},
	}

	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out Event
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.ID != in.ID || out.Type != in.Type || out.Source != in.Source || out.Timestamp != in.Timestamp {
		t.Fatalf("round trip mismatch: %+v", out)
	}
	if out.Payload["order_id"] != "o-42" {
		t.Fatalf("payload order_id = %v, want o-42", out.Payload["order_id"])
	}
}