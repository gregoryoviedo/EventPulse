package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewEventGeneratesID(t *testing.T) {
	e, err := NewEvent("", "src", "evt.type", map[string]any{"k": "v"}, time.Time{})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	if len(e.ID) != 32 {
		t.Fatalf("generated ID length = %d, want 32 hex chars", len(e.ID))
	}
}

func TestNewEventFallsBackToCurrentTimeUTC(t *testing.T) {
	before := time.Now().UTC()
	e, err := NewEvent("id", "src", "evt.type", map[string]any{"k": "v"}, time.Time{})
	after := time.Now().UTC()
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	if e.Timestamp.Before(before) || e.Timestamp.After(after) {
		t.Fatalf("Timestamp = %v, want between %v and %v", e.Timestamp, before, after)
	}
	if _, offset := e.Timestamp.Zone(); offset != 0 {
		t.Fatalf("Timestamp zone offset = %d, want UTC", offset)
	}
}

func TestNewEventNormalizesTimestampToUTC(t *testing.T) {
	local := time.Date(2026, 9, 10, 12, 0, 0, 0, time.FixedZone("CST", -6*3600))
	e, err := NewEvent("id", "src", "evt.type", map[string]any{"k": "v"}, local)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	want := local.UTC()
	if !e.Timestamp.Equal(want) {
		t.Fatalf("Timestamp = %v, want %v (UTC)", e.Timestamp, want)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Event)
		wantErr error
	}{
		{name: "valid"},
		{
			name:    "missing source",
			mutate:  func(e *Event) { e.Source = "" },
			wantErr: ErrMissingSource,
		},
		{
			name:    "missing event type",
			mutate:  func(e *Event) { e.EventType = "" },
			wantErr: ErrMissingEventType,
		},
		{
			name:    "empty payload",
			mutate:  func(e *Event) { e.Payload = map[string]any{} },
			wantErr: ErrMissingPayload,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := Event{
				ID:        "id",
				Source:    "src",
				EventType: "evt.type",
				Payload:   map[string]any{"k": "v"},
			}
			if tc.mutate != nil {
				tc.mutate(&e)
			}

			err := e.Validate()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Validate = %v, want %v", err, tc.wantErr)
			}
		})
	}
}