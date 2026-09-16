package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func validInput() eventInput {
	return eventInput{
		EventID:    "evt_01HZTEST",
		CustomerID: "cust_00042",
		Channel:    "email",
		Type:       "opened",
		OccurredAt: "2020-01-01T10:00:00Z",
	}
}

func TestValidateEvent(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		mutate func(*eventInput)
		want   string
	}{
		{"valid", func(i *eventInput) {}, ""},
		{"valid with campaign and payload", func(i *eventInput) {
			i.CampaignID = "camp_007"
			i.Payload = []byte(`{"variant":"B"}`)
		}, ""},
		{"missing event_id", func(i *eventInput) { i.EventID = "" }, "event_id is required"},
		{"missing customer_id", func(i *eventInput) { i.CustomerID = "" }, "customer_id is required"},
		{"missing channel", func(i *eventInput) { i.Channel = "" }, "invalid channel"},
		{"bad channel", func(i *eventInput) { i.Channel = "carrier-pigeon" }, "invalid channel"},
		{"missing type", func(i *eventInput) { i.Type = "" }, "invalid type"},
		{"bad type", func(i *eventInput) { i.Type = "exploded" }, "invalid type"},
		{"missing occurred_at", func(i *eventInput) { i.OccurredAt = "" }, "occurred_at is required"},
		{"unparseable occurred_at", func(i *eventInput) { i.OccurredAt = "yesterday" }, "occurred_at must be RFC3339"},
		{"future occurred_at", func(i *eventInput) { i.OccurredAt = "2027-01-01T00:00:00Z" }, "occurred_at is in the future"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := validInput()
			tc.mutate(&in)
			assert.Equal(t, tc.want, validateEvent(in, now))
		})
	}
}

func TestValidateEvent_AllChannelsAndTypes(t *testing.T) {
	now := time.Now()
	for ch := range validChannels {
		for ty := range validTypes {
			in := validInput()
			in.Channel, in.Type = ch, ty
			assert.Empty(t, validateEvent(in, now), "channel=%s type=%s", ch, ty)
		}
	}
}

// CONTRACTS §2: ON CONFLICT DO NOTHING RETURNING id — row returned means new
// (accepted); no row means the event_id already exists (duplicate).
func TestIsDuplicate(t *testing.T) {
	cases := []struct {
		name        string
		rowReturned bool
		want        bool
	}{
		{"row returned = new event, not duplicate", true, false},
		{"no row returned = existing event_id, duplicate", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isDuplicate(tc.rowReturned))
		})
	}
}

// CONTRACTS §2: on ProcessTx error attempts++; attempts >= max → DLQ.
func TestShouldDeadLetter(t *testing.T) {
	cases := []struct {
		name        string
		attempts    int
		maxAttempts int
		want        bool
	}{
		{"first failure below max retries", 1, 5, false},
		{"one below max retries", 4, 5, false},
		{"reaching max dead-letters", 5, 5, true},
		{"beyond max dead-letters", 7, 5, true},
		{"max of 1 dead-letters immediately", 1, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldDeadLetter(tc.attempts, tc.maxAttempts))
		})
	}
}
