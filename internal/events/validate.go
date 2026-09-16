package events

import "time"

// Enums mirror the CHECK constraints in migrations/000001_init.up.sql and the
// openapi Channel/EventType schemas. Keep in sync.
var (
	validChannels = map[string]bool{
		"email": true, "sms": true, "whatsapp": true, "push": true, "web": true,
	}
	validTypes = map[string]bool{
		"sent": true, "delivered": true, "opened": true, "clicked": true,
		"converted": true, "bounced": true, "unsubscribed": true, "complained": true,
	}
)

// validateEvent returns "" for a valid item, else the rejection reason that
// goes into rejected[{index,reason}] (CONTRACTS §2). Pure — no I/O.
func validateEvent(in eventInput, now time.Time) string {
	if in.EventID == "" {
		return "event_id is required"
	}
	if in.CustomerID == "" {
		return "customer_id is required"
	}
	if !validChannels[in.Channel] {
		return "invalid channel"
	}
	if !validTypes[in.Type] {
		return "invalid type"
	}
	if in.OccurredAt == "" {
		return "occurred_at is required"
	}
	occ, err := time.Parse(time.RFC3339, in.OccurredAt)
	if err != nil {
		return "occurred_at must be RFC3339"
	}
	if occ.After(now) {
		return "occurred_at is in the future"
	}
	return ""
}
