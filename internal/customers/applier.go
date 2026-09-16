package customers

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// ApplierEvent is the event view the worker hands us. It mirrors A1's
// StoredEvent at the §2 seam; the worker adapts between the two.
type ApplierEvent struct {
	EventDBID  int64
	EventID    string
	CustomerID int64           // internal BIGINT — resolved upstream
	CampaignID *int64          // nil when the event has no campaign
	Channel    string          // email|sms|whatsapp|push|web
	Type       string          // sent|delivered|opened|clicked|converted|bounced|unsubscribed|complained
	OccurredAt time.Time       // client-side event time; may be out of order
	Payload    json.RawMessage // optional free-form, unused by aggregation
}

// Applier applies processed events to engagement_profiles. It is stateless:
// all I/O goes through the caller's transaction, so it satisfies A1's
// events.Processor seam (CONTRACTS §2) without owning any connections.
type Applier struct{}

func NewApplier() *Applier { return &Applier{} }

// ProcessTx applies one event inside the CALLER's transaction (the worker
// owns Begin/Commit — CONTRACTS §2 "Atomic processing"). All mutations are
// commutative or max-based so out-of-order and replayed deliveries converge:
//
//  1. Ensure the profile row exists, then SELECT ... FOR UPDATE — the row
//     lock serializes concurrent workers processing events for the SAME
//     customer; different customers proceed in parallel.
//  2. Merge counters in Go under the lock (channel_counts jsonb, pos/neg,
//     conversions, total_events) — plain increments are commutative.
//  3. Fold the event into the raw decayed score per §7 using evt.OccurredAt;
//     the anchor never moves backward, so OOO events are exact, not approx.
//  4. last_event_at/last_event_type move only when evt.OccurredAt is newer
//     (max semantics, GREATEST-equivalent — the type must follow the max row,
//     so the compare happens in Go where both fields move together).
//  5. 'sent' events also append to sends (frequency-cap / audience-dedup
//     basis). Skipped when CampaignID is nil because sends.campaign_id is
//     NOT NULL — an unattributed send cannot be capped anyway.
//  6. customers.last_event_at bumps via SQL GREATEST (NULL-safe in Postgres:
//     GREATEST ignores NULL args).
//
// Idempotency: the worker only calls us while events.status != 'processed',
// and flips the status in this same tx — a crash before XACK redelivers, sees
// 'processed', and skips; a rollback undoes our writes with everything else.
func (a *Applier) ProcessTx(ctx context.Context, tx pgx.Tx, evt ApplierEvent) error {
	// 1. Ensure + lock the profile row.
	if _, err := tx.Exec(ctx, `
		INSERT INTO engagement_profiles (customer_id) VALUES ($1)
		ON CONFLICT (customer_id) DO NOTHING`, evt.CustomerID); err != nil {
		return err
	}
	p, err := loadProfile(ctx, tx, evt.CustomerID, true)
	if err != nil {
		return err
	}

	// 2. Commutative counter merges (channel_counts shape per migration:
	// {"email":{"opened":3,...}}).
	if p.ChannelCounts == nil {
		p.ChannelCounts = map[string]map[string]int{}
	}
	if p.ChannelCounts[evt.Channel] == nil {
		p.ChannelCounts[evt.Channel] = map[string]int{}
	}
	p.ChannelCounts[evt.Channel][evt.Type]++
	p.TotalEvents++
	if isPositiveType(evt.Type) {
		p.PositiveEvents++
	}
	if isNegativeType(evt.Type) {
		p.NegativeEvents++
	}
	if evt.Type == "converted" {
		p.Conversions++
	}

	// 3. §7 score fold — OOO-safe, anchor never regresses.
	p.RawScore, p.ScoreUpdatedAt = applyEvent(p.RawScore, p.ScoreUpdatedAt, evt.OccurredAt, eventWeights[evt.Type])

	// 4. Max-based last_event_* (type must travel with its timestamp).
	if p.LastEventAt == nil || evt.OccurredAt.After(*p.LastEventAt) {
		at := evt.OccurredAt.UTC()
		p.LastEventAt = &at
		p.LastEventType = &evt.Type
	}

	// Preferred channel is persisted because §5 audience filters need a
	// column; derived identically at read from channel_counts.
	p.PreferredChannel = preferredChannel(p.ChannelCounts)

	if _, err := tx.Exec(ctx, `
		UPDATE engagement_profiles
		SET total_events = $2, channel_counts = $3, positive_events = $4,
		    negative_events = $5, conversions = $6, last_event_at = $7,
		    last_event_type = $8, engagement_score = $9, score_updated_at = $10,
		    preferred_channel = $11, updated_at = now()
		WHERE customer_id = $1`,
		evt.CustomerID, p.TotalEvents, p.ChannelCounts, p.PositiveEvents,
		p.NegativeEvents, p.Conversions, p.LastEventAt, p.LastEventType,
		p.RawScore, p.ScoreUpdatedAt, p.PreferredChannel); err != nil {
		return err
	}

	// 5. sends row for 'sent' events with an attributable campaign.
	if evt.Type == "sent" && evt.CampaignID != nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO sends (customer_id, campaign_id, channel, sent_at)
			VALUES ($1, $2, $3, $4)`,
			evt.CustomerID, *evt.CampaignID, evt.Channel, evt.OccurredAt); err != nil {
			return err
		}
	}

	// 6. Customer-level recency marker — GREATEST keeps the max under OOO.
	if _, err := tx.Exec(ctx, `
		UPDATE customers SET last_event_at = GREATEST(last_event_at, $2)
		WHERE id = $1`, evt.CustomerID, evt.OccurredAt); err != nil {
		return err
	}
	return nil
}
