package campaigns

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// ApplyEventTx folds one processed event into the campaign_metrics rollup
// inside the CALLER's transaction — the A1 worker composes this alongside
// customers.Applier.ProcessTx so the counter commits atomically with
// events.status='processed' (CONTRACTS §2 "Atomic processing"). A crash
// before XACK redelivers, the worker sees 'processed', and skips — no
// double-count.
//
// The upsert is a plain increment — commutative, so out-of-order and
// replayed deliveries converge. Events with no attributable campaign
// (campaignID <= 0) are skipped: the rollup is keyed on campaign_id.
func ApplyEventTx(ctx context.Context, tx pgx.Tx, campaignID int64, eventType string, channel string, occurredAt time.Time) error {
	if campaignID <= 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO campaign_metrics (campaign_id, channel, event_type, day, count)
		VALUES ($1, $2, $3, $4::date, 1)
		ON CONFLICT (campaign_id, channel, event_type, day)
		DO UPDATE SET count = campaign_metrics.count + 1`,
		campaignID, channel, eventType, occurredAt)
	return err
}
