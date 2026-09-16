-- Worker-maintained per-campaign daily rollup.
-- One row per (campaign_id, channel, event_type, day); the worker upserts
-- count += 1 per processed event inside the same transaction that flips
-- events.status='processed' (CONTRACTS §2 atomic processing). The
-- read/analytics path then queries this rollup instead of scanning the
-- forever-growing events table.
CREATE TABLE campaign_metrics (
    campaign_id BIGINT NOT NULL REFERENCES campaigns(id),
    channel     TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    day         DATE NOT NULL,
    count       BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (campaign_id, channel, event_type, day)
);

-- One-time backfill of pre-existing events so the seeded dataset has correct
-- counters. Events with no campaign are uncountable (rollup key requires a
-- campaign_id). No-op on a fresh database.
INSERT INTO campaign_metrics (campaign_id, channel, event_type, day, count)
SELECT campaign_id, channel, type, occurred_at::date, COUNT(*)
FROM events
WHERE campaign_id IS NOT NULL
GROUP BY 1, 2, 3, 4;

-- Covering index: any remaining per-campaign events queries become index-only.
CREATE INDEX IF NOT EXISTS idx_events_campaign_type_time
    ON events(campaign_id, type, occurred_at);
