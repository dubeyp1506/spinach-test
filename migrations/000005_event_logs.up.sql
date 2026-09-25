-- Queryable event log + request correlation.
--
-- events.request_id carries the X-Request-ID of the ingest call onto the
-- durable row, so the worker (which only receives a pointer from the stream)
-- can log with the same correlation id — reconciler/sweeper republishes
-- included, because the worker reads it from the row it locks.
ALTER TABLE events ADD COLUMN IF NOT EXISTS request_id TEXT;

-- One row per lifecycle step of an event (ingested, duplicate, processed,
-- retry, dead_lettered, replayed). Rows are written inside the SAME
-- transaction as the step they describe, so the log can never claim a step
-- that rolled back. customer_id/campaign_id are deliberately not foreign keys:
-- the log must never block deleting a customer or campaign.
--
-- Write cost: EVENT_LOG_MODE=all adds ~2 rows per event (ingested +
-- processed); EVENT_LOG_MODE=errors records only retry/dead_lettered/replayed.
-- Retention: the worker prunes rows older than EVENT_LOG_RETENTION_DAYS.
CREATE TABLE IF NOT EXISTS event_logs (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_id    TEXT,
    customer_id BIGINT,
    campaign_id BIGINT,
    stage       TEXT NOT NULL CHECK (stage IN ('ingested','duplicate','processed','retry','dead_lettered','replayed')),
    level       TEXT NOT NULL CHECK (level IN ('debug','info','warn','error')),
    message     TEXT NOT NULL,
    request_id  TEXT,
    worker      TEXT,
    attempt     INT,
    details     JSONB NOT NULL DEFAULT '{}'
);

-- Every index serves a named GET /logs filter or the retention prune.
CREATE INDEX IF NOT EXISTS idx_event_logs_event    ON event_logs (event_id, id);
CREATE INDEX IF NOT EXISTS idx_event_logs_customer ON event_logs (customer_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_event_logs_request  ON event_logs (request_id) WHERE request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_event_logs_problems ON event_logs (id DESC) WHERE level IN ('warn','error');
CREATE INDEX IF NOT EXISTS idx_event_logs_created  ON event_logs (created_at);
