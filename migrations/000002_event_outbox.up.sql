-- Transactional outbox: guarantees DB->queue delivery.
-- INSERT into this table happens in the SAME tx as the events row, so a
-- committed event can never be stranded unpublished. A reconciler publishes
-- unpublished rows; consumers are idempotent so double-publish is safe.
CREATE TABLE event_outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_db_id  BIGINT NOT NULL REFERENCES events(id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON event_outbox(id) WHERE published_at IS NULL;
