-- Activity log: one row per user operation (API call) — who did what, with
-- what result, how fast. Written by the API's activity middleware through
-- an async batch writer, so recording never adds latency to the request.
-- Request bodies are NOT stored (size + PII); query strings are, capped.
CREATE TABLE IF NOT EXISTS activity_logs (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    request_id  TEXT,
    action      TEXT NOT NULL,          -- e.g. customer.view, prediction.channel
    method      TEXT NOT NULL,
    route       TEXT,                   -- route template, e.g. /api/v1/customers/:id
    path        TEXT NOT NULL,
    query       TEXT,
    entity      TEXT,                   -- path :id when present (cust_00042, camp_007, dlq id)
    status      INT  NOT NULL,
    latency_ms  INT  NOT NULL,
    client_ip   TEXT,
    user_agent  TEXT,
    summary     TEXT,                   -- handler-supplied outcome, e.g. "recommended email (high)"
    error       TEXT                    -- ErrorBody code + message on 4xx/5xx
);

-- Every index serves a GET /activity filter or the retention prune.
CREATE INDEX IF NOT EXISTS idx_activity_action   ON activity_logs (action, id DESC);
CREATE INDEX IF NOT EXISTS idx_activity_entity   ON activity_logs (entity, id DESC) WHERE entity IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_activity_request  ON activity_logs (request_id) WHERE request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_activity_failures ON activity_logs (id DESC) WHERE status >= 400;
CREATE INDEX IF NOT EXISTS idx_activity_created  ON activity_logs (created_at);
