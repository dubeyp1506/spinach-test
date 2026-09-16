-- MarTech Intelligence & Campaign Decision Engine — initial schema
-- TEXT + CHECK constraints used instead of PG enums for migration flexibility.

CREATE TABLE customers (
    id              BIGSERIAL PRIMARY KEY,
    external_id     TEXT NOT NULL UNIQUE,          -- e.g. 'cust_00042'
    email           TEXT,
    attributes      JSONB NOT NULL DEFAULT '{}',   -- segment, region, plan, etc.
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_event_at   TIMESTAMPTZ
);

CREATE TABLE campaigns (
    id                      BIGSERIAL PRIMARY KEY,
    external_id             TEXT NOT NULL UNIQUE,
    name                    TEXT NOT NULL,
    objective               TEXT NOT NULL CHECK (objective IN ('conversion','engagement','retention','reactivation','awareness')),
    channel                 TEXT NOT NULL CHECK (channel IN ('email','sms','whatsapp','push','web')),
    status                  TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','active','paused','completed')),
    frequency_cap           INT NOT NULL DEFAULT 3,
    frequency_window_hours  INT NOT NULL DEFAULT 168,  -- per-customer send cap window
    audience_filter         JSONB NOT NULL DEFAULT '{}',
    started_at              TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL UNIQUE,          -- idempotency key (source of truth for dedup)
    customer_id     BIGINT NOT NULL REFERENCES customers(id),
    campaign_id     BIGINT REFERENCES campaigns(id),
    channel         TEXT NOT NULL CHECK (channel IN ('email','sms','whatsapp','push','web')),
    type            TEXT NOT NULL CHECK (type IN ('sent','delivered','opened','clicked','converted','bounced','unsubscribed','complained')),
    occurred_at     TIMESTAMPTZ NOT NULL,          -- client-side event time; may arrive out of order
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload         JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processed','failed','duplicate')),
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    processed_at    TIMESTAMPTZ
);
CREATE INDEX idx_events_customer_time ON events(customer_id, occurred_at DESC);
CREATE INDEX idx_events_campaign_type ON events(campaign_id, type);
CREATE INDEX idx_events_type_time ON events(type, occurred_at);
CREATE INDEX idx_events_status ON events(status) WHERE status = 'pending';

-- Dead letter queue for events that exhaust retries
CREATE TABLE events_dlq (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT,
    payload     JSONB NOT NULL,                    -- original event payload for replay
    error       TEXT NOT NULL,
    attempts    INT NOT NULL,
    failed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ
);

-- Materialized per-customer engagement profile, maintained by workers.
-- channel_counts shape: {"email":{"opened":3,"clicked":1},"sms":{...}}
CREATE TABLE engagement_profiles (
    customer_id      BIGINT PRIMARY KEY REFERENCES customers(id),
    total_events     INT NOT NULL DEFAULT 0,
    channel_counts   JSONB NOT NULL DEFAULT '{}',
    positive_events  INT NOT NULL DEFAULT 0,       -- opened + clicked + converted
    negative_events  INT NOT NULL DEFAULT 0,       -- bounced + unsubscribed + complained
    conversions      INT NOT NULL DEFAULT 0,
    last_event_at    TIMESTAMPTZ,
    last_event_type  TEXT,
    engagement_score DOUBLE PRECISION NOT NULL DEFAULT 0,  -- decayed continuously
    score_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    activity_trend   TEXT NOT NULL DEFAULT 'stable' CHECK (activity_trend IN ('rising','stable','declining')),
    preferred_channel TEXT CHECK (preferred_channel IN ('email','sms','whatsapp','push','web')),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_profiles_score ON engagement_profiles(engagement_score DESC);
CREATE INDEX idx_profiles_last_event ON engagement_profiles(last_event_at DESC);

-- Campaign sends — basis for frequency capping and audience dedup
CREATE TABLE sends (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(id),
    campaign_id BIGINT NOT NULL REFERENCES campaigns(id),
    channel     TEXT NOT NULL CHECK (channel IN ('email','sms','whatsapp','push','web')),
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_sends_freq ON sends(customer_id, sent_at DESC);
CREATE INDEX idx_sends_campaign ON sends(campaign_id, sent_at DESC);
