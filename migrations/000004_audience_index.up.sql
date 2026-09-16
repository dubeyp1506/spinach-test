-- Scalability fix for POST /audience/recommend (internal/audience).
--
-- The pre-filter scans engagement_profiles in engagement_score DESC order
-- under a bounded LIMIT, joining customers on PK for is_active. is_active
-- lives on customers, not engagement_profiles, so it cannot be indexed here.
--
-- This covering index lets Postgres satisfy ORDER BY engagement_score DESC
-- plus the profile-side predicates (engagement_score >= $1, last_event_at,
-- preferred_channel = ANY) AND every column the pre-filter selects from
-- engagement_profiles — including the join key customer_id — as an
-- index-only scan: non-matching entries are discarded without a heap fetch.
CREATE INDEX IF NOT EXISTS idx_profiles_audience
    ON engagement_profiles (engagement_score DESC)
    INCLUDE (customer_id, preferred_channel, last_event_at,
             conversions, positive_events, negative_events,
             total_events, activity_trend);
