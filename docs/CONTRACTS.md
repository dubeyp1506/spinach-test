# CONTRACTS.md — Multi-Agent Coordination Contract

This file is the single source of truth for interfaces between workstreams.
**Do not change anything here without updating dependents.** If you need a
change, update this file in the same PR that changes the code.

## 1. Ownership Matrix (exclusive file ownership)

| Agent | Owns | May read (not edit) |
|---|---|---|
| A0 Architect | `cmd/*` skeleton, `internal/config`, `internal/core`, `internal/store`, `internal/queue` (interfaces only), `migrations`, root files | — |
| A1 Ingestion | `internal/events/**`, `internal/queue/redis_streams.go`, `cmd/worker/main.go` (loop), `internal/system` (health/dlq handlers) | core, store, config |
| A2 Customers | `internal/customers/**` | core, store |
| A3 Audience | `internal/audience/**` | core, store, `customers.ScoringEngine` interface |
| A4 Campaigns | `internal/campaigns/**` | core, store |
| A5 AI | `internal/ai/**` | core, `campaigns.AnalyticsService` interface |
| A6 Data gen | `cmd/seed/**`, `scripts/**` | migrations (schema) |
| A7 Tests | `tests/**`, `internal/**/*_test.go` for cross-cutting cases | all |
| A8 Frontend | `web/**` | openapi.yaml |
| A9 Docs/DevOps | `docs/SYSTEM_DESIGN.md`, `docs/AI_DESIGN.md`, `README.md`, deploy configs | all |

Shared files (`cmd/api/main.go`, `go.mod`, `migrations/`) — changes only via
explicit contract update. Router registration happens in `cmd/api/main.go`;
each module exposes `RegisterRoutes(rg *gin.RouterGroup, deps Deps)`.

## 2. Event Contract

### POST /events — ingestion (A1)

Accepts one event or a batch (max 500). Always async: `202 Accepted`.

```json
// Request
{"events": [{
  "event_id": "evt_01HZ...",      // REQUIRED, client idempotency key, unique
  "customer_id": "cust_00042",    // REQUIRED, customers.external_id
  "campaign_id": "camp_007",      // optional, campaigns.external_id
  "channel": "email",             // REQUIRED: email|sms|whatsapp|push|web
  "type": "opened",               // REQUIRED: sent|delivered|opened|clicked|converted|bounced|unsubscribed|complained
  "occurred_at": "2026-09-16T10:30:00Z", // REQUIRED, RFC3339; may be out of order
  "payload": {"subject": "...", "variant": "B"}  // optional free-form
}]}

// Response 202
{"accepted": 1, "duplicates": 0, "rejected": [{"index": 0, "reason": "..."}]}
```

Rules:
- Validation errors (bad channel/type, missing fields, occurred_at in future) →
  per-item rejection in `rejected[]`; valid items still accepted. Whole-request
  JSON malformed → 400.
- Duplicate `event_id` → counted in `duplicates`, NOT an error (idempotent).
- Dedup: Redis `SETNX` fast-path + `events.event_id UNIQUE` as source of truth.
- After insert (`status='pending'`) → `XADD stream:events` → workers process.

### Processing semantics (A1 worker)

- Consumer group `event-workers`, `XREADGROUP` + `XAUTOCLAIM` for stale msgs.
- Retry: attempts++ on failure; `attempts >= 5` → `XADD stream:events:dlq` +
  row in `events_dlq` + event `status='failed'`. Success → `status='processed'`.
- Out-of-order: aggregation is commutative counters; `last_event_*` uses
  `GREATEST/least` on `occurred_at`; timeline sorts at read time.
- Per-customer mutation inside a tx with `SELECT ... FOR UPDATE` on the
  `engagement_profiles` row (serializes concurrent same-customer events).

## 3. Go Interfaces

```go
// internal/customers (A2 implements, A3 consumes)
type ScoringEngine interface {
    // Score recomputes and persists a customer's engagement score.
    Score(ctx context.Context, customerID int64) (*ScoreResult, error)
    // ScoreFromProfile is the pure algorithm, no I/O — unit-testable.
    ScoreFromProfile(p *Profile, now time.Time) float64
}
type ScoreResult struct {
    Score    float64   `json:"score"`
    Trend    string    `json:"trend"`            // rising|stable|declining
    Channel  string    `json:"preferred_channel"`
    Computed time.Time `json:"computed_at"`
}

// internal/campaigns (A4 implements, A5 consumes)
type AnalyticsService interface {
    Metrics(ctx context.Context, campaignID int64) (*CampaignMetrics, error)
}
type CampaignMetrics struct {
    CampaignID   int64
    Sends        int64
    Delivered    int64
    Opens        int64
    Clicks       int64
    Conversions  int64
    Bounces      int64
    Unsubscribes int64
    OpenRate, ClickRate, ConversionRate float64
    ByChannel    map[string]ChannelMetrics
    FirstEventAt, LastEventAt time.Time
}

// internal/ai (A5 owns; providers behind this interface)
type LLMProvider interface {
    Name() string
    Complete(ctx context.Context, prompt string) (string, error)
}
// Chain: groq → gemini → deterministic rule-based fallback (never fails the request).
```

## 4. Endpoint Contracts (all under `/api/v1`)

| Method & Path | Owner | Summary |
|---|---|---|
| POST `/events` | A1 | ingest, 202, see §2 |
| GET `/customers/{id}` | A2 | profile + live engagement score |
| GET `/customers/{id}/timeline` | A2 | cursor-paginated events, newest first, `?channel=&type=` filters |
| GET `/campaigns` | A4 | list w/ summary metrics, `?status=&channel=`, cursor pagination |
| GET `/campaigns/{id}/analytics` | A4 | CampaignMetrics JSON + computed anomaly flags |
| POST `/audience/recommend` | A3 | ranked audience, see §5 |
| POST `/campaigns/{id}/analyze` | A5 | `{facts, analysis, provider, fallback_used}` |
| POST `/campaigns/{id}/recommend` | A5 | `{facts, recommendations[], provider, fallback_used}` |
| GET `/system/health` | A1 | `{status, postgres, redis, queue_depth, dlq_size}` |
| GET `/system/dlq` | A1 | cursor-paginated DLQ rows |
| POST `/system/dlq/{id}/replay` | A1 | re-enqueue a DLQ entry (bonus) |

`{id}` path params accept the row's `external_id` (string). Errors use
`core.ErrorBody`. Lists use `core.ListResponse` cursor pagination.

## 5. Audience Recommendation Contract (A3)

```json
// POST /audience/recommend request
{
  "objective": "conversion",          // conversion|engagement|retention|reactivation|awareness
  "channel": "email",
  "size": 1000,                        // required audience size (top-K)
  "conditions": {
    "min_score": 0.2, "last_active_days": 30,
    "channels": ["email","sms"],       // preferred-channel filter
    "exclude_campaign_ids": ["camp_007"],
    "respect_frequency_cap": true
  }
}
// Response 200
{"candidates":[{"customer_id":"cust_00042","score":0.83,"rank":1,
  "reasons":["score=0.83 top-quartile","preferred_channel=email matches","converted 2x in 30d"]}],
 "meta":{"candidates_considered":42103,"filtered_out":7897,"took_ms":14}}
```

Algorithm contract: SQL pre-filter on indexed columns → **min-heap top-K**
(O(N log K), not O(N log N) sort) → frequency-cap check via `sends` →
reason generation. Must document complexity in code.

## 6. Conventions

- `external_id` everywhere in APIs; BIGINT ids internal only.
- Errors: `core.RespondError`. Never `c.JSON(500, gin.H{"error": ...})` inline.
- Logging: `log/slog`, JSON in production, `slog.Info("msg", "key", val)`.
- Context: pass `ctx` through every I/O call; honor cancellation.
- Tests: `testify`, table-driven, `-race` clean. Pure logic must not need DB.
- Time: always `TIMESTAMPTZ` / `time.Time` UTC.
- Every HTTP handler gets a route comment citing the contract section.

## 7. Scoring Algorithm Contract (A2 owns design)

Inputs required: recency (exponential decay), frequency, conversions,
positive/negative signals, per-channel engagement. Signature fixed by §3.
Complexity and reasoning documented in `internal/customers/scoring.go` and
`docs/SYSTEM_DESIGN.md`.
