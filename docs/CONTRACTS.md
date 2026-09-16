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
| A7 Tests | `tests/**` ONLY (integration, race, failure, API tests). Module agents own their own `internal/**/*_test.go` unit tests — A7 never edits them | all |
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
- **Dedup authority order: PostgreSQL `events.event_id UNIQUE` is the ONLY
  authority.** `INSERT ... ON CONFLICT DO NOTHING RETURNING id`; no row
  returned ⇒ duplicate. Redis `SETNX` may be set AFTER successful commit as a
  read-optimization, but must NEVER be used to classify duplicates (a stale
  Redis key must not suppress a legitimate retry).
- **Transactional outbox (REQUIRED):** in the SAME transaction as the events
  insert, `INSERT INTO event_outbox(event_db_id)`. After commit, the handler
  does a best-effort pipelined `XADD stream:events` for low latency, then
  DELETEs the published outbox rows (their only job was carrying the id).
  A reconciler loop (runs in the worker process) every ~5s:
  `SELECT ... FROM event_outbox WHERE published_at IS NULL FOR UPDATE SKIP
  LOCKED` → `XADD` → `DELETE` successful rows. Double-publish is safe because
  the consumer is idempotent (see below). A pending-events sweeper on the
  same tick re-enqueues `events.status='pending'` older than 30s — covers
  stream entries lost/evicted after publish.
- Batch shape: ONE tx per request — 2 `ANY($1)` external-id lookups + one
  multi-row `INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING` + one
  `unnest` outbox insert. A failed batch tx → **503** (client retries the
  batch idempotently), never a 202 with everything itemized as rejected.
- Body cap 4 MiB; rate limit charges **one token per event** (not per
  request).

### Processing semantics (A1 worker)

- Consumer group `event-workers`, `XREADGROUP` + `XAUTOCLAIM` for stale msgs.
  Each read batch is fanned out over `WORKER_CONCURRENCY` goroutines
  (default 8); row locking makes contention safe.
- **Atomic processing (REQUIRED):** per message, ONE Postgres transaction:
  `SELECT ... FROM events WHERE id=$1 FOR UPDATE` → if `status='processed'`
  or `'duplicate'` → COMMIT + XACK + return (idempotent redelivery) →
  `processor.ProcessTx(ctx, tx, evt)` (profile mutation + sends insert, owned
  by A2) → `UPDATE events SET status='processed', processed_at=now()` →
  COMMIT → then XACK. Aggregation and completion commit atomically; a crash
  before ACK is harmless because redelivery sees `status='processed'`.
- Retry: on error, rollback, `attempts++` in a separate tx; `attempts >= 5` →
  `status='failed'` + `events_dlq` row + `XADD stream:events:dlq` + XACK.
- Out-of-order: aggregation is commutative counters; `last_event_*` uses
  `GREATEST` on `occurred_at`; timeline sorts at read time. Scoring OOO math
  is specified in §7.
- Per-customer serialization via `SELECT ... FOR UPDATE` on the
  `engagement_profiles` row inside the same tx.

### Processor interface (seam between A1 and A2)

```go
// defined by A1 in internal/events (consumer-defined)
type Processor interface {
    ProcessTx(ctx context.Context, tx pgx.Tx, evt StoredEvent) error
}
// A2 ships customers.Applier satisfying this; integration wires it in
// cmd/worker. A1 ships a NoopProcessor stub so the worker compiles/runs alone.
```

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
 "meta":{"candidates_considered":42103,"filtered_out":7897,"took_ms":14,
  "candidate_pool_size":10000,"candidates_truncated":true,
  "shortfall_reason":"frequency_cap_exhausted"}}
```

`meta.candidate_pool_size` = the SQL LIMIT applied (`min(10·size, 200000)`);
`candidates_truncated` = true when the pool filled (top-K is approximate —
re-weighting happens in Go); `shortfall_reason` is `omitempty`, set only
when the frequency-cap reserve under-fills the requested size.
`min_score` is normalized [0,1) and is inverted to raw stored-score units
(`raw = k·s/(1−s)`, k=10) before the indexed filter.

Algorithm contract: SQL pre-filter on indexed columns bounded to
10·size candidates → **min-heap top-K** (O(pool log K), not O(N log N)
sort) → frequency-cap check via `sends` → reason generation. Endpoint is
rate-limited per-IP (10/min, burst 20). Must document complexity in code.

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

### Out-of-order-safe score math (REQUIRED semantics)

Model: `score(now) = Σ w_i · exp(-λ·(now − t_i))`. Persist a **raw** decayed
score plus `score_updated_at` anchor on `engagement_profiles`. For a new event
at time `t` with weight `w`:

- `t >= anchor`: `raw' = raw · exp(-λ·(t − anchor)) + w`; `anchor = t`
- `t < anchor` (out-of-order): `raw' = raw + w · exp(-λ·(anchor − t))`;
  anchor unchanged (older event contributes less — correct decay)

Half-life ≈ 14 days ⇒ `λ = ln2 / 14d`. **Never decay a normalized value:**
normalization `score/(score+k)` (or tanh) is applied ONLY at read/display
time. Keep the raw score in the DB. Clamp negative totals at 0.

## 8. AI Workload Isolation (A5)

- Separate `http.Client` w/ per-provider timeout (`cfg.LLMTimeoutMs`).
- Concurrency semaphore (e.g. 8) capping in-flight LLM calls; excess → 429.
- Per-provider circuit breaker: open after 3 consecutive failures, half-open
  after 30s.
- Response cache keyed `campaign_id + objective + endpoint + prompt_version`,
  TTL 60s, Redis `SET ... EX` (survives multi-instance). Checked BEFORE the
  metrics query — a hit skips both the DB and the provider. Facts in a
  cached body are up to 60s stale.
- Stricter rate limit on `/campaigns/*/analyze|recommend` than on `/events`.

## 9. Deployment Topology

- **Production design:** separate `api` and `worker` deployments (compose
  already does this).
- **Demo/free-tier mode:** `RUN_EMBEDDED_WORKER=true` starts the worker loop
  as a goroutine inside the api binary (single Cloud Run/Render service).
  Worker package must expose `Run(ctx, pool, rdb, cfg, processor)` callable
  from either binary. Document this trade-off in DEPLOYMENT.md.

## 10. Synthetic Data Corrections (A6)

- `events.event_id` is UNIQUE ⇒ the events table holds **unique ids only**.
  Duplicates are demonstrated as *repeated ingestion requests*: the seeder
  also writes `scripts/dupe_batch.json` (~2% of generated payloads repeated)
  that can be POSTed to `/events` to show `duplicates` counting.
- Seed `events_dlq` rows directly (invalid payloads) so `/system/dlq` shows
  data; at least one automated test must drive a real event to the DLQ.
- Seeded `engagement_profiles` MUST use the §7 formula (raw decayed score),
  consistent with generated events.

## 11. Observability (all agents)

- Every API router is mounted under middleware `core.RequestID()` +
  `core.AccessLog()` (wired once in cmd/api at integration).
- Log `request_id` on errors. Bump `core.Metrics.EventsIngested/Duplicates`
  in the ingest path.
- Design doc must name concrete signals: queue depth, oldest pending age,
  retry/DLQ rate, row-lock wait, LLM latency/invalid-rate/fallback-rate,
  audience scan counts.
