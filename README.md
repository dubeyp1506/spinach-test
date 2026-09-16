# MarTech Intelligence & Campaign Decision Engine

Backend-first MarTech platform in Go: asynchronous, idempotent event
ingestion → per-customer engagement intelligence → audience recommendation
→ AI campaign analysis — built on Postgres 16 + Redis 7 Streams with a
transactional outbox, consumer-group workers, and a bulkheaded LLM layer.

> Design rationale, failure-mode analysis, and trade-offs: see
> **[docs/SYSTEM_DESIGN.md](docs/SYSTEM_DESIGN.md)** (start here) ·
> [docs/AI_DESIGN.md](docs/AI_DESIGN.md) ·
> [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
> [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) ·
> [docs/CONTRACTS.md](docs/CONTRACTS.md) ·
> [docs/openapi.yaml](docs/openapi.yaml)

## Stack

Go 1.26 · Gin · pgx v5 · go-redis v9 · golang-migrate · Postgres 16 ·
Redis 7 · Groq/Gemini (free-tier LLMs) + deterministic fallback · Docker.

## Quickstart (local)

Prereqs: Go 1.26, Docker (compose plugin), `golang-migrate` CLI optional
(api/worker auto-run migrations on boot).

```bash
cp .env.example .env            # fill GROQ_API_KEY / GEMINI_API_KEY (optional — fallback works without)
make up                         # postgres + redis only
make migrate                    # or let the binaries run them on start
make seed                       # 50k customers, 100k+ events, 20 campaigns,
                                # duplicates, OOO events, seeded DLQ rows
make run-api                    # :8080
make run-worker                 # separate terminal; scale: run more of these
```

Or all-docker: `docker compose up --build` (api + 2 worker replicas).

Verify:

```bash
curl localhost:8080/api/v1/system/health
# → {"status":"ok","postgres":"ok","redis":"ok","queue_depth":…,"dlq_size":…}
```

## Demo flow — the 5 reviewer steps

Each step exercises a different correctness property.

**1. Ingest a batch — async, validated, idempotent.**

```bash
curl -X POST localhost:8080/api/v1/events -H 'content-type: application/json' -d '{
  "events": [
    {"event_id":"evt_demo_001","customer_id":"cust_00042","campaign_id":"camp_007",
     "channel":"email","type":"opened","occurred_at":"2026-09-16T10:30:00Z"},
    {"event_id":"evt_demo_001","customer_id":"cust_00042","campaign_id":"camp_007",
     "channel":"email","type":"opened","occurred_at":"2026-09-16T10:30:00Z"},
    {"event_id":"evt_demo_bad","customer_id":"cust_00042","channel":"fax",
     "type":"opened","occurred_at":"2026-09-16T10:30:00Z"}
  ]}'
# → 202 {"accepted":1,"duplicates":1,"rejected":[{"index":2,"reason":"…"}]}
```

Then replay the seeded duplicate batch (~2% repeated `event_id`s) to show
dedup counting at scale:

```bash
curl -X POST localhost:8080/api/v1/events -H 'content-type: application/json' \
  --data @scripts/dupe_batch.json
```

Postgres `UNIQUE(event_id)` is the only dedup authority — Redis hints are
post-commit and advisory only (SYSTEM_DESIGN.md §7.1).

**2. Watch async processing + reliability.**

```bash
curl localhost:8080/api/v1/system/health        # queue_depth drains to ~0
curl 'localhost:8080/api/v1/system/dlq?limit=10' # seeded failures visible
curl -X POST localhost:8080/api/v1/system/dlq/1/replay   # replay a DLQ entry
```

Kill a worker mid-run if you like — peers `XAUTOCLAIM` its pending
messages and the atomic process-tx makes re-execution a no-op
(SYSTEM_DESIGN.md §5).

**3. Customer intelligence — decayed score, OOO-safe.**

```bash
curl localhost:8080/api/v1/customers/cust_00042
curl 'localhost:8080/api/v1/customers/cust_00042/timeline?limit=20&channel=email'
```

Score is a raw decayed sum (half-life 14 d) normalized only at read time;
out-of-order `occurred_at` is handled by anchored decay (SYSTEM_DESIGN.md
§6). The seeder ships OOO events so this is exercised, not just claimed.

**4. Analytics + audience recommendation.**

```bash
curl localhost:8080/api/v1/campaigns                          # list + summary
curl localhost:8080/api/v1/campaigns/camp_007/analytics       # metrics + anomaly flags

curl -X POST localhost:8080/api/v1/audience/recommend \
  -H 'content-type: application/json' -d '{
    "objective":"conversion","channel":"email","size":50,
    "conditions":{"min_score":0.2,"last_active_days":30,
                  "channels":["email"],"respect_frequency_cap":true}}'
# → ranked candidates with per-candidate reasons +
#   meta{candidates_considered, filtered_out, took_ms}
```

Ranking is indexed-prefilter → min-heap top-K (O(N log K)) → batched
frequency-cap check on `sends` (SYSTEM_DESIGN.md §7.7–7.8).

**5. AI analysis — with the bulkhead visible.**

```bash
curl -X POST localhost:8080/api/v1/campaigns/camp_007/analyze
curl -X POST localhost:8080/api/v1/campaigns/camp_007/recommend \
  -H 'content-type: application/json' -d '{"objective":"conversion"}'
```

Response: `{facts, analysis|recommendations, provider, fallback_used}`.
`facts` are computed in Go; the LLM only narrates them (number-containment
validation makes invented stats un-serveable — AI_DESIGN.md §4). With no
API keys, or with Groq down, you get `provider:"rule-fallback"`,
`fallback_used:true` — a 200, not an error.

## API surface (all under `/api/v1`)

| Method | Path | Purpose |
|---|---|---|
| POST | `/events` | Ingest ≤500 events; `202 {accepted, duplicates, rejected[]}` |
| GET | `/customers/{id}` | Profile + live engagement score |
| GET | `/customers/{id}/timeline` | Cursor-paginated events, newest first, `?channel=&type=` |
| GET | `/campaigns` | List + summary metrics, `?status=&channel=`, cursor pages |
| GET | `/campaigns/{id}/analytics` | Counts, rates, per-channel, anomaly flags |
| POST | `/audience/recommend` | Top-K ranked, dedup'd, frequency-capped audience |
| POST | `/campaigns/{id}/analyze` | AI narrative over computed facts |
| POST | `/campaigns/{id}/recommend` | AI recommendations for an objective |
| GET | `/system/health` | `{status, postgres, redis, queue_depth, dlq_size}` |
| GET | `/system/dlq` | Failed events, cursor-paginated |
| POST | `/system/dlq/{id}/replay` | Re-enqueue a dead-lettered event |

Conventions: `external_id` in all paths; `ErrorBody` envelope on errors;
cursor pagination (`?limit≤200&cursor=`); `X-Request-ID` echoed for
correlation. Full schemas: `docs/openapi.yaml`.

## Layout

```
cmd/api        HTTP server (Gin); RUN_EMBEDDED_WORKER=true runs the worker in-process
cmd/worker     consumer-group worker + outbox reconciler (standalone)
cmd/seed       synthetic data generator (50k customers, 100k+ events, 20 campaigns)
internal/
  core         RequestID/AccessLog middleware, ErrorBody, cursor pagination, metrics
  config       env-driven config
  store        pgx pool (MaxConns=20) + migrations runner
  queue        Producer/Consumer interfaces; stream names (contract-fixed)
  events       ingest + worker processing (A1)
  customers    profiles + scoring engine (A2)
  audience     top-K recommender (A3)
  campaigns    metrics service (A4)
  ai           LLM provider chain + bulkhead (A5)
migrations     SQL schema (000001 init, 000002 event_outbox)
docs           SYSTEM_DESIGN · AI_DESIGN · ARCHITECTURE · DEPLOYMENT · CONTRACTS · openapi.yaml
scripts        dupe_batch.json (seeded duplicates demo)
tests          integration / race / failure / API tests
web            minimal frontend (A8)
```

## Test & lint

```bash
make test          # go test -race ./...
make lint          # gofmt + go vet
make build         # go build ./...
```

CI (`.github/workflows/ci.yml`): gofmt, vet, build, `go test -race -short`.

## Configuration

All via env (see `.env.example`): `DATABASE_URL`, `REDIS_URL`,
`WORKER_BATCH_SIZE=100`, `WORKER_MAX_ATTEMPTS=5`,
`WORKER_CLAIM_IDLE_MS=30000`, `GROQ_API_KEY`/`GEMINI_API_KEY`,
`LLM_TIMEOUT_MS=15000`, `RATE_LIMIT_RPS=500`/`BURST=1000`,
`RUN_EMBEDDED_WORKER=false` (single-service demo mode — DEPLOYMENT.md §3).

## Deploy

Free-tier single-service recipe (Cloud Run or Render + Neon + Upstash):
**docs/DEPLOYMENT.md**.
