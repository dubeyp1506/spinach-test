# Deployment

Two topologies, deliberately:

| | Production design | Demo / free-tier |
|---|---|---|
| Services | `api` × N + `worker` × M (compose already runs 2 worker replicas) | **one** service: `api` with `RUN_EMBEDDED_WORKER=true` |
| Why | independent scaling, independent failure domains | free tiers give you one web service; a scaled-to-zero worker processes nothing |
| Cost of the trade | — | worker shares the API process's fate and lifecycle (§3) |

Free-tier recipe that actually works end-to-end: **Cloud Run** (or Render)
+ **Neon** (Postgres) + **Upstash** (Redis) + **Groq/Gemini** keys.

---

## 1. Build

The `Dockerfile` is multi-stage: `golang:1.26-alpine` builds three static
binaries (`api`, `worker`, `seed`, `CGO_ENABLED=0`) into `alpine:3.20`.
`CMD ["/app/api"]`; the worker image is the same image with
`command: ["/app/worker"]` (see `docker-compose.yml`).

```bash
docker build -t martech .
docker run --rm -p 8080:8080 --env-file .env martech            # api
docker run --rm --env-file .env martech /app/worker             # worker (prod split)
docker run --rm --env-file .env martech /app/seed               # seed against remote DB
```

Migrations run automatically on `api`/`worker` boot
(`store.RunMigrations`) — no separate migrate job needed for a single-node
deploy. (On multi-replica boot, golang-migrate's advisory lock serializes
concurrent runners; first boot wins.)

## 2. Managed dependencies (free tier)

**Neon (Postgres).** Create a project, copy the pooled connection string
into `DATABASE_URL` (`postgres://…?sslmode=require`). Notes:

- Keep `sslmode=require` — Neon's endpoint is public.
- Free tier autosuspends compute after idle; first request after suspend
  pays a ~1 s wake. The pgx pool (`MaxConns=20`) reconnects transparently;
  expect one slow health-check after idle.
- Free tier storage (~0.5 GB) fits the seeded dataset (~50k customers,
  100k+ events ≈ tens of MB) comfortably.

**Upstash (Redis).** Create a regional database, copy the `rediss://` URL
into `REDIS_URL` (TLS — go-redis handles `rediss://` via `ParseURL`).

- **Eviction caveat — and why it doesn't break correctness.** Free-tier
  Redis is small and may evict under memory pressure. In this system Redis
  holds (a) stream messages, which are *pointers* to Postgres rows — if an
  unpublished one is evicted the outbox reconciler republishes within ~5 s,
  and if a *published-but-unconsumed* entry is evicted the pending sweeper
  re-enqueues any `events.status='pending'` row older than 30 s;
  `stream:events` is also `MAXLEN ~1M` bounded so it can't exhaust memory
  on its own; (b) AI cache entries — an eviction is just a cache miss; (c)
  rate-limit buckets — a reset just refills the bucket. The dedup authority
  is `events.event_id UNIQUE` in Postgres, so eviction can never convert a
  duplicate into an accepted event. Worst case is latency and a cache
  miss, never lost or duplicated data. That property is designed, not
  accidental (SYSTEM_DESIGN.md §5.1, §7.1).
- Prefer a single fixed `maxmemory-policy` like `allkeys-lru`; avoid
  `noeviction` on the free tier (writes start failing at the cap).

**LLM keys.** `GROQ_API_KEY` (primary, `llama-3.1-8b-instant`) and
`GEMINI_API_KEY` (backup, `gemini-1.5-flash`). Both are optional at boot —
with neither set, `analyze`/`recommend` still return 200 via
`rule-fallback` (`fallback_used: true`). Free-tier rate limits exist; the
5-minute response cache + breaker absorb them (AI_DESIGN.md §6).

## 3. Cloud Run — single service with embedded worker

```bash
gcloud run deploy martech \
  --image gcr.io/PROJECT/martech \
  --set-env-vars "ENV=production,RUN_EMBEDDED_WORKER=true,WORKER_BATCH_SIZE=100" \
  --set-secrets "DATABASE_URL=db-url:latest,REDIS_URL=redis-url:latest,GROQ_API_KEY=groq:latest,GEMINI_API_KEY=gemini:latest" \
  --port 8080 --min-instances 1 --max-instances 2
```

- `RUN_EMBEDDED_WORKER=true` starts the worker loop (consumer +
  reconciler) as a goroutine inside the api binary — one billable service.
- **`--min-instances 1` matters.** With scale-to-zero, the embedded worker
  also scales to zero: events ingest fine (they're durable in Postgres),
  but nothing processes them until the next request wakes the service —
  freshness silently degrades to "on next request." For a demo,
  `min-instances=1` keeps one warm container so the worker goroutine is
  always alive. If you accept scale-to-zero, document that processing is
  lazy.
- **The demo-vs-prod trade-off, explicitly.** Embedded mode couples the
  worker to the API lifecycle: an API deploy/restart kills in-flight
  processing (safe — atomic tx rolls back, message is reclaimed via
  `XAUTOCLAIM` by… nothing, if it's the only instance — so worst case a
  message waits `WORKER_CLAIM_IDLE_MS` and is reclaimed by the *next*
  instance), and you can't scale workers independently. In production you
  run separate `api` and `worker` deployments (compose shows the shape);
  the worker exposes `Run(ctx, pool, rdb, cfg, processor)` precisely so
  both binaries — and the embedded goroutine — share one implementation
  (contract §9).
- Cold starts: container is ~20 MB (static binaries + alpine), so cold
  start is dominated by Neon wake + pool init (~1–2 s).
- `PORT` is injected by Cloud Run; config already reads it.

## 4. Render — equivalent recipe

One **Web Service** from the same Dockerfile:

- Build: Docker; start command `/app/api` (the image default).
- Env: same vars as §3, `RUN_EMBEDDED_WORKER=true`.
- Render free web services also sleep on idle — same caveat as Cloud Run
  scale-to-zero: processing pauses while asleep, resumes on wake; events
  are never lost (outbox).
- For a split deployment on a paid tier: a **Background Worker** service
  running `/app/worker`, same env, `RUN_EMBEDDED_WORKER=false` on the web
  service.

## 5. Environment reference

| Var | Required | Default | Note |
|---|---|---|---|
| `DATABASE_URL` | yes | local postgres | Neon pooled URL, `sslmode=require` |
| `REDIS_URL` | yes | local redis | Upstash `rediss://…` |
| `PORT` | yes (platform sets) | 8080 | Cloud Run/Render inject it |
| `RUN_EMBEDDED_WORKER` | demo | `false` | `true` for single-service deploys |
| `WORKER_BATCH_SIZE` | no | 100 | XREADGROUP count |
| `WORKER_CONCURRENCY` | no | 8 | parallel process-tx goroutines per batch |
| `WORKER_MAX_ATTEMPTS` | no | 5 | → DLQ after this |
| `WORKER_CLAIM_IDLE_MS` | no | 30000 | XAUTOCLAIM threshold |
| `DB_MAX_CONNS` | no | 20 | pgx pool size (size vs Postgres max_connections) |
| `DB_STATEMENT_TIMEOUT_MS` | no | 10000 | per-query bound — slow scans can't pin conns |
| `GROQ_API_KEY` / `GEMINI_API_KEY` | no | — | absent → rule fallback still serves |
| `LLM_TIMEOUT_MS` | no | 15000 | per-provider HTTP timeout |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | no | 500 / 1000 | edge token bucket |

## 6. Post-deploy checks

```bash
curl https://SERVICE/api/v1/system/health     # postgres/redis ok, queue_depth, dlq_size
/app/seed                                       # one-off seed job vs Neon (or run cmd locally)
# then the README 5-step demo flow against the public URL
```

Watch `queue_depth` right after seeding — with the embedded worker it
should drain to ~0; if it doesn't, the worker goroutine isn't running
(`RUN_EMBEDDED_WORKER` unset, or instance scaled to zero).

## 7. Honest free-tier limits (summary)

- **Scale-to-zero / sleep** pauses event processing, not ingestion; fix
  with `min-instances=1` or accept lazy freshness.
- **Redis eviction** can drop stream messages/cache/hints — correctness
  survives by design (Postgres outbox + UNIQUE authority); latency and
  cache hit-rate don't.
- **Neon autosuspend** adds ~1 s to the first request after idle.
- **Single embedded worker** caps processing throughput at one process's
  worth (~batch 100/loop) — plenty for demo volume; production splits and
  scales workers independently.
- **LLM free quotas** are outside our control; breaker + cache + fallback
  keep the endpoint at 200 regardless.
