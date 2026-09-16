# Architecture

The same diagram lives in SYSTEM_DESIGN.md §2; this file keeps the Mermaid
source and an ASCII rendering for environments without Mermaid
(GitHub renders the fenced `mermaid` block natively).

## Mermaid source

```mermaid
flowchart TB
    subgraph SRC["Event sources"]
        C["ESP / SMS / push / web SDKs<br/>(clients own event_id idempotency keys)"]
    end

    subgraph API["api service — Gin (stateless, N replicas)"]
        MW["RequestID + AccessLog + body limit"]
        RL["Rate limiter<br/>500 rps / burst 1000<br/>(stricter on AI routes)"]
        VAL["Per-item validation<br/>schema, enums, occurred_at ≤ now"]
        ING["Ingest handler"]
        READ["Read handlers<br/>customers · campaigns · audience"]
        AIH["AI handlers<br/>analyze · recommend"]
    end

    subgraph PG[("PostgreSQL 16 — system of record")]
        EV["events<br/>UNIQUE(event_id) · status · attempts"]
        OB["event_outbox<br/>partial idx on published_at IS NULL"]
        PR["engagement_profiles<br/>counters · raw decayed score"]
        SND["sends<br/>frequency-cap source"]
        DQT["events_dlq"]
    end

    subgraph RD[("Redis 7 — transport + caches")]
        ST["stream:events"]
        SDQ["stream:events:dlq"]
        CACHE["AI response cache (5 min TTL)<br/>post-commit dedup hints"]
    end

    subgraph WK["worker × N — consumer group 'event-workers'"]
        RCN["Outbox reconciler (~5 s)<br/>SELECT … FOR UPDATE SKIP LOCKED → XADD"]
        XR["XREADGROUP (batch 100)<br/>+ XAUTOCLAIM idle > 30 s"]
        PTX["Process tx:<br/>SELECT event FOR UPDATE → status check →<br/>aggregate profile + sends → status='processed'"]
    end

    subgraph AIB["AI layer — bulkheaded"]
        SEM["Semaphore (8 in-flight)<br/>excess → 429"]
        BRK["Per-provider breaker<br/>3 fails → open, half-open 30 s"]
        CHN["groq → gemini → rule fallback"]
    end
    LLM[/"Groq / Gemini HTTP APIs"/]

    C -->|"POST /api/v1/events"| MW --> RL --> VAL --> ING
    ING -->|"ONE tx: INSERT events (ON CONFLICT DO NOTHING)<br/>+ INSERT event_outbox"| PG
    ING -->|"best-effort XADD (failure ignored)"| ST
    RCN -->|"poll unpublished"| OB
    RCN -->|"XADD (at-least-once)"| ST
    ST --> XR --> PTX
    PTX -->|"aggregate + status, ONE tx"| PG
    PTX -->|"attempts ≥ 5"| DQT
    PTX -->|"XADD"| SDQ
    PTX -->|"XACK after commit"| ST
    READ --> PG
    AIH --> SEM --> BRK --> CHN --> LLM
    AIH --> CACHE
    READ -->|"queue depth / dlq size"| RD
```

## ASCII fallback

```
                          ┌────────────────────────────────────────────────────────┐
  Event sources           │                      api (Gin, stateless × N)           │
  ESP/SMS/push/web ──────►│  RequestID · AccessLog · body-limit                      │
  (event_id = client      │  rate limit 500 rps/burst 1000 (stricter on AI routes)   │
   idempotency key)       │  per-item validation                                     │
                          │     │                                                  │
                          │     ▼                                                  │
                          │  ingest ──────┐     reads: customers / campaigns /     │
                          │               │     audience / system                  │
                          └───────────────┼────────────────────────────────────────┘
                                          │
              ONE Postgres tx             │            best-effort XADD
              ┌───────────────────────────┼───────────────────────────┐
              ▼                           ▼                           ▼
   ┌──────────────────────────────┐                    ┌───────────────────────────┐
   │      PostgreSQL 16           │                    │         Redis 7           │
   │  events  (UNIQUE event_id,   │                    │  stream:events            │
   │          status, attempts)   │                    │  stream:events:dlq        │
   │  event_outbox (published_at) │◄── poll ~5s ──┐    │  AI cache (5 min TTL)     │
   │  engagement_profiles         │              │    │  post-commit dedup hints  │
   │  sends · events_dlq          │              │    └───────────▲───────────────┘
   └──────────────▲───────────────┘              │                │
                  │ ONE tx per message:          │                │ XADD
                  │ SELECT event FOR UPDATE      │                │
                  │ → status check (idempotent)  │                │
                  │ → profile+sends aggregate    │                │
                  │ → status='processed'         │                │
   ┌──────────────┴───────────────────────────┐  │                │
   │        worker × N  (group event-workers) │  │                │
   │  reconciler ─────────────────────────────┘──┘ (at-least-once)  │
   │  XREADGROUP (100) + XAUTOCLAIM (idle>30s)                      │
   │  attempts≥5 → events_dlq + XADD stream:events:dlq + XACK       │
   └──────────────────────────────────────────────────────────────┘

   AI path (isolated bulkhead):
   /campaigns/{id}/analyze|recommend
        → facts computed in Go (AnalyticsService.Metrics; no PII)
        → cache (campaign_id+metrics_version+prompt_version, 5 min)
        → semaphore 8 (excess → 429)
        → per-provider breaker (3 fails → open; half-open 30 s)
        → groq → gemini → rule-fallback   (always 200; provider +
          fallback_used reported; output schema + number-containment
          validation before serving)
```

## Reading notes

- **Postgres is the system of record**; Redis is transport/cache. Losing
  Redis degrades latency, never correctness — the outbox reconciler
  republishes anything committed-but-unpublished.
- **At-least-once delivery + idempotent consumer**: double-publish is
  expected; the `FOR UPDATE` status check inside the process tx makes
  re-delivery a no-op.
- **Two binaries, one codebase**: `cmd/api` and `cmd/worker` share
  internals; `RUN_EMBEDDED_WORKER=true` runs the worker loop as a
  goroutine in the api binary for single-service free-tier deploys
  (DEPLOYMENT.md §3).
- The **AI layer never touches the DB or event payloads** — it consumes
  computed `CampaignMetrics` only, so no PII reaches the prompt.
