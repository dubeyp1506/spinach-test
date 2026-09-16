# AI Design — Campaign Analysis & Recommendation Layer

Contract reference: `docs/CONTRACTS.md` §3 (`LLMProvider`), §8 (bulkhead),
§4 (`analyze`/`recommend` response shapes).

The AI layer turns computed campaign metrics into natural-language analysis
and recommendations. Its defining constraint: **the LLM narrates facts; it
never produces them.** Every number the user sees was computed in Go before
the prompt was built. This single decision drives most of the design below.

---

## 1. Interface and provider chain

```go
// internal/ai — the only surface the rest of the system sees
type LLMProvider interface {
    Name() string
    Complete(ctx context.Context, prompt string) (string, error)
}
```

**Chain: `groq → gemini → rule-fallback`** (order set by `LLM_PRIMARY`,
default `groq`).

| Stage | What it is | When it serves |
|---|---|---|
| `groq` | `llama-3.1-8b-instant` via Groq API | default — fast free tier, low latency |
| `gemini` | `gemini-1.5-flash` via Gemini API | groq errors, breaker open, or invalid output after retry |
| `rule-fallback` | deterministic Go code — no network | both providers unavailable/failing; **always succeeds** |

Each stage is tried only if the previous one **failed or produced invalid
output** — the chain is a reliability mechanism, not an ensemble. The
response carries `provider` (which stage answered) and `fallback_used`
(bool) so callers and reviewers can see degradation explicitly; the HTTP
contract is that these endpoints **always return 200** with structurally
valid output.

Why this order: Groq's free tier is the fastest/cheapest path for
Llama-class models; Gemini's free tier is an independent failure domain
(different vendor, different quota). The rule fallback is not a "lesser
model" — it's a template engine over the same facts, so its output is
always well-formed by construction.

---

## 2. Why facts are computed in Go

**Problem.** If the LLM receives raw events (or nothing) and is asked to
"summarize performance," it will invent statistics: plausible open rates,
hallucinated totals, trends unsupported by data. For a take-home and for
production this is the cardinal sin — the API would be asserting numbers
that aren't in the database.

**Approach.** `AnalyticsService.Metrics(campaignID)` (contract §3) computes
a `CampaignMetrics` struct entirely in Go: sends, delivered, opens, clicks,
conversions, bounces, unsubscribes, rates, per-channel breakdown, first/
last event timestamps. The prompt embeds **this struct serialized**, and
the response echoes it back as `facts`. The LLM's job is interpretation
only: what do these numbers mean, where is performance weak, what to try
next.

**Consequences.**
- Output becomes *verifiable*: every number in the analysis can be checked
  against `facts` (see §4, number containment).
- The rule fallback is a first-class citizen — it renders from the same
  `facts`, so degraded mode loses prose quality but never correctness.
- Prompt injection surface shrinks: no raw user/event text enters the
  prompt, only numbers.

**Trade-off.** The model can't discover patterns we didn't compute (e.g.
"time-of-day clustering"). Accepted deliberately — computed-in-Go is the
only way to keep numeric claims auditable; richer facts are added by
extending `CampaignMetrics`, not by loosening the leash.

---

## 3. Prompt strategy and context construction

**Structure** (same skeleton for `analyze` and `recommend`):

```
[system role]    You are a marketing analytics engine. You receive a JSON
                 object of precomputed facts. Never invent numbers; only
                 restate numbers present in facts (rounding to 1 decimal is
                 allowed). Respond with JSON only, matching the given schema.
[task]           analyze → {summary, strengths[], weaknesses[], anomalies[],
                           confidence}
                 recommend → objective + {recommendations[{area, suggestion,
                             reasoning, confidence}]}
[facts]          {"campaign_id":"camp_007","objective":"conversion",
                  "channel":"email","sends":12340,"delivered":12101,
                  "opens":4521,"clicks":612,"conversions":203,
                  "open_rate":0.3736,"click_rate":0.0506,
                  "conversion_rate":0.0168,"by_channel":{...},
                  "anomaly_flags":[...]}
[schema]         exact JSON shape expected (mirrors openapi.yaml)
```

Design points:

- **No PII, ever.** The prompt contains aggregate counts/rates only — no
  emails, names, customer ids, or raw event payloads. This is structural
  (the layer receives `CampaignMetrics`, not rows), so it can't be
  violated by a careless prompt edit.
- **Facts are the whole context.** No retrieval, no customer data, no
  cross-campaign history in v1 — keeps context small (~hundreds of
  tokens), latency low, and the audit surface tiny.
- **Output contract in the prompt.** We ask for JSON matching the
  openapi.yaml shape; the response is parsed and schema-validated (§4).
- **`confidence` is model-reported** but range-checked `[0,1]`; it's a
  signal for the UI, not a computed quantity — documented as such.
- **Prompt versioning**: `prompt_version` is part of the cache key, so
  prompt edits can't serve stale cached responses.

---

## 4. Output validation

LLM output passes three gates before it's served; any failure counts as a
provider failure and advances the chain (§5):

1. **Schema** — response parses as JSON and matches the endpoint's shape
   (`analyze`: summary/strengths/weaknesses/anomalies/confidence;
   `recommend`: `recommendations[]` with `area` ∈
   {audience, channel, timing, segmentation, strategy, risk}).
2. **Confidence range** — every `confidence` ∈ `[0,1]`; out-of-range is a
   malformed output, not a clamp (a model that emits `confidence: 87`
   didn't follow the contract).
3. **Number containment** — every numeric literal appearing in
   `summary`/`suggestion`/`reasoning` must match a value in `facts` or a
   directly derived quantity (rate × 100 for percent display, with a
   rounding tolerance of ±0.05 for 1-decimal restatements). A statistic
   not traceable to `facts` ⇒ reject. **This is the guardrail that makes
   the LLM unable to fabricate metrics.**

The rule fallback skips validation (its output is generated, not sampled)
but produces the same schema.

---

## 5. Malformed-output handling

```
provider returns output
  ├─ valid              → serve, record provider
  ├─ invalid, retry left → retry once with corrective suffix:
  │    "Your previous response was invalid: <reason>. Return only JSON
  │     matching the schema."
  └─ still invalid      → next provider in chain → … → rule fallback
```

One corrective retry per provider (not per chain) bounds worst-case work at
`providers × 2` calls; validation failures and transport failures advance
the chain identically — a provider that can't produce valid output is, for
our purposes, down.

**Instrumented**: `invalid-output rate` per provider is a first-class
metric (§7). A provider returning well-formed-but-fabricated output is
caught by gate 3 and shows up in the same counter.

---

## 6. The bulkhead (contract §8)

The AI layer is isolated so an LLM outage is a degraded feature, never a
site incident. Four independent mechanisms:

| Mechanism | Config | Effect |
|---|---|---|
| Dedicated `http.Client` | `LLM_TIMEOUT_MS` (15 s) per provider | a hung provider can't hold connections or goroutines; worst case 15 s/stage |
| Concurrency semaphore | 8 in-flight LLM calls | excess → `429` immediately; caps memory, goroutines, and provider spend; protects the rest of the API's DB/CPU budget |
| Circuit breaker per provider | open after **3 consecutive failures**, half-open probe after **30 s** | a down provider stops consuming the 15 s timeout budget; half-open limits probe traffic |
| Response cache | Redis `SET … EX 300`, key `campaign_id + metrics_version + prompt_version` | repeated analyze/recommend of unchanged metrics → ~0 LLM calls; shared across API replicas (not in-process) |

Plus **a stricter rate limit on `/campaigns/*/analyze|recommend`** than on
`/events` — AI calls are the most expensive request in the system
(upstream call + token spend), so they get the tightest budget.

Ordering rationale: cache check → semaphore → breaker → provider. The
semaphore sits inside the cache so cache hits never consume an LLM slot;
the breaker sits inside the semaphore so queued callers don't pile onto an
already-open circuit.

---

## 7. Observability

Signals emitted by the AI layer (contract §11 list; in-process counters in
demo, Prometheus/OTel in prod — see SYSTEM_DESIGN.md §9):

- `llm_latency_ms` per provider (histogram, p50/p99)
- `llm_invalid_rate` per provider (validation-gate failures / calls)
- `llm_fallback_rate` (share of requests served by rule fallback)
- breaker state transitions + time-in-open per provider
- cache hit rate; semaphore rejections (429s)
- `request_id` logged on every provider error — correlates a bad analysis
  back to the exact request

Health signal for reviewers: `provider` + `fallback_used` in every
response makes degradation observable without reading logs.

---

## 8. Rule-based fallback

The fallback renders `facts` through deterministic templates:

- `summary`: rates vs. fixed bands (e.g. open rate < 15% → "below typical
  email engagement bands")
- `strengths`/`weaknesses`: per-channel rate comparisons, anomaly flags
- `recommend`: template suggestions keyed by `objective` (e.g.
  low click rate → "test subject-line/CTA variants", channel weakness →
  "shift volume toward <best channel>")
- `confidence`: fixed modest value, honestly lower than a model's

It never calls the network and cannot fail — that is the property the
chain depends on. Its `provider` is reported as `rule-fallback`.

---

## 9. Adding a provider

1. Implement `LLMProvider` (`Name()`, `Complete(ctx, prompt) (string,
   error)`) in `internal/ai/` — the provider is a thin HTTP adapter; it
   sees only the prompt string and returns raw text.
2. Register it in the chain in `internal/ai` (ordered list); position
   decides failover order. Put it before `rule-fallback`.
3. Give it its own breaker instance and timeout (per-provider — a slow
   provider must not share fate with a fast one).
4. Add config keys to `.env.example` + `internal/config` (`<NAME>_API_KEY`,
   `<NAME>_MODEL`).
5. No changes to validation, caching, semaphore, or endpoints — all of
   that is above the provider interface. If the new provider needs a
   different output format, adapt it inside its `Complete`, not in the
   validator.

---

## 10. Limitations (stated)

- **Facts window is a single campaign snapshot.** No cross-campaign
  comparisons or customer-level segments in context — richer facts come
  from extending `CampaignMetrics`, deliberately.
- **Number containment is conservative.** Correctly derived restatements
  outside the tolerance (e.g. computing a ratio the facts don't expose)
  are rejected and advance the chain. Bias toward false rejects over
  fabricated stats — documented, tunable.
- **Confidence is self-reported**, not calibrated. Treated as a display
  hint, never a gate.
- **In-process breakers** don't coordinate across replicas — a fleet sees
  N independent breakers (per-replica fail counts). Fine at demo scale;
  the §11 LLM gateway in SYSTEM_DESIGN.md is the fleet-level answer.
- **Free-tier providers have rate quotas** outside our control; the cache
  + breaker + fallback exist precisely because quota exhaustion is a
  when, not an if.
- **The fallback is a template, not intelligence.** `fallback_used` exists
  so this is never disguised.
