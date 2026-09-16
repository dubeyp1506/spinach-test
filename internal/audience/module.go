// Package audience implements POST /api/v1/audience/recommend (CONTRACTS §5):
// ranked, deduplicated, frequency-capped top-K audience selection.
//
// Pipeline (complexity documented at each step):
//  1. SQL pre-filter, bounded by LIMIT       — O(pool) scan, idx_profiles_audience
//  2. Bounded min-heap top-K (heap.go)       — O(N log K) time, O(K) space
//  3. Frequency cap via ONE batched query    — O(K) ids, no N+1
//  4. Dedup by customer_id                   — O(K)
//  5. Reason generation (reasons.go)         — O(K)
//  6. Objective weighting (weights.go)       — O(1) per candidate
package audience

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
)

// Frequency-cap fallback when no campaign exists for the requested
// channel/objective context. Matches the campaigns table defaults
// (frequency_cap 3, frequency_window_hours 168 = 7 days) so behavior is
// consistent whether or not a campaign row exists yet.
const (
	defaultFreqCap         = 3
	defaultFreqWindowHours = 168
)

// Candidate-pool bound for the SQL pre-filter. Re-weighting in Go (step 6)
// means exact top-K by weighted score requires a full scan; instead we pull
// the top candidateMultiplier·size rows by raw stored score as the candidate
// pool — a documented approximation, since the final ranking re-weights.
// candidatePoolCeiling caps worst-case memory/time per request at scale.
const (
	candidateMultiplier  = 10
	candidatePoolCeiling = 200_000
)

// scoreNormK must mirror normK in internal/customers/scoring.go — the k in
// the read-time normalization s/(s+k) applied to the raw stored score. It is
// duplicated here because customers.normK is unexported.
const scoreNormK = 10.0

// candidatePoolLimit is the LIMIT applied to the pre-filter: the top
// 10·size rows by raw engagement_score, capped at candidatePoolCeiling.
func candidatePoolLimit(size int) int {
	if limit := size * candidateMultiplier; limit < candidatePoolCeiling {
		return limit
	}
	return candidatePoolCeiling
}

// rawMinScore converts the contract's normalized min_score ([0,1) semantics)
// into the raw units stored in engagement_profiles.engagement_score. The
// read-time normalization is s/(s+k) (customers.normalize), so the inverse
// is raw = k·min/(1−min). Two documented approximations:
//   - normalize() clamps raw<=0 to 0, so min_score<=0 must apply no filter —
//     a negative-raw customer still presents score 0 ≥ min_score.
//   - the stored raw value is anchored at score_updated_at and keeps
//     decaying until the next write; we compare it un-decayed, so the bound
//     is over-inclusive (decay only shrinks raw scores). Re-decaying in SQL
//     would cost a per-row exp() over the scan — not worth it.
func rawMinScore(minScore float64) float64 {
	switch {
	case minScore <= 0:
		return -math.MaxFloat64 // no filter: every finite score qualifies
	case minScore >= 1:
		return math.MaxFloat64 // normalized scores are < 1: nothing matches
	default:
		return scoreNormK * minScore / (1 - minScore)
	}
}

// Module owns the audience recommendation endpoint.
type Module struct {
	pool *pgxpool.Pool
	rdb  *redis.Client // per-IP rate limiter state; nil → limiter fails open
	cfg  *config.Config
}

func New(pool *pgxpool.Pool, rdb *redis.Client, cfg *config.Config) *Module {
	// rdb backs the per-IP rate limiter; nil → fails open (never block the
	// endpoint on Redis config).
	return &Module{pool: pool, rdb: rdb, cfg: cfg}
}

// RegisterRoutes mounts POST /audience/recommend — CONTRACTS §5.
func (m *Module) RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/audience/recommend", m.rateLimit(), m.recommend)
}

// Step 1 — SQL pre-filter. All predicates sit on indexed/not-null columns;
// ORDER BY engagement_score DESC rides idx_profiles_audience (a covering
// index, migration 000004). Excluded campaigns are resolved from external_id
// inside the same query (NOT EXISTS over sends joined to campaigns), so an
// exclusion applies even to campaigns with zero sends and costs no extra
// round-trip.
//
// LIMIT $5 bounds the stream: an unbounded ORDER BY would pull every
// matching profile (~10M rows at scale) through one shared pool conn plus a
// per-row NOT EXISTS probe. The bound is the top 10·size rows by raw stored
// score — an approximation because Go re-weighting (step 6) can reorder
// within the pool; meta.candidates_truncated reports when the limit hit.
const prefilterSQL = `
SELECT c.id, c.external_id, p.engagement_score,
       COALESCE(p.preferred_channel, ''),
       p.conversions, p.positive_events, p.negative_events, p.total_events,
       p.last_event_at, p.activity_trend
FROM customers c
JOIN engagement_profiles p ON p.customer_id = c.id
WHERE c.is_active
  AND p.engagement_score >= $1
  AND ($2::int <= 0 OR p.last_event_at >= now() - make_interval(days => $2))
  AND ($3::text[] IS NULL OR p.preferred_channel = ANY($3))
  AND NOT EXISTS (
        SELECT 1
        FROM sends s
        JOIN campaigns xc ON xc.id = s.campaign_id
        WHERE s.customer_id = c.id
          AND xc.external_id = ANY($4::text[])
      )
ORDER BY p.engagement_score DESC
LIMIT $5`

// Cap context: most relevant campaign for the channel/objective pair,
// preferring active ones then newest. No row → schema defaults (3 / 168h).
const capContextSQL = `
SELECT frequency_cap, frequency_window_hours
FROM campaigns
WHERE channel = $1 AND objective = $2
ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END, created_at DESC
LIMIT 1`

// Step 3 — ONE batched query for every heap survivor (no N+1).
const sendCountsSQL = `
SELECT customer_id, COUNT(*)::int
FROM sends
WHERE customer_id = ANY($1::bigint[])
  AND sent_at > now() - make_interval(hours => $2)
GROUP BY customer_id`

// recommend handles POST /api/v1/audience/recommend — CONTRACTS §5.
func (m *Module) recommend(c *gin.Context) {
	start := time.Now()

	var req RecommendRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		core.BadRequest(c, "malformed request body", err.Error())
		return
	}
	if err := req.validate(); err != nil {
		core.BadRequest(c, err.Error(), nil)
		return
	}

	ctx := c.Request.Context()
	now := time.Now()

	ranked, considered, truncated, err := m.selectTopK(ctx, &req, now)
	if err != nil {
		slog.Error("audience prefilter", "request_id", c.GetString("request_id"), "err", err)
		core.Internal(c, err)
		return
	}

	// Step 3 — frequency cap. Survivors keep heap (rank) order: capped members
	// are dropped and the bounded reserve backfills in order.
	var counts map[int64]int
	capN := 0
	if req.respectCap() {
		var windowHours int
		capN, windowHours = m.capContext(ctx, req.Channel, req.Objective)
		counts, err = m.sendCounts(ctx, idsOf(ranked), windowHours)
		if err != nil {
			slog.Error("audience freqcap", "request_id", c.GetString("request_id"), "err", err)
			core.Internal(c, err)
			return
		}
	}

	out := make([]Candidate, 0, req.Size)
	for _, cd := range dedupByCustomer(ranked) { // Step 4
		if counts[cd.customerID] >= capN && req.respectCap() {
			continue // at/over cap
		}
		rank := len(out) + 1
		out = append(out, Candidate{
			CustomerID: cd.externalID,
			Score:      displayScore(cd.weighted),
			Rank:       rank,
			Reasons:    buildReasons(cd, &req, rank, considered, now), // Step 5
		})
		if len(out) == req.Size {
			break
		}
	}

	meta := Meta{
		CandidatesConsidered: considered,
		FilteredOut:          considered - len(out),
		TookMs:               time.Since(start).Milliseconds(),
		CandidatePoolSize:    candidatePoolLimit(req.Size),
		CandidatesTruncated:  truncated,
	}
	// The 2·size heap reserve can still under-fill when more than `size`
	// survivors are frequency-capped — surface that instead of silently
	// returning a short page.
	if req.respectCap() && len(out) < req.Size {
		meta.ShortfallReason = "frequency_cap_exhausted"
	}
	c.JSON(200, RecommendResponse{Candidates: out, Meta: meta})
}

// selectTopK streams the pre-filtered rows through the bounded min-heap.
// When the frequency cap is respected the heap keeps 2·size survivors — a
// bounded reserve (still O(K) space) so capped slots backfill without a
// second DB scan. Worst case: more than `size` survivors are capped and the
// response legitimately returns fewer than `size` candidates.
func (m *Module) selectTopK(ctx context.Context, req *RecommendRequest, now time.Time) ([]candidate, int, bool, error) {
	heapCap := req.Size
	if req.respectCap() {
		heapCap *= 2
	}
	th := newTopKHeap(heapCap)

	// Bound the stream (see prefilterSQL). min_score arrives in normalized
	// [0,1) units and is converted to raw stored-score units in Go so the
	// comparison stays a plain indexed column filter.
	poolLimit := candidatePoolLimit(req.Size)
	rows, err := m.pool.Query(ctx, prefilterSQL,
		rawMinScore(req.Conditions.MinScore),
		req.Conditions.LastActiveDays,
		nilIfEmpty(req.Conditions.Channels),
		nilIfEmpty(req.Conditions.ExcludeCampaignIDs),
		poolLimit,
	)
	if err != nil {
		return nil, 0, false, fmt.Errorf("prefilter query: %w", err)
	}
	defer rows.Close()

	considered := 0
	for rows.Next() {
		var cd candidate
		if err := rows.Scan(
			&cd.customerID, &cd.externalID, &cd.rawScore,
			&cd.preferredChannel,
			&cd.conversions, &cd.positiveEvents, &cd.negativeEvents,
			&cd.totalEvents, &cd.lastEventAt, &cd.trend,
		); err != nil {
			return nil, 0, false, fmt.Errorf("scan candidate: %w", err)
		}
		considered++
		cd.weighted = weightedScore(req.Objective, cd, now) // Step 6
		th.Add(cd)                                          // Step 2
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("prefilter rows: %w", err)
	}
	// considered == poolLimit ⇒ the LIMIT cut the stream: the true top-K by
	// weighted score may sit in rows we never read.
	return th.Sorted(), considered, considered == poolLimit, nil
}

// capContext resolves (frequency_cap, window_hours) from the campaigns table
// for the target channel/objective, preferring active campaigns; falls back
// to the schema defaults when no campaign matches.
func (m *Module) capContext(ctx context.Context, channel, objective string) (fcap, windowHours int) {
	err := m.pool.QueryRow(ctx, capContextSQL, channel, objective).Scan(&fcap, &windowHours)
	if err != nil {
		return defaultFreqCap, defaultFreqWindowHours
	}
	return fcap, windowHours
}

// sendCounts runs the single batched frequency query over the heap survivors.
func (m *Module) sendCounts(ctx context.Context, ids []int64, windowHours int) (map[int64]int, error) {
	counts := make(map[int64]int, len(ids))
	if len(ids) == 0 {
		return counts, nil
	}
	rows, err := m.pool.Query(ctx, sendCountsSQL, ids, windowHours)
	if err != nil {
		return nil, fmt.Errorf("freqcap query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("scan freqcap: %w", err)
		}
		counts[id] = n
	}
	return counts, rows.Err()
}

// dedupByCustomer removes repeated customer_ids, keeping the first (best
// ranked) occurrence. O(K) — CONTRACTS §5 step 4. The pre-filter already
// yields unique customers (PK join), so this is a defensive guarantee that
// the pipeline, not the query, owns the invariant.
func dedupByCustomer(in []candidate) []candidate {
	seen := make(map[int64]struct{}, len(in))
	out := in[:0]
	for _, cd := range in {
		if _, dup := seen[cd.customerID]; dup {
			continue
		}
		seen[cd.customerID] = struct{}{}
		out = append(out, cd)
	}
	return out
}

func idsOf(cands []candidate) []int64 {
	ids := make([]int64, len(cands))
	for i, c := range cands {
		ids[i] = c.customerID
	}
	return ids
}

// nilIfEmpty maps an empty slice to NULL so the SQL `IS NULL OR = ANY($n)`
// guards treat "not provided" as "no filter".
func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}
