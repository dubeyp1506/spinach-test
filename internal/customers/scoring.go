// Package customers owns the customer-intelligence workstream (CONTRACTS §3,
// §4, §7): engagement scoring, engagement_profiles materialization via the
// worker-facing Applier, and the /customers read API.
//
// Scoring design (CONTRACTS §7 — REQUIRED out-of-order-safe semantics):
//
//	score(now) = Σ w_i · exp(-λ·(now − t_i)),   λ = ln2 / 14d  (half-life 14d)
//
// Why this shape: recency-weighted engagement where each event contributes an
// independent additive term. Because the sum is commutative, an event that
// arrives out of order can be folded in later and yields the SAME total as if
// it had been processed in order (up to float rounding) — no reordering, no
// event log replay, no per-event storage needed on the profile.
//
// Persistence: engagement_profiles.engagement_score stores the RAW (unbounded)
// decayed sum and score_updated_at is the anchor — the point in time the raw
// value is decayed to. For a new event at time t with weight w:
//
//	t >= anchor: raw' = raw·exp(-λ·(t−anchor)) + w ; anchor = t
//	t <  anchor: raw' = raw + w·exp(-λ·(anchor−t)) ; anchor unchanged
//
// The anchor NEVER moves backward: moving it back would re-age every historical
// term. Negative-weight OOO events attenuate by the same factor, which is the
// mathematically consistent thing to do.
//
// Negative-score decision: §7 ends with "clamp negative totals at 0". We persist
// the raw sum UNCLAMPED. Rationale: a complaint-heavy customer should carry a
// real negative balance so that a single later open does not resurrect them
// from 0 to "engaged" (clamping on write destroys information and makes
// -100+1 indistinguishable from 0+1). The clamp happens at display time inside
// normalize(). Negative raw also decays toward 0 over time, matching the model
// exactly (each negative term's exp decay shrinks toward 0).
//
// Normalization — read/display only, NEVER persisted: s/(s+k) with k = 10.
// k=10 maps a customer with ~10 fresh weight-units (≈ 5 opens + 2 clicks +
// deliveries inside one half-life) to ≈0.5, saturating smoothly toward 1.0 for
// heavy users. Keeping the raw score in the DB lets us retune k without a
// backfill.
//
// Trend: derived at read — counts of positive events (opened|clicked|
// converted) in the last 7d vs the prior 7d: cur>prev → rising, cur<prev →
// declining, else stable. Chosen over storing window counters because the
// events table is already the source of truth (idx_events_customer_time) and
// stored windows would drift when events arrive out of order. Engine.Score
// persists the computed value onto activity_trend purely as a cache for
// SQL-side consumers.
//
// Preferred channel: argmax over channel_counts of (opened+clicked+converted)
// per channel; ties break alphabetically for determinism; NULL when the
// customer has no positive events. Persisted by the Applier because §5
// audience filters need a column to filter on; recomputed identically at read.
//
// Complexity:
//   - ScoreFromProfile / applyEvent / normalize / decayFactor: O(1).
//   - preferredChannel: O(C·T) bounded by 5 channels × 8 types — effectively
//     O(1), runs on the already-loaded channel_counts map.
//   - trendFor: one COUNT over idx_events_customer_time bounded to a 14d
//     window for a single customer.
//   - Engine.Score: O(1) locked profile row + the bounded trend scan.
package customers

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spinach/martech-engine/internal/core"
)

const (
	// halfLife is the decay half-life mandated by CONTRACTS §7 (≈14 days).
	halfLife = 14 * 24 * time.Hour
	// normK is the k in s/(s+k) read-time normalization — see package doc.
	normK = 10.0
	// trendWindow is the 7d window compared against the prior 7d for trend.
	trendWindow = 7 * 24 * time.Hour
)

// lambda is the exponential decay constant: ln2 / half-life, per second.
var lambda = math.Ln2 / halfLife.Seconds()

// eventWeights maps events.type to its §7 weight. sent carries no signal.
var eventWeights = map[string]float64{
	"sent":         0,
	"delivered":    0.2,
	"opened":       1,
	"clicked":      2,
	"converted":    5,
	"bounced":      -1,
	"unsubscribed": -3,
	"complained":   -5,
}

// isPositiveType matches the schema's definition of positive_events
// (opened + clicked + converted). delivered is weighted but not "positive".
func isPositiveType(t string) bool {
	switch t {
	case "opened", "clicked", "converted":
		return true
	}
	return false
}

// isNegativeType matches the schema's definition of negative_events
// (bounced + unsubscribed + complained).
func isNegativeType(t string) bool {
	switch t {
	case "bounced", "unsubscribed", "complained":
		return true
	}
	return false
}

// decayFactor returns exp(-λ·d). d <= 0 yields 1 so a future/anchor-equal
// timestamp can never inflate the score (clocks, replay edge cases).
func decayFactor(d time.Duration) float64 {
	if d <= 0 {
		return 1
	}
	return math.Exp(-lambda * d.Seconds())
}

// applyEvent folds one event of weight w occurring at t into a raw score
// anchored at `anchor`, per CONTRACTS §7. Returns the new raw and new anchor.
// Both branches are exact: folding a late event in attenuates it by precisely
// the decay it would have accrued, so apply order does not change the total.
func applyEvent(raw float64, anchor, t time.Time, w float64) (float64, time.Time) {
	if t.Before(anchor) {
		// Out-of-order: older event contributes less; anchor stays put.
		return raw + w*decayFactor(anchor.Sub(t)), anchor
	}
	return raw*decayFactor(t.Sub(anchor)) + w, t
}

// decayRaw decays a stored raw score from its anchor to `now`. Unclamped —
// negative raw decays toward 0, exactly per the Σ model.
func decayRaw(raw float64, anchor, now time.Time) float64 {
	return raw * decayFactor(now.Sub(anchor))
}

// normalize maps a raw score to [0,1): max(0,s)/(max(0,s)+k). Display-only;
// the raw value in the DB is never normalized (CONTRACTS §7).
func normalize(raw float64) float64 {
	if raw <= 0 {
		return 0
	}
	// float64 can round s/(s+k) up to exactly 1.0 for astronomically large s;
	// Nextafter keeps the documented [0,1) range.
	return min(raw/(raw+normK), math.Nextafter(1, 0))
}

// trendFromCounts buckets 7d-vs-prior-7d positive-event counts into the
// activity_trend CHECK constraint domain.
func trendFromCounts(current, prior int) string {
	switch {
	case current > prior:
		return "rising"
	case current < prior:
		return "declining"
	default:
		return "stable"
	}
}

// preferredChannel returns the channel with the most positive events
// (opened+clicked+converted). Ties break alphabetically for determinism.
// nil when there are no positive events — column stays NULL.
func preferredChannel(counts map[string]map[string]int) *string {
	best := ""
	bestN := 0
	for ch, types := range counts {
		n := types["opened"] + types["clicked"] + types["converted"]
		if n > bestN || (n == bestN && n > 0 && (best == "" || ch < best)) {
			best, bestN = ch, n
		}
	}
	if bestN == 0 {
		return nil
	}
	return &best
}

// Profile is the in-memory view of one engagement_profiles row — the input
// to the pure scoring path.
type Profile struct {
	CustomerID       int64
	TotalEvents      int
	ChannelCounts    map[string]map[string]int
	PositiveEvents   int
	NegativeEvents   int
	Conversions      int
	LastEventAt      *time.Time
	LastEventType    *string
	RawScore         float64   // engagement_score — raw decayed sum, unclamped
	ScoreUpdatedAt   time.Time // anchor the raw score is decayed to
	ActivityTrend    string
	PreferredChannel *string
}

// ScoreResult is the §3 return type consumed by A3 (audience).
type ScoreResult struct {
	Score    float64   `json:"score"`
	Trend    string    `json:"trend"` // rising|stable|declining
	Channel  string    `json:"preferred_channel"`
	Computed time.Time `json:"computed_at"`
}

// ScoringEngine is the §3 interface A2 implements and A3 consumes.
type ScoringEngine interface {
	Score(ctx context.Context, customerID int64) (*ScoreResult, error)
	ScoreFromProfile(p *Profile, now time.Time) float64
}

// Engine implements ScoringEngine against Postgres.
type Engine struct {
	pool *pgxpool.Pool
}

func NewEngine(pool *pgxpool.Pool) *Engine { return &Engine{pool: pool} }

var _ ScoringEngine = (*Engine)(nil)

// rowQuerier is satisfied by both *pgxpool.Pool and pgx.Tx, letting the read
// helpers run inside or outside a transaction.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ScoreFromProfile is the pure, I/O-free algorithm (§3): decay the stored raw
// score from its anchor to `now`, then normalize for display.
func (e *Engine) ScoreFromProfile(p *Profile, now time.Time) float64 {
	return normalize(decayRaw(p.RawScore, p.ScoreUpdatedAt, now))
}

// Score recomputes and persists a customer's engagement score (§3). It takes
// the profile row FOR UPDATE so a concurrent worker ProcessTx cannot interleave
// a partial write; re-anchoring at `now` is the §7 in-order branch with w=0.
// Returns core.ErrNotFound when the customer has no engagement profile.
func (e *Engine) Score(ctx context.Context, customerID int64) (*ScoreResult, error) {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	p, err := loadProfile(ctx, tx, customerID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	raw := decayRaw(p.RawScore, p.ScoreUpdatedAt, now)
	trend, err := trendFor(ctx, tx, customerID, now)
	if err != nil {
		return nil, err
	}
	ch := preferredChannel(p.ChannelCounts)

	if _, err := tx.Exec(ctx, `
		UPDATE engagement_profiles
		SET engagement_score = $2, score_updated_at = $3, activity_trend = $4,
		    preferred_channel = $5, updated_at = now()
		WHERE customer_id = $1`,
		customerID, raw, now, trend, ch); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	res := &ScoreResult{Score: normalize(raw), Trend: trend, Computed: now}
	if ch != nil {
		res.Channel = *ch
	}
	return res, nil
}

// loadProfile fetches one engagement_profiles row; forUpdate takes the row
// lock used to serialize concurrent same-customer mutations.
func loadProfile(ctx context.Context, q rowQuerier, customerID int64, forUpdate bool) (*Profile, error) {
	sql := `
		SELECT customer_id, total_events, channel_counts, positive_events,
		       negative_events, conversions, last_event_at, last_event_type,
		       engagement_score, score_updated_at, activity_trend, preferred_channel
		FROM engagement_profiles
		WHERE customer_id = $1`
	if forUpdate {
		sql += ` FOR UPDATE`
	}
	p := &Profile{}
	err := q.QueryRow(ctx, sql, customerID).Scan(
		&p.CustomerID, &p.TotalEvents, &p.ChannelCounts, &p.PositiveEvents,
		&p.NegativeEvents, &p.Conversions, &p.LastEventAt, &p.LastEventType,
		&p.RawScore, &p.ScoreUpdatedAt, &p.ActivityTrend, &p.PreferredChannel)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// trendFor derives the 7d-vs-prior-7d positive-event trend straight from the
// events table (source of truth), bounded by idx_events_customer_time.
func trendFor(ctx context.Context, q rowQuerier, customerID int64, now time.Time) (string, error) {
	curStart := now.Add(-trendWindow)
	priorStart := now.Add(-2 * trendWindow)
	var cur, prior int
	err := q.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE occurred_at >= $2),
		       count(*) FILTER (WHERE occurred_at <  $2)
		FROM events
		WHERE customer_id = $1
		  AND type IN ('opened','clicked','converted')
		  AND occurred_at >= $3`,
		customerID, curStart, priorStart).Scan(&cur, &prior)
	if err != nil {
		return "", err
	}
	return trendFromCounts(cur, prior), nil
}
