package predict

import (
	"context"
	"errors"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// normK mirrors customers.normK (read-time score normalization s/(s+k)).
// Kept local because modules never import each other (CONTRACTS §1).
const normK = 10.0

// platformEvidence reads layers 1–2 plus risk counters from the
// campaign_metrics rollup — O(rollup rows in the window), never an events
// scan. One query: FILTER splits this objective from the others.
func platformEvidence(ctx context.Context, pool *pgxpool.Pool, objective, success string, channels []string, lookbackDays int) (map[string]*ChannelEvidence, error) {
	types := []string{success, "delivered", "sent", "bounced", "unsubscribed", "complained"}
	rows, err := pool.Query(ctx, `
		SELECT m.channel, m.event_type,
		       COALESCE(SUM(m.count) FILTER (WHERE c.objective = $1), 0)::bigint AS objective_n,
		       COALESCE(SUM(m.count), 0)::bigint                                 AS total_n
		FROM campaign_metrics m
		JOIN campaigns c ON c.id = m.campaign_id
		WHERE m.day >= current_date - $2::int
		  AND m.event_type = ANY($3::text[])
		  AND m.channel = ANY($4::text[])
		GROUP BY m.channel, m.event_type`, objective, lookbackDays, types, channels)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ev := make(map[string]*ChannelEvidence, len(channels))
	for _, ch := range channels {
		ev[ch] = &ChannelEvidence{Channel: ch}
	}
	for rows.Next() {
		var ch, typ string
		var objN, totalN int64
		if err := rows.Scan(&ch, &typ, &objN, &totalN); err != nil {
			return nil, err
		}
		e := ev[ch]
		if e == nil {
			continue
		}
		// success may equal another listed type only for "delivered", which
		// is never a success event, so the cases are disjoint.
		switch typ {
		case success:
			e.Objective.Successes = objN
			e.OtherObj.Successes = totalN - objN
		case "delivered":
			e.Objective.Delivered = objN
			e.OtherObj.Delivered = totalN - objN
			e.DeliveredAll = totalN
		case "sent":
			e.Sent = totalN
		case "bounced":
			e.Bounced = totalN
		case "unsubscribed", "complained":
			e.OptOuts += totalN
		}
	}
	return ev, rows.Err()
}

// AudienceFilter selects the target audience for layer 3. Same semantics as
// the audience recommender's conditions.
type AudienceFilter struct {
	MinScore       float64 `json:"min_score"`
	LastActiveDays int     `json:"last_active_days"`
}

// rawMinScore inverts s/(s+k): a normalized floor in [0,1) → raw units.
func rawMinScore(s float64) float64 {
	if s <= 0 {
		return -math.MaxFloat64
	}
	return normK * s / (1 - s)
}

// Target evidence (layer 3) counts CAMPAIGN-ATTRIBUTED events only, in the
// same lookback window as the platform layer, so numerator and denominator
// mean "responses to messages we sent". engagement_profiles.channel_counts
// is not usable here: it also counts organic activity (web visits, app
// opens) that has clicks but no delivered message, which inflates rates
// past 100%.

// audienceEvidence sums the audience's campaign responses per channel. It
// walks idx_events_customer_time for every audience member, so cost is
// O(audience events in the window): fine at the seeded 50k customers; at
// millions it needs a per-customer×channel daily rollup maintained by the
// worker (docs/SYSTEM_DESIGN.md §14).
func audienceEvidence(ctx context.Context, pool *pgxpool.Pool, f AudienceFilter, success string, lookbackDays int) (map[string]Counts, int64, error) {
	const audienceCTE = `
		WITH aud AS (
			SELECT p.customer_id
			FROM engagement_profiles p
			JOIN customers c ON c.id = p.customer_id
			WHERE c.is_active
			  AND p.engagement_score >= $1
			  AND ($2::int <= 0 OR p.last_event_at >= now() - make_interval(days => $2))
		)`
	var size int64
	if err := pool.QueryRow(ctx, audienceCTE+` SELECT COUNT(*) FROM aud`,
		rawMinScore(f.MinScore), f.LastActiveDays).Scan(&size); err != nil {
		return nil, 0, err
	}
	out := map[string]Counts{}
	if size == 0 {
		return out, 0, nil
	}
	rows, err := pool.Query(ctx, audienceCTE+`
		SELECT e.channel,
		       COUNT(*) FILTER (WHERE e.type = $3),
		       COUNT(*) FILTER (WHERE e.type = 'delivered')
		FROM aud
		JOIN events e ON e.customer_id = aud.customer_id
		WHERE e.campaign_id IS NOT NULL
		  AND e.type IN ($3, 'delivered')
		  AND e.occurred_at >= now() - make_interval(days => $4)
		GROUP BY e.channel`, rawMinScore(f.MinScore), f.LastActiveDays, success, lookbackDays)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var ch string
		var c Counts
		if err := rows.Scan(&ch, &c.Successes, &c.Delivered); err != nil {
			return nil, 0, err
		}
		out[ch] = c
	}
	return out, size, rows.Err()
}

var errUnknownCustomer = errors.New("unknown customer")

// customerEvidence reads one customer's campaign responses per channel —
// a bounded range scan on idx_events_customer_time.
func customerEvidence(ctx context.Context, pool *pgxpool.Pool, externalID, success string, lookbackDays int) (map[string]Counts, error) {
	var customerID int64
	err := pool.QueryRow(ctx, `SELECT id FROM customers WHERE external_id = $1`, externalID).Scan(&customerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errUnknownCustomer
	}
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
		SELECT channel,
		       COUNT(*) FILTER (WHERE type = $2),
		       COUNT(*) FILTER (WHERE type = 'delivered')
		FROM events
		WHERE customer_id = $1
		  AND occurred_at >= now() - make_interval(days => $3)
		  AND campaign_id IS NOT NULL
		  AND type IN ($2, 'delivered')
		GROUP BY channel`, customerID, success, lookbackDays)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Counts{}
	for rows.Next() {
		var ch string
		var c Counts
		if err := rows.Scan(&ch, &c.Successes, &c.Delivered); err != nil {
			return nil, err
		}
		out[ch] = c
	}
	return out, rows.Err()
}
