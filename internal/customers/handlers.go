package customers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"

	"github.com/spinach/martech-engine/internal/core"
)

// engagementResponse is the §4 engagement block of GET /customers/{id}.
// score is normalized live (ScoreFromProfile, decayed to now); raw_score shows
// the decayed-to-now raw sum unclamped — a negative value is real information
// (a complaint debt), so it is reported honestly rather than hidden.
type engagementResponse struct {
	Score            float64                   `json:"score"`
	RawScore         float64                   `json:"raw_score"`
	Trend            string                    `json:"trend"`
	PreferredChannel *string                   `json:"preferred_channel"`
	TotalEvents      int                       `json:"total_events"`
	PositiveEvents   int                       `json:"positive_events"`
	NegativeEvents   int                       `json:"negative_events"`
	Conversions      int                       `json:"conversions"`
	LastEventAt      *time.Time                `json:"last_event_at"`
	ChannelCounts    map[string]map[string]int `json:"channel_counts"`
}

type customerResponse struct {
	CustomerID string             `json:"customer_id"` // external_id
	Email      *string            `json:"email"`
	Attributes json.RawMessage    `json:"attributes"`
	IsActive   bool               `json:"is_active"`
	Engagement engagementResponse `json:"engagement"`
}

// getCustomer serves CONTRACTS §4 GET /customers/{id} (external_id).
func (m *Module) getCustomer(c *gin.Context) {
	ctx := c.Request.Context()
	extID := c.Param("id")

	var id int64
	var email *string
	var attrs json.RawMessage
	var active bool
	err := m.pool.QueryRow(ctx, `
		SELECT id, email, attributes, is_active
		FROM customers WHERE external_id = $1`, extID).
		Scan(&id, &email, &attrs, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		core.NotFound(c, "customer "+extID)
		return
	}
	if err != nil {
		core.Internal(c, err)
		return
	}

	// A customer with no events yet has no profile row — show zeroed
	// engagement rather than 404-ing the whole customer.
	now := time.Now().UTC()
	p, err := loadProfile(ctx, m.pool, id, false)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		p = &Profile{CustomerID: id, ScoreUpdatedAt: now}
	case err != nil:
		core.Internal(c, err)
		return
	}
	if p.ChannelCounts == nil {
		p.ChannelCounts = map[string]map[string]int{}
	}

	trend, err := trendFor(ctx, m.pool, id, now)
	if err != nil {
		core.Internal(c, err)
		return
	}

	c.JSON(http.StatusOK, customerResponse{
		CustomerID: extID,
		Email:      email,
		Attributes: attrs,
		IsActive:   active,
		Engagement: engagementResponse{
			Score:            m.engine.ScoreFromProfile(p, now),
			RawScore:         decayRaw(p.RawScore, p.ScoreUpdatedAt, now),
			Trend:            trend,
			PreferredChannel: preferredChannel(p.ChannelCounts),
			TotalEvents:      p.TotalEvents,
			PositiveEvents:   p.PositiveEvents,
			NegativeEvents:   p.NegativeEvents,
			Conversions:      p.Conversions,
			LastEventAt:      p.LastEventAt,
			ChannelCounts:    p.ChannelCounts,
		},
	})
}

// timelineItem is one row of GET /customers/{id}/timeline. campaign_id is the
// campaign's external_id per the §6 convention (external ids in APIs).
type timelineItem struct {
	EventID    string          `json:"event_id"`
	CampaignID *string         `json:"campaign_id,omitempty"`
	Channel    string          `json:"channel"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

// getTimeline serves CONTRACTS §4 GET /customers/{id}/timeline — events for
// one customer, occurred_at DESC (id DESC tiebreak), cursor pagination over
// (occurred_at, id), optional ?channel= and ?type= filters, core.ListResponse.
func (m *Module) getTimeline(c *gin.Context) {
	ctx := c.Request.Context()
	extID := c.Param("id")

	var custID int64
	err := m.pool.QueryRow(ctx,
		`SELECT id FROM customers WHERE external_id = $1`, extID).Scan(&custID)
	if errors.Is(err, pgx.ErrNoRows) {
		core.NotFound(c, "customer "+extID)
		return
	}
	if err != nil {
		core.Internal(c, err)
		return
	}

	page := core.ParsePage(c)
	q := `
		SELECT e.id, e.event_id, e.channel, e.type, e.occurred_at, e.payload,
		       camp.external_id
		FROM events e
		LEFT JOIN campaigns camp ON camp.id = e.campaign_id
		WHERE e.customer_id = $1`
	args := []any{custID}

	if ch := c.Query("channel"); ch != "" {
		args = append(args, ch)
		q += fmt.Sprintf(` AND e.channel = $%d`, len(args))
	}
	if typ := c.Query("type"); typ != "" {
		args = append(args, typ)
		q += fmt.Sprintf(` AND e.type = $%d`, len(args))
	}
	if page.Cursor != "" {
		at, id, err := decodeCursor(page.Cursor)
		if err != nil {
			core.BadRequest(c, "invalid cursor", nil)
			return
		}
		args = append(args, at, id)
		q += fmt.Sprintf(` AND (e.occurred_at < $%d OR (e.occurred_at = $%d AND e.id < $%d))`,
			len(args)-1, len(args)-1, len(args))
	}
	args = append(args, page.Limit+1) // +1 row to detect has_more
	q += fmt.Sprintf(` ORDER BY e.occurred_at DESC, e.id DESC LIMIT $%d`, len(args))

	rows, err := m.pool.Query(ctx, q, args...)
	if err != nil {
		core.Internal(c, err)
		return
	}
	defer rows.Close()

	type scanned struct {
		dbID int64
		item timelineItem
	}
	var fetched []scanned
	for rows.Next() {
		var s scanned
		if err := rows.Scan(&s.dbID, &s.item.EventID, &s.item.Channel,
			&s.item.Type, &s.item.OccurredAt, &s.item.Payload,
			&s.item.CampaignID); err != nil {
			core.Internal(c, err)
			return
		}
		fetched = append(fetched, s)
	}
	if err := rows.Err(); err != nil {
		core.Internal(c, err)
		return
	}

	hasMore := len(fetched) > page.Limit
	if hasMore {
		fetched = fetched[:page.Limit]
	}
	items := make([]timelineItem, len(fetched))
	for i, s := range fetched {
		items[i] = s.item
	}
	next := ""
	if hasMore {
		last := fetched[len(fetched)-1]
		next = encodeCursor(last.item.OccurredAt, last.dbID)
	}
	c.JSON(http.StatusOK, core.ListResponse[timelineItem]{
		Data:       items,
		NextCursor: next,
		HasMore:    hasMore,
	})
}

// encodeCursor packs the (occurred_at, id) resume position as opaque base64.
// RFC3339Nano never contains '|', so the split is unambiguous.
func encodeCursor(t time.Time, id int64) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(t.UTC().Format(time.RFC3339Nano) + "|" + strconv.FormatInt(id, 10)))
}

func decodeCursor(s string) (time.Time, int64, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, 0, err
	}
	ts, ids, ok := strings.Cut(string(b), "|")
	if !ok {
		return time.Time{}, 0, fmt.Errorf("malformed cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, 0, err
	}
	id, err := strconv.ParseInt(ids, 10, 64)
	if err != nil {
		return time.Time{}, 0, err
	}
	return t, id, nil
}
