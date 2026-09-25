package predict

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spinach/martech-engine/internal/core"
)

const (
	defaultLookbackDays = 90
	maxLookbackDays     = 365
	modelName           = "beta-binomial-hierarchical-v1"
)

var allChannels = []string{"email", "sms", "whatsapp", "push", "web"}

// Module serves POST /api/v1/predictions/channel.
type Module struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Module { return &Module{pool: pool} }

func (m *Module) RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/predictions/channel", m.predictChannel)
}

// Request is the POST body. Scope is inferred: customer_id → customer,
// audience → audience, neither → platform. customer_id and audience are
// mutually exclusive.
type Request struct {
	Objective    string          `json:"objective"`
	Channels     []string        `json:"channels"`
	CustomerID   string          `json:"customer_id"`
	Audience     *AudienceFilter `json:"audience"`
	LookbackDays int             `json:"lookback_days"`
}

// Evidence is the raw data behind one channel's estimate, returned so the
// prediction can be audited.
type Evidence struct {
	ObjectiveDelivered int64  `json:"objective_delivered"`
	ObjectiveSuccesses int64  `json:"objective_successes"`
	OtherDelivered     int64  `json:"other_objectives_delivered"`
	OtherSuccesses     int64  `json:"other_objectives_successes"`
	TargetDelivered    *int64 `json:"target_delivered,omitempty"`
	TargetSuccesses    *int64 `json:"target_successes,omitempty"`
}

type ChannelPrediction struct {
	Rank          int        `json:"rank"`
	Channel       string     `json:"channel"`
	PredictedRate float64    `json:"predicted_rate"`
	Interval90    [2]float64 `json:"interval_90"`
	ProbBest      float64    `json:"prob_best"`
	LowData       bool       `json:"low_data"`
	OptOutRate    float64    `json:"opt_out_rate"`
	BounceRate    float64    `json:"bounce_rate"`
	Evidence      Evidence   `json:"evidence"`
	Reasons       []string   `json:"reasons"`
}

type Meta struct {
	LookbackDays int    `json:"lookback_days"`
	AudienceSize *int64 `json:"audience_size,omitempty"`
	Model        string `json:"model"`
	Samples      int    `json:"samples"`
	TookMs       int64  `json:"took_ms"`
}

type Response struct {
	Objective          string              `json:"objective"`
	Scope              Scope               `json:"scope"`
	CustomerID         string              `json:"customer_id,omitempty"`
	SuccessEvent       string              `json:"success_event"`
	RecommendedChannel string              `json:"recommended_channel"`
	Confidence         string              `json:"confidence"`
	Advice             string              `json:"advice"`
	Channels           []ChannelPrediction `json:"channels"`
	Meta               Meta                `json:"meta"`
}

// predictChannel handles POST /api/v1/predictions/channel.
func (m *Module) predictChannel(c *gin.Context) {
	start := time.Now()
	ctx := c.Request.Context()
	log := core.Log(ctx)

	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		core.BadRequest(c, "malformed request body", nil)
		return
	}
	channels, errs := validate(&req)
	if len(errs) > 0 {
		core.BadRequest(c, "invalid prediction request", errs)
		return
	}
	success := successEvent[req.Objective]

	scope := ScopePlatform
	switch {
	case req.CustomerID != "":
		scope = ScopeCustomer
	case req.Audience != nil:
		scope = ScopeAudience
	}

	ev, err := platformEvidence(ctx, m.pool, req.Objective, success, channels, req.LookbackDays)
	if err != nil {
		log.Error("predict platform evidence", "err", err)
		core.Internal(c, err)
		return
	}

	var audienceSize *int64
	switch scope {
	case ScopeAudience:
		target, size, err := audienceEvidence(ctx, m.pool, *req.Audience, success, req.LookbackDays)
		if err != nil {
			log.Error("predict audience evidence", "err", err)
			core.Internal(c, err)
			return
		}
		audienceSize = &size
		for ch, cnt := range target {
			if e := ev[ch]; e != nil {
				e.Target = cnt
			}
		}
	case ScopeCustomer:
		target, err := customerEvidence(ctx, m.pool, req.CustomerID, success, req.LookbackDays)
		if errors.Is(err, errUnknownCustomer) {
			core.NotFound(c, "customer")
			return
		}
		if err != nil {
			log.Error("predict customer evidence", "err", err)
			core.Internal(c, err)
			return
		}
		for ch, cnt := range target {
			if e := ev[ch]; e != nil {
				e.Target = cnt
			}
		}
	}

	ordered := make([]ChannelEvidence, 0, len(channels))
	for _, ch := range channels {
		ordered = append(ordered, *ev[ch])
	}
	resp := buildResponse(req, scope, success, ordered)
	resp.Meta = Meta{
		LookbackDays: req.LookbackDays,
		AudienceSize: audienceSize,
		Model:        modelName,
		Samples:      samples,
		TookMs:       time.Since(start).Milliseconds(),
	}
	log.Info("channel prediction",
		"objective", req.Objective, "scope", scope,
		"recommended", resp.RecommendedChannel, "confidence", resp.Confidence,
		"prob_best", resp.Channels[0].ProbBest, "took_ms", resp.Meta.TookMs)
	c.JSON(http.StatusOK, resp)
}

// validate normalizes req in place (defaults, deduped channel list) and
// returns the channel list plus field errors.
func validate(req *Request) ([]string, map[string]string) {
	errs := map[string]string{}
	if _, ok := successEvent[req.Objective]; !ok {
		errs["objective"] = "must be one of conversion, engagement, retention, reactivation, awareness"
	}
	var channels []string
	if len(req.Channels) == 0 {
		channels = slices.Clone(allChannels)
	} else {
		for _, ch := range req.Channels {
			if !slices.Contains(allChannels, ch) {
				errs["channels"] = "each channel must be one of email, sms, whatsapp, push, web"
				break
			}
			if !slices.Contains(channels, ch) {
				channels = append(channels, ch)
			}
		}
	}
	if req.CustomerID != "" && req.Audience != nil {
		errs["customer_id"] = "customer_id and audience are mutually exclusive"
	}
	if req.Audience != nil {
		if req.Audience.MinScore < 0 || req.Audience.MinScore >= 1 {
			errs["audience.min_score"] = "must be in [0, 1)"
		}
		if req.Audience.LastActiveDays < 0 {
			errs["audience.last_active_days"] = "must be >= 0"
		}
	}
	switch {
	case req.LookbackDays == 0:
		req.LookbackDays = defaultLookbackDays
	case req.LookbackDays < 1 || req.LookbackDays > maxLookbackDays:
		errs["lookback_days"] = fmt.Sprintf("must be between 1 and %d", maxLookbackDays)
	}
	return channels, errs
}

// buildResponse is the pure core of the endpoint: model + ranking + reasons.
func buildResponse(req Request, scope Scope, success string, ev []ChannelEvidence) Response {
	sims := Simulate(Posteriors(ev, success, scope))

	// Risk totals across candidate channels. Each channel is compared with
	// the OTHER channels (leave-one-out): including itself would let one bad
	// channel inflate the average enough to hide itself.
	var optOuts, delivered, bounced, sent int64
	for _, e := range ev {
		optOuts += e.OptOuts
		delivered += e.DeliveredAll
		bounced += e.Bounced
		sent += e.Sent
	}

	preds := make([]ChannelPrediction, len(ev))
	for i, e := range ev {
		s := sims[i]
		o, x, t := e.Objective.clamp(), e.OtherObj.clamp(), e.Target.clamp()
		p := ChannelPrediction{
			Channel:       e.Channel,
			PredictedRate: round4(s.Mean),
			Interval90:    [2]float64{round4(s.Lower), round4(s.Upper)},
			ProbBest:      round4(s.ProbBest),
			LowData:       o.Delivered+t.Delivered < lowDataBelow,
			OptOutRate:    round4(ratio(e.OptOuts, e.DeliveredAll)),
			BounceRate:    round4(ratio(e.Bounced, e.Sent)),
			Evidence: Evidence{
				ObjectiveDelivered: o.Delivered, ObjectiveSuccesses: o.Successes,
				OtherDelivered: x.Delivered, OtherSuccesses: x.Successes,
			},
		}
		if scope != ScopePlatform {
			p.Evidence.TargetDelivered = &t.Delivered
			p.Evidence.TargetSuccesses = &t.Successes
		}
		othersOptOut := ratio(optOuts-e.OptOuts, delivered-e.DeliveredAll)
		othersBounce := ratio(bounced-e.Bounced, sent-e.Sent)
		p.Reasons = reasons(req, scope, success, e, p, othersOptOut, othersBounce)
		preds[i] = p
	}

	// Rank by expected rate; P(best) breaks exact ties.
	sort.SliceStable(preds, func(i, j int) bool {
		if preds[i].PredictedRate != preds[j].PredictedRate {
			return preds[i].PredictedRate > preds[j].PredictedRate
		}
		return preds[i].ProbBest > preds[j].ProbBest
	})
	for i := range preds {
		preds[i].Rank = i + 1
	}

	best := preds[0]
	conf := Confidence(best.ProbBest)
	if len(preds) == 1 {
		conf = "high" // nothing to compare against
	}
	return Response{
		Objective:          req.Objective,
		Scope:              scope,
		CustomerID:         req.CustomerID,
		SuccessEvent:       success,
		RecommendedChannel: best.Channel,
		Confidence:         conf,
		Advice:             advice(preds, conf, success, scope),
		Channels:           preds,
	}
}

func advice(preds []ChannelPrediction, conf, success string, scope Scope) string {
	best := preds[0]
	// The challenger is whichever other channel is most likely to actually be
	// best — often a thin-data channel with a wide interval, not rank 2.
	challenger := preds[min(1, len(preds)-1)]
	for _, p := range preds[1:] {
		if p.ProbBest > challenger.ProbBest {
			challenger = p
		}
	}
	switch {
	case len(preds) == 1:
		return fmt.Sprintf("Only %s was evaluated.", best.Channel)
	case conf == "high":
		return fmt.Sprintf("Send on %s: it has a %s chance of the highest %s rate.",
			best.Channel, pct(best.ProbBest), success)
	case conf == "medium":
		return fmt.Sprintf("%s is the likely winner (%s chance best); %s is the main challenger.",
			best.Channel, pct(best.ProbBest), challenger.Channel)
	case scope == ScopeCustomer:
		return fmt.Sprintf("Not enough history to separate channels for this customer; %s is the best guess (%s chance best). Their response will sharpen the next prediction.",
			best.Channel, pct(best.ProbBest))
	default:
		return fmt.Sprintf("No clear winner between %s and %s (%s vs %s chance best). Run an A/B split on a slice of the audience before committing the rest.",
			best.Channel, challenger.Channel, pct(best.ProbBest), pct(challenger.ProbBest))
	}
}

func reasons(req Request, scope Scope, success string, e ChannelEvidence, p ChannelPrediction, avgOptOut, avgBounce float64) []string {
	o, x, t := e.Objective.clamp(), e.OtherObj.clamp(), e.Target.clamp()
	var out []string
	if o.Delivered > 0 {
		out = append(out, fmt.Sprintf("%s %s rate over %s deliveries in %s campaigns (last %dd)",
			pct(o.rate()), success, thousands(o.Delivered), req.Objective, req.LookbackDays))
	} else if x.Delivered > 0 {
		out = append(out, fmt.Sprintf("no %s-campaign history; estimate borrows %s from %s deliveries on other objectives",
			req.Objective, pct(x.rate()), thousands(x.Delivered)))
	} else {
		out = append(out, "no history on this channel; estimate is the platform average")
	}
	if scope != ScopePlatform {
		who := "audience"
		if scope == ScopeCustomer {
			who = "customer"
		}
		if t.Delivered > 0 {
			out = append(out, fmt.Sprintf("%s history: %s %s over %s deliveries",
				who, thousands(t.Successes), success, thousands(t.Delivered)))
		} else {
			out = append(out, fmt.Sprintf("%s has never been reached on this channel", who))
		}
	}
	if p.LowData {
		out = append(out, "low data: treat as a directional estimate")
	}
	if e.DeliveredAll >= 100 && avgOptOut > 0 && p.OptOutRate > 2*avgOptOut {
		out = append(out, fmt.Sprintf("high opt-out risk: %s unsubscribes/complaints vs %s on the other channels",
			pct(p.OptOutRate), pct(avgOptOut)))
	}
	if e.Sent >= 100 && avgBounce > 0 && p.BounceRate > 2*avgBounce {
		out = append(out, fmt.Sprintf("high bounce rate: %s vs %s on the other channels", pct(p.BounceRate), pct(avgBounce)))
	}
	return out
}

func ratio(n, d int64) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / float64(d)
}

func round4(f float64) float64 { return float64(int64(f*10000+0.5)) / 10000 }

func pct(f float64) string { return fmt.Sprintf("%.1f%%", f*100) }

// thousands formats 12345 → "12,345".
func thousands(n int64) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return "-" + thousands(-n)
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String()
}
