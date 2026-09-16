// Package seedgen synthesizes deterministic demo data for the MarTech engine.
// It is pure (no I/O) so distributions and the CONTRACTS §7 scoring fold are
// unit-testable without a database; cmd/seed persists the resulting Dataset.
// Synthetic-data semantics: CONTRACTS.md §10.
//
// Scale note: the -events flag is a *minimum* event target. The spec requires
// ~5% of customers to be high-frequency with 50-500 events each, so at
// -customers 50000 the HF cohort alone produces >=125k events and the flag
// acts as a floor, not an exact count.
package seedgen

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

// CHECK-constraint vocabularies (migrations/000001_init.up.sql).
var (
	Channels         = []string{"email", "sms", "whatsapp", "push", "web"}
	Objectives       = []string{"conversion", "engagement", "retention", "reactivation", "awareness"}
	CampaignStatuses = []string{"draft", "active", "paused", "completed"}
	EventTypes       = []string{"sent", "delivered", "opened", "clicked", "converted", "bounced", "unsubscribed", "complained"}
)

// Config controls Generate. Now is the "seed time" anchor for all timestamps;
// zero means time.Now().UTC().
type Config struct {
	Customers int
	Events    int // minimum total events target (see package doc)
	Campaigns int
	Seed      int64
	Now       time.Time
}

// Customer mirrors a customers row. HighFreq is generation metadata (not a
// column): high-frequency customers get organic top-up events to reach
// 50-500 total events each.
type Customer struct {
	ExternalID string
	Email      string
	Attributes map[string]any // {segment, region, age_band}
	IsActive   bool
	CreatedAt  time.Time
	HighFreq   bool
}

// Campaign mirrors a campaigns row. StartedAt nil -> SQL NULL (drafts).
type Campaign struct {
	ExternalID           string
	Name                 string
	Objective            string
	Channel              string
	Status               string
	FrequencyCap         int
	FrequencyWindowHours int
	AudienceFilter       map[string]any
	StartedAt            *time.Time
	CreatedAt            time.Time
}

// Event mirrors an events row. CustomerIdx/CampaignIdx are indexes into
// Dataset.Customers/Campaigns (-1 -> SQL NULL for campaign_id); cmd/seed
// resolves them to BIGINT ids after inserting the parent rows.
//
// Events are stored in INSERTION order: ~1% sit in groups whose insertion
// order deliberately differs from occurred_at order (OutOfOrder), exercising
// the OOO paths in CONTRACTS §2/§7 — e.g. an 'opened' written after a
// later-occurring 'clicked'.
type Event struct {
	EventID     string
	CustomerIdx int
	CampaignIdx int
	Channel     string
	Type        string
	OccurredAt  time.Time
	ReceivedAt  time.Time
	Payload     map[string]any
	Status      string // "processed" | "failed" (seeds are never left pending)
	Attempts    int
	LastError   *string
	ProcessedAt *time.Time
	OutOfOrder  bool
}

// Send mirrors a sends row — one per processed 'sent' event (CONTRACTS §2:
// sends are written by the processor, so failed events produce none).
type Send struct {
	CustomerIdx int
	CampaignIdx int
	Channel     string
	SentAt      time.Time
}

// DLQRow mirrors an events_dlq row: invalid payloads seeded directly so
// /system/dlq shows data (CONTRACTS §10).
type DLQRow struct {
	EventID  *string
	Payload  map[string]any // the malformed inbound payload
	Error    string
	Attempts int
	FailedAt time.Time
}

// DupeEvent is one element of the POST /events batch written to
// scripts/dupe_batch.json: it re-emits an already-seeded event_id so the
// dedup path can be demonstrated (CONTRACTS §10 — duplicates are repeated
// ingestion requests, never duplicate table rows).
type DupeEvent struct {
	EventID    string         `json:"event_id"`
	CustomerID string         `json:"customer_id"`
	CampaignID string         `json:"campaign_id,omitempty"`
	Channel    string         `json:"channel"`
	Type       string         `json:"type"`
	OccurredAt string         `json:"occurred_at"`
	Payload    map[string]any `json:"payload,omitempty"`
}

// DupeBatch is the top-level shape of scripts/dupe_batch.json.
type DupeBatch struct {
	Events []DupeEvent `json:"events"`
}

// Dataset is everything cmd/seed persists, in dependency order.
type Dataset struct {
	Customers []Customer
	Campaigns []Campaign
	Events    []Event // insertion order (see Event.OutOfOrder)
	Sends     []Send
	Profiles  []Profile
	DLQ       []DLQRow
	Dupes     DupeBatch

	OutOfOrderCount int // events displaced vs occurred_at order (~1%)
	FailedCount     int // events with status='failed' (~0.3%)
}

// --- generation --------------------------------------------------------------

type custRole int

const (
	roleRegular custRole = iota
	roleInactive
	roleHighFreq
)

// sendReq is one planned campaign send: which campaign and when.
type sendReq struct {
	campIdx int
	at      time.Time
}

// Generate builds the full synthetic dataset deterministically from cfg.Seed.
func Generate(cfg Config) *Dataset {
	r := rand.New(rand.NewSource(cfg.Seed))
	now := cfg.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ds := &Dataset{}
	ds.Customers = genCustomers(r, cfg.Customers, now)
	ds.Campaigns = genCampaigns(r, cfg.Campaigns, now)
	roles := assignRoles(r, ds, cfg.Customers)
	perCust := make([]int, len(ds.Customers)) // running per-customer event counts

	// Campaign-funnel events.
	for i := range ds.Customers {
		for _, s := range planSends(r, ds, roles[i], now) {
			emitFunnel(r, ds, perCust, i, s, now)
		}
	}

	// High-frequency top-up: organic (campaign-less) events until each HF
	// customer reaches its 50-500 event target (spec distribution).
	for i := range ds.Customers {
		if roles[i] != roleHighFreq {
			continue
		}
		target := 50 + int(r.ExpFloat64()*60)
		if target > 500 {
			target = 500
		}
		emitOrganic(r, ds, perCust, i, target-perCust[i], now)
	}

	// If still under the -events floor (unusually large flag vs customer
	// count), top up HF customers further, capped at 500 total events each.
	for i := range ds.Customers {
		if len(ds.Events) >= cfg.Events {
			break
		}
		if roles[i] != roleHighFreq {
			continue
		}
		emitOrganic(r, ds, perCust, i, min(500-perCust[i], cfg.Events-len(ds.Events)), now)
	}

	// event_ids must be UNIQUE in events (CONTRACTS §10) — assign in
	// insertion order after all OOO displacement is done.
	for i := range ds.Events {
		ds.Events[i].EventID = fmt.Sprintf("evt_%09d", i+1)
	}

	ds.Sends = buildSends(ds)
	ds.Profiles = buildProfiles(ds, now)
	ds.DLQ = genDLQ(r, ds, now)
	ds.Dupes = genDupes(r, ds)
	return ds
}

// --- customers & campaigns ---------------------------------------------------

func genCustomers(r *rand.Rand, n int, now time.Time) []Customer {
	custs := make([]Customer, n)
	for i := range custs {
		ext := fmt.Sprintf("cust_%05d", i+1)
		custs[i] = Customer{
			ExternalID: ext,
			Email:      ext + "@example.com",
			Attributes: map[string]any{
				"segment":  pickWeighted(r, []string{"free", "pro", "enterprise"}, []float64{0.65, 0.25, 0.10}),
				"region":   pickWeighted(r, []string{"NA", "EU", "APAC", "LATAM"}, []float64{0.45, 0.25, 0.20, 0.10}),
				"age_band": pickWeighted(r, []string{"18-24", "25-34", "35-44", "45-54", "55+"}, []float64{0.20, 0.30, 0.25, 0.15, 0.10}),
			},
			IsActive:  true, // flipped for the inactive cohort in Generate
			CreatedAt: now.Add(-time.Duration(91+r.Intn(275)) * 24 * time.Hour),
		}
	}
	return custs
}

// assignRoles partitions customers: ~10% inactive (is_active=false, old or
// no events) and ~5% high-frequency, per spec.
func assignRoles(r *rand.Rand, ds *Dataset, n int) []custRole {
	roles := make([]custRole, n)
	perm := r.Perm(n)
	nInactive := int(math.Round(0.10 * float64(n)))
	nHF := int(math.Round(0.05 * float64(n)))
	for i := 0; i < nInactive && i < len(perm); i++ {
		roles[perm[i]] = roleInactive
		ds.Customers[perm[i]].IsActive = false
	}
	for i := nInactive; i < nInactive+nHF && i < len(perm); i++ {
		roles[perm[i]] = roleHighFreq
		ds.Customers[perm[i]].HighFreq = true
	}
	return roles
}

func genCampaigns(r *rand.Rand, n int, now time.Time) []Campaign {
	camps := make([]Campaign, n)
	for i := range camps {
		objective := pickWeighted(r, Objectives, []float64{0.30, 0.25, 0.20, 0.15, 0.10})
		channel := pickWeighted(r, Channels, []float64{0.45, 0.15, 0.10, 0.20, 0.10})
		status := pickWeighted(r, CampaignStatuses, []float64{0.10, 0.50, 0.15, 0.25})
		var started *time.Time
		created := now.Add(-time.Duration(91+r.Intn(30)) * 24 * time.Hour)
		if status != "draft" {
			// started_at spread over the past 90 days.
			t := now.Add(-time.Duration(r.Intn(90*24))*time.Hour - time.Duration(r.Intn(60))*time.Minute)
			started = &t
			created = t.Add(-time.Duration(1+r.Intn(10)) * 24 * time.Hour)
		}
		audience := map[string]any{}
		if r.Float64() < 0.5 {
			audience["segments"] = []string{pickWeighted(r, []string{"free", "pro", "enterprise"}, []float64{0.5, 0.3, 0.2})}
		}
		camps[i] = Campaign{
			ExternalID:           fmt.Sprintf("camp_%03d", i+1),
			Name:                 fmt.Sprintf("%s %s #%d", capitalize(channel), objective, i+1),
			Objective:            objective,
			Channel:              channel,
			Status:               status,
			FrequencyCap:         2 + r.Intn(4), // 2-5
			FrequencyWindowHours: 168,
			AudienceFilter:       audience,
			StartedAt:            started,
			CreatedAt:            created,
		}
	}
	return camps
}

// --- events ------------------------------------------------------------------

// planSends decides a customer's campaign sends, clustered near campaign
// starts over the past 90 days:
//   - inactive: 0, or 1-2 sends on the oldest campaigns ("old events" for
//     lapsed users)
//   - high-frequency: 3-8 sends (organic events top them up to 50-500)
//   - regular: power-law-ish 0-3 sends -> ~0-7 realized events (spec: 0-5)
func planSends(r *rand.Rand, ds *Dataset, role custRole, now time.Time) []sendReq {
	var n int
	old := false
	switch role {
	case roleInactive:
		if r.Float64() < 0.20 {
			n, old = 1+r.Intn(2), true
		}
	case roleHighFreq:
		n = 3 + r.Intn(6)
	default:
		n = pickWeightedIndex(r, []float64{0.35, 0.40, 0.20, 0.05}) // 0-3 sends
	}
	out := make([]sendReq, 0, n)
	for k := 0; k < n; k++ {
		ci := pickCampaignIndex(r, ds, old, now)
		if ci < 0 {
			continue
		}
		st := *ds.Campaigns[ci].StartedAt
		var at time.Time
		if old {
			at = st.Add(time.Duration(r.Intn(72)) * time.Hour)
		} else {
			// exponential jitter keeps sends clustered near campaign starts.
			at = st.Add(time.Duration(r.ExpFloat64() * float64(36*time.Hour)))
		}
		if at.After(now) {
			at = now.Add(-time.Duration(1+r.Intn(3600)) * time.Second)
		}
		out = append(out, sendReq{campIdx: ci, at: at})
	}
	return out
}

// pickCampaignIndex chooses a started (non-draft) campaign. For old sends it
// requires a campaign that started >=75d ago; returns -1 if none qualify.
func pickCampaignIndex(r *rand.Rand, ds *Dataset, old bool, now time.Time) int {
	var eligible []int
	cutoff := now.Add(-75 * 24 * time.Hour)
	for i := range ds.Campaigns {
		c := &ds.Campaigns[i]
		if c.StartedAt == nil || (old && c.StartedAt.After(cutoff)) {
			continue
		}
		eligible = append(eligible, i)
	}
	if len(eligible) == 0 {
		return -1
	}
	return eligible[r.Intn(len(eligible))]
}

// emitFunnel appends one send's realistic funnel to ds.Events:
// sent -> bounced (~2.5%, terminal) | delivered (~97%) -> opened (~35%) ->
// clicked (~15% of opens) -> converted (~10% of clicks), plus unsubscribed
// (~0.4%) / complained (~0.2%) tails on delivered.
func emitFunnel(r *rand.Rand, ds *Dataset, perCust []int, custIdx int, s sendReq, now time.Time) {
	channel := ds.Campaigns[s.campIdx].Channel
	t0 := s.at
	mk := func(typ string, at time.Time) Event {
		if at.After(now) {
			at = now
		}
		return Event{
			CustomerIdx: custIdx,
			CampaignIdx: s.campIdx,
			Channel:     channel,
			Type:        typ,
			OccurredAt:  at,
			ReceivedAt:  clampTime(at.Add(jitter(r, 500*time.Millisecond, 90*time.Second)), now),
			Payload:     payloadFor(r, typ),
			Status:      "processed",
			Attempts:    1,
		}
	}

	evts := []Event{mk("sent", t0)}
	if r.Float64() < 0.025 {
		evts = append(evts, mk("bounced", t0.Add(jitter(r, 5*time.Second, 10*time.Minute))))
	} else if r.Float64() < 0.97 {
		tDel := t0.Add(jitter(r, 5*time.Second, 10*time.Minute))
		evts = append(evts, mk("delivered", tDel))
		if r.Float64() < 0.35 {
			tOpen := tDel.Add(jitter(r, time.Minute, 36*time.Hour))
			evts = append(evts, mk("opened", tOpen))
			if r.Float64() < 0.15 {
				tClick := tOpen.Add(jitter(r, 30*time.Second, 3*time.Hour))
				evts = append(evts, mk("clicked", tClick))
				if r.Float64() < 0.10 {
					evts = append(evts, mk("converted", tClick.Add(jitter(r, 5*time.Minute, 48*time.Hour))))
				}
			}
		}
		if r.Float64() < 0.004 {
			evts = append(evts, mk("unsubscribed", tDel.Add(jitter(r, 5*time.Minute, 96*time.Hour))))
		}
		if r.Float64() < 0.002 {
			evts = append(evts, mk("complained", tDel.Add(jitter(r, 5*time.Minute, 96*time.Hour))))
		}
	}
	finishGroup(r, ds, perCust, evts, now)
}

// emitOrganic appends n campaign-less events for a high-frequency customer —
// direct web/app/push activity that explains their 50-500 event volume.
func emitOrganic(r *rand.Rand, ds *Dataset, perCust []int, custIdx, n int, now time.Time) {
	if n <= 0 {
		return
	}
	evts := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		typ := pickWeighted(r,
			[]string{"opened", "clicked", "delivered", "converted", "bounced", "unsubscribed", "complained"},
			[]float64{0.42, 0.30, 0.14, 0.06, 0.03, 0.03, 0.02})
		at := now.Add(-time.Duration(r.Intn(90*24))*time.Hour - time.Duration(r.Intn(3600))*time.Second)
		evts = append(evts, Event{
			CustomerIdx: custIdx,
			CampaignIdx: -1, // organic — no campaign
			Channel:     pickWeighted(r, Channels, []float64{0.15, 0.10, 0.05, 0.25, 0.45}),
			Type:        typ,
			OccurredAt:  at,
			ReceivedAt:  clampTime(at.Add(jitter(r, 500*time.Millisecond, 90*time.Second)), now),
			Payload:     map[string]any{"source": "organic"},
			Status:      "processed",
			Attempts:    1,
		})
	}
	finishGroup(r, ds, perCust, evts, now)
}

// finishGroup applies per-group OOO displacement and failure marking, bumps
// per-customer event counts, then appends the group in insertion order.
//
// Out-of-order (~1% of events, CONTRACTS §10): swap two events inside the
// group so a later-occurring event is written first — e.g. an 'opened' row
// written after the later-occurring 'clicked'. This exercises the OOO paths
// (§7 decay math, GREATEST last_event_*, commutative counters). Marked
// events also get a late received_at to model delayed arrival.
func finishGroup(r *rand.Rand, ds *Dataset, perCust []int, evts []Event, now time.Time) {
	if len(evts) >= 2 && r.Float64() < 0.012 {
		i, j := r.Intn(len(evts)), r.Intn(len(evts))
		for j == i {
			j = r.Intn(len(evts))
		}
		evts[i], evts[j] = evts[j], evts[i]
		for _, k := range []int{i, j} {
			evts[k].OutOfOrder = true
			evts[k].ReceivedAt = clampTime(
				evts[k].OccurredAt.Add(jitter(r, time.Hour, 48*time.Hour)), now)
			ds.OutOfOrderCount++
		}
	}
	// ~0.3% of events exhaust retries -> status='failed' (spec: "failed
	// events"). Failed rows keep their unique event_id but get no sends row
	// and are excluded from engagement_profiles (never processed).
	for k := range evts {
		if r.Float64() < 0.003 {
			evts[k].Status = "failed"
			evts[k].Attempts = 5
			evts[k].ProcessedAt = nil
			errText := pickWeighted(r,
				[]string{"profile row lock timeout", "processor panic: simulated failure", "max attempts (5) exhausted"},
				[]float64{0.4, 0.3, 0.3})
			evts[k].LastError = &errText
			ds.FailedCount++
		} else {
			p := evts[k].ReceivedAt.Add(jitter(r, 10*time.Millisecond, 1500*time.Millisecond))
			evts[k].ProcessedAt = &p
		}
	}
	for k := range evts {
		perCust[evts[k].CustomerIdx]++
	}
	ds.Events = append(ds.Events, evts...)
}

// --- sends / dlq / dupes -----------------------------------------------------

func buildSends(ds *Dataset) []Send {
	var sends []Send
	for i := range ds.Events {
		e := &ds.Events[i]
		if e.Type == "sent" && e.Status == "processed" && e.CampaignIdx >= 0 {
			sends = append(sends, Send{
				CustomerIdx: e.CustomerIdx,
				CampaignIdx: e.CampaignIdx,
				Channel:     e.Channel,
				SentAt:      e.OccurredAt,
			})
		}
	}
	return sends
}

// genDLQ writes ~0.5% of event volume as invalid-payload rows (CONTRACTS §10)
// so /system/dlq shows data. Payloads mirror the POST /events item shape with
// realistic validation failures matching internal/events/validate.go.
func genDLQ(r *rand.Rand, ds *Dataset, now time.Time) []DLQRow {
	n := int(math.Round(0.005 * float64(len(ds.Events))))
	if n < 50 {
		n = 50
	}
	rows := make([]DLQRow, 0, n)
	for i := 0; i < n; i++ {
		cust := ds.Customers[r.Intn(len(ds.Customers))].ExternalID
		at := now.Add(-time.Duration(r.Intn(10*24)) * time.Hour).Format(time.RFC3339)
		var payload map[string]any
		var eventID *string
		var errText string
		switch r.Intn(5) {
		case 0: // bad channel
			id := fmt.Sprintf("evt_dlq_%06d", i)
			eventID = &id
			payload = map[string]any{"event_id": id, "customer_id": cust, "channel": "smoke_signal", "type": "opened", "occurred_at": at}
			errText = "invalid channel"
		case 1: // missing event_id
			payload = map[string]any{"customer_id": cust, "channel": "email", "type": "clicked", "occurred_at": at}
			errText = "event_id is required"
		case 2: // bad type
			id := fmt.Sprintf("evt_dlq_%06d", i)
			eventID = &id
			payload = map[string]any{"event_id": id, "customer_id": cust, "channel": "sms", "type": "hovered", "occurred_at": at}
			errText = "invalid type"
		case 3: // malformed occurred_at
			id := fmt.Sprintf("evt_dlq_%06d", i)
			eventID = &id
			payload = map[string]any{"event_id": id, "customer_id": cust, "channel": "push", "type": "delivered", "occurred_at": "not-a-timestamp"}
			errText = "occurred_at must be RFC3339"
		default: // missing customer_id
			id := fmt.Sprintf("evt_dlq_%06d", i)
			eventID = &id
			payload = map[string]any{"event_id": id, "channel": "web", "type": "opened", "occurred_at": at}
			errText = "customer_id is required"
		}
		rows = append(rows, DLQRow{
			EventID:  eventID,
			Payload:  payload,
			Error:    errText,
			Attempts: 5,
			FailedAt: now.Add(-time.Duration(r.Intn(10*24)) * time.Hour),
		})
	}
	return rows
}

// genDupes samples already-seeded events into a POST /events batch (~2000
// payloads ~= 2% of a 100k send volume). Posting scripts/dupe_batch.json
// re-emits these event_ids so ingestion reports them in `duplicates`.
func genDupes(r *rand.Rand, ds *Dataset) DupeBatch {
	var sent int
	for i := range ds.Events {
		if ds.Events[i].Type == "sent" {
			sent++
		}
	}
	n := int(math.Round(0.02 * float64(sent)))
	if n < 2000 {
		n = 2000
	}
	if n > len(ds.Events) {
		n = len(ds.Events)
	}
	seen := make(map[int]struct{}, n)
	out := DupeBatch{Events: make([]DupeEvent, 0, n)}
	for len(out.Events) < n {
		i := r.Intn(len(ds.Events))
		if _, ok := seen[i]; ok {
			continue
		}
		seen[i] = struct{}{}
		e := ds.Events[i]
		d := DupeEvent{
			EventID:    e.EventID,
			CustomerID: ds.Customers[e.CustomerIdx].ExternalID,
			Channel:    e.Channel,
			Type:       e.Type,
			OccurredAt: e.OccurredAt.UTC().Format(time.RFC3339),
			Payload:    e.Payload,
		}
		if e.CampaignIdx >= 0 {
			d.CampaignID = ds.Campaigns[e.CampaignIdx].ExternalID
		}
		out.Events = append(out.Events, d)
	}
	return out
}

// --- helpers ------------------------------------------------------------------

func pickWeighted(r *rand.Rand, items []string, w []float64) string {
	return items[pickWeightedIndex(r, w)]
}

func pickWeightedIndex(r *rand.Rand, w []float64) int {
	x := r.Float64()
	acc := 0.0
	for i, wi := range w {
		acc += wi
		if x < acc {
			return i
		}
	}
	return len(w) - 1
}

func jitter(r *rand.Rand, lo, hi time.Duration) time.Duration {
	return lo + time.Duration(r.Float64()*float64(hi-lo))
}

func clampTime(t, max time.Time) time.Time {
	if t.After(max) {
		return max
	}
	return t
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return string(s[0]-32) + s[1:]
}

// payloadFor returns a shared, read-only payload variant per event type —
// shared maps keep the in-memory dataset small at ~500k events.
func payloadFor(r *rand.Rand, typ string) map[string]any {
	pool := payloadPool[typ]
	if len(pool) == 0 {
		return map[string]any{}
	}
	return pool[r.Intn(len(pool))]
}

var payloadPool = map[string][]map[string]any{
	"sent": {
		{"subject": "Big sale this week", "variant": "A", "template_id": "tpl_01"},
		{"subject": "Your weekly digest", "variant": "B", "template_id": "tpl_02"},
		{"subject": "We miss you", "variant": "A", "template_id": "tpl_03"},
	},
	"delivered": {
		{"mta": "mta-1"}, {"mta": "mta-2"},
	},
	"opened": {
		{"user_agent": "Mozilla/5.0 (iPhone)", "device": "mobile"},
		{"user_agent": "Mozilla/5.0 (Macintosh)", "device": "desktop"},
	},
	"clicked": {
		{"url": "https://example.com/offer", "device": "mobile"},
		{"url": "https://example.com/pricing", "device": "desktop"},
	},
	"converted": {
		{"order_id": "ord_a", "amount": 49.99},
		{"order_id": "ord_b", "amount": 129.00},
	},
	"bounced": {
		{"reason": "mailbox full", "code": "552"},
		{"reason": "user unknown", "code": "550"},
	},
	"unsubscribed": {
		{"method": "link"},
	},
	"complained": {
		{"feedback_type": "abuse"},
	},
}

// sortedEventsCopy returns events sorted by occurred_at — the §7 fold
// requires occurred_at order regardless of insertion order.
func sortedEventsCopy(evts []Event) []Event {
	out := make([]Event, len(evts))
	copy(out, evts)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].OccurredAt.Before(out[j].OccurredAt)
	})
	return out
}
