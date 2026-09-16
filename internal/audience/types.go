package audience

import (
	"fmt"
	"time"
)

// Request/response DTOs for POST /api/v1/audience/recommend — CONTRACTS §5.

// Conditions are the optional audience filters. RespectFrequencyCap is a
// pointer so "absent" is distinguishable from "false": the OpenAPI schema
// declares `default: true`, so nil means apply the cap.
type Conditions struct {
	MinScore            float64  `json:"min_score"`
	LastActiveDays      int      `json:"last_active_days"`
	Channels            []string `json:"channels"`
	ExcludeCampaignIDs  []string `json:"exclude_campaign_ids"`
	RespectFrequencyCap *bool    `json:"respect_frequency_cap"`
}

type RecommendRequest struct {
	Objective  string     `json:"objective"`
	Channel    string     `json:"channel"`
	Size       int        `json:"size"`
	Conditions Conditions `json:"conditions"`
}

// Candidate is one ranked audience member in the response.
type Candidate struct {
	CustomerID string   `json:"customer_id"`
	Score      float64  `json:"score"`
	Rank       int      `json:"rank"`
	Reasons    []string `json:"reasons"`
}

// Meta reports the selection funnel. FilteredOut counts considered rows that
// did not make the final cut (below top-K, frequency-capped, or duplicate).
type Meta struct {
	CandidatesConsidered int   `json:"candidates_considered"`
	FilteredOut          int   `json:"filtered_out"`
	TookMs               int64 `json:"took_ms"`
}

type RecommendResponse struct {
	Candidates []Candidate `json:"candidates"`
	Meta       Meta        `json:"meta"`
}

// candidate is the internal row carried through the pipeline. It stays a
// plain struct (no pointers) so the top-K heap is cache-friendly.
type candidate struct {
	customerID       int64
	externalID       string
	rawScore         float64 // engagement_profiles.engagement_score
	weighted         float64 // after objective weighting (weights.go)
	preferredChannel string
	conversions      int
	positiveEvents   int
	negativeEvents   int
	totalEvents      int
	lastEventAt      *time.Time // NULL-able in the schema
	trend            string
}

var validObjectives = map[string]bool{
	"conversion": true, "engagement": true, "retention": true,
	"reactivation": true, "awareness": true,
}

var validChannels = map[string]bool{
	"email": true, "sms": true, "whatsapp": true, "push": true, "web": true,
}

// validate enforces the contract enums and bounds. Returns a message suitable
// for a 400 validation_failed body.
func (r *RecommendRequest) validate() error {
	if !validObjectives[r.Objective] {
		return fmt.Errorf("objective must be one of conversion|engagement|retention|reactivation|awareness")
	}
	if !validChannels[r.Channel] {
		return fmt.Errorf("channel must be one of email|sms|whatsapp|push|web")
	}
	if r.Size < 1 || r.Size > 100000 {
		return fmt.Errorf("size must be between 1 and 100000")
	}
	if r.Conditions.MinScore < 0 {
		return fmt.Errorf("conditions.min_score must be >= 0")
	}
	if r.Conditions.LastActiveDays < 0 {
		return fmt.Errorf("conditions.last_active_days must be >= 0")
	}
	for _, ch := range r.Conditions.Channels {
		if !validChannels[ch] {
			return fmt.Errorf("conditions.channels contains invalid channel %q", ch)
		}
	}
	return nil
}

// respectCap resolves the openapi `default: true` for respect_frequency_cap.
func (r *RecommendRequest) respectCap() bool {
	return r.Conditions.RespectFrequencyCap == nil || *r.Conditions.RespectFrequencyCap
}
