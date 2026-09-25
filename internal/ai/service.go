package ai

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
)

// cacheTTL is the Redis response-cache lifetime (CONTRACTS §8). Keyed on
// campaign+endpoint+objective only — NOT on a metrics hash — so a hit skips
// the per-campaign metrics scan AND the LLM call. Facts in a cached body are
// up to cacheTTL stale; an acceptable, documented trade-off.
const cacheTTL = 60 * time.Second

// Service owns the AI endpoints and the provider chain bulkhead.
type Service struct {
	pool    *pgxpool.Pool
	rdb     *redis.Client
	cfg     *config.Config
	mp      MetricsProvider
	chain   *Chain
	limiter *ipRateLimiter
}

// New wires the service. Integration calls it in cmd/api and mounts
// RegisterRoutes on the /api/v1 group; the campaigns AnalyticsService is
// adapted to MetricsProvider at that seam.
func New(pool *pgxpool.Pool, rdb *redis.Client, cfg *config.Config, mp MetricsProvider) *Service {
	return &Service{
		pool:    pool,
		rdb:     rdb,
		cfg:     cfg,
		mp:      mp,
		chain:   NewChain(cfg),
		limiter: newIPRateLimiter(),
	}
}

// RegisterRoutes mounts the A5 endpoints with their dedicated rate limit.
func (s *Service) RegisterRoutes(rg *gin.RouterGroup) {
	g := rg.Group("/campaigns", s.rateLimit())
	// POST /api/v1/campaigns/{id}/analyze — CONTRACTS §4 (A5), isolation §8.
	g.POST("/:id/analyze", s.handleAnalyze)
	// POST /api/v1/campaigns/{id}/recommend — CONTRACTS §4 (A5), isolation §8.
	g.POST("/:id/recommend", s.handleRecommend)
}

// AnalyzeResponse is the openapi AIAnalysisResponse shape.
type AnalyzeResponse struct {
	Facts        *Facts    `json:"facts"`
	Analysis     *Analysis `json:"analysis"`
	Provider     string    `json:"provider"`
	FallbackUsed bool      `json:"fallback_used"`
}

// RecommendResponse is the openapi AIRecommendResponse shape.
type RecommendResponse struct {
	Facts           *Facts           `json:"facts"`
	Recommendations []Recommendation `json:"recommendations"`
	Provider        string           `json:"provider"`
	FallbackUsed    bool             `json:"fallback_used"`
}

func (s *Service) handleAnalyze(c *gin.Context) {
	ctx := c.Request.Context()
	key := cacheKey(c.Param("id"), "", "analyze-"+PromptVersion)
	if body, hit := s.cacheGet(ctx, key); hit {
		core.NoteActivity(c, "analysis served from cache")
		c.Data(http.StatusOK, "application/json", body)
		return
	}
	facts, factsJSON, ok := s.loadFacts(c, "")
	if !ok {
		return
	}
	res, err := s.chain.Execute(ctx, AnalyzePrompt(factsJSON), func(raw string) (any, error) {
		return ParseAnalysis(raw, factsJSON)
	})
	if err != nil {
		s.respondChainError(c, err)
		return
	}
	resp, err := json.Marshal(AnalyzeResponse{
		Facts:        facts,
		Analysis:     res.Output.(*Analysis),
		Provider:     res.Provider,
		FallbackUsed: res.FallbackUsed,
	})
	if err != nil {
		core.Internal(c, err)
		return
	}
	core.NoteActivity(c, "analysis by %s (fallback %t)", res.Provider, res.FallbackUsed)
	s.cacheSet(ctx, key, resp)
	c.Data(http.StatusOK, "application/json", resp)
}

func (s *Service) handleRecommend(c *gin.Context) {
	ctx := c.Request.Context()
	// Body is optional: {"objective": "conversion"}. Malformed/empty body → "".
	var body struct {
		Objective string `json:"objective"`
	}
	if c.Request.Body != nil {
		_ = c.ShouldBindJSON(&body)
	}
	key := cacheKey(c.Param("id"), body.Objective, "recommend-"+PromptVersion)
	if body2, hit := s.cacheGet(ctx, key); hit {
		core.NoteActivity(c, "recommendations served from cache")
		c.Data(http.StatusOK, "application/json", body2)
		return
	}
	facts, factsJSON, ok := s.loadFacts(c, body.Objective)
	if !ok {
		return
	}
	res, err := s.chain.Execute(ctx, RecommendPrompt(factsJSON), func(raw string) (any, error) {
		return ParseRecommendations(raw, factsJSON)
	})
	if err != nil {
		s.respondChainError(c, err)
		return
	}
	resp, err := json.Marshal(RecommendResponse{
		Facts:           facts,
		Recommendations: res.Output.([]Recommendation),
		Provider:        res.Provider,
		FallbackUsed:    res.FallbackUsed,
	})
	if err != nil {
		core.Internal(c, err)
		return
	}
	core.NoteActivity(c, "%d recommendations by %s (fallback %t)",
		len(res.Output.([]Recommendation)), res.Provider, res.FallbackUsed)
	s.cacheSet(ctx, key, resp)
	c.Data(http.StatusOK, "application/json", resp)
}

// respondChainError maps chain errors: overload → 429 (§8), anything else 500
// (unreachable while the deterministic fallback is present).
func (s *Service) respondChainError(c *gin.Context, err error) {
	if errors.Is(err, ErrOverloaded) {
		core.RespondError(c, http.StatusTooManyRequests, "ai_overloaded",
			"too many concurrent AI requests", nil)
		return
	}
	slog.Error("ai chain failed", "err", err, "request_id", c.GetString("request_id"))
	core.Internal(c, err)
}

// loadFacts resolves the campaign and its metrics, then builds the facts
// object. objective defaults to the campaign's stored objective when empty.
func (s *Service) loadFacts(c *gin.Context, objective string) (*Facts, []byte, bool) {
	ctx := c.Request.Context()
	extID := c.Param("id")
	if s.pool == nil {
		core.Unavailable(c, "postgres")
		return nil, nil, false
	}
	id, campObjective, err := s.resolveCampaign(ctx, extID)
	if errors.Is(err, core.ErrNotFound) {
		core.NotFound(c, "campaign")
		return nil, nil, false
	}
	if err != nil {
		slog.Error("ai resolve campaign", "err", err, "request_id", c.GetString("request_id"))
		core.Internal(c, err)
		return nil, nil, false
	}
	var m *Metrics
	if s.mp != nil {
		m, err = s.mp.Metrics(ctx, id)
		switch {
		case err == nil:
		case errors.Is(err, core.ErrNotFound):
			m = nil // zero-event campaign → insufficient-data output below
		default:
			slog.Error("ai metrics", "err", err, "request_id", c.GetString("request_id"))
			core.Internal(c, err)
			return nil, nil, false
		}
	}
	if m == nil {
		m = &Metrics{CampaignID: id}
	}
	if objective == "" {
		objective = campObjective
	}
	facts := BuildFacts(extID, m, objective)
	factsJSON, err := json.Marshal(facts)
	if err != nil {
		core.Internal(c, err)
		return nil, nil, false
	}
	return facts, factsJSON, true
}

// resolveCampaign maps external_id → internal id (and the stored objective,
// used as the default recommend objective). 404 via core.ErrNotFound.
func (s *Service) resolveCampaign(ctx context.Context, externalID string) (int64, string, error) {
	var id int64
	var objective string
	err := s.pool.QueryRow(ctx,
		`SELECT id, objective FROM campaigns WHERE external_id=$1`, externalID).
		Scan(&id, &objective)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", core.ErrNotFound
	}
	return id, objective, err
}

func cacheKey(externalID, objective, promptVer string) string {
	return "ai:" + externalID + ":" + objective + ":" + promptVer
}

func (s *Service) cacheGet(ctx context.Context, key string) ([]byte, bool) {
	if s.rdb == nil {
		return nil, false
	}
	b, err := s.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false // miss or redis down — best-effort cache
	}
	return b, true
}

func (s *Service) cacheSet(ctx context.Context, key string, val []byte) {
	if s.rdb == nil {
		return
	}
	if err := s.rdb.Set(ctx, key, val, cacheTTL).Err(); err != nil {
		slog.Warn("ai cache set", "err", err)
	}
}
