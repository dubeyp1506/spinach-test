package events

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/core"
)

// tokenBucketScript is an atomic Redis token bucket. KEYS[1]=bucket key;
// ARGV = rate (tokens/s), burst, now_ms, cost. Returns 1 allowed / 0 denied.
var tokenBucketScript = redis.NewScript(`
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])
local cost  = tonumber(ARGV[4])
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then tokens = burst end
if ts == nil then ts = now end
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate / 1000)
local allowed = 0
if tokens >= cost then
	tokens = tokens - cost
	allowed = 1
end
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.max(1000, math.ceil(burst / rate * 2000)))
return allowed
`)

// rateLimit throttles POST /events per client IP using the Redis token bucket
// sized by cfg.RateLimitRPS/RateLimitBurst (CONTRACTS §2). The cost is the
// number of events in the batch — a single request carries up to maxBatch
// events, so charging one token per request would under-price the endpoint
// ~500x. Returns false after writing the 429. Fails open if Redis is
// unreachable — ingestion must not drop events because the limiter broke;
// durability comes from Postgres, not Redis.
func (s *Service) rateLimit(c *gin.Context, cost int) bool {
	rps, burst := s.cfg.RateLimitRPS, s.cfg.RateLimitBurst
	if rps <= 0 || burst <= 0 || cost <= 0 {
		return true
	}
	key := "ratelimit:events:" + c.ClientIP()
	allowed, err := tokenBucketScript.Run(
		c.Request.Context(), s.rdb, []string{key},
		rps, burst, time.Now().UnixMilli(), cost,
	).Int()
	if err != nil {
		slog.Warn("rate limiter unavailable, failing open",
			"request_id", c.GetString("request_id"), "err", err)
		return true
	}
	if allowed == 0 {
		core.RespondError(c, http.StatusTooManyRequests,
			"rate_limited", "rate limit exceeded", nil)
		return false
	}
	return true
}
