package audience

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/core"
)

// Per-IP throttle for POST /audience/recommend. Far stricter than POST
// /events (cfg.RateLimitRPS≈500/s): every call scans up to
// candidatePoolCeiling rows on one of the shared pool connections, so a
// single abusive IP can otherwise starve the whole API.
const (
	recommendRatePerMin = 10 // sustained tokens per minute per client IP
	recommendBurst      = 20
)

// tokenBucketScript is the same atomic Redis token bucket used by
// internal/events/ratelimit.go. The events copy is unexported, so it is
// duplicated here rather than shared — keep the two in sync.
// KEYS[1]=bucket key; ARGV = rate (tokens/s), burst, now_ms, cost.
// Returns 1 allowed / 0 denied.
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

// rateLimit throttles recommend per client IP via the Redis token bucket.
// Fails open when Redis is unreachable or unconfigured — the read endpoint
// must not hard-depend on the limiter's availability.
func (m *Module) rateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		if m.rdb == nil {
			c.Next()
			return
		}
		key := "ratelimit:audience:" + c.ClientIP()
		allowed, err := tokenBucketScript.Run(
			c.Request.Context(), m.rdb, []string{key},
			recommendRatePerMin/60.0, recommendBurst, time.Now().UnixMilli(), 1,
		).Int()
		if err != nil {
			slog.Warn("rate limiter unavailable, failing open",
				"request_id", c.GetString("request_id"), "err", err)
			c.Next()
			return
		}
		if allowed == 0 {
			core.RespondError(c, http.StatusTooManyRequests,
				"rate_limited", "rate limit exceeded", nil)
			return
		}
		c.Next()
	}
}
