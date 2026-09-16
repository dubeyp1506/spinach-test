package ai

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spinach/martech-engine/internal/core"
)

// AI endpoints get a stricter fixed-window limit than /events (CONTRACTS §8):
// ~30 requests/minute per client IP, enforced in-process.
const (
	aiRequestsPerWindow = 30
	aiWindow            = time.Minute
)

type ipWindow struct {
	start time.Time
	count int
}

type ipRateLimiter struct {
	mu   sync.Mutex
	hits map[string]ipWindow
	now  func() time.Time
}

func newIPRateLimiter() *ipRateLimiter {
	return &ipRateLimiter{hits: map[string]ipWindow{}, now: time.Now}
}

func (l *ipRateLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	w := l.hits[ip]
	if now.Sub(w.start) >= aiWindow {
		w = ipWindow{start: now}
	}
	if w.count >= aiRequestsPerWindow {
		l.hits[ip] = w
		return false
	}
	w.count++
	l.hits[ip] = w
	// Bound memory: sweep stale windows when the map grows large.
	if len(l.hits) > 10000 {
		for k, v := range l.hits {
			if now.Sub(v.start) >= aiWindow {
				delete(l.hits, k)
			}
		}
	}
	return true
}

// rateLimit is the tighter per-IP middleware for the AI routes only.
func (s *Service) rateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !s.limiter.allow(c.ClientIP()) {
			core.RespondError(c, http.StatusTooManyRequests, "rate_limited",
				"AI endpoint rate limit exceeded", nil)
			return
		}
		c.Next()
	}
}
