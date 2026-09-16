package ai

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAIRateLimiter(t *testing.T) {
	l := newIPRateLimiter()
	now := time.Now()
	l.now = func() time.Time { return now }

	for i := 0; i < aiRequestsPerWindow; i++ {
		require.True(t, l.allow("1.2.3.4"), "request %d should be allowed", i)
	}
	assert.False(t, l.allow("1.2.3.4"), "request over the window limit is denied")
	assert.True(t, l.allow("5.6.7.8"), "limit is per-IP")

	now = now.Add(aiWindow + time.Second)
	assert.True(t, l.allow("1.2.3.4"), "window resets after the period")
}
