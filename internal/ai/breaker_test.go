package ai

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreakerOpenHalfOpen(t *testing.T) {
	now := time.Now()
	b := newCircuitBreaker()
	b.now = func() time.Time { return now }

	// Two failures: still closed.
	b.recordFailure()
	b.recordFailure()
	assert.True(t, b.allow())

	// Third consecutive failure: opens.
	b.recordFailure()
	assert.False(t, b.allow())

	// Still within cooldown: stays open.
	now = now.Add(breakerCooldown - time.Second)
	assert.False(t, b.allow())

	// Cooldown elapsed: half-open admits exactly one probe.
	now = now.Add(2 * time.Second)
	assert.True(t, b.allow())
	assert.False(t, b.allow(), "second call blocked while probe in flight")

	// Probe failure: re-opens with a fresh cooldown.
	b.recordFailure()
	assert.False(t, b.allow())
	now = now.Add(breakerCooldown + time.Second)
	assert.True(t, b.allow())

	// Probe success: closes.
	b.recordSuccess()
	assert.True(t, b.allow())
	assert.True(t, b.allow(), "closed breaker admits all calls")
}

func TestCircuitBreakerSuccessResets(t *testing.T) {
	b := newCircuitBreaker()
	b.recordFailure()
	b.recordFailure()
	b.recordSuccess() // consecutive counter resets
	b.recordFailure()
	b.recordFailure()
	assert.True(t, b.allow(), "two failures after reset must not open")
}
