package ai

import (
	"sync"
	"time"
)

// Per-provider circuit breaker (CONTRACTS §8): opens after 3 consecutive
// failures, half-opens after a 30s cooldown with a single probe request.
const (
	breakerFailureThreshold = 3
	breakerCooldown         = 30 * time.Second
)

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type circuitBreaker struct {
	mu          sync.Mutex
	state       breakerState
	consecutive int
	openedAt    time.Time
	probing     bool // a half-open probe request is in flight
	now         func() time.Time
}

func newCircuitBreaker() *circuitBreaker {
	return &circuitBreaker{now: time.Now}
}

// allow reports whether a call may proceed. In the open state it returns
// false until the cooldown elapses, then admits exactly one probe.
func (b *circuitBreaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerOpen:
		if b.now().Sub(b.openedAt) >= breakerCooldown {
			b.state = breakerHalfOpen
			b.probing = true
			return true
		}
		return false
	case breakerHalfOpen:
		if b.probing {
			return false
		}
		b.probing = true
		return true
	default:
		return true
	}
}

func (b *circuitBreaker) recordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = breakerClosed
	b.consecutive = 0
	b.probing = false
}

func (b *circuitBreaker) recordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive++
	if b.state == breakerHalfOpen {
		b.state = breakerOpen
		b.openedAt = b.now()
		b.probing = false
		return
	}
	if b.consecutive >= breakerFailureThreshold {
		b.state = breakerOpen
		b.openedAt = b.now()
	}
}
