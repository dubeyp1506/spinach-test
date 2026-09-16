package customers

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecayHalfLife(t *testing.T) {
	now := time.Now().UTC()

	t.Run("raw halves after 14 days", func(t *testing.T) {
		got := decayRaw(8, now.Add(-halfLife), now)
		assert.InDelta(t, 4.0, got, 1e-9)
	})
	t.Run("quarter after 28 days", func(t *testing.T) {
		got := decayRaw(8, now.Add(-2*halfLife), now)
		assert.InDelta(t, 2.0, got, 1e-9)
	})
	t.Run("no decay at anchor", func(t *testing.T) {
		assert.Equal(t, 8.0, decayRaw(8, now, now))
	})
	t.Run("now before anchor never inflates", func(t *testing.T) {
		assert.Equal(t, 8.0, decayRaw(8, now, now.Add(-time.Hour)))
	})
	t.Run("negative raw decays toward zero", func(t *testing.T) {
		got := decayRaw(-4, now.Add(-halfLife), now)
		assert.InDelta(t, -2.0, got, 1e-9)
	})
}

func TestEventWeights(t *testing.T) {
	want := map[string]float64{
		"sent": 0, "delivered": 0.2, "opened": 1, "clicked": 2,
		"converted": 5, "bounced": -1, "unsubscribed": -3, "complained": -5,
	}
	for typ, w := range want {
		require.Contains(t, eventWeights, typ)
		assert.Equal(t, w, eventWeights[typ], "weight[%s]", typ)
	}
}

func TestApplyEvent(t *testing.T) {
	anchor := time.Now().UTC()

	t.Run("in-order event adds full weight and moves anchor", func(t *testing.T) {
		at := anchor.Add(24 * time.Hour)
		raw, newAnchor := applyEvent(0, anchor, at, 2)
		assert.Equal(t, 2.0, raw)
		assert.Equal(t, at, newAnchor)
	})
	t.Run("in-order decays existing raw to new anchor", func(t *testing.T) {
		at := anchor.Add(halfLife)
		raw, _ := applyEvent(4, anchor, at, 1)
		assert.InDelta(t, 4.0*0.5+1, raw, 1e-9)
	})
	t.Run("out-of-order event is attenuated, anchor unchanged", func(t *testing.T) {
		late := anchor.Add(-halfLife)
		raw, newAnchor := applyEvent(3, anchor, late, 2)
		assert.InDelta(t, 3.0+2*0.5, raw, 1e-9) // w·exp(-λ·14d) = w/2
		assert.Equal(t, anchor, newAnchor)
	})
	t.Run("event at anchor adds full weight", func(t *testing.T) {
		raw, newAnchor := applyEvent(1, anchor, anchor, 5)
		assert.Equal(t, 6.0, raw)
		assert.Equal(t, anchor, newAnchor)
	})
}

// TestApplyEventOrderIndependence is the core OOO guarantee: any arrival
// order of the same event set converges to the identical raw score.
func TestApplyEventOrderIndependence(t *testing.T) {
	base := time.Now().UTC()
	type ev struct {
		dt time.Duration
		w  float64
	}
	events := []ev{
		{1 * 24 * time.Hour, 1},   // opened
		{2 * 24 * time.Hour, 5},   // converted
		{3 * 24 * time.Hour, 2},   // clicked
		{-1 * 24 * time.Hour, -3}, // unsubscribed (before anchor base)
	}

	run := func(order []int) float64 {
		raw, anchor := 0.0, base
		for _, i := range order {
			raw, anchor = applyEvent(raw, anchor, base.Add(events[i].dt), events[i].w)
		}
		return raw
	}

	inOrder := run([]int{3, 0, 1, 2}) // chronological
	ooo := run([]int{0, 2, 3, 1})     // shuffled: late + OOO events
	assert.InDelta(t, inOrder, ooo, 1e-9)

	// Closed form: Σ w_i·exp(-λ·(T−t_i)) at final anchor T = base+3d.
	final := base.Add(3 * 24 * time.Hour)
	want := 0.0
	for _, e := range events {
		want += e.w * decayFactor(final.Sub(base.Add(e.dt)))
	}
	assert.InDelta(t, want, inOrder, 1e-9)
}

func TestAnchorNeverMovesBackward(t *testing.T) {
	anchor := time.Now().UTC()
	for _, dt := range []time.Duration{-time.Hour, -halfLife, -30 * 24 * time.Hour} {
		_, newAnchor := applyEvent(1, anchor, anchor.Add(dt), 1)
		assert.Equal(t, anchor, newAnchor, "dt=%v", dt)
	}
}

func TestNormalize(t *testing.T) {
	t.Run("bounded in [0,1) and monotone", func(t *testing.T) {
		prev := -1.0
		for _, raw := range []float64{-100, -5, -0.001, 0, 0.2, 1, 5, 10, 50, 500, 1e6, 1e12} {
			got := normalize(raw)
			assert.GreaterOrEqual(t, got, 0.0, "raw=%v", raw)
			assert.Less(t, got, 1.0, "raw=%v", raw)
			assert.GreaterOrEqual(t, got, prev, "monotonic at raw=%v", raw)
			prev = got
		}
	})
	t.Run("clamps negatives at 0 for display", func(t *testing.T) {
		assert.Equal(t, 0.0, normalize(-5))
		assert.Equal(t, 0.0, normalize(0))
	})
	t.Run("raw == k maps to 0.5", func(t *testing.T) {
		assert.InDelta(t, 0.5, normalize(normK), 1e-12)
	})
	t.Run("approaches but never reaches 1", func(t *testing.T) {
		assert.Less(t, normalize(math.MaxFloat64/2), 1.0)
	})
}

func TestTrendFromCounts(t *testing.T) {
	cases := []struct {
		cur, prior int
		want       string
	}{
		{0, 0, "stable"},
		{3, 1, "rising"},
		{1, 0, "rising"},
		{0, 2, "declining"},
		{2, 5, "declining"},
		{4, 4, "stable"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, trendFromCounts(tc.cur, tc.prior),
			"cur=%d prior=%d", tc.cur, tc.prior)
	}
}

func TestPreferredChannel(t *testing.T) {
	t.Run("argmax positive events", func(t *testing.T) {
		ch := preferredChannel(map[string]map[string]int{
			"email": {"opened": 2, "delivered": 9}, // delivered doesn't count
			"sms":   {"clicked": 1},
		})
		require.NotNil(t, ch)
		assert.Equal(t, "email", *ch)
	})
	t.Run("delivered/sent only → nil", func(t *testing.T) {
		assert.Nil(t, preferredChannel(map[string]map[string]int{
			"email": {"sent": 5, "delivered": 5},
		}))
	})
	t.Run("negative events only → nil", func(t *testing.T) {
		assert.Nil(t, preferredChannel(map[string]map[string]int{
			"sms": {"bounced": 3},
		}))
	})
	t.Run("nil map → nil", func(t *testing.T) {
		assert.Nil(t, preferredChannel(nil))
	})
	t.Run("tie breaks alphabetically", func(t *testing.T) {
		ch := preferredChannel(map[string]map[string]int{
			"whatsapp": {"opened": 1},
			"email":    {"opened": 1},
		})
		require.NotNil(t, ch)
		assert.Equal(t, "email", *ch)
	})
}

func TestScoreFromProfile(t *testing.T) {
	e := NewEngine(nil) // pure path — pool unused
	now := time.Now().UTC()

	t.Run("fresh raw normalizes", func(t *testing.T) {
		p := &Profile{RawScore: normK, ScoreUpdatedAt: now}
		assert.InDelta(t, 0.5, e.ScoreFromProfile(p, now), 1e-12)
	})
	t.Run("decays then normalizes", func(t *testing.T) {
		// raw 20 one half-life ago → 10 now → 10/(10+10) = 0.5
		p := &Profile{RawScore: 2 * normK, ScoreUpdatedAt: now.Add(-halfLife)}
		assert.InDelta(t, 0.5, e.ScoreFromProfile(p, now), 1e-9)
	})
	t.Run("negative raw displays as 0", func(t *testing.T) {
		p := &Profile{RawScore: -5, ScoreUpdatedAt: now}
		assert.Equal(t, 0.0, e.ScoreFromProfile(p, now))
	})
	t.Run("empty profile is 0", func(t *testing.T) {
		p := &Profile{ScoreUpdatedAt: now.Add(-365 * 24 * time.Hour)}
		assert.Equal(t, 0.0, e.ScoreFromProfile(p, now))
	})
}

func TestCursorRoundTrip(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Microsecond)
	cur := encodeCursor(at, 4242)
	gotAt, gotID, err := decodeCursor(cur)
	require.NoError(t, err)
	assert.Equal(t, at, gotAt)
	assert.Equal(t, int64(4242), gotID)

	for _, bad := range []string{"", "!!!", "bm90LXRpbWV8MQ==", "aGVsbG8="} {
		_, _, err := decodeCursor(bad)
		assert.Error(t, err, "cursor %q", bad)
	}
}
