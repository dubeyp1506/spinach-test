// Package tests contains black-box integration tests for the live MarTech
// stack (Postgres on :5433, Redis on :6379, API on :8080 with the embedded
// worker). Every file cites the CONTRACTS.md section it verifies.
//
// The tests talk to the running services directly: net/http for the API,
// pgx for Postgres, go-redis for Redis. When the API is unreachable at test
// start the tests SKIP, keeping CI green on machines without the stack.
//
// Env overrides (with dev defaults):
//
//	MARTECH_API   default http://localhost:8080/api/v1
//	DATABASE_URL  default postgres://martech:martech@localhost:5433/martech?sslmode=disable
//	REDIS_URL     default redis://localhost:6379/0
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	apiBase  = envOr("MARTECH_API", "http://localhost:8080/api/v1")
	dbURL    = envOr("DATABASE_URL", "postgres://martech:martech@localhost:5433/martech?sslmode=disable")
	redisURL = envOr("REDIS_URL", "redis://localhost:6379/0")
)

// runTag makes every event_id / external_id unique per test run so the suite
// is rerunnable against the same database (CONTRACTS §2: event_id UNIQUE).
var runTag = fmt.Sprintf("%d%s", time.Now().Unix(), uuid.NewString()[:8])

var httpClient = &http.Client{Timeout: 15 * time.Second}

var (
	stackOnce sync.Once
	stackErr  error
	db        *pgxpool.Pool
	rdb       *redis.Client
)

// requireStack probes the API once per run and opens shared DB/Redis clients.
// Tests calling it skip cleanly when the stack is down.
func requireStack(t *testing.T) {
	t.Helper()
	stackOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+"/system/health", nil)
		if err != nil {
			stackErr = err
			return
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			stackErr = fmt.Errorf("api unreachable at %s: %w", apiBase, err)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			stackErr = fmt.Errorf("api health returned %d", resp.StatusCode)
			return
		}
		if db, err = pgxpool.New(ctx, dbURL); err != nil {
			stackErr = fmt.Errorf("pgx pool: %w", err)
			return
		}
		if err = db.Ping(ctx); err != nil {
			stackErr = fmt.Errorf("postgres unreachable: %w", err)
			return
		}
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			stackErr = fmt.Errorf("redis url: %w", err)
			return
		}
		rdb = redis.NewClient(opt)
		if err = rdb.Ping(ctx).Err(); err != nil {
			stackErr = fmt.Errorf("redis unreachable: %w", err)
			return
		}
	})
	if stackErr != nil {
		t.Skipf("live stack unavailable: %v", stackErr)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// doJSON performs one API call and returns the status code plus raw body.
// body==nil sends no payload; a string body is sent verbatim (for malformed
// JSON tests); anything else is marshaled.
func doJSON(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rdr = bytes.NewBufferString(b)
	default:
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, apiBase+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if rdr != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw
}

// ingestResponse mirrors CONTRACTS §2: {"accepted","duplicates","rejected"}.
type ingestResponse struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
	Rejected   []struct {
		Index  int    `json:"index"`
		Reason string `json:"reason"`
	} `json:"rejected"`
}

// postEventBatch POSTs {"events":[...]} and requires HTTP 202.
func postEventBatch(t *testing.T, events any) ingestResponse {
	t.Helper()
	status, raw := doJSON(t, http.MethodPost, "/events", map[string]any{"events": events})
	if status != http.StatusAccepted {
		t.Fatalf("POST /events status=%d body=%s", status, raw)
	}
	var out ingestResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode ingest response: %v (%s)", err, raw)
	}
	return out
}

// uniqueEventID returns a per-run-unique event_id so reruns never collide
// with persisted rows.
func uniqueEventID(prefix string) string {
	return fmt.Sprintf("evt_it_%s_%s_%s", prefix, runTag, uuid.NewString()[:8])
}

// validEvent builds a contract-valid event map. occurred_at is 1 minute in
// the past so it is never rejected as "in the future".
func validEvent(eventID, customerID string) map[string]any {
	return map[string]any{
		"event_id":    eventID,
		"customer_id": customerID,
		"channel":     "email",
		"type":        "opened",
		"occurred_at": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
	}
}

// pollUntil polls cond every 200ms until it returns true or the timeout
// elapses, then fails the test.
func pollUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, desc)
}

// engagement mirrors the §4 engagement block of GET /customers/{id}.
type engagement struct {
	Score            float64                   `json:"score"`
	RawScore         float64                   `json:"raw_score"`
	Trend            string                    `json:"trend"`
	PreferredChannel *string                   `json:"preferred_channel"`
	TotalEvents      int                       `json:"total_events"`
	PositiveEvents   int                       `json:"positive_events"`
	NegativeEvents   int                       `json:"negative_events"`
	Conversions      int                       `json:"conversions"`
	LastEventAt      *time.Time                `json:"last_event_at"`
	ChannelCounts    map[string]map[string]int `json:"channel_counts"`
}

type customerProfile struct {
	CustomerID string     `json:"customer_id"`
	IsActive   bool       `json:"is_active"`
	Engagement engagement `json:"engagement"`
}

func getCustomer(t *testing.T, extID string) customerProfile {
	t.Helper()
	status, raw := doJSON(t, http.MethodGet, "/customers/"+extID, nil)
	if status != http.StatusOK {
		t.Fatalf("GET /customers/%s status=%d body=%s", extID, status, raw)
	}
	var out customerProfile
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode customer: %v (%s)", err, raw)
	}
	return out
}

// --- seeded-data pickers (never hardcode external_ids) ---

func queryString(t *testing.T, sql string, args ...any) string {
	t.Helper()
	var s string
	if err := db.QueryRow(context.Background(), sql, args...).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return s
}

func queryInt(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

// pickCustomer returns the external_id of an arbitrary active seeded customer.
func pickCustomer(t *testing.T) string {
	t.Helper()
	return queryString(t,
		`SELECT external_id FROM customers WHERE is_active ORDER BY random() LIMIT 1`)
}

// pickHighActivityCustomer returns the external_id of a seeded customer with
// the most recorded events — guaranteed to have an engagement profile.
func pickHighActivityCustomer(t *testing.T) string {
	t.Helper()
	return queryString(t, `
		SELECT c.external_id FROM engagement_profiles p
		JOIN customers c ON c.id = p.customer_id
		WHERE c.is_active
		ORDER BY p.total_events DESC LIMIT 1`)
}

// pickCampaign returns a real campaigns.external_id.
func pickCampaign(t *testing.T) string {
	t.Helper()
	return queryString(t, `SELECT external_id FROM campaigns ORDER BY id LIMIT 1`)
}

// newTestCustomer inserts a private customer row with a run-unique
// external_id. Used by tests that assert exact counter deltas so unrelated
// tests can never collide on the same profile. Rows are left in place (test
// artifacts on a dev seed DB; deleting would race in-flight processing).
func newTestCustomer(t *testing.T) string {
	t.Helper()
	ext := "cust_it_" + runTag + "_" + uuid.NewString()[:8]
	if _, err := db.Exec(context.Background(),
		`INSERT INTO customers (external_id, email) VALUES ($1, $2)`,
		ext, ext+"@test.local"); err != nil {
		t.Fatalf("insert test customer: %v", err)
	}
	return ext
}

// eventStatus returns the events.status for an event_id, or "" if absent.
func eventStatus(t *testing.T, eventID string) string {
	t.Helper()
	var s string
	err := db.QueryRow(context.Background(),
		`SELECT status FROM events WHERE event_id = $1`, eventID).Scan(&s)
	if err != nil {
		return ""
	}
	return s
}

// profileTotalEvents reads engagement_profiles.total_events directly
// (0 when the profile row does not exist yet).
func profileTotalEvents(t *testing.T, extID string) int64 {
	t.Helper()
	return queryInt(t, `
		SELECT COALESCE(p.total_events, 0)
		FROM customers c
		LEFT JOIN engagement_profiles p ON p.customer_id = c.id
		WHERE c.external_id = $1`, extID)
}
