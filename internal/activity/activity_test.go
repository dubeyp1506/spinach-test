package activity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spinach/martech-engine/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDB counts inserted rows per statement instead of hitting Postgres.
type fakeDB struct {
	mu      sync.Mutex
	batches []int
}

func (f *fakeDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "INSERT INTO activity_logs") {
		f.mu.Lock()
		f.batches = append(f.batches, len(args[0].([]time.Time)))
		f.mu.Unlock()
	}
	return pgconn.NewCommandTag("DELETE 0"), nil
}

func (f *fakeDB) rows() (total, statements int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.batches {
		total += n
	}
	return total, len(f.batches)
}

func TestActionFor(t *testing.T) {
	assert.Equal(t, "customer.view", ActionFor("GET", "/api/v1/customers/:id"))
	assert.Equal(t, "prediction.channel", ActionFor("POST", "/api/v1/predictions/channel"))
	assert.Equal(t, "unknown", ActionFor("GET", ""), "unmatched path")
	assert.Equal(t, "DELETE /api/v1/things/:id", ActionFor("DELETE", "/api/v1/things/:id"),
		"a new route is still recorded, never silently skipped")
}

func newRouter(r *Recorder) *gin.Engine {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	e.Use(core.RequestID(), Middleware(r))
	v1 := e.Group("/api/v1")
	e.GET("/app/index.html", func(c *gin.Context) { c.Status(http.StatusOK) })
	v1.GET("/customers/:id", func(c *gin.Context) {
		core.NoteActivity(c, "viewed profile: score %.2f", 0.42)
		c.JSON(http.StatusOK, gin.H{})
	})
	v1.POST("/predictions/channel", func(c *gin.Context) {
		core.BadRequest(c, "invalid prediction request", nil)
	})
	v1.GET("/system/health", func(c *gin.Context) { c.Status(http.StatusOK) })
	v1.GET("/activity", func(c *gin.Context) { c.Status(http.StatusOK) })
	return e
}

func do(e *gin.Engine, method, target string) {
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-Request-ID", "req-7")
	req.Header.Set("User-Agent", "test-agent")
	e.ServeHTTP(httptest.NewRecorder(), req)
}

func TestMiddlewareRecordsOperations(t *testing.T) {
	r := NewRecorder(&fakeDB{}, 7)
	e := newRouter(r)

	do(e, http.MethodGet, "/api/v1/customers/cust_00042?x=1")
	do(e, http.MethodPost, "/api/v1/predictions/channel")
	do(e, http.MethodGet, "/api/v1/system/health") // machine traffic: skipped
	do(e, http.MethodGet, "/api/v1/activity")      // self-referential: skipped
	do(e, http.MethodGet, "/api/v1/nope")          // 404: recorded as unknown
	do(e, http.MethodGet, "/app/index.html")       // static UI: not an API operation

	require.Len(t, r.ch, 3)
	view := <-r.ch
	assert.Equal(t, "customer.view", view.Action)
	assert.Equal(t, "cust_00042", view.Entity)
	assert.Equal(t, "x=1", view.Query)
	assert.Equal(t, http.StatusOK, view.Status)
	assert.Equal(t, "req-7", view.RequestID)
	assert.Equal(t, "viewed profile: score 0.42", view.Summary)
	assert.Equal(t, "test-agent", view.UserAgent)
	assert.Empty(t, view.Error)

	failed := <-r.ch
	assert.Equal(t, "prediction.channel", failed.Action)
	assert.Equal(t, http.StatusBadRequest, failed.Status)
	assert.Equal(t, "validation_failed: invalid prediction request", failed.Error,
		"core.RespondError feeds the error into the activity row")

	unknown := <-r.ch
	assert.Equal(t, "unknown", unknown.Action)
	assert.Equal(t, http.StatusNotFound, unknown.Status)
	assert.Equal(t, "/api/v1/nope", unknown.Path)
}

// A full buffer drops and counts — it never blocks the request.
func TestRecordNeverBlocks(t *testing.T) {
	r := NewRecorder(&fakeDB{}, 7)
	done := make(chan struct{})
	go func() {
		for range bufferSize + 10 {
			r.Record(Entry{Action: "x"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Record blocked on a full buffer")
	}
	assert.Equal(t, int64(10), r.Dropped())
}

// Entries are batched (one INSERT for many rows) and drained on shutdown.
func TestRunBatchesAndDrainsOnShutdown(t *testing.T) {
	db := &fakeDB{}
	r := NewRecorder(db, 7)
	for range 450 {
		r.Record(Entry{At: time.Now(), Action: "customer.view", Method: "GET", Path: "/x", Status: 200})
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	cancel()
	<-done

	total, statements := db.rows()
	assert.Equal(t, 450, total, "every buffered entry is written")
	assert.LessOrEqual(t, statements, 3, "written in batches of up to 200, not row by row")
}

func TestTruncate(t *testing.T) {
	assert.Equal(t, "abc", truncate("  abc  ", 10))
	out := truncate(strings.Repeat("q", 600), maxQueryLen)
	assert.True(t, strings.HasSuffix(out, "…"))
	assert.LessOrEqual(t, len(out), maxQueryLen+4)
}

// With Recovery innermost (as in cmd/api), a panicking handler is still
// recorded — as a 500 with a generic error.
func TestPanicIsRecorded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRecorder(&fakeDB{}, 7)
	e := gin.New()
	e.Use(core.RequestID(), Middleware(r), gin.Recovery())
	e.GET("/api/v1/campaigns", func(c *gin.Context) { panic("boom") })
	do(e, http.MethodGet, "/api/v1/campaigns")

	require.Len(t, r.ch, 1)
	got := <-r.ch
	assert.Equal(t, "campaign.list", got.Action)
	assert.Equal(t, http.StatusInternalServerError, got.Status)
	assert.Equal(t, "internal error", got.Error)
}
