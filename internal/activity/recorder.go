// Package activity records every user operation — each API call — in the
// activity_logs table (migration 000006) and serves GET /api/v1/activity.
//
// Recording must never slow down or fail a request, so the middleware only
// hands an Entry to a buffered channel; a background Recorder batches them
// into one INSERT every flushInterval (or every maxBatch entries). If the
// buffer is full — the database is down or badly slow — the entry is
// dropped and counted instead of blocking the caller. The trade-off: rows
// in the buffer at a hard crash are lost; this is an operator view, not an
// audit ledger (that would need a synchronous write or a durable log).
package activity

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spinach/martech-engine/internal/core"
)

const (
	bufferSize    = 4096
	maxBatch      = 200
	flushInterval = 500 * time.Millisecond
	pruneInterval = time.Hour
	pruneBatch    = 5000
)

// Entry is one activity_logs row.
type Entry struct {
	At        time.Time
	RequestID string
	Action    string
	Method    string
	Route     string
	Path      string
	Query     string
	Entity    string
	Status    int
	LatencyMs int64
	ClientIP  string
	UserAgent string
	Summary   string
	Error     string
}

// Execer is satisfied by *pgxpool.Pool (and a fake in tests).
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Recorder buffers entries and writes them in batches.
type Recorder struct {
	db            Execer
	ch            chan Entry
	retentionDays int
	dropped       atomic.Int64
}

func NewRecorder(db Execer, retentionDays int) *Recorder {
	return &Recorder{db: db, ch: make(chan Entry, bufferSize), retentionDays: retentionDays}
}

// Record enqueues e without ever blocking.
func (r *Recorder) Record(e Entry) {
	select {
	case r.ch <- e:
	default:
		if n := r.dropped.Add(1); n == 1 || n%1000 == 0 {
			slog.Warn("activity log buffer full; dropping entries", "dropped_total", n)
		}
	}
}

// Dropped reports how many entries were discarded because the buffer was full.
func (r *Recorder) Dropped() int64 { return r.dropped.Load() }

// Run flushes batches until ctx is cancelled, then drains what is buffered.
func (r *Recorder) Run(ctx context.Context) {
	flush := time.NewTicker(flushInterval)
	defer flush.Stop()
	prune := time.NewTicker(pruneInterval)
	defer prune.Stop()

	batch := make([]Entry, 0, maxBatch)
	write := func(wctx context.Context) {
		if len(batch) == 0 {
			return
		}
		if err := r.insert(wctx, batch); err != nil {
			slog.Error("activity log write failed", "rows", len(batch), "err", err)
		}
		batch = batch[:0]
	}
	r.prune(ctx)
	for {
		select {
		case e := <-r.ch:
			batch = append(batch, e)
			if len(batch) >= maxBatch {
				write(ctx)
			}
		case <-flush.C:
			write(ctx)
		case <-prune.C:
			r.prune(ctx)
		case <-ctx.Done():
			// Shutdown: drain the buffer with a short, fresh deadline.
			dctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			for {
				select {
				case e := <-r.ch:
					batch = append(batch, e)
					if len(batch) >= maxBatch {
						write(dctx)
					}
				default:
					write(dctx)
					return
				}
			}
		}
	}
}

// insertSQL writes a whole batch in one statement (parallel arrays unnested).
const insertSQL = `
INSERT INTO activity_logs (created_at, request_id, action, method, route, path, query,
                           entity, status, latency_ms, client_ip, user_agent, summary, error)
SELECT * FROM unnest($1::timestamptz[], $2::text[], $3::text[], $4::text[], $5::text[],
                     $6::text[], $7::text[], $8::text[], $9::int[], $10::int[],
                     $11::text[], $12::text[], $13::text[], $14::text[])`

func (r *Recorder) insert(ctx context.Context, batch []Entry) error {
	n := len(batch)
	var (
		at                               = make([]time.Time, n)
		reqIDs, actions, methods, routes = make([]*string, n), make([]string, n), make([]string, n), make([]*string, n)
		paths, queries, entities         = make([]string, n), make([]*string, n), make([]*string, n)
		statuses, latencies              = make([]int32, n), make([]int32, n)
		ips, agents, summaries, errs     = make([]*string, n), make([]*string, n), make([]*string, n), make([]*string, n)
	)
	for i, e := range batch {
		at[i] = e.At
		reqIDs[i] = nilIfEmpty(e.RequestID)
		actions[i] = e.Action
		methods[i] = e.Method
		routes[i] = nilIfEmpty(e.Route)
		paths[i] = e.Path
		queries[i] = nilIfEmpty(e.Query)
		entities[i] = nilIfEmpty(e.Entity)
		statuses[i] = int32(e.Status)
		latencies[i] = int32(min(e.LatencyMs, 1<<31-1))
		ips[i] = nilIfEmpty(e.ClientIP)
		agents[i] = nilIfEmpty(e.UserAgent)
		summaries[i] = nilIfEmpty(e.Summary)
		errs[i] = nilIfEmpty(e.Error)
	}
	_, err := r.db.Exec(ctx, insertSQL, at, reqIDs, actions, methods, routes, paths, queries,
		entities, statuses, latencies, ips, agents, summaries, errs)
	return err
}

// prune bounds the table to the retention window, one batch per call.
func (r *Recorder) prune(ctx context.Context) {
	if r.retentionDays <= 0 {
		return
	}
	tag, err := r.db.Exec(ctx, `
		DELETE FROM activity_logs WHERE id IN (
			SELECT id FROM activity_logs
			WHERE created_at < now() - make_interval(days => $1)
			LIMIT $2)`, r.retentionDays, pruneBatch)
	if err != nil {
		if ctx.Err() == nil {
			core.Log(ctx).Error("activity log prune", "err", err)
		}
		return
	}
	if n := tag.RowsAffected(); n > 0 {
		core.Log(ctx).Info("activity log pruned", "rows", n, "retention_days", r.retentionDays)
	}
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
