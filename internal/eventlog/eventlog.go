// Package eventlog records what happened to each event — ingested,
// duplicate, processed, retry, dead_lettered, replayed — in the event_logs
// table (migration 000005), and serves GET /api/v1/logs to query it.
//
// Entries are written with the caller's transaction (Write takes a pgx.Tx or
// the pool), so a log row commits or rolls back together with the step it
// describes: the log never claims "processed" for a transaction that rolled
// back. stdout logs (slog) remain the firehose for operators; this table is
// the per-event audit trail a support engineer or reviewer can query by
// event_id, customer, campaign or request_id.
package eventlog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Stage is one step in an event's lifecycle. Values match the event_logs
// CHECK constraint.
type Stage string

const (
	StageIngested     Stage = "ingested"
	StageDuplicate    Stage = "duplicate"
	StageProcessed    Stage = "processed"
	StageRetry        Stage = "retry"
	StageDeadLettered Stage = "dead_lettered"
	StageReplayed     Stage = "replayed"
)

// Level mirrors the event_logs.level CHECK constraint.
type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

// Mode is EVENT_LOG_MODE: which stages get a row.
type Mode string

const (
	ModeAll    Mode = "all"    // every stage (~2 extra rows per event)
	ModeErrors Mode = "errors" // retry, dead_lettered, replayed only
	ModeOff    Mode = "off"
)

// Records reports whether this mode writes a row for stage. The happy path
// (ingested, duplicate, processed) is the bulk of the volume, so "errors"
// mode keeps the table small at scale while still recording every problem
// and every operator action.
func (m Mode) Records(s Stage) bool {
	switch m {
	case ModeAll:
		return true
	case ModeErrors:
		return s == StageRetry || s == StageDeadLettered || s == StageReplayed
	default:
		return false
	}
}

// Entry is one event_logs row. Zero-valued optional fields are stored NULL.
type Entry struct {
	EventID    string
	CustomerID int64 // internal id; 0 → NULL
	CampaignID int64 // internal id; 0 → NULL
	Stage      Stage
	Level      Level
	Message    string
	RequestID  string
	Worker     string
	Attempt    int // 0 → NULL
	Details    map[string]any
}

// Execer is satisfied by pgx.Tx, *pgxpool.Pool and *pgx.Conn.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// insertSQL writes any number of entries in ONE statement: parallel arrays
// unnested row-wise, so a 500-event ingest batch costs one round trip, not 500.
const insertSQL = `
INSERT INTO event_logs (event_id, customer_id, campaign_id, stage, level, message,
                        request_id, worker, attempt, details)
SELECT t.event_id, t.customer_id, t.campaign_id, t.stage, t.level, t.message,
       t.request_id, t.worker, t.attempt, t.details::jsonb
FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::text[],
            $7::text[], $8::text[], $9::int[], $10::text[])
     AS t(event_id, customer_id, campaign_id, stage, level, message,
          request_id, worker, attempt, details)`

// Write inserts the entries the mode records, in one statement on db. It
// returns nil without touching the database when nothing qualifies.
func Write(ctx context.Context, db Execer, mode Mode, entries ...Entry) error {
	kept := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if mode.Records(e.Stage) {
			kept = append(kept, e)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	n := len(kept)
	var (
		eventIDs    = make([]*string, n)
		customerIDs = make([]*int64, n)
		campaignIDs = make([]*int64, n)
		stages      = make([]string, n)
		levels      = make([]string, n)
		messages    = make([]string, n)
		requestIDs  = make([]*string, n)
		workers     = make([]*string, n)
		attempts    = make([]*int32, n)
		details     = make([]string, n)
	)
	for i, e := range kept {
		eventIDs[i] = nilIfEmpty(e.EventID)
		customerIDs[i] = nilIfZero(e.CustomerID)
		campaignIDs[i] = nilIfZero(e.CampaignID)
		stages[i] = string(e.Stage)
		levels[i] = string(e.Level)
		messages[i] = e.Message
		requestIDs[i] = nilIfEmpty(e.RequestID)
		workers[i] = nilIfEmpty(e.Worker)
		if e.Attempt > 0 {
			a := int32(e.Attempt)
			attempts[i] = &a
		}
		details[i] = "{}"
		if len(e.Details) > 0 {
			raw, err := json.Marshal(e.Details)
			if err != nil {
				return fmt.Errorf("eventlog: marshal details: %w", err)
			}
			details[i] = string(raw)
		}
	}
	_, err := db.Exec(ctx, insertSQL, eventIDs, customerIDs, campaignIDs, stages,
		levels, messages, requestIDs, workers, attempts, details)
	return err
}

// WriteIsolated writes entries inside a SAVEPOINT of tx, so a failed log
// insert rolls back only the log rows and never aborts the caller's
// transaction: logging must not be able to block ingest or processing. The
// savepoint costs two extra statements, paid only when the mode records one
// of the entries. The caller decides how to report the returned error.
func WriteIsolated(ctx context.Context, tx pgx.Tx, mode Mode, entries ...Entry) error {
	recorded := false
	for _, e := range entries {
		if mode.Records(e.Stage) {
			recorded = true
			break
		}
	}
	if !recorded {
		return nil
	}
	sp, err := tx.Begin(ctx) // nested Begin = SAVEPOINT
	if err != nil {
		return err
	}
	if err := Write(ctx, sp, mode, entries...); err != nil {
		_ = sp.Rollback(ctx) // ROLLBACK TO SAVEPOINT; outer tx stays usable
		return err
	}
	return sp.Commit(ctx) // RELEASE SAVEPOINT
}

// pruneBatch bounds one retention DELETE so it never holds locks or bloats
// WAL for long; the worker calls Prune periodically and it catches up.
const pruneBatch = 5000

// Prune deletes up to pruneBatch rows older than retentionDays and returns
// how many it removed. Production at high volume would partition event_logs
// by day and DROP old partitions instead (instant, no dead tuples).
func Prune(ctx context.Context, db Execer, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	tag, err := db.Exec(ctx, `
		DELETE FROM event_logs WHERE id IN (
			SELECT id FROM event_logs
			WHERE created_at < now() - make_interval(days => $1)
			LIMIT $2)`, retentionDays, pruneBatch)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

type workerKey struct{}

// WithWorker tags ctx with the worker's consumer name, recorded on the
// processed/retry/dead_lettered rows so a log line points at the process
// that handled the event.
func WithWorker(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, workerKey{}, name)
}

// WorkerFrom returns the worker name stored by WithWorker, or "".
func WorkerFrom(ctx context.Context) string {
	s, _ := ctx.Value(workerKey{}).(string)
	return s
}

// truncate keeps error strings in log rows bounded; a driver error can embed
// a whole query.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ErrorDetail is the details payload for failure stages.
func ErrorDetail(err string) map[string]any {
	return map[string]any{"error": truncate(strings.TrimSpace(err), 500)}
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nilIfZero(n int64) *int64 {
	if n == 0 {
		return nil
	}
	return &n
}
