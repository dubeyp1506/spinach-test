package eventlog

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeExec records statements instead of hitting Postgres.
type fakeExec struct {
	calls [][]any
	sql   []string
}

func (f *fakeExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.sql = append(f.sql, sql)
	f.calls = append(f.calls, args)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func TestModeRecords(t *testing.T) {
	all := []Stage{StageIngested, StageDuplicate, StageProcessed, StageRetry, StageDeadLettered, StageReplayed}
	for _, s := range all {
		assert.True(t, ModeAll.Records(s), "all records %s", s)
		assert.False(t, ModeOff.Records(s), "off records nothing (%s)", s)
	}
	// errors mode: problems + operator actions only — the happy path is the volume.
	for s, want := range map[Stage]bool{
		StageIngested: false, StageDuplicate: false, StageProcessed: false,
		StageRetry: true, StageDeadLettered: true, StageReplayed: true,
	} {
		assert.Equal(t, want, ModeErrors.Records(s), "errors mode, %s", s)
	}
}

// A whole batch is one statement, filtered by mode, with zero values → NULL.
func TestWriteBatchesAndFilters(t *testing.T) {
	f := &fakeExec{}
	err := Write(context.Background(), f, ModeErrors,
		Entry{EventID: "e1", Stage: StageIngested, Level: LevelInfo, Message: "m"},
		Entry{EventID: "e2", CustomerID: 7, Stage: StageRetry, Level: LevelWarn, Message: "m", Attempt: 2,
			Details: ErrorDetail("boom")},
		Entry{Stage: StageReplayed, Level: LevelInfo, Message: "m"},
	)
	require.NoError(t, err)
	require.Len(t, f.calls, 1, "one INSERT for the whole batch")

	args := f.calls[0]
	eventIDs := args[0].([]*string)
	require.Len(t, eventIDs, 2, "ingested filtered out in errors mode")
	assert.Equal(t, "e2", *eventIDs[0])
	assert.Nil(t, eventIDs[1], "empty event_id → NULL")
	assert.Equal(t, int64(7), *args[1].([]*int64)[0])
	assert.Nil(t, args[2].([]*int64)[0], "campaign 0 → NULL")
	assert.Equal(t, int32(2), *args[8].([]*int32)[0])
	assert.Nil(t, args[8].([]*int32)[1], "attempt 0 → NULL")
	assert.JSONEq(t, `{"error":"boom"}`, args[9].([]string)[0])
	assert.Equal(t, "{}", args[9].([]string)[1])
}

func TestWriteNothingToRecord(t *testing.T) {
	f := &fakeExec{}
	require.NoError(t, Write(context.Background(), f, ModeOff, Entry{Stage: StageRetry}))
	require.NoError(t, Write(context.Background(), f, ModeAll))
	assert.Empty(t, f.calls, "no statement when nothing qualifies")
}

func TestErrorDetailTruncates(t *testing.T) {
	d := ErrorDetail("  " + strings.Repeat("x", 2000) + "  ")
	msg := d["error"].(string)
	assert.True(t, strings.HasPrefix(msg, "xxx"))
	assert.LessOrEqual(t, len(msg), 504, "bounded to 500 bytes + ellipsis")
}

func TestWorkerContext(t *testing.T) {
	ctx := WithWorker(context.Background(), "host-42")
	assert.Equal(t, "host-42", WorkerFrom(ctx))
	assert.Equal(t, "", WorkerFrom(context.Background()))
}
