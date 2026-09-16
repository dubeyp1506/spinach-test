// Redis Streams implementation of the Producer/Consumer contract (queue.go).
// Owned by the ingestion workstream (A1) per CONTRACTS.md §1.
package queue

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// readBlock caps how long XREADGROUP waits for new messages. Short enough that
// workers notice shutdown promptly, long enough to avoid busy-polling.
const readBlock = 5 * time.Second

// streamMaxLen bounds stream:events via approximate MAXLEN trimming on every
// XADD. The stream is only a pointer buffer — the Postgres event_outbox plus
// the worker's reconcileLoop are the durability layer (CONTRACTS §2) — so
// trimming loses nothing correctness-wise.
const streamMaxLen = 1_000_000

// Streams implements Producer and Consumer on top of Redis Streams.
type Streams struct {
	rdb *redis.Client
	// groups caches consumer groups this instance has already created so we
	// don't pay an XGROUP CREATE round-trip on every Read/ClaimStale poll.
	groups sync.Map // group name -> struct{}
}

func NewStreams(rdb *redis.Client) *Streams {
	return &Streams{rdb: rdb}
}

var (
	_ Producer = (*Streams)(nil)
	_ Consumer = (*Streams)(nil)
)

// Enqueue XADDs the message onto stream:events (CONTRACTS §2), bounding the
// stream with approximate MAXLEN so acked entries don't accumulate forever.
func (s *Streams) Enqueue(ctx context.Context, msg EventMessage) error {
	return s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamEvents,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: map[string]any{
			"event_db_id": msg.EventDBID,
			"event_id":    msg.EventID,
		},
	}).Err()
}

// Depth returns the pending count — messages delivered to a consumer group
// but not yet XACKed — summed across groups via XINFO GROUPS. That is the
// real backlog; XLEN would report all-time entries retained until trimming.
func (s *Streams) Depth(ctx context.Context) (int64, error) {
	groups, err := s.rdb.XInfoGroups(ctx, StreamEvents).Result()
	if err != nil {
		return 0, err
	}
	var pending int64
	for _, g := range groups {
		pending += g.Pending
	}
	return pending, nil
}

// Read creates the consumer group once (cached per Streams instance; a
// NOGROUP error triggers one re-create + retry for post-flush recovery),
// then XREADGROUPs up to n new messages.
func (s *Streams) Read(ctx context.Context, group, consumer string, n int64) ([]Message, error) {
	if err := s.ensureGroup(ctx, group); err != nil {
		return nil, err
	}
	streams, err := s.readGroup(ctx, group, consumer, n)
	if isNoGroupErr(err) {
		// The group vanished after we cached it (e.g. Redis flush): drop the
		// cache entry, re-create once, and retry the read.
		s.groups.Delete(group)
		if gerr := s.ensureGroup(ctx, group); gerr != nil {
			return nil, err
		}
		streams, err = s.readGroup(ctx, group, consumer, n)
	}
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Message
	for _, st := range streams {
		for _, m := range st.Messages {
			out = append(out, toMessage(m))
		}
	}
	return out, nil
}

// ClaimStale reclaims pending entries idle longer than idleMs via XAUTOCLAIM.
// Scans from "0-0" each call; acceptable because claimed messages are processed
// and acked promptly, keeping the pending list small. Group creation is cached
// as in Read, with one re-create + retry on NOGROUP.
func (s *Streams) ClaimStale(ctx context.Context, group, consumer string, idleMs int64, n int64) ([]Message, error) {
	if err := s.ensureGroup(ctx, group); err != nil {
		return nil, err
	}
	msgs, _, err := s.autoClaim(ctx, group, consumer, idleMs, n)
	if isNoGroupErr(err) {
		s.groups.Delete(group)
		if gerr := s.ensureGroup(ctx, group); gerr != nil {
			return nil, err
		}
		msgs, _, err = s.autoClaim(ctx, group, consumer, idleMs, n)
	}
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, toMessage(m))
	}
	return out, nil
}

// Ack XACKs stream entry ids for the group.
func (s *Streams) Ack(ctx context.Context, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.rdb.XAck(ctx, StreamEvents, group, ids...).Err()
}

// PublishDLQ XADDs a permanently-failed message onto stream:events:dlq.
func (s *Streams) PublishDLQ(ctx context.Context, msg EventMessage, errMsg string) error {
	return s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamDLQ,
		Values: map[string]any{
			"event_db_id": msg.EventDBID,
			"event_id":    msg.EventID,
			"error":       errMsg,
		},
	}).Err()
}

func (s *Streams) readGroup(ctx context.Context, group, consumer string, n int64) ([]redis.XStream, error) {
	return s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{StreamEvents, ">"},
		Count:    n,
		Block:    readBlock,
	}).Result()
}

func (s *Streams) autoClaim(ctx context.Context, group, consumer string, idleMs int64, n int64) ([]redis.XMessage, string, error) {
	return s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   StreamEvents,
		Group:    group,
		Consumer: consumer,
		MinIdle:  time.Duration(idleMs) * time.Millisecond,
		Start:    "0-0",
		Count:    n,
	}).Result()
}

// ensureGroup creates the consumer group at most once per Streams instance
// (BUSYGROUP ignored). Callers clear the cache entry and call again when a
// NOGROUP error shows the group was lost server-side.
func (s *Streams) ensureGroup(ctx context.Context, group string) error {
	if _, ok := s.groups.Load(group); ok {
		return nil
	}
	// Start at "0" so rows enqueued before the group existed are still read.
	err := s.rdb.XGroupCreateMkStream(ctx, StreamEvents, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	s.groups.Store(group, struct{}{})
	return nil
}

// isNoGroupErr reports a NOGROUP error: the stream or group was dropped
// server-side (e.g. FLUSHALL) after we cached its creation.
func isNoGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

func toMessage(m redis.XMessage) Message {
	return Message{
		ID: m.ID,
		Payload: EventMessage{
			EventDBID: toInt64(m.Values["event_db_id"]),
			EventID:   toString(m.Values["event_id"]),
		},
		Attempts: int(toInt64(m.Values["attempts"])),
	}
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case float64:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	default:
		return 0
	}
}
