// Synthetic data generator for the MarTech engine (CONTRACTS §10).
//
// Generates customers, campaigns, a realistic event funnel (sent -> delivered
// -> opened -> clicked -> converted plus bounced/unsubscribed/complained
// tails), sends rows, §7-consistent engagement_profiles, seeded events_dlq
// rows, and scripts/dupe_batch.json for the dedup demo. All inserted
// event_ids are unique (events.event_id UNIQUE); duplicates are demonstrated
// by re-POSTing the batch file.
//
// Usage:
//
//	DATABASE_URL=postgres://martech:martech@localhost:5432/martech?sslmode=disable \
//	  go run ./cmd/seed -customers 50000 -events 100000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spinach/martech-engine/internal/seedgen"
)

func main() {
	var (
		customers = flag.Int("customers", 50000, "number of customers to generate")
		events    = flag.Int("events", 100000, "minimum total events target (HF customers get 50-500 each, so actual totals usually exceed this)")
		campaigns = flag.Int("campaigns", 20, "number of campaigns to generate")
		seed      = flag.Int64("seed", 42, "deterministic RNG seed")
		dsn       = flag.String("dsn", os.Getenv("DATABASE_URL"), "postgres DSN (default: env DATABASE_URL)")
		truncate  = flag.Bool("truncate", true, "TRUNCATE all tables RESTART IDENTITY CASCADE before seeding")
		dupeFile  = flag.String("dupe-file", "scripts/dupe_batch.json", "output path for the duplicate-ingestion demo batch ('' disables)")
	)
	flag.Parse()

	if *dsn == "" {
		slog.Error("no database DSN: set DATABASE_URL or pass -dsn")
		os.Exit(1)
	}
	if *customers <= 0 || *campaigns <= 0 || *events < 0 {
		slog.Error("invalid sizes", "customers", *customers, "campaigns", *campaigns, "events", *events)
		os.Exit(1)
	}

	ctx := context.Background()
	started := time.Now()

	slog.Info("generating dataset", "customers", *customers, "events", *events,
		"campaigns", *campaigns, "seed", *seed)
	ds := seedgen.Generate(seedgen.Config{
		Customers: *customers,
		Events:    *events,
		Campaigns: *campaigns,
		Seed:      *seed,
	})
	slog.Info("generation done",
		"events", len(ds.Events), "sends", len(ds.Sends),
		"profiles", len(ds.Profiles), "dlq", len(ds.DLQ),
		"dupes", len(ds.Dupes.Events),
		"out_of_order", ds.OutOfOrderCount, "failed", ds.FailedCount,
		"took", time.Since(started).Round(time.Millisecond))

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		slog.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		slog.Error("ping postgres", "err", err)
		os.Exit(1)
	}

	if *truncate {
		slog.Info("truncating tables")
		if _, err := pool.Exec(ctx, `TRUNCATE events, events_dlq, engagement_profiles, sends, event_outbox, customers, campaigns RESTART IDENTITY CASCADE`); err != nil {
			slog.Error("truncate", "err", err)
			os.Exit(1)
		}
	}

	// Single tx: either the whole dataset lands or nothing does.
	tx, err := pool.Begin(ctx)
	if err != nil {
		slog.Error("begin tx", "err", err)
		os.Exit(1)
	}
	defer tx.Rollback(ctx) // no-op after Commit

	custIDs, err := insertCustomers(ctx, tx, ds)
	if err != nil {
		slog.Error("insert customers", "err", err)
		os.Exit(1)
	}
	campIDs, err := insertCampaigns(ctx, tx, ds)
	if err != nil {
		slog.Error("insert campaigns", "err", err)
		os.Exit(1)
	}
	if err := insertEvents(ctx, tx, ds, custIDs, campIDs); err != nil {
		slog.Error("insert events", "err", err)
		os.Exit(1)
	}
	if err := insertSends(ctx, tx, ds, custIDs, campIDs); err != nil {
		slog.Error("insert sends", "err", err)
		os.Exit(1)
	}
	if err := insertProfiles(ctx, tx, ds, custIDs); err != nil {
		slog.Error("insert engagement_profiles", "err", err)
		os.Exit(1)
	}
	if err := insertDLQ(ctx, tx, ds); err != nil {
		slog.Error("insert events_dlq", "err", err)
		os.Exit(1)
	}
	// Keep customers.last_event_at consistent with the seeded events.
	if _, err := tx.Exec(ctx, `UPDATE customers c SET last_event_at = p.last_event_at
		FROM engagement_profiles p WHERE p.customer_id = c.id`); err != nil {
		slog.Error("update customers.last_event_at", "err", err)
		os.Exit(1)
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Error("commit", "err", err)
		os.Exit(1)
	}

	if *dupeFile != "" {
		if err := writeDupeFile(*dupeFile, ds.Dupes); err != nil {
			slog.Error("write dupe file", "err", err)
			os.Exit(1)
		}
		slog.Info("wrote dupe batch", "path", *dupeFile, "events", len(ds.Dupes.Events))
	}

	// Summary counts straight from Postgres.
	counts, err := tableCounts(ctx, pool)
	if err != nil {
		slog.Error("summary counts", "err", err)
		os.Exit(1)
	}
	for _, kv := range counts {
		slog.Info("table", "name", kv.name, "rows", kv.n)
	}
	slog.Info("seed complete", "duration", time.Since(started).Round(time.Millisecond))
}

// insertCustomers bulk-loads customers and returns external_id -> BIGINT id.
func insertCustomers(ctx context.Context, tx pgx.Tx, ds *seedgen.Dataset) (map[string]int64, error) {
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"customers"},
		[]string{"external_id", "email", "attributes", "is_active", "created_at"},
		pgx.CopyFromSlice(len(ds.Customers), func(i int) ([]any, error) {
			c := ds.Customers[i]
			attrs, err := json.Marshal(c.Attributes)
			if err != nil {
				return nil, err
			}
			return []any{c.ExternalID, c.Email, attrs, c.IsActive, c.CreatedAt}, nil
		}))
	if err != nil {
		return nil, err
	}
	return idMap(ctx, tx, "customers")
}

// insertCampaigns bulk-loads campaigns and returns external_id -> BIGINT id.
func insertCampaigns(ctx context.Context, tx pgx.Tx, ds *seedgen.Dataset) (map[string]int64, error) {
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"campaigns"},
		[]string{"external_id", "name", "objective", "channel", "status",
			"frequency_cap", "frequency_window_hours", "audience_filter", "started_at", "created_at"},
		pgx.CopyFromSlice(len(ds.Campaigns), func(i int) ([]any, error) {
			c := ds.Campaigns[i]
			aud, err := json.Marshal(c.AudienceFilter)
			if err != nil {
				return nil, err
			}
			var started any
			if c.StartedAt != nil {
				started = *c.StartedAt
			}
			return []any{c.ExternalID, c.Name, c.Objective, c.Channel, c.Status,
				c.FrequencyCap, c.FrequencyWindowHours, aud, started, c.CreatedAt}, nil
		}))
	if err != nil {
		return nil, err
	}
	return idMap(ctx, tx, "campaigns")
}

func insertEvents(ctx context.Context, tx pgx.Tx, ds *seedgen.Dataset, custIDs, campIDs map[string]int64) error {
	n := len(ds.Events)
	const logEvery = 10000
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"events"},
		[]string{"event_id", "customer_id", "campaign_id", "channel", "type",
			"occurred_at", "received_at", "payload", "status", "attempts",
			"last_error", "processed_at"},
		pgx.CopyFromSlice(n, func(i int) ([]any, error) {
			e := ds.Events[i]
			if i%logEvery == 0 {
				slog.Info("copy events", "row", i, "of", n)
			}
			var camp any
			if e.CampaignIdx >= 0 {
				camp = campIDs[ds.Campaigns[e.CampaignIdx].ExternalID]
			}
			payload, err := json.Marshal(e.Payload)
			if err != nil {
				return nil, err
			}
			var lastErr any
			if e.LastError != nil {
				lastErr = *e.LastError
			}
			var processedAt any
			if e.ProcessedAt != nil {
				processedAt = *e.ProcessedAt
			}
			return []any{e.EventID, custIDs[ds.Customers[e.CustomerIdx].ExternalID],
				camp, e.Channel, e.Type, e.OccurredAt, e.ReceivedAt, payload,
				e.Status, e.Attempts, lastErr, processedAt}, nil
		}))
	return err
}

func insertSends(ctx context.Context, tx pgx.Tx, ds *seedgen.Dataset, custIDs, campIDs map[string]int64) error {
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"sends"},
		[]string{"customer_id", "campaign_id", "channel", "sent_at"},
		pgx.CopyFromSlice(len(ds.Sends), func(i int) ([]any, error) {
			s := ds.Sends[i]
			return []any{custIDs[ds.Customers[s.CustomerIdx].ExternalID],
				campIDs[ds.Campaigns[s.CampaignIdx].ExternalID], s.Channel, s.SentAt}, nil
		}))
	return err
}

func insertProfiles(ctx context.Context, tx pgx.Tx, ds *seedgen.Dataset, custIDs map[string]int64) error {
	now := time.Now().UTC()
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"engagement_profiles"},
		[]string{"customer_id", "total_events", "channel_counts", "positive_events",
			"negative_events", "conversions", "last_event_at", "last_event_type",
			"engagement_score", "score_updated_at", "activity_trend",
			"preferred_channel", "updated_at"},
		pgx.CopyFromSlice(len(ds.Profiles), func(i int) ([]any, error) {
			p := ds.Profiles[i]
			cc, err := json.Marshal(p.ChannelCounts)
			if err != nil {
				return nil, err
			}
			var lastType any
			if p.LastEventType != "" {
				lastType = p.LastEventType
			}
			var pref any
			if p.PreferredChannel != "" {
				pref = p.PreferredChannel
			}
			var lastAt any
			if !p.LastEventAt.IsZero() {
				lastAt = p.LastEventAt
			}
			return []any{custIDs[ds.Customers[p.CustomerIdx].ExternalID],
				p.TotalEvents, cc, p.PositiveEvents, p.NegativeEvents,
				p.Conversions, lastAt, lastType, p.EngagementScore,
				p.ScoreUpdatedAt, p.ActivityTrend, pref, now}, nil
		}))
	return err
}

func insertDLQ(ctx context.Context, tx pgx.Tx, ds *seedgen.Dataset) error {
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"events_dlq"},
		[]string{"event_id", "payload", "error", "attempts", "failed_at"},
		pgx.CopyFromSlice(len(ds.DLQ), func(i int) ([]any, error) {
			d := ds.DLQ[i]
			payload, err := json.Marshal(d.Payload)
			if err != nil {
				return nil, err
			}
			var eventID any
			if d.EventID != nil {
				eventID = *d.EventID
			}
			return []any{eventID, payload, d.Error, d.Attempts, d.FailedAt}, nil
		}))
	return err
}

// idMap reads back external_id -> id inside the seed tx so generated rows
// reference real BIGINT keys (works whether or not -truncate ran).
func idMap(ctx context.Context, tx pgx.Tx, table string) (map[string]int64, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT id, external_id FROM %s", pgx.Identifier{table}.Sanitize()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := make(map[string]int64, 1<<16)
	for rows.Next() {
		var id int64
		var ext string
		if err := rows.Scan(&id, &ext); err != nil {
			return nil, err
		}
		m[ext] = id
	}
	return m, rows.Err()
}

func writeDupeFile(path string, b seedgen.DupeBatch) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

type tableCount struct {
	name string
	n    int64
}

func tableCounts(ctx context.Context, pool *pgxpool.Pool) ([]tableCount, error) {
	tables := []string{"customers", "campaigns", "events", "sends", "engagement_profiles", "events_dlq"}
	out := make([]tableCount, 0, len(tables))
	for _, t := range tables {
		var n int64
		if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", pgx.Identifier{t}.Sanitize())).Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, tableCount{t, n})
	}
	return out, nil
}
