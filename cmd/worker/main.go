package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
	"github.com/spinach/martech-engine/internal/events"
	"github.com/spinach/martech-engine/internal/queue"
	"github.com/spinach/martech-engine/internal/store"
	"github.com/spinach/martech-engine/internal/wire"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	core.SetupLogging("worker", cfg.LogLevel, cfg.LogFormat)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.RunMigrations(cfg.DatabaseURL); err != nil {
		slog.Warn("migrations", "err", err)
	}
	pool, err := store.NewPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns, cfg.DBStatementTimeoutMs)
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb, err := queue.NewClient(cfg.RedisURL)
	if err != nil {
		slog.Error("redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	// Processor seam (CONTRACTS §2): customers.Applier applies profile
	// mutations inside the worker's transaction (atomic status+aggregate).
	slog.Info("worker started")
	events.Run(ctx, pool, rdb, cfg, wire.NewProcessor())
	slog.Info("worker stopped")
}
