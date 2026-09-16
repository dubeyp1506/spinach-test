package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/queue"
	"github.com/spinach/martech-engine/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if err := store.RunMigrations(cfg.DatabaseURL); err != nil {
		slog.Warn("migrations", "err", err)
	}
	pool, err := store.NewPool(ctx, cfg.DatabaseURL)
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

	r := gin.Default()
	// Module routers are registered here by their owning workstreams.
	_ = pool
	_ = rdb
	_ = cfg

	slog.Info("api listening", "port", cfg.Port)
	if err := r.Run(fmt.Sprintf(":%d", cfg.Port)); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}
