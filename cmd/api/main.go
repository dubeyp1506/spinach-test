package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/spinach/martech-engine/internal/ai"
	"github.com/spinach/martech-engine/internal/audience"
	"github.com/spinach/martech-engine/internal/campaigns"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
	"github.com/spinach/martech-engine/internal/customers"
	"github.com/spinach/martech-engine/internal/events"
	"github.com/spinach/martech-engine/internal/queue"
	"github.com/spinach/martech-engine/internal/store"
	"github.com/spinach/martech-engine/internal/system"
	"github.com/spinach/martech-engine/internal/wire"
	"github.com/spinach/martech-engine/web"
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

	if cfg.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Recovery(), core.RequestID(), core.AccessLog())

	v1 := r.Group("/api/v1")
	events.NewService(pool, rdb, cfg).RegisterRoutes(v1)
	customers.New(pool, cfg).RegisterRoutes(v1)
	audience.New(pool, cfg).RegisterRoutes(v1)
	campaignsSvc := campaigns.New(pool, cfg)
	campaignsSvc.RegisterRoutes(v1)
	ai.New(pool, rdb, cfg, wire.NewMetricsProvider(campaignsSvc)).RegisterRoutes(v1)
	system.RegisterRoutes(v1, pool, rdb, cfg)

	// Demo topology (CONTRACTS §9): run the event worker in-process so a
	// single free-tier service still processes the queue. Production deploys
	// a separate worker binary instead.
	if cfg.RunEmbeddedWorker {
		go events.Run(ctx, pool, rdb, cfg, wire.NewProcessor())
		slog.Info("embedded worker started")
	}

	r.StaticFS("/app", http.FS(web.FS))
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/app/") })

	slog.Info("api listening", "port", cfg.Port)
	if err := r.Run(fmt.Sprintf(":%d", cfg.Port)); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}
