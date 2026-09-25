package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spinach/martech-engine/internal/activity"
	"github.com/spinach/martech-engine/internal/ai"
	"github.com/spinach/martech-engine/internal/audience"
	"github.com/spinach/martech-engine/internal/campaigns"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
	"github.com/spinach/martech-engine/internal/customers"
	"github.com/spinach/martech-engine/internal/eventlog"
	"github.com/spinach/martech-engine/internal/events"
	"github.com/spinach/martech-engine/internal/predict"
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
	core.SetupLogging("api", cfg.LogLevel, cfg.LogFormat)

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

	if cfg.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	// Order matters: Recovery is innermost so a handler panic becomes a 500
	// that AccessLog and the activity log still see.
	r.Use(core.RequestID(), core.AccessLog())

	// Activity log: every API operation, recorded off the request path by a
	// batching writer. Mounted on the engine (not the /api/v1 group) so
	// unknown API paths are recorded too. The recorder's context is
	// cancelled only after the server has drained, so requests finishing
	// during shutdown are still recorded.
	recCtx, stopRecorder := context.WithCancel(context.Background())
	recDone := make(chan struct{})
	if cfg.ActivityLogEnabled {
		rec := activity.NewRecorder(pool, cfg.EventLogRetentionDays)
		go func() { defer close(recDone); rec.Run(recCtx) }()
		r.Use(activity.Middleware(rec))
	} else {
		close(recDone)
	}
	r.Use(gin.Recovery())

	v1 := r.Group("/api/v1")
	events.NewService(pool, rdb, cfg).RegisterRoutes(v1)
	customers.New(pool, cfg).RegisterRoutes(v1)
	audience.New(pool, rdb, cfg).RegisterRoutes(v1)
	campaignsSvc := campaigns.New(pool, cfg)
	campaignsSvc.RegisterRoutes(v1)
	ai.New(pool, rdb, cfg, wire.NewMetricsProvider(campaignsSvc)).RegisterRoutes(v1)
	system.RegisterRoutes(v1, pool, rdb, cfg)
	eventlog.RegisterRoutes(v1, pool)
	activity.RegisterRoutes(v1, pool)
	predict.New(pool).RegisterRoutes(v1)

	// Demo topology (CONTRACTS §9): run the event worker in-process so a
	// single free-tier service still processes the queue. Production deploys
	// a separate worker binary instead.
	if cfg.RunEmbeddedWorker {
		go events.Run(ctx, pool, rdb, cfg, wire.NewProcessor())
		slog.Info("embedded worker started")
	}

	r.StaticFS("/app", http.FS(web.FS))
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/app/") })

	// Bounded server: slowloris + runaway-request protection, graceful drain
	// on SIGTERM so deploys don't kill in-flight batches.
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		slog.Info("api listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown", "err", err)
	}
	stopRecorder() // flush the activity entries of the drained requests
	<-recDone
}
