package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port        int
	Env         string
	DatabaseURL string
	RedisURL    string

	WorkerConsumerGroup string
	WorkerBatchSize     int64
	WorkerMaxAttempts   int
	WorkerClaimIdleMs   int64
	WorkerConcurrency   int // parallel processMessage goroutines per consume loop

	LLMPrimary   string // "groq" | "gemini"
	GroqAPIKey   string
	GroqModel    string
	GeminiAPIKey string
	GeminiModel  string
	LLMTimeoutMs int

	RateLimitRPS   int
	RateLimitBurst int

	// Demo topology: run the event worker as a goroutine inside the api
	// binary so free-tier single-service deploys still process events.
	RunEmbeddedWorker bool

	DBMaxConns           int
	DBStatementTimeoutMs int

	// Logging. LogFormat "json" (default) or "text"; LogLevel debug|info|warn|error.
	LogLevel  string
	LogFormat string
	// EventLogMode controls the queryable event_logs table (GET /logs):
	// "all" records every lifecycle step, "errors" only retry/dead_lettered/
	// replayed, "off" disables it. EventLogRetentionDays bounds its size.
	EventLogMode          string
	EventLogRetentionDays int
}

func Load() (*Config, error) {
	c := &Config{
		Port:                  envInt("PORT", 8080),
		Env:                   envStr("ENV", "development"),
		DatabaseURL:           envStr("DATABASE_URL", "postgres://martech:martech@localhost:5433/martech?sslmode=disable"),
		RedisURL:              envStr("REDIS_URL", "redis://localhost:6379/0"),
		WorkerConsumerGroup:   envStr("WORKER_CONSUMER_GROUP", "event-workers"),
		WorkerBatchSize:       int64(envInt("WORKER_BATCH_SIZE", 100)),
		WorkerMaxAttempts:     envInt("WORKER_MAX_ATTEMPTS", 5),
		WorkerClaimIdleMs:     int64(envInt("WORKER_CLAIM_IDLE_MS", 30000)),
		WorkerConcurrency:     envInt("WORKER_CONCURRENCY", 8),
		LLMPrimary:            envStr("LLM_PRIMARY", "groq"),
		GroqAPIKey:            envStr("GROQ_API_KEY", ""),
		GroqModel:             envStr("GROQ_MODEL", "llama-3.1-8b-instant"),
		GeminiAPIKey:          envStr("GEMINI_API_KEY", ""),
		GeminiModel:           envStr("GEMINI_MODEL", "gemini-3.5-flash-lite"),
		LLMTimeoutMs:          envInt("LLM_TIMEOUT_MS", 15000),
		RateLimitRPS:          envInt("RATE_LIMIT_RPS", 500),
		RateLimitBurst:        envInt("RATE_LIMIT_BURST", 1000),
		RunEmbeddedWorker:     envStr("RUN_EMBEDDED_WORKER", "false") == "true",
		DBMaxConns:            envInt("DB_MAX_CONNS", 20),
		DBStatementTimeoutMs:  envInt("DB_STATEMENT_TIMEOUT_MS", 10000),
		LogLevel:              envStr("LOG_LEVEL", "info"),
		LogFormat:             envStr("LOG_FORMAT", "json"),
		EventLogMode:          envStr("EVENT_LOG_MODE", "all"),
		EventLogRetentionDays: envInt("EVENT_LOG_RETENTION_DAYS", 7),
	}
	if c.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	switch c.EventLogMode {
	case "all", "errors", "off":
	default:
		return nil, fmt.Errorf("EVENT_LOG_MODE must be all, errors or off (got %q)", c.EventLogMode)
	}
	return c, nil
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
