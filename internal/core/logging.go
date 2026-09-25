package core

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// SetupLogging installs the process-wide slog default: JSON lines on stdout
// (one object per line — what log shippers like Loki, CloudWatch and Cloud
// Logging parse natively) or human-readable text for local dev. Every line
// carries the service name, so api and worker output can share one sink.
func SetupLogging(service, level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: ParseLevel(level)}
	var h slog.Handler
	if strings.EqualFold(format, "text") {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	l := slog.New(h).With("service", service)
	slog.SetDefault(l)
	return l
}

// ParseLevel maps LOG_LEVEL to a slog level; unknown values fall back to info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type loggerKey struct{}

// WithLogger returns ctx carrying l, so code deep in a call chain logs with
// the fields attached upstream (request_id, worker, event_id) without
// threading them through every signature.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// Log returns the logger stored in ctx, or the process default.
func Log(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}
