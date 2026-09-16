// Package queue defines the event-queue contract. Both the API (producer) and
// workers (consumer group) depend on these interfaces — implementations live
// in this package and are owned by the ingestion workstream.
package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Stream names — fixed contract, do not change without updating CONTRACTS.md.
const (
	StreamEvents = "stream:events" // pending event processing
	StreamDLQ    = "stream:events:dlq"
)

// EventMessage is the payload carried on the stream. It references the events
// row by primary key; the row itself is the durable source of truth.
type EventMessage struct {
	EventDBID int64  `json:"event_db_id"`
	EventID   string `json:"event_id"`
}

type Producer interface {
	// Enqueue publishes a message to the events stream.
	Enqueue(ctx context.Context, msg EventMessage) error
	// Depth returns the approximate pending depth of the events stream.
	Depth(ctx context.Context) (int64, error)
}

type Consumer interface {
	// Read fetches up to n messages for this consumer group, blocking up to
	// the implementation's block timeout. Returns messages needing XACK.
	Read(ctx context.Context, group, consumer string, n int64) ([]Message, error)
	// ClaimStale reclaims messages idle longer than idleMs (failed workers).
	ClaimStale(ctx context.Context, group, consumer string, idleMs int64, n int64) ([]Message, error)
	// Ack marks a message processed.
	Ack(ctx context.Context, group string, ids ...string) error
	// PublishDLQ moves a permanently-failed message to the DLQ stream.
	PublishDLQ(ctx context.Context, msg EventMessage, errMsg string) error
}

type Message struct {
	ID       string // stream entry id, needed for Ack
	Payload  EventMessage
	Attempts int
}

func NewClient(redisURL string) (*redis.Client, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opt), nil
}
