// Package events implements event ingestion (POST /api/v1/events), the
// transactional-outbox producer path, and the consumer-group worker loop.
// Contract: CONTRACTS.md §2.
package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// StoredEvent mirrors one row of the events table — the durable source of
// truth handed to the Processor inside the atomic processing tx.
type StoredEvent struct {
	ID          int64           `json:"id"`
	EventID     string          `json:"event_id"`
	CustomerID  int64           `json:"customer_id"`
	CampaignID  *int64          `json:"campaign_id,omitempty"`
	Channel     string          `json:"channel"`
	Type        string          `json:"type"`
	OccurredAt  time.Time       `json:"occurred_at"`
	ReceivedAt  time.Time       `json:"received_at"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	LastError   *string         `json:"last_error,omitempty"`
	ProcessedAt *time.Time      `json:"processed_at,omitempty"`
}

// Processor is the A1/A2 seam (CONTRACTS §2): the worker calls ProcessTx
// inside the same Postgres transaction that locks the events row and marks it
// processed, so profile mutation and completion commit atomically. A2 ships
// customers.Applier satisfying this; integration wires it in cmd/worker.
type Processor interface {
	ProcessTx(ctx context.Context, tx pgx.Tx, evt StoredEvent) error
}

// NoopProcessor lets the worker compile and run standalone until the real
// customers.Applier is wired in.
type NoopProcessor struct{}

func (NoopProcessor) ProcessTx(ctx context.Context, tx pgx.Tx, evt StoredEvent) error {
	return nil
}

// maxBatch is the hard cap from CONTRACTS §2 / openapi EventBatchRequest.
const maxBatch = 500

// eventInput is one inbound event as posted by a client. external_ids only —
// internal BIGINT ids never appear on the wire (CONTRACTS §6).
type eventInput struct {
	EventID    string          `json:"event_id"`
	CustomerID string          `json:"customer_id"`
	CampaignID string          `json:"campaign_id"`
	Channel    string          `json:"channel"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurred_at"` // RFC3339; kept raw so a bad value rejects the item, not the batch
	Payload    json.RawMessage `json:"payload"`
}

// Events items stay raw so a type-malformed item rejects individually in
// rejected[{index,reason}] rather than 400-ing the whole batch (CONTRACTS §2).
type batchRequest struct {
	Events []json.RawMessage `json:"events"`
}

type rejectedItem struct {
	Index  int    `json:"index"`
	Reason string `json:"reason"`
}

// batchResponse is the 202 body: {accepted, duplicates, rejected[]}.
type batchResponse struct {
	Accepted   int            `json:"accepted"`
	Duplicates int            `json:"duplicates"`
	Rejected   []rejectedItem `json:"rejected"`
}
