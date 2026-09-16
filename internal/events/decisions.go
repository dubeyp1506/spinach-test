package events

// ingestOutcome is the per-item classification for a validated event.
type ingestOutcome int

const (
	outcomeAccepted ingestOutcome = iota
	outcomeDuplicate
	outcomeRejected // reason carried alongside
)

// isDuplicate classifies the result of
// `INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING id`: no row returned
// means the event_id already exists (CONTRACTS §2 — Postgres UNIQUE is the
// ONLY dedup authority; Redis SETNX is a post-commit hint, never authority).
// Pure — unit-tested without a DB.
func isDuplicate(rowReturned bool) bool {
	return !rowReturned
}

// shouldDeadLetter decides whether an event that just failed processing is
// sent to the DLQ (attempts has already been incremented). Pure — unit-tested.
func shouldDeadLetter(attempts, maxAttempts int) bool {
	return attempts >= maxAttempts
}
