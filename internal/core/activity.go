package core

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

// Context keys read by the activity-log middleware (internal/activity).
// They live in core because every module already imports it and modules
// never import each other (CONTRACTS §1).
const (
	ActivitySummaryKey = "activity_summary"
	ActivityErrorKey   = "activity_error"
)

// NoteActivity attaches a one-line, human-readable outcome to the current
// request's activity-log row, e.g. "recommended email (high confidence)".
// Keep it short and free of PII; it is shown in the UI's Logs section.
func NoteActivity(c *gin.Context, format string, args ...any) {
	c.Set(ActivitySummaryKey, fmt.Sprintf(format, args...))
}
