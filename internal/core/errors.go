package core

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ErrorBody is the single error envelope for every endpoint. Never deviate.
type ErrorBody struct {
	Error   string `json:"error"`             // machine-readable code, e.g. "validation_failed"
	Message string `json:"message"`           // human-readable summary
	Details any    `json:"details,omitempty"` // field-level or extra info
}

func RespondError(c *gin.Context, status int, code, msg string, details any) {
	c.Set(ActivityErrorKey, code+": "+msg) // every error lands in the activity log
	c.AbortWithStatusJSON(status, ErrorBody{Error: code, Message: msg, Details: details})
}

func BadRequest(c *gin.Context, msg string, details any) {
	RespondError(c, http.StatusBadRequest, "validation_failed", msg, details)
}

func NotFound(c *gin.Context, what string) {
	RespondError(c, http.StatusNotFound, "not_found", what+" not found", nil)
}

func Internal(c *gin.Context, err error) {
	RespondError(c, http.StatusInternalServerError, "internal_error", "internal error", nil)
}

func Unavailable(c *gin.Context, what string) {
	RespondError(c, http.StatusServiceUnavailable, "dependency_unavailable", what+" unavailable", nil)
}

var ErrNotFound = errors.New("not found")
