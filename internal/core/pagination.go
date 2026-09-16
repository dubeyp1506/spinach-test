package core

import (
	"strconv"

	"github.com/gin-gonic/gin"
)

// Cursor-based pagination contract for every list endpoint.
// Request:  ?limit=50&cursor=<opaque>
// Response: {"data": [...], "next_cursor": "...", "has_more": true}

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

type Page struct {
	Limit  int
	Cursor string
}

func ParsePage(c *gin.Context) Page {
	limit := DefaultLimit
	if s := c.Query("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			limit = min(n, MaxLimit)
		}
	}
	return Page{Limit: limit, Cursor: c.Query("cursor")}
}

type ListResponse[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
	HasMore    bool   `json:"has_more"`
}
