package customers

import (
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spinach/martech-engine/internal/config"
)

// Module bundles the customers workstream for registration in cmd/api.
type Module struct {
	pool   *pgxpool.Pool
	cfg    *config.Config
	engine *Engine
}

func New(pool *pgxpool.Pool, cfg *config.Config) *Module {
	return &Module{pool: pool, cfg: cfg, engine: NewEngine(pool)}
}

// RegisterRoutes mounts the CONTRACTS §4 customer endpoints. The group is
// expected to already sit under /api/v1.
func (m *Module) RegisterRoutes(rg *gin.RouterGroup) {
	// CONTRACTS §4: GET /customers/{id} — profile + live engagement score.
	rg.GET("/customers/:id", m.getCustomer)
	// CONTRACTS §4: GET /customers/{id}/timeline — cursor-paginated events,
	// newest first, ?channel=&type= filters.
	rg.GET("/customers/:id/timeline", m.getTimeline)
}
