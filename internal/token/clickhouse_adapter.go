package token

import (
	"github.com/goverland-labs/goverland-platform-events/events/core"
	"time"
)

type ClickhouseAdapter struct {
}

func (c ClickhouseAdapter) GetInsertQuery() string {
	return "INSERT INTO token_price (dao_id, created_at, price) VALUES (?, ?, ?)"
}

func (c ClickhouseAdapter) Values(v *core.TokenPricePayload) []any {

	return []any{
		v.DaoID,
		time.Now(),
		float32(v.Price),
	}
}

func (c ClickhouseAdapter) GetCategoryID(v *core.TokenPricePayload) uint32 {
	return v.DaoID.ID()
}
