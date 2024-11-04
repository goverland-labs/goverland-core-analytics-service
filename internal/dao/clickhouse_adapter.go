package dao

import (
	"time"

	"github.com/goverland-labs/goverland-platform-events/events/core"

	"github.com/goverland-labs/goverland-core-analytics-service/pkg/helpers"
)

type Payload struct {
	Action string
	DAO    *core.DaoPayload
}

type ClickhouseAdapter struct {
}

func (c ClickhouseAdapter) GetInsertQuery() string {
	return "INSERT INTO daos_raw (dao_id, event_type, created_at, network, strategies, categories, followers_count, proposals_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
}

func (c ClickhouseAdapter) Values(pl Payload) []any {
	return []any{
		pl.DAO.ID,
		pl.Action,
		time.Now(),
		pl.DAO.Network,
		helpers.AsJSON(pl.DAO.Strategies),
		pl.DAO.Categories,
		int32(pl.DAO.FollowersCount),
		int32(pl.DAO.ProposalsCount),
	}
}

func (c ClickhouseAdapter) GetCategoryID(pl Payload) uint32 {
	return pl.DAO.ID.ID()
}
