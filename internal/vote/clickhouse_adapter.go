package vote

import (
	"time"

	"github.com/goverland-labs/goverland-platform-events/events/core"

	"github.com/goverland-labs/analytics-service/pkg/helpers"
)

type ClickhouseAdapter struct {
}

func (c ClickhouseAdapter) GetInsertQuery() string {
	return "INSERT INTO votes_raw (dao_id, proposal_id, created_at, voter, app, choice, vp, vp_by_strategy, vp_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
}

func (c ClickhouseAdapter) Values(v *core.VotePayload) []any {
	createdAt := time.Unix(int64(v.Created), 0)

	return []any{
		v.DaoID,
		v.ProposalID,
		createdAt,
		v.Voter,
		v.App,
		helpers.AsJSON(v.Choice),
		v.Vp,
		v.VpByStrategy,
		v.VpState,
	}
}

func (c ClickhouseAdapter) GetCategoryID(v *core.VotePayload) uint32 {
	return v.DaoID.ID()
}
