package proposal

import (
	"time"

	"github.com/goverland-labs/goverland-platform-events/events/core"

	"github.com/goverland-labs/analytics-service/pkg/helpers"
)

type Payload struct {
	Action   string
	Proposal *core.ProposalPayload
}

type ClickhouseAdapter struct {
}

func (c ClickhouseAdapter) GetInsertQuery() string {
	return "INSERT INTO proposals_raw (dao_id, event_type, created_at, proposal_id, network, strategies, author, type, title, body, choices, start, end, quorum, state, scores, scores_state, scores_total, scores_updated, votes, spam) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
}

func (c ClickhouseAdapter) Values(pl Payload) []any {
	createdAt := time.Unix(int64(pl.Proposal.Created), 0)

	return []any{
		pl.Proposal.DaoID,
		pl.Action,
		createdAt,
		pl.Proposal.ID,
		pl.Proposal.Network,
		helpers.AsJSON(pl.Proposal.Strategies),
		pl.Proposal.Author,
		pl.Proposal.Type,
		pl.Proposal.Title,
		pl.Proposal.Body,
		pl.Proposal.Choices,
		int64(pl.Proposal.Start),
		int64(pl.Proposal.End),
		float32(pl.Proposal.Quorum),
		pl.Proposal.State,
		pl.Proposal.Scores,
		pl.Proposal.ScoresState,
		pl.Proposal.ScoresTotal,
		int32(pl.Proposal.ScoresUpdated),
		int32(pl.Proposal.Votes),
		pl.Proposal.Spam,
	}
}

func (c ClickhouseAdapter) GetCategoryID(pl Payload) uint32 {
	return pl.Proposal.DaoID.ID()
}
