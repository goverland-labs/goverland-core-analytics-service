package item

import (
	"github.com/google/uuid"
	"time"
)

const (
	None              EventType = ""
	ProposalCreated   EventType = "proposal_created"
	ProposalSucceeded EventType = "proposal_succeeded"
	VoteCreated       EventType = "vote_created"
)

type AnalyticsItem struct {
	DaoID      uuid.UUID `json:"dao_id"`
	CreatedAt  time.Time `json:"created_at"`
	ProposalID string    `json:"proposal_id"`
	EventType  EventType `json:"event_type"`
	Voter      string    `json:"voter"`
	DaoNewVote bool      `json:"dao_new_vote"`
}

type MonthlyActiveUser struct {
	PeriodStarted  time.Time
	ActiveUsers    uint64
	NewActiveUsers uint64
}

type EventType string
