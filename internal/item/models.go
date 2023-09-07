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

var BucketMinVotes = map[uint8]uint64{
	1: 1,
	2: 2,
	3: 3,
	4: 5,
	5: 8,
	6: 13,
}

type AnalyticsItem struct {
	DaoID      uuid.UUID `json:"dao_id"`
	CreatedAt  int       `json:"created_at"`
	ProposalID string    `json:"proposal_id"`
	EventType  EventType `json:"event_type"`
	Voter      string    `json:"voter"`
}

type MonthlyActiveUser struct {
	PeriodStarted  time.Time
	ActiveUsers    uint64
	NewActiveUsers uint64
}

type Bucket struct {
	GroupId uint8
	Voters  uint64
}

type EventType string
