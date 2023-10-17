package item

import (
	"github.com/google/uuid"
	events "github.com/goverland-labs/platform-events/events/core"
	"time"
)

const (
	None                        EventType = ""
	DaoCreated                  EventType = "dao_created"
	DaoUpdated                  EventType = "dao_updated"
	ProposalCreated             EventType = "proposal_created"
	ProposalUpdated             EventType = "proposal_updated"
	ProposalVotingStarted       EventType = "proposal_voting_started"
	ProposalVotingEnded         EventType = "proposal_voting_ended"
	ProposalVotingQuorumReached EventType = "proposal_voting_quorum_reached"
	ProposalVotingStartsSoon    EventType = "proposal_voting_starts_soon"
	ProposalVotingEndsSoon      EventType = "proposal_voting_ends_soon"
	VoteCreated                 EventType = "vote_created"
)

var EventTypeByAction = map[string]EventType{
	events.SubjectProposalCreated:             ProposalCreated,
	events.SubjectProposalUpdated:             ProposalUpdated,
	events.SubjectProposalUpdatedState:        ProposalUpdated,
	events.SubjectProposalVotingStarted:       ProposalVotingStarted,
	events.SubjectProposalVotingEnded:         ProposalVotingEnded,
	events.SubjectProposalVotingQuorumReached: ProposalVotingQuorumReached,
	events.SubjectProposalVotingStartsSoon:    ProposalVotingStartsSoon,
	events.SubjectProposalVotingEndsSoon:      ProposalVotingEndsSoon,
}

var BucketMinVotes = map[uint8]string{
	1: "1",
	2: "2",
	3: "3-4",
	4: "5-7",
	5: "8-12",
	6: "13+",
}

type AnalyticsItem struct {
	DaoID          uuid.UUID                `json:"dao_id"`
	EventType      EventType                `json:"event_type"`
	EventTime      time.Time                `json:"event_time"`
	CreatedAt      int                      `json:"created_at"`
	ProposalID     string                   `json:"proposal_id"`
	Voter          string                   `json:"voter"`
	Network        string                   `json:"network"`
	Strategies     []events.StrategyPayload `json:"strategies"`
	Categories     Categories               `json:"categories"`
	FollowersCount int                      `json:"followers_count"`
	ProposalsCount int                      `json:"proposals_count"`
	Author         string                   `json:"author"`
	Type           string                   `json:"type"`
	Title          string                   `json:"title"`
	Body           string                   `json:"body"`
	Choices        Choices                  `json:"choices"`
	Start          int                      `json:"start"`
	End            int                      `json:"end"`
	Quorum         float64                  `json:"quorum"`
	State          string                   `json:"state"`
	Scores         Scores                   `json:"scores"`
	ScoresState    string                   `json:"scores_state"`
	ScoresTotal    float32                  `json:"scores_total"`
	ScoresUpdated  int                      `json:"scores_updated"`
	Votes          int                      `json:"votes"`
	App            string                   `json:"app"`
	Choice         int                      `json:"choice"`
	Vp             float64                  `json:"vp"`
	VpByStrategy   []float64                `json:"vp_by_strategy"`
	VpState        string                   `json:"vp_state"`
}

type MonthlyActiveUser struct {
	PeriodStarted  time.Time
	ActiveUsers    uint64
	NewActiveUsers uint64
}

type ProposalsByMonth struct {
	PeriodStarted  time.Time
	ProposalsCount uint64
}

type Bucket struct {
	GroupId uint8
	Voters  uint64
}

type ExclusiveVoters struct {
	Count   uint32
	Percent uint32
}

type EventType string

type Strategy struct {
	Name    string
	Network string
	Params  map[string]interface{}
}

type Strategies []Strategy

type Categories []string

type Choices []string

type Scores []float32
