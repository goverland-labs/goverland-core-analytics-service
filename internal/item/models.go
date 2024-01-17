package item

import (
	"github.com/google/uuid"
	events "github.com/goverland-labs/platform-events/events/core"
	"time"
)

const (
	None       EventType = ""
	DaoCreated EventType = "dao_created"
	DaoUpdated EventType = "dao_updated"
)

var BucketMinVotes = map[uint32]string{
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

type MonthlyUser struct {
	PeriodStarted time.Time
	ActiveUsers   uint64
}

type MonthlyTotal struct {
	PeriodStarted time.Time
	Total         uint64
	TotalOfNew    uint64
}

type ProposalsByMonth struct {
	PeriodStarted  time.Time
	ProposalsCount uint64
}

type Bucket struct {
	GroupId uint32
	Voters  uint64
}

type FinalProposalCounts struct {
	Succeeded uint32
	Finished  uint32
}

type ExclusiveVoters struct {
	Exclusive uint32
	Total     uint32
}

type DaoVoters struct {
	DaoID       uuid.UUID
	VotersCount uint32
}

type TotalForDaos struct {
	DaoID uuid.UUID
	Total float64
}

type MutualDao struct {
	DaoID         uuid.UUID
	VotersCount   uint32
	VotersPercent float32
}

type VoterWithVp struct {
	Voter      string
	VpAvg      float32
	VotesCount uint32
}

type EventType string

type Strategy struct {
	Name    string
	Network string
	Params  map[string]interface{}
}

type VoterTotals struct {
	VoterTotal           uint64
	VoterTotalPrevPeriod uint64
	VotesTotal           uint64
	VotesTotalPrevPeriod uint64
}

type ActiveDaoProposalTotals struct {
	DaoTotal                uint64
	DaoTotalPrevPeriod      uint64
	ProposalTotal           uint64
	ProposalTotalPrevPeriod uint64
}

type TotalsForTwoPeriods struct {
	Current  uint64
	Previous uint64
}
type EcosystemTotals struct {
	Daos      TotalsForTwoPeriods
	Proposals TotalsForTwoPeriods
	Voters    TotalsForTwoPeriods
	Votes     TotalsForTwoPeriods
}

type Strategies []Strategy

type Categories []string

type Choices []string

type Scores []float32
