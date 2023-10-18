package item

import (
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/goverland-labs/platform-events/events/core"
)

type Repo struct {
	db *gorm.DB
}

func NewRepo(db *gorm.DB) *Repo {
	return &Repo{db: db}
}

func (r *Repo) GetMonthlyActiveUsersByDaoId(id uuid.UUID) ([]*MonthlyActiveUser, error) {
	var res []*MonthlyActiveUser
	err := r.db.Raw(`
		SELECT toStartOfMonth(v.created_at) AS PeriodStarted,
		       count(distinct v.voter) AS ActiveUsers,
		       countIf(distinct v.voter, v.created_at = firstVote) AS NewActiveUsers
		FROM votes_raw v 
		INNER JOIN (
		    SELECT min(created_at) AS firstVote,
		           voter
		    FROM votes_raw
		    WHERE dao_id = ?
		    GROUP BY voter
		) votes ON v.voter = votes.voter
		WHERE v.dao_id = ? 
		GROUP BY PeriodStarted
		ORDER BY PeriodStarted
		WITH FILL STEP INTERVAL 1 MONTH`, id, id).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetVoterBucketsByDaoId(id uuid.UUID) ([]*Bucket, error) {
	var res []*Bucket
	err := r.db.Raw(`
		SELECT GroupId,
		       count() AS Voters
		FROM (
		    SELECT count() AS bucket
		    FROM votes_raw
		    WHERE dao_id = ?
		    GROUP BY voter
		) AS votes_count
		GROUP BY multiIf(bucket = 1, 1, bucket = 2, 2, bucket < 5, 3, bucket < 8, 4, bucket < 13, 5, bucket >= 13, 6, 7) AS GroupId
		ORDER BY GroupId
		WITH FILL TO 7`, id).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetExclusiveVotersByDaoId(id uuid.UUID) (*ExclusiveVoters, error) {
	var res *ExclusiveVoters
	err := r.db.Raw(`
		SELECT countIf(daoCount = 1),
		       toInt8(countIf(daoCount = 1)/count()*100)
		FROM (
		    SELECT voter,
		           uniqExact(dao_id) daoCount
		    FROM votes_raw
			WHERE voter IN (SELECT distinct(voter) FROM votes_raw WHERE dao_id = ? GROUP BY voter) AS daos GROUP BY voter)`, id).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetMonthlyNewProposalsByDaoId(id uuid.UUID) ([]*ProposalsByMonth, error) {
	var res []*ProposalsByMonth
	err := r.db.Raw(`
		SELECT toStartOfMonth(created_at) AS PeriodStarted,
		       count(distinct proposal_id) AS ProposalsCount
		FROM proposals_raw 
		WHERE dao_id = ? and event_type = ? 
		GROUP BY PeriodStarted
		ORDER BY PeriodStarted
		WITH FILL STEP INTERVAL 1 MONTH`, id, core.SubjectProposalCreated).
		Scan(&res).
		Error

	return res, err
}
