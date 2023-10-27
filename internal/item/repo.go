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
	var au, nau []*MonthlyUser

	var err = r.db.Raw(`SELECT month_start AS PeriodStarted,
       							   uniqExactMerge(voters_count) AS ActiveUsers
							 FROM dao_voters_count
								WHERE dao_id = ?
								GROUP BY dao_id, PeriodStarted
								ORDER BY PeriodStarted
								WITH FILL STEP INTERVAL 1 MONTH`, id).
		Scan(&au).
		Error
	if err != nil {
		return nil, err
	}

	err = r.db.Raw(`SELECT PeriodStarted,
							   uniqExact(voter) AS ActiveUsers
						FROM (SELECT toStartOfMonth(minMerge(start_date)) as PeriodStarted, voter from dao_voters_start_mv WHERE dao_id = ? group by dao_id, voter) dv
						GROUP BY PeriodStarted
						ORDER BY PeriodStarted
						WITH FILL STEP INTERVAL 1 MONTH`, id).
		Scan(&nau).
		Error
	if err != nil {
		return nil, err
	}

	res := make([]*MonthlyActiveUser, len(au))
	nauLen := len(nau)
	for i, muser := range au {
		var nuCount uint64 = 0
		if i < nauLen {
			nuCount = nau[i].ActiveUsers
		}
		res[i] = &MonthlyActiveUser{
			PeriodStarted:  muser.PeriodStarted,
			ActiveUsers:    muser.ActiveUsers,
			NewActiveUsers: nuCount,
		}
	}

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
		SELECT countIf(daoCount = 1) as Count,
		       multiIf(count() = 0, 0, toInt8(countIf(daoCount = 1)/count()*100)) as Percent
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

func (r *Repo) GetPercentSucceededProposalsByDaoId(id uuid.UUID) (uint32, error) {
	var res *FinalProposalCounts
	err := r.db.Raw(`select countIf(state='succeeded') as Succeeded, countIf(state in ('succeeded', 'failed', 'defeated')) as Finished 
    							from(
									select proposal_id, argMax(state, created_at) as state from proposals_raw
                             			where dao_id = ?
                                                group by proposal_id)`, id).
		Scan(&res).
		Error
	if res.Finished == 0 {
		return 0, err
	} else {
		return uint32(float32(res.Succeeded) / float32(res.Finished) * 100), err
	}
}
