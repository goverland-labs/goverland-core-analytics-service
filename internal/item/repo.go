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
		    SELECT uniq(proposal_id) AS bucket
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
		SELECT countIf(daoCount = 1) as Exclusive,
		       count() as Total
		FROM (
			 SELECT voter,
					uniq(dao_id) daoCount
			 FROM dao_voters_start_mv
			 WHERE voter IN (SELECT distinct(voter) FROM dao_voters_start_mv WHERE dao_id = ?) AS daos GROUP BY voter)`, id).
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

func (r *Repo) GetProposalsCountByDaoId(id uuid.UUID) (*FinalProposalCounts, error) {
	var res *FinalProposalCounts
	err := r.db.Raw(`select countIf(state='succeeded') as Succeeded, countIf(state in ('succeeded', 'failed', 'defeated')) as Finished 
    							from(
									select proposal_id, argMax(state, created_at) as state from proposals_raw
                             			where dao_id = ?
                                                group by proposal_id)`, id).
		Scan(&res).
		Error
	return res, err
}

func (r *Repo) GetMutualDaos(id uuid.UUID, limit uint64) ([]*Dao, error) {
	var res []*Dao
	err := r.db.Raw(`
		select dao_id as DaoID, uniqExact(voter) as VotersCount from dao_voters_start_mv 
		    where voter in (select voter from dao_voters_start_mv where dao_id = ?)
				group by dao_id 
				order by multiIf(dao_id = ?, 1,2), VotersCount desc 
				Limit ?`, id, id, limit).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetTopVotersByVp(id uuid.UUID, limit uint64) ([]*VoterWithVp, error) {
	var res []*VoterWithVp
	err := r.db.Raw(`
		select voter as Voter, avg(vp) as VpAvg, count() as VotesCount 
			from votes_raw 
				where dao_id = ? and 
				      voter in (select voter from votes_raw where dao_id = ? and dateDiff('month', created_at, today()) <=6)
		        group by voter 
		        order by (VpAvg, VotesCount) desc 
		        limit ?`, id, id, limit).
		Scan(&res).
		Error

	return res, err
}
