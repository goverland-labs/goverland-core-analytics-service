package item

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
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
								WITH FILL STEP INTERVAL 1 MONTH 
								SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`, id).
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
						WITH FILL STEP INTERVAL 1 MONTH 
						SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`, id).
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
		WITH FILL TO 7 
		SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`, id).
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
			 WHERE voter IN (SELECT distinct(voter) FROM dao_voters_start_mv WHERE dao_id = ?) AS daos GROUP BY voter) 
			 SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 21600`, id).
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
		WHERE dao_id = ? 
		GROUP BY PeriodStarted
		ORDER BY PeriodStarted
		WITH FILL STEP INTERVAL 1 MONTH`, id).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetProposalsCountByDaoId(id uuid.UUID) (*FinalProposalCounts, error) {
	var res *FinalProposalCounts
	err := r.db.Raw(`select uniqIf(proposal_id, state='succeeded') as Succeeded, uniq(proposal_id) as Finished
							from proposals_raw
								where dao_id = ? and state in ('succeeded', 'failed', 'defeated')`, id).
		Scan(&res).
		Error
	return res, err
}

func (r *Repo) GetMutualDaos(id uuid.UUID, limit uint64) ([]*DaoVoters, error) {
	var res []*DaoVoters
	err := r.db.Raw(`
		select dao_id as DaoID, uniq(voter) as VotersCount from dao_voters_start_mv 
		    where voter in (select voter from dao_voters_start_mv where dao_id = ?)
				group by dao_id 
				order by multiIf(dao_id = ?, 1,2), VotersCount desc 
				Limit ?  
				SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 21600`, id, id, limit).
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
		        limit ? 
		        SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, id, id, limit).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetVoterTotalsForPeriods(periodInDays uint32) (*VoterTotals, error) {
	var res *VoterTotals
	err := r.db.Raw(`select uniqIf(voter, dateDiff('day', created_at, today()) <= ?) as VoterTotal,
						     	uniqIf(voter, dateDiff('day', created_at, today()) > ?) as VoterTotalPrevPeriod,
						     	uniqIf((voter, proposal_id), dateDiff('day', created_at, today()) <= ?) as VotesTotal,
							    uniqIf((voter, proposal_id), dateDiff('day', created_at, today()) > ?) as VotesTotalPrevPeriod
						 from votes_raw 
						 	where dateDiff('day', created_at, today()) <= ? and created_at <= today()
    					 SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, periodInDays, periodInDays, periodInDays, periodInDays, 2*periodInDays).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetDaoProposalTotalsForPeriods(periodInDays uint32) (*ActiveDaoProposalTotals, error) {
	var res *ActiveDaoProposalTotals
	err := r.db.Raw(`select uniqIf(dao_id, dateDiff('day', created_at, today()) <= ?) as DaoTotal,
						     	uniqIf(dao_id, dateDiff('day', created_at, today()) > ?) as DaoTotalPrevPeriod,
						     	uniqIf(proposal_id, dateDiff('day', created_at, today()) <= ?) as ProposalTotal,
							    uniqIf(proposal_id, dateDiff('day', created_at, today()) > ?) as ProposalTotalPrevPeriod
						 from proposals_raw 
						 	where dateDiff('day', created_at, today()) <= ?
    					 SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, periodInDays, periodInDays, periodInDays, periodInDays, 2*periodInDays).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetMonthlyDaos() ([]*MonthlyTotal, error) {
	var res []*MonthlyTotal

	var err = r.db.Raw(`select toStartOfMonth(p.created_at) AS PeriodStarted,
		       					   uniq(p.dao_id) AS Total,
		       					   uniqIf(p.dao_id, p.created_at = firstProposalTime) AS TotalOfNew
							FROM proposals_raw p
								INNER JOIN (
									SELECT min(created_at) AS firstProposalTime,
										   dao_id
									FROM proposals_raw
									GROUP BY dao_id
								) first_proposals ON p.dao_id = first_proposals.dao_id
							GROUP BY PeriodStarted
							ORDER BY PeriodStarted
							WITH FILL STEP INTERVAL 1 MONTH`).
		Scan(&res).
		Error
	if err != nil {
		return nil, err
	}

	return res, err
}

func (r *Repo) GetMonthlyProposals() ([]*MonthlyTotal, error) {
	var res []*MonthlyTotal
	err := r.db.Raw(`SELECT toStartOfMonth(created_at) AS PeriodStarted,
       							uniq(proposal_id) AS Total
						  FROM proposals_raw
							GROUP BY PeriodStarted
							ORDER BY PeriodStarted
							WITH FILL STEP INTERVAL 1 MONTH`).
		Scan(&res).
		Error

	return res, err
}

func (r *Repo) GetMonthlyVoters() ([]*MonthlyTotal, error) {
	var au, nau []*MonthlyUser

	var err = r.db.Raw(`SELECT month_start AS PeriodStarted,
       							   uniqMerge(voters_count) AS ActiveUsers
							 FROM voters_monthly_count_mv
								GROUP BY PeriodStarted
								ORDER BY PeriodStarted
								WITH FILL STEP INTERVAL 1 MONTH 
								SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`).
		Scan(&au).
		Error
	if err != nil {
		return nil, err
	}

	err = r.db.Raw(`SELECT PeriodStarted,
							   uniq(voter) AS ActiveUsers
						FROM (SELECT toStartOfMonth(minMerge(start_date)) as PeriodStarted, voter from voters_start_mv group by voter) dv
						GROUP BY PeriodStarted
						ORDER BY PeriodStarted
						WITH FILL STEP INTERVAL 1 MONTH 
						SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`).
		Scan(&nau).
		Error
	if err != nil {
		return nil, err
	}

	res := make([]*MonthlyTotal, len(au))
	nauLen := len(nau)
	for i, muser := range au {
		var nuCount uint64 = 0
		if i < nauLen {
			nuCount = nau[i].ActiveUsers
		}
		res[i] = &MonthlyTotal{
			PeriodStarted: muser.PeriodStarted,
			Total:         muser.ActiveUsers,
			TotalOfNew:    nuCount,
		}
	}

	return res, err
}

func (r *Repo) GetDaoProposalForPeriod(period uint8) (map[uuid.UUID]float64, error) {
	var res []*TotalForDaos
	err := r.db.Raw(`select dao_id as DaoID, uniq(proposal_id) as Total 
					     	from proposals_raw 
						  		where event_type = 'core.proposal.created' and dateDiff('day', created_day, today()) <= ? and created_day <= today()
                                            and proposal_id in (select proposal_id from votes_raw group by proposal_id having uniq(voter) >= 5) group by dao_id`, period).
		Scan(&res).
		Error

	return convertResultToMap(res), err
}

func (r *Repo) GetDaoVotersForPeriod(period uint8) (map[uuid.UUID]float64, error) {
	var res []*TotalForDaos
	var err error
	if period == 0 {
		err = r.db.Raw(`select dao_id as DaoID, uniq(voter) as Total 
							from votes_raw group by dao_id`).
			Scan(&res).
			Error
	} else {
		err = r.db.Raw(`select dao_id as DaoID, uniq(voter) as Total 
							from votes_raw
								where dateDiff('day', created_day, today())<=? group by dao_id`, period).
			Scan(&res).
			Error
	}
	return convertResultToMap(res), err
}

func (r *Repo) GetDaoVotesForPeriod(period uint8) (map[uuid.UUID]float64, error) {
	var res []*TotalForDaos
	var err error
	if period == 0 {
		err = r.db.Raw(`select dao_id as DaoID, uniq(voter, proposal_id) as Total 
							from votes_raw group by dao_id`).
			Scan(&res).
			Error
	} else {
		err = r.db.Raw(`select dao_id as DaoID, uniq(voter, proposal_id) as Total 
							from votes_raw
								where dateDiff('day', created_day, today())<=? group by dao_id`, period).
			Scan(&res).
			Error
	}

	return convertResultToMap(res), err
}

func (r *Repo) GetDaos() ([]uuid.UUID, error) {
	var res []uuid.UUID
	err := r.db.Raw(`select distinct dao_id from daos_raw`).
		Scan(&res).
		Error

	return res, err
}

func convertResultToMap(res []*TotalForDaos) map[uuid.UUID]float64 {
	m := make(map[uuid.UUID]float64)
	for _, v := range res {
		m[v.DaoID] = v.Total
	}

	return m
}
