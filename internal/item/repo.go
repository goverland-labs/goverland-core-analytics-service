package item

import (
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Repo struct {
	db *gorm.DB
}

func NewRepo(db *gorm.DB) *Repo {
	return &Repo{db: db}
}

func (r *Repo) GetMonthlyActiveUsersByDaoId(id uuid.UUID, period uint32) ([]*MonthlyActiveUser, error) {
	var au, nau []*MonthlyUser
	var err error
	if period == 1 {
		var err = r.db.Raw(`SELECT toStartOfDay(created_at) as PeriodStarted, uniq(voter) as ActiveUsers
								FROM votes_raw where dao_id = ? and PeriodStarted > date_sub(MONTH, 1, toStartOfDay(today()))
								GROUP BY PeriodStarted
								ORDER BY PeriodStarted
								WITH FILL FROM date_sub(MONTH, 1, toStartOfDay(today())) TO date_add(DAY, 1, toStartOfDay(today())) STEP INTERVAL 1 DAY
								SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`, id).
			Scan(&au).
			Error
		if err != nil {
			return nil, err
		}

		err = r.db.Raw(`SELECT PeriodStarted, uniqExact(voter) AS ActiveUsers
							FROM (SELECT toStartOfDay(minMerge(start_date)) as PeriodStarted, voter from dao_voters_start_mv WHERE dao_id = ? group by dao_id, voter) dv
							WHERE PeriodStarted > date_sub(MONTH, 1, toStartOfDay(today())) GROUP BY PeriodStarted
							ORDER BY PeriodStarted
							WITH FILL FROM date_sub(MONTH, 1, toStartOfDay(today())) TO date_add(DAY, 1, toStartOfDay(today())) STEP INTERVAL 1 DAY
							SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`, id).
			Scan(&nau).
			Error
		if err != nil {
			return nil, err
		}
	} else {
		pc := ""
		wpc := ""
		ft := ""
		if period != 0 {
			pc = fmt.Sprintf(" and PeriodStarted > date_sub(MONTH, %d, toStartOfMonth(today())) ", period)
			wpc = fmt.Sprintf(" where PeriodStarted > date_sub(MONTH, %d, toStartOfMonth(today())) ", period)
			ft = fmt.Sprintf("FROM date_sub(MONTH, %d, toStartOfMonth(today())) TO date_add(MONTH, 1, toStartOfMonth(today()))", period-1)
		}

		var err = r.db.Raw(`SELECT month_start AS PeriodStarted,
       							   uniqExactMerge(voters_count) AS ActiveUsers
							 FROM dao_voters_count
								WHERE dao_id = ?`+pc+`
								GROUP BY dao_id, PeriodStarted
								ORDER BY PeriodStarted
								WITH FILL `+ft+` STEP INTERVAL 1 MONTH 
								SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`, id).
			Scan(&au).
			Error
		if err != nil {
			return nil, err
		}

		err = r.db.Raw(`SELECT PeriodStarted,
							   uniqExact(voter) AS ActiveUsers
						FROM (SELECT toStartOfMonth(minMerge(start_date)) as PeriodStarted, voter from dao_voters_start_mv WHERE dao_id = ? group by dao_id, voter) dv
						`+wpc+`GROUP BY PeriodStarted
						ORDER BY PeriodStarted
						WITH FILL `+ft+` STEP INTERVAL 1 MONTH 
						SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`, id).
			Scan(&nau).
			Error
		if err != nil {
			return nil, err
		}
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

func (r *Repo) GetVotesGroupsByDaoId(id uuid.UUID) ([]*Bucket, error) {
	var res []*Bucket
	err := r.db.Raw(`
		SELECT GroupId,
		       count() AS Voters
		FROM (
		    SELECT uniq(proposal_id) AS GroupId
		    FROM votes_raw
		    WHERE dao_id = ?
		    GROUP BY voter
		) AS votes_count
		GROUP BY GroupId
		ORDER BY GroupId
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

func (r *Repo) GetMonthlyNewProposalsByDaoId(id uuid.UUID, period uint32) ([]*ProposalsByMonth, error) {
	var res []*ProposalsByMonth
	var err error
	if period == 1 {
		err = r.db.Raw(`
		SELECT toStartOfDay(created_at) AS PeriodStarted,
		       uniq(proposal_id) AS ProposalsCount,
		       uniqIf(proposal_id, spam=true) AS SpamCount
		FROM proposals_raw 
		WHERE dao_id = ? and created_at >= date_sub(MONTH, 1, toStartOfDay(today()))
		GROUP BY PeriodStarted
		ORDER BY PeriodStarted
		WITH FILL FROM date_sub(MONTH, 1, toStartOfDay(today())) TO date_add(DAY, 1, toStartOfDay(today())) STEP INTERVAL 1 DAY`, id, period).
			Scan(&res).
			Error
	} else {
		ft := ""
		if period != 0 {
			ft = fmt.Sprintf("FROM date_sub(MONTH, %d, toStartOfMonth(today())) TO date_add(MONTH, 1, toStartOfMonth(today()))", period-1)
		}

		err = r.db.Raw(`
		SELECT toStartOfMonth(created_at) AS PeriodStarted,
		       uniq(proposal_id) AS ProposalsCount,
		       uniqIf(proposal_id, spam=true) AS SpamCount
		FROM proposals_raw 
		WHERE dao_id = ? and (0 = ? or PeriodStarted > date_sub(MONTH, ?, toStartOfMonth(today())))
		GROUP BY PeriodStarted
		ORDER BY PeriodStarted
		WITH FILL `+ft+` STEP INTERVAL 1 MONTH`, id, period, period).
			Scan(&res).
			Error
	}

	return res, err
}

func (r *Repo) GetProposalsCountByDaoId(id uuid.UUID) (*FinalProposalCounts, error) {
	var res *FinalProposalCounts
	err := r.db.Raw(`select countIf(status='succeeded') as Succeeded, count() as Finished 
							from (
								select argMax(state, created_at) as status
								from proposals_raw
								where dao_id = ? and state in ('succeeded', 'failed', 'defeated')
								group by proposal_id)`, id).
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

func (r *Repo) GetTopVotersByVp(id uuid.UUID, offset int, limit int, period uint32) ([]*VoterWithVp, error) {
	var res []*VoterWithVp
	var err error
	if period == 0 {
		err = r.db.Raw(`
		select voter as Voter, avg(vp) as VpAvg, uniq(proposal_id) as VotesCount 
			from votes_raw 
				where dao_id = ?
		        group by voter 
		        order by (VpAvg, VotesCount, max(created_at)) desc limit ? offset ?
		        SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, id, limit, offset).
			Scan(&res).
			Error
	} else {
		err = r.db.Raw(`
		select voter as Voter, avg(vp) as VpAvg, uniq(proposal_id) as VotesCount 
			from votes_raw 
				where dao_id = ? and created_at >= date_sub(MONTH, ?, today())
		        group by voter 
		        order by (VpAvg, VotesCount, max(created_at)) desc limit ? offset ?
		        SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, id, period, limit, offset).
			Scan(&res).
			Error
	}

	return res, err
}

func (r *Repo) GetTotalVpAvgForActiveVoters(id uuid.UUID, period uint32) (*VpAvgTotal, error) {
	var res *VpAvgTotal
	var err error
	if period == 0 {
		err = r.db.Raw(`select sum(VpAvg) as VpAvgs, uniq(Voter) as Voters from
                                   (select voter as Voter, avg(vp) as VpAvg
                        			from votes_raw
                        			where dao_id = ?
                        			group by voter) 
		        SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, id).
			Scan(&res).
			Error
	} else {
		err = r.db.Raw(`select sum(VpAvg) as VpAvgs, uniq(Voter) as Voters from
                                   (select voter as Voter, avg(vp) as VpAvg
                        			from votes_raw
                        			where dao_id = ? and created_at >= date_sub(MONTH, ?, today())
                        			group by voter) 
		        SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, id, period).
			Scan(&res).
			Error
	}

	return res, err
}

func (r *Repo) GetVpAvgList(id uuid.UUID, period uint32, price float32) ([]float32, error) {
	var res []float32
	var err error
	if period == 0 {
		err = r.db.Raw(`
							select avg(vp) * ? as VpAvg
							from votes_raw
							where dao_id = ?
							group by voter order by VpAvg
		        SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200`, price, id).
			Scan(&res).
			Error
	} else {
		err = r.db.Raw(`
		select avg(vp) * ? as VpAvg
			from votes_raw 
				where dao_id = ? and created_at >= date_sub(MONTH, ?, today())
		        group by voter order by VpAvg
		        SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, price, id, period).
			Scan(&res).
			Error
	}
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

func (r *Repo) GetGoverlandIndexAdditives() (map[uuid.UUID]float64, error) {
	var res []*TotalForDaos
	var err error
	err = r.db.Raw(`select dao_id as DaoID, argMax(additive, start_at) as Total 
							from goverland_index_additive
								where start_at<=today() and (finish_at>=today() or finish_at is null)
								group by dao_id`).
		Scan(&res).
		Error
	return convertResultToMap(res), err
}

func (r *Repo) GetTokenPrice(id uuid.UUID) (float32, error) {
	var res float32
	var err error
	err = r.db.Raw(`select argMax(price, created_day) 
							from token_price
								where dao_id = ?`, id).
		Scan(&res).
		Error
	return res, err
}

func (r *Repo) GetTopDaos(category string, interval string, pricePeriod string) ([]*TopDao, error) {
	var res []*TopDao
	i, ok := Intervals[interval]
	if !ok {
		i = 1
	}
	err := r.db.Raw(`with tokens as (
    						select dao_id, max(created_at) as period_end, argMax(price, created_at) as current_price, 
								   min(created_day) as period_start, argMin(price, created_at) as period_start_price
    						from token_price where created_at <= now() and created_at >= multiIf(?='1W', date_sub(WEEK, 1, now()), ?='1M', date_sub(MONTH, 1, now()), date_sub(HOUR, 24, now())) 
							and multiIf('new'=?, dao_id in (select distinct dao_id from daos_raw where event_type='dao_created' and created_day >= date_sub(MONTH , 3, today())), true)
							group by dao_id
							),
     						  proposals as (
         					select p.dao_id, proposal_id, argMax(scores_total, event_time) as vp, argMax(votes, event_time) as voters  
							from proposals_raw p where
								 p.dao_id in (select distinct t.dao_id from tokens t where period_end >= date_sub(DAY, 1, now())) and
								 toDateTime("end") <= today() and toDateTime("end") >= multiIf(?=0, date_sub(WEEK, 1, today()), date_sub(MONTH, ?, today()))  
							group by p.dao_id, proposal_id
     						)
						select rowNumberInAllBlocks() + 1 as Index, p.dao_id as DaoID, sum(p.voters) as Voters, uniq(p.proposal_id) as Proposals,
							   sum(p.vp)/Proposals as AvpToken, max(t.current_price) * AvpToken as AvpUsd, max(t.current_price) as TokenPrice,
							   multiIf(max(t.period_start_price) = 0, 0, (TokenPrice - max(t.period_start_price)) / max(t.period_start_price)) as TokenPriceChange
						from proposals p
						inner join tokens t on t.dao_id = p.dao_id
						group by p.dao_id order by AvpUsd desc
						SETTINGS use_query_cache = true, query_cache_min_query_duration = 3000, query_cache_ttl = 43200,
    						query_cache_store_results_of_queries_with_nondeterministic_functions = true`, pricePeriod, pricePeriod, category, i, i).
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
