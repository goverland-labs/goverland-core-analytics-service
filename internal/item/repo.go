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
	var res []*MonthlyActiveUser
	err := r.db.Raw(`select toStartOfMonth(v.created_at) as PeriodStarted,
       				count(distinct v.voter) as ActiveUsers, countIf(distinct v.voter, v.created_at = firstVote) as NewActiveUsers from votes_view v 
       		    	inner join
					(select min(created_at) as firstVote, voter from votes_view
					where dao_id = ? group by voter) votes
					on v.voter = votes.voter where v.dao_id = ? 
					group by PeriodStarted
					order by PeriodStarted
					with fill step interval 1 month`, id, id).Scan(&res).Error

	return res, err
}

func (r *Repo) GetVoterBucketsByDaoId(id uuid.UUID) ([]*Bucket, error) {
	var res []*Bucket
	err := r.db.Raw(`select GroupId, count() as Voters from
							(select count() as bucket from votes_view
							where dao_id = ? group by voter) as votes_count
							group by multiIf(bucket = 1, 1, bucket = 2, 2, bucket < 5, 3, bucket < 8, 4, bucket < 13, 5, bucket >= 13, 6, 7) as GroupId
							order by GroupId with fill to 7`, id).Scan(&res).Error

	return res, err
}

func (r *Repo) GetExclusiveVoters(id uuid.UUID) (*ExclusiveVoters, error) {
	var res *ExclusiveVoters
	err := r.db.Raw(`select countIf(daoCount = 1), toInt8(countIf(daoCount = 1)/count()*100) from (select voter, uniqExact(dao_id) daoCount from votes_view 
                         where voter in (select distinct(voter) from votes_view where dao_id = ? group by voter) as daos;`, id).Scan(&res).Error

	return res, err
}
