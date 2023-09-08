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
       				count(distinct v.voter) as ActiveUsers, countIf(distinct v.voter, v.created_at = firstVote) as NewActiveUsers from analytics_view v 
       		    	inner join
					(select min(created_at) as firstVote, voter from analytics_view
					where dao_id = ? and event_type = ? group by voter) votes
					on v.voter = votes.voter where v.dao_id = ? and event_type = ?
					group by PeriodStarted
					order by PeriodStarted
					with fill step interval 1 month`, id, VoteCreated, id, VoteCreated).Scan(&res).Error

	return res, err
}

func (r *Repo) GetVoterBucketsByDaoId(id uuid.UUID) ([]*Bucket, error) {
	var res []*Bucket
	err := r.db.Raw(`select GroupId, count() as Voters from
							(select count() as bucket from analytics_view
							where dao_id = ? and event_type = ? group by voter) as votes_count
							group by multiIf(bucket = 1, 1, bucket = 2, 2, bucket < 5, 3, bucket < 8, 4, bucket < 13, 5, bucket >= 13, 6, 7) as GroupId
							order by GroupId with fill to 7`, id, VoteCreated).Scan(&res).Error

	return res, err
}
