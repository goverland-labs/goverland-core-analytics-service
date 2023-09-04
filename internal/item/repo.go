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
	err := r.db.Raw(`select date_trunc('month', v.created_at) as PeriodStarted,
       				count(distinct v.voter) as ActiveUsers, countIf(distinct v.voter, v.created_at = firstVote) as NewActiveUsers from analytics_view v 
       		    	inner join
					(select min(created_at) as firstVote, voter from analytics_view
					where dao_id = ? and event_type = 'vote_created' group by voter) votes
					on v.voter = votes.voter where v.dao_id = ? and event_type = 'vote_created'
					group by PeriodStarted
					order by PeriodStarted
					with fill step interval 1 month`, id, id).Scan(&res).Error

	return res, err
}
