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
	err := r.db.Raw(`select date_trunc('month', created_at) as PeriodStarted, 
       						count(distinct voter) as ActiveUsers, countIf(distinct voter, dao_new_vote) as NewActiveUsers 
						from analytics_view where event_type = 'vote_created' and dao_id = ? 
						                    group by PeriodStarted order by PeriodStarted`, id).Scan(&res).Error

	return res, err
}
