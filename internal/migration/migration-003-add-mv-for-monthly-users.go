package migration

import (
	"gorm.io/gorm"
)

func Migration003InitMv(conn *gorm.DB) error {
	queries := []string{
		`create table dao_voters_count
			(
				dao_id      UUID,
				month_start Date,
				voters_count AggregateFunction(uniqExact, String)
			) ENGINE = AggregatingMergeTree ORDER BY (dao_id, month_start)`,
		`create MATERIALIZED VIEW dao_voters_count_mv to dao_voters_count AS
			SELECT
				dao_id,
				toStartOfMonth(created_at) AS month_start,
				uniqExactState(voter) as voters_count
			from votes_raw
			group by dao_id, month_start`,
		`create table dao_voters_start
		(
			dao_id              UUID,
			voter               String,
			start_date          AggregateFunction(min, DateTime)
		) ENGINE = AggregatingMergeTree
			  ORDER BY (dao_id, voter);`,
		`CREATE MATERIALIZED VIEW dao_voters_start_mv to dao_voters_start AS
			SELECT
				dao_id,
				voter,
				minState(created_at) as start_date
			from votes_raw
			group by dao_id, voter`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
