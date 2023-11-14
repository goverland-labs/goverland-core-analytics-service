package migration

import (
	"gorm.io/gorm"
)

func Migration004InitMv(conn *gorm.DB) error {
	queries := []string{
		`create table voters_monthly_count
		(
			month_start Date,
			voters_count AggregateFunction(uniq, String)
		) ENGINE = AggregatingMergeTree ORDER BY month_start`,
		`create MATERIALIZED VIEW voters_monthly_count_mv to voters_monthly_count AS
			SELECT
				toStartOfMonth(created_at) AS month_start,
				uniqState(voter) as voters_count
			from votes_raw
			group by month_start`,
		`create table voters_start
		(
			voter               String,
			start_date          AggregateFunction(min, DateTime)
		) ENGINE = AggregatingMergeTree ORDER BY voter`,
		`CREATE MATERIALIZED VIEW voters_start_mv to voters_start AS
			SELECT
				voter,
				minState(created_at) as start_date
			from votes_raw
			group by voter`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
