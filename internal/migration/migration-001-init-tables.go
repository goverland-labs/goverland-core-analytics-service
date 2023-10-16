package migration

import (
	"gorm.io/gorm"
)

func Migration001InitTables(conn *gorm.DB) error {
	queries := []string{
		`create table daos_raw (
			dao_id 			UUID,
			event_type  	LowCardinality(String),
			created_day  	Date default toDate(created_at),
			created_at  	DateTime,
			network 		LowCardinality(String),
			strategies		String,
			categories		Array(String),
			followers_count Int32,
			proposals_count	Int32
		) ENGINE = MergeTree ORDER BY (dao_id, created_day)`,
		`create table proposals_raw (
			dao_id          UUID,
			event_type      LowCardinality(String),
			created_day     Date default toDate(created_at),
			created_at      DateTime,
			proposal_id     String,
			network         LowCardinality(String),
			strategies		String,
			author			Nullable(String),
			type			String,
			title			Nullable(String),
			body			Nullable(String),
			choices			Array(String),
			start			Int64,
			end				Int64, 
			quorum			Float32,
			state			LowCardinality(String),
			scores			Array(Float32),
			scores_state	String,
			scores_total	Float32,
			scores_updated	Int32,
			votes			Int32
		) ENGINE = MergeTree ORDER BY (dao_id, created_day)`,
		`create table votes_raw (
			dao_id		    UUID,
			created_day     Date default toDate(created_at),
			created_at      DateTime,
			proposal_id     String,
			voter		    String,
			app				String,
			choice			String,
			vp				Float64,
			vp_by_strategy  Array(Float64),
			vp_state		String
		) ENGINE = MergeTree ORDER BY (dao_id, proposal_id, created_day)`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
