package migration

import (
	"fmt"
	"gorm.io/gorm"
)

func GetAllMigrations(params map[string]string) []*Migration {
	return []*Migration{
		NewMigration(1, initDatabase, params),
	}
}

func initDatabase(conn *gorm.DB, params map[string]string) error {
	queries := []string{
		fmt.Sprintf(`create table analytics
            (
                dao_id 			UUID,
				event_type  	LowCardinality(String),
				event_time  	DateTime,
				created_at  	DateTime,
				proposal_id 	String,
				voter 			String,
				network 		LowCardinality(String),
				strategies		Array(String),
				categories		Array(String),
				followers_count Int32,
				proposals_count	Int32,
				author			String,
				type			String,
				title			String,
				body			String,
				choices			Array(String),
				start			Int32,
				end				Int32, 
				quorum			Float32,
				state			LowCardinality(String),
				scores			Array(Float32),
				scores_state	String,
				scores_total	Float32,
				scores_updated	Int32,
				votes			Int32,
				app				String,
				choice			Int32,
				vp				Float64,
				vp_by_strategy  Array(Float64),
				vp_state		String
            ) 	ENGINE = NATS
				SETTINGS nats_url = '%s', nats_subjects = 'analytics', nats_format = 'JSONEachRow',
				date_time_input_format = 'best_effort'`, params["nats_url"]),
		`create MATERIALIZED VIEW votes_view
			(
				dao_id		    UUID,
				created_at      DateTime,
				proposal_id     String,
				voter		    String,
				app				String,
				choice			Int32,
				vp				Float64,
				vp_by_strategy  Array(Float64),
				vp_state		String
			)
				ENGINE = MergeTree
				order by dao_id
				AS
				SELECT dao_id, created_at, proposal_id, voter, app, choice, vp, vp_by_strategy, vp_state
				FROM analytics where event_type = 'vote_created'`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
