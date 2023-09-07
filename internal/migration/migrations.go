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
                dao_id		 UUID,
                created_at   DateTime,
                proposal_id  String,
                event_type	 String,
                voter		 String
            ) 	ENGINE = NATS
				SETTINGS nats_url = '%s', nats_subjects = 'analytics', nats_format = 'JSONEachRow',
				date_time_input_format = 'best_effort'`, params["nats_url"]),
		`create MATERIALIZED VIEW analytics_view
			(
				dao_id		 UUID,
				created_at   DateTime,
				proposal_id  String,
				event_type	 String,
				voter		 String
			)
				ENGINE = MergeTree
				order by dao_id
				AS
				SELECT *
				FROM analytics`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
