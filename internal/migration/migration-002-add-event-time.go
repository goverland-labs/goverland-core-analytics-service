package migration

import (
	"gorm.io/gorm"
)

func Migration002AddEventTime(conn *gorm.DB) error {
	queries := []string{
		`alter table proposals_raw add column event_time DateTime default now();`,
		`alter table daos_raw add column event_time DateTime default now();`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
