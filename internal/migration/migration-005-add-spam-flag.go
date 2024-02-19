package migration

import (
	"gorm.io/gorm"
)

func Migration005AddSpamFlag(conn *gorm.DB) error {
	queries := []string{
		`alter table proposals_raw add column spam Bool default false;`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
