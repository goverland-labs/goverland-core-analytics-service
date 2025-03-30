package migration

import (
	"gorm.io/gorm"
)

func Migration008AddWhitelistDao(conn *gorm.DB) error {
	queries := []string{
		`CREATE TABLE whitelist
				(
					dao_id UUID,
					original_id String,
					feature_type String,
					disabled Bool,
					created_at Date
				)
					ENGINE = TinyLog;`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
