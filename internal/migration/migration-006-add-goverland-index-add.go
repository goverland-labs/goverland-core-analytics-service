package migration

import (
	"gorm.io/gorm"
)

func Migration006AddGoverlandIndexAdditive(conn *gorm.DB) error {
	queries := []string{
		`CREATE TABLE goverland_index_additive
				(
					dao_id UUID,
					additive Float64,
					start_at Date,
					finish_at Date
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
