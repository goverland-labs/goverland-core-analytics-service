package migration

import (
	"gorm.io/gorm"
)

func Migration007TokenPriceTable(conn *gorm.DB) error {
	queries := []string{
		`create table token_price (
			dao_id 			UUID,
			created_day  	Date default toDate(created_at),
			created_at  	DateTime,
			price	        Float32
		) ENGINE = MergeTree ORDER BY (dao_id, created_day);`,
	}

	for _, query := range queries {
		if err := conn.Exec(query).Error; err != nil {
			return err
		}
	}

	return nil
}
