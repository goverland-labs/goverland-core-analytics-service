package migration

import (
	"errors"
	"time"

	"gorm.io/gorm"
)

type Migrator func(conn *gorm.DB) error

type Migration struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time
	Version   uint8    `gorm:"unique"`
	Migrator  Migrator `gorm:"-"`
}

func GetAllMigrations() []*Migration {
	return []*Migration{
		NewMigration(1, Migration001InitTables),
		NewMigration(2, Migration002AddEventTime),
	}
}

func NewMigration(version uint8, migrator Migrator) *Migration {
	return &Migration{
		Version:  version,
		Migrator: migrator,
	}
}

func lastAppliedMigrationVersion(conn *gorm.DB) (uint8, error) {
	var result Migration

	err := conn.Model(&Migration{}).Order("version desc").
		First(&result).
		Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	return result.Version, nil
}

func saveMigration(conn *gorm.DB, migration *Migration) error {
	return conn.Create(migration).Error
}

func ApplyMigrations(conn *gorm.DB, migrations []*Migration) error {
	if err := conn.AutoMigrate(&Migration{}); err != nil {
		return err
	}
	lastAppliedVersion, err := lastAppliedMigrationVersion(conn)
	if err != nil {
		return err
	}
	for _, m := range migrations {
		if m.Version <= lastAppliedVersion {
			continue
		}

		if err := m.Migrator(conn); err != nil {
			return err
		}

		if err := saveMigration(conn, m); err != nil {
			return err
		}
	}

	return nil
}
