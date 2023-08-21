package config

type ClickHouse struct {
	Host     string `env:"CLICKHOUSE_HOST" envDefault:"127.0.0.1:9000"`
	DB       string `env:"CLICKHOUSE_DB" envDefault:"default"`
	User     string `env:"CLICKHOUSE_USER" envDefault:"default"`
	Password string `env:"CLICKHOUSE_PASSWORD" envDefault:""`
	Debug    bool   `env:"CLICKHOUSE_DEBUG" envDefault:"false"`
	NatsUrl  string `env:"CLICKHOUSE_NATS_URL" envDefault:"127.0.0.1:4222"`
}
