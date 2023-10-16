package config

type App struct {
	LogLevel    string `env:"LOG_LEVEL" envDefault:"info"`
	Prometheus  Prometheus
	Health      Health
	Pprof       Pprof
	Nats        Nats
	ClickHouse  ClickHouse
	InternalAPI InternalAPI
}
