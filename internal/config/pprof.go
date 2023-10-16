package config

type Pprof struct {
	Enabled bool   `env:"PPROF_ENABLED" envDefault:"false"`
	Listen  string `env:"PPROF_LISTEN" envDefault:":6060"`
}
