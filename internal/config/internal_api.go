package config

type InternalAPI struct {
	Bind string `env:"INTERNAL_API_GRPC_SERVER_BIND" envDefault:":11000"`
}
