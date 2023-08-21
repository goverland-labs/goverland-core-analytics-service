package internal

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/s-larionov/process-manager"
	gormCh "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"os"
	"os/signal"
	"syscall"

	"github.com/goverland-labs/analytics-service/internal/communicate"
	"github.com/goverland-labs/analytics-service/internal/config"
	"github.com/goverland-labs/analytics-service/internal/item"
	"github.com/goverland-labs/analytics-service/internal/migration"
	"github.com/goverland-labs/analytics-service/internal/proposal"
	"github.com/goverland-labs/analytics-service/pkg/health"
	"github.com/goverland-labs/analytics-service/pkg/prometheus"
)

type Application struct {
	sigChan <-chan os.Signal
	manager *process.Manager
	cfg     config.App
	db      *gorm.DB
}

func NewApplication(cfg config.App) (*Application, error) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	a := &Application{
		sigChan: sigChan,
		cfg:     cfg,
		manager: process.NewManager(),
	}

	err := a.bootstrap()
	if err != nil {
		return nil, err
	}

	return a, nil
}

func (a *Application) Run() {
	a.manager.StartAll()
	a.registerShutdown()
}

func (a *Application) bootstrap() error {
	initializers := []func() error{
		a.initDB,
		// Init Dependencies
		a.initServices,

		// Init Workers: System
		a.initPrometheusWorker,
		a.initHealthWorker,
	}

	for _, initializer := range initializers {
		if err := initializer(); err != nil {
			return err
		}
	}

	return nil
}

func (a *Application) initDB() error {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{a.cfg.ClickHouse.Host},
		Auth: clickhouse.Auth{
			Database: a.cfg.ClickHouse.DB,
			Username: a.cfg.ClickHouse.User,
			Password: a.cfg.ClickHouse.Password,
		},
		Debug: a.cfg.ClickHouse.Debug,
	})

	db, err := gorm.Open(gormCh.New(gormCh.Config{Conn: conn}), &gorm.Config{})
	if err != nil {
		return err
	}

	err = migration.ApplyMigrations(db,
		migration.GetAllMigrations(map[string]string{"nats_url": a.cfg.ClickHouse.NatsUrl}))
	if err != nil {
		return err
	}

	a.db = db

	return err
}

func (a *Application) initServices() error {
	nc, err := nats.Connect(
		a.cfg.Nats.URL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(a.cfg.Nats.MaxReconnects),
		nats.ReconnectWait(a.cfg.Nats.ReconnectTimeout),
	)
	if err != nil {
		return err
	}

	pb, err := communicate.NewPublisher(nc)
	if err != nil {
		return err
	}

	err = a.initProposalConsumer(nc, pb)
	if err != nil {
		return fmt.Errorf("init proposal: %w", err)
	}

	return nil
}

func (a *Application) initProposalConsumer(nc *nats.Conn, pb *communicate.Publisher) error {
	service, err := item.NewService(pb)
	if err != nil {
		return fmt.Errorf("proposal service: %w", err)
	}

	cs, err := proposal.NewConsumer(nc, service)
	if err != nil {
		return fmt.Errorf("proposal consumer: %w", err)
	}

	a.manager.AddWorker(process.NewCallbackWorker("proposal-consumer", cs.Start))

	return nil
}

func (a *Application) initPrometheusWorker() error {
	srv := prometheus.NewServer(a.cfg.Prometheus.Listen, "/metrics")
	a.manager.AddWorker(process.NewServerWorker("prometheus", srv))

	return nil
}

func (a *Application) initHealthWorker() error {
	srv := health.NewHealthCheckServer(a.cfg.Health.Listen, "/status", health.DefaultHandler(a.manager))
	a.manager.AddWorker(process.NewServerWorker("health", srv))

	return nil
}

func (a *Application) registerShutdown() {
	go func(manager *process.Manager) {
		<-a.sigChan

		manager.StopAll()
	}(a.manager)

	a.manager.AwaitAll()
}
