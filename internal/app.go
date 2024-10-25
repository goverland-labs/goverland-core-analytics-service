package internal

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/goverland-labs/analytics-api/protobuf/internalapi"
	"github.com/goverland-labs/goverland-platform-events/events/core"
	"github.com/goverland-labs/goverland-platform-events/pkg/natsclient"
	"github.com/nats-io/nats.go"
	"github.com/s-larionov/process-manager"
	gormCh "gorm.io/driver/clickhouse"
	"gorm.io/gorm"

	"github.com/goverland-labs/analytics-service/internal/dao"
	"github.com/goverland-labs/analytics-service/internal/storage"
	"github.com/goverland-labs/analytics-service/pkg/grpcsrv"
	"github.com/goverland-labs/analytics-service/pkg/pprofhandler"

	"github.com/goverland-labs/analytics-service/internal/config"
	"github.com/goverland-labs/analytics-service/internal/item"
	"github.com/goverland-labs/analytics-service/internal/migration"
	"github.com/goverland-labs/analytics-service/internal/proposal"
	"github.com/goverland-labs/analytics-service/internal/token"
	"github.com/goverland-labs/analytics-service/internal/vote"
	"github.com/goverland-labs/analytics-service/pkg/health"
	"github.com/goverland-labs/analytics-service/pkg/prometheus"
)

type Application struct {
	sigChan <-chan os.Signal
	manager *process.Manager
	cfg     config.App
	db      *gorm.DB

	natsPublisher    *natsclient.Publisher
	repo             *item.Repo
	service          *item.Service
	clickhouseConn   *sql.DB
	tokensStorage    *storage.ClickhouseWorker[*core.TokenPricePayload]
	votesStorage     *storage.ClickhouseWorker[*core.VotePayload]
	proposalsStorage *storage.ClickhouseWorker[proposal.Payload]
	daosStorage      *storage.ClickhouseWorker[dao.Payload]
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
		// Init Dependencies
		a.initClickhouse,
		a.initNats,
		a.initServices,

		// Init Workers: Clickhouse Storage Workers (should be before consumers!!!)
		a.initDaosStorageWorker,
		a.initProposalsStorageWorker,
		a.initVotesStorageWorker,
		a.initTokensStorageWorker,

		// Init Workers: Consumers
		a.initDaosConsumerWorker,
		a.initProposalsConsumerWorker,
		a.initVotesConsumerWorker,
		a.initTokensConsumerWorker,

		// Init Workers: Application
		a.initGRPCWorker,
		a.initPopularityIndexWorker,

		// Init Workers: System
		a.initPrometheusWorker,
		a.initHealthWorker,
		a.initPprofWorker,
	}

	for _, initializer := range initializers {
		if err := initializer(); err != nil {
			return err
		}
	}

	return nil
}

func (a *Application) initClickhouse() error {
	a.clickhouseConn = clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{a.cfg.ClickHouse.Host},
		Auth: clickhouse.Auth{
			Database: a.cfg.ClickHouse.DB,
			Username: a.cfg.ClickHouse.User,
			Password: a.cfg.ClickHouse.Password,
		},
		Debug: a.cfg.ClickHouse.Debug,
	})

	db, err := gorm.Open(gormCh.New(gormCh.Config{Conn: a.clickhouseConn}), &gorm.Config{})
	if err != nil {
		return err
	}

	err = migration.ApplyMigrations(db, migration.GetAllMigrations())
	if err != nil {
		return err
	}

	a.db = db
	a.repo = item.NewRepo(a.db)

	return err
}

func (a *Application) initNats() error {
	conn, err := nats.Connect(
		a.cfg.Nats.URL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(a.cfg.Nats.MaxReconnects),
		nats.ReconnectWait(a.cfg.Nats.ReconnectTimeout),
	)
	if err != nil {
		return err
	}

	pb, err := natsclient.NewPublisher(conn)
	if err != nil {
		return err
	}
	a.natsPublisher = pb

	return nil
}

func (a *Application) createNatsConnection() (*nats.Conn, error) {
	conn, err := nats.Connect(
		a.cfg.Nats.URL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(a.cfg.Nats.MaxReconnects),
		nats.ReconnectWait(a.cfg.Nats.ReconnectTimeout),
	)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (a *Application) initServices() error {
	service, err := item.NewService(a.natsPublisher, a.repo)
	if err != nil {
		return fmt.Errorf("service: %w", err)
	}
	a.service = service

	return nil
}

func (a *Application) initDaosStorageWorker() error {
	// TODO: Move parameters to the config
	a.daosStorage = storage.NewClickhouseWorker[dao.Payload]("daos", a.clickhouseConn, dao.ClickhouseAdapter{}, 1000, 5*time.Minute)
	a.manager.AddWorker(process.NewCallbackWorker("daos ch storage", a.daosStorage.Start))

	return nil
}

func (a *Application) initDaosConsumerWorker() error {
	conn, err := a.createNatsConnection()
	if err != nil {
		return err
	}

	worker := dao.NewConsumer(conn, a.daosStorage)
	a.manager.AddWorker(process.NewCallbackWorker("daos consumer", worker.Start))

	return nil
}

func (a *Application) initProposalsStorageWorker() error {
	// TODO: Move parameters to the config
	a.proposalsStorage = storage.NewClickhouseWorker[proposal.Payload]("proposals", a.clickhouseConn, proposal.ClickhouseAdapter{}, 1000, 5*time.Minute)
	a.manager.AddWorker(process.NewCallbackWorker("proposals ch storage", a.proposalsStorage.Start))

	return nil
}

func (a *Application) initProposalsConsumerWorker() error {
	conn, err := a.createNatsConnection()
	if err != nil {
		return err
	}

	worker := proposal.NewConsumer(conn, a.proposalsStorage)
	a.manager.AddWorker(process.NewCallbackWorker("proposals consumer", worker.Start))

	return nil
}

func (a *Application) initVotesStorageWorker() error {
	// TODO: Move parameters to the config
	a.votesStorage = storage.NewClickhouseWorker[*core.VotePayload]("votes", a.clickhouseConn, vote.ClickhouseAdapter{}, 50000, 5*time.Minute)
	a.manager.AddWorker(process.NewCallbackWorker("votes ch storage", a.votesStorage.Start))

	return nil
}

func (a *Application) initVotesConsumerWorker() error {
	conn, err := a.createNatsConnection()
	if err != nil {
		return err
	}

	worker := vote.NewConsumer(conn, a.votesStorage)
	a.manager.AddWorker(process.NewCallbackWorker("votes consumer", worker.Start))

	return nil
}

func (a *Application) initTokensStorageWorker() error {
	a.tokensStorage = storage.NewClickhouseWorker[*core.TokenPricePayload]("tokens", a.clickhouseConn, token.ClickhouseAdapter{}, 50000, 5*time.Minute)
	a.manager.AddWorker(process.NewCallbackWorker("tokens ch storage", a.tokensStorage.Start))

	return nil
}

func (a *Application) initTokensConsumerWorker() error {
	conn, err := a.createNatsConnection()
	if err != nil {
		return err
	}

	worker := token.NewConsumer(conn, a.tokensStorage)
	a.manager.AddWorker(process.NewCallbackWorker("tokens consumer", worker.Start))

	return nil
}

func (a *Application) initGRPCWorker() error {
	srv := grpcsrv.NewGrpcServer()
	internalapi.RegisterAnalyticsServer(srv, item.NewServer(a.service))

	a.manager.AddWorker(grpcsrv.NewGrpcServerWorker("gRPC server", srv, a.cfg.InternalAPI.Bind))

	return nil
}

func (a *Application) initPopularityIndexWorker() error {

	worker := item.NewPopularityWorker(a.service)
	a.manager.AddWorker(process.NewCallbackWorker("popularity index calculation", worker.Process))

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

func (a *Application) initPprofWorker() error {
	if !a.cfg.Pprof.Enabled {
		return nil
	}

	srv := pprofhandler.NewPprofServer(a.cfg.Pprof.Listen)
	a.manager.AddWorker(process.NewServerWorker("pprof", srv))

	return nil
}

func (a *Application) registerShutdown() {
	go func(manager *process.Manager) {
		<-a.sigChan

		manager.StopAll()
	}(a.manager)

	a.manager.AwaitAll()
}
