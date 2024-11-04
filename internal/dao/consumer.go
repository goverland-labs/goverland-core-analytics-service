package dao

import (
	"context"
	"fmt"
	"time"

	pevents "github.com/goverland-labs/goverland-platform-events/events/core"
	client "github.com/goverland-labs/goverland-platform-events/pkg/natsclient"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/goverland-labs/goverland-core-analytics-service/internal/config"
	"github.com/goverland-labs/goverland-core-analytics-service/internal/item"
	"github.com/goverland-labs/goverland-core-analytics-service/internal/metrics"
	"github.com/goverland-labs/goverland-core-analytics-service/pkg/helpers"
)

const groupName = "dao"

var subjects = []string{
	pevents.SubjectDaoCreated,
	pevents.SubjectDaoUpdated,
}

type closable interface {
	Close() error
}

type storage interface {
	Store(group uint32, items ...Payload) error
}

type Consumer struct {
	conn      *nats.Conn
	consumers []closable
	storage   storage
}

func NewConsumer(nc *nats.Conn, s storage) *Consumer {
	return &Consumer{
		conn:      nc,
		consumers: make([]closable, 0),
		storage:   s,
	}
}

func (c *Consumer) handler(action string) pevents.DaoHandler {
	return func(payload pevents.DaoPayload) error {
		var err error

		defer func(start time.Time) {
			metricHandleHistogram.
				WithLabelValues("handle_dao", metrics.ErrLabelValue(err)).
				Observe(time.Since(start).Seconds())
		}(time.Now())

		eventType := item.None
		switch action {
		case pevents.SubjectDaoCreated:
			eventType = item.DaoCreated
		case pevents.SubjectDaoUpdated:
			eventType = item.DaoUpdated
		}

		err = c.storage.Store(payload.ID.ID(), Payload{
			Action: string(eventType),
			DAO:    helpers.Ptr(payload),
		})

		log.Debug().Str("dao_id", payload.ID.String()).Msg("dao was processed")

		return err
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	group := config.GenerateGroupName(groupName)
	for _, subj := range subjects {
		consumer, err := client.NewConsumer(ctx, c.conn, group, subj, c.handler(subj), client.WithMaxAckPending(10))
		if err != nil {
			return fmt.Errorf("consume for %s/%s: %w", group, subj, err)
		}

		c.consumers = append(c.consumers, consumer)
	}

	log.Info().Msg("dao consumers are started")

	// todo: handle correct stopping the consumer by context
	<-ctx.Done()
	return c.stop()
}

func (c *Consumer) stop() error {
	for _, cs := range c.consumers {
		if err := cs.Close(); err != nil {
			log.Error().Err(err).Msg("cant close dao consumer")
		}
	}

	return nil
}
