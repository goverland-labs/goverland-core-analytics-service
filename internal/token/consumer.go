package token

import (
	"context"
	"fmt"
	"time"

	pevents "github.com/goverland-labs/goverland-platform-events/events/core"
	client "github.com/goverland-labs/goverland-platform-events/pkg/natsclient"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/goverland-labs/analytics-service/internal/config"
	"github.com/goverland-labs/analytics-service/internal/metrics"
	"github.com/goverland-labs/analytics-service/pkg/helpers"
)

const (
	groupName = "token"
)

type closable interface {
	Close() error
}

type storage interface {
	Store(group uint32, items ...*pevents.TokenPricePayload) error
}

type Consumer struct {
	conn      *nats.Conn
	consumers []closable
	storage   storage
}

func NewConsumer(nc *nats.Conn, st storage) *Consumer {
	return &Consumer{
		conn:      nc,
		consumers: make([]closable, 0),
		storage:   st,
	}
}

func (c *Consumer) handler() pevents.TokenPricesHandler {
	return func(payload pevents.TokenPricesPayload) error {
		var err error
		defer func(start time.Time) {
			metricHandleHistogram.
				WithLabelValues("handle_tokens", metrics.ErrLabelValue(err)).
				Observe(time.Since(start).Seconds())
		}(time.Now())

		for _, t := range payload {
			err = c.storage.Store(t.DaoID.ID(), helpers.Ptr(t))

			if err != nil {
				return err
			}
		}

		log.Debug().Int("count", len(payload)).Msg("tokens were processed")

		return err
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	group := config.GenerateGroupName(groupName)

	consumer, err := client.NewConsumer(ctx, c.conn, group, pevents.DaoTokenPriceUpdated, c.handler(), client.WithMaxAckPending(10))
	if err != nil {
		return fmt.Errorf("consume for %s/%s: %w", group, pevents.DaoTokenPriceUpdated, err)
	}

	c.consumers = append(c.consumers, consumer)

	log.Info().Msg("token consumer is started")

	<-ctx.Done()
	return c.stop()
}

func (c *Consumer) stop() error {
	for _, cs := range c.consumers {
		if err := cs.Close(); err != nil {
			log.Error().Err(err).Msg("cant close tokens consumer")
		}
	}

	return nil
}
