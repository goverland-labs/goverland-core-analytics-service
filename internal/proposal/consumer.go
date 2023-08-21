package proposal

import (
	"context"
	"fmt"
	"time"

	pevents "github.com/goverland-labs/platform-events/events/core"
	client "github.com/goverland-labs/platform-events/pkg/natsclient"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/goverland-labs/analytics-service/internal/config"
	"github.com/goverland-labs/analytics-service/internal/item"
	"github.com/goverland-labs/analytics-service/internal/metrics"
)

const (
	groupName = "proposal"
)

type closable interface {
	Close() error
}

type Consumer struct {
	conn      *nats.Conn
	service   *item.Service
	consumers []closable
}

func NewConsumer(nc *nats.Conn, s *item.Service) (*Consumer, error) {
	c := &Consumer{
		conn:      nc,
		service:   s,
		consumers: make([]closable, 0),
	}

	return c, nil
}

func (c *Consumer) handler(action string) pevents.ProposalHandler {
	return func(payload pevents.ProposalPayload) error {
		var err error
		defer func(start time.Time) {
			metricHandleHistogram.
				WithLabelValues("handle_proposal", metrics.ErrLabelValue(err)).
				Observe(time.Since(start).Seconds())
		}(time.Now())
		eventType := item.None
		switch action {
		case pevents.SubjectProposalCreated:
			eventType = item.ProposalCreated
		case pevents.SubjectProposalVotingEnded:
			eventType = item.ProposalSucceeded
		}

		err = c.service.HandleItem(context.TODO(), c.service.ConvertToAnalyticsItem(payload, eventType))
		if err != nil {
			log.Error().Err(err).Msg("process proposal")
		}

		log.Debug().Msgf("proposal was processed: %s", payload.ID)

		return err
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	group := config.GenerateGroupName(groupName)
	for _, subj := range []string{pevents.SubjectProposalCreated, pevents.SubjectProposalVotingEnded,
		pevents.SubjectProposalUpdatedState} {
		consumer, err := client.NewConsumer(ctx, c.conn, group, subj, c.handler(subj))
		if err != nil {
			return fmt.Errorf("consume for %s/%s: %w", group, subj, err)
		}

		c.consumers = append(c.consumers, consumer)
	}

	log.Info().Msg("proposal consumers is started")

	// todo: handle correct stopping the consumer by context
	<-ctx.Done()
	return c.stop()
}

func (c *Consumer) stop() error {
	for _, cs := range c.consumers {
		if err := cs.Close(); err != nil {
			log.Error().Err(err).Msg("cant close proposal consumer")
		}
	}

	return nil
}
