package proposal

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

const groupName = "proposal"

var subjects = []string{
	pevents.SubjectProposalCreated,
	pevents.SubjectProposalUpdated,
	pevents.SubjectProposalUpdatedState,
	pevents.SubjectProposalVotingStarted,
	pevents.SubjectProposalVotingEnded,
	pevents.SubjectProposalVotingQuorumReached,
	pevents.SubjectProposalVotingStartsSoon,
	pevents.SubjectProposalVotingEndsSoon,
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

func NewConsumer(nc *nats.Conn, st storage) *Consumer {
	return &Consumer{
		conn:      nc,
		consumers: make([]closable, 0),
		storage:   st,
	}
}

func (c *Consumer) handler(action string) pevents.ProposalHandler {
	return func(payload pevents.ProposalPayload) error {
		var err error

		defer func(start time.Time) {
			metricHandleHistogram.
				WithLabelValues("handle_proposal", metrics.ErrLabelValue(err)).
				Observe(time.Since(start).Seconds())
		}(time.Now())

		err = c.storage.Store(payload.DaoID.ID(), Payload{
			Action:   action,
			Proposal: helpers.Ptr(payload),
		})

		log.Debug().Str("proposal_id", payload.ID).Msg("proposal was processed")

		return err
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	group := config.GenerateGroupName(groupName)
	for _, subj := range subjects {
		consumer, err := client.NewConsumer(ctx, c.conn, group, subj, c.handler(subj), client.WithMaxAckPending(2))
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
