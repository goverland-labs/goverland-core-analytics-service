package item

import (
	"context"
	pevents "github.com/goverland-labs/platform-events/events/core"
	"github.com/rs/zerolog/log"
	"time"
)

type Publisher interface {
	PublishJSON(ctx context.Context, subject string, obj any) error
}

type Service struct {
	events Publisher
}

func NewService(p Publisher) (*Service, error) {
	return &Service{
		events: p,
	}, nil
}

func (s *Service) HandleItem(ctx context.Context, ai *AnalyticsItem) error {
	if err := s.events.PublishJSON(ctx, "analytics", ai); err != nil {
		log.Error().Err(err).Msgf("publish event #%s #%s", ai.ProposalID, ai.Voter)
	}

	return nil
}

func (s *Service) ConvertToAnalyticsItem(pl pevents.ProposalPayload, et EventType) *AnalyticsItem {
	return &AnalyticsItem{
		DaoID:      pl.DaoID,
		CreatedAt:  time.Now(),
		ProposalID: pl.ID,
		EventType:  et,
		Voter:      "",
		DaoNewVote: false,
	}
}
