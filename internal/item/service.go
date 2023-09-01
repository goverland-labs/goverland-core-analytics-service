package item

import (
	"context"
	"github.com/google/uuid"
	pevents "github.com/goverland-labs/platform-events/events/core"
	"github.com/rs/zerolog/log"
	"time"
)

type Publisher interface {
	PublishJSON(ctx context.Context, subject string, obj any) error
}

type DataProvider interface {
	GetMonthlyActiveUsersByDaoId(id uuid.UUID) ([]*MonthlyActiveUser, error)
}

type Service struct {
	events Publisher
	repo   DataProvider
}

func NewService(p Publisher, r DataProvider) (*Service, error) {
	return &Service{
		events: p,
		repo:   r,
	}, nil
}

func (s *Service) HandleItem(ctx context.Context, ai any) error {
	if err := s.events.PublishJSON(ctx, "analytics", ai); err != nil {
		log.Error().Err(err).Msgf("publish event")
	}

	return nil
}

func (s *Service) GetMonthlyActiveUsers(id uuid.UUID) ([]*MonthlyActiveUser, error) {
	return s.repo.GetMonthlyActiveUsersByDaoId(id)
}

func (s *Service) ConvertToAnalyticsItem(pl pevents.ProposalPayload, et EventType) *AnalyticsItem {
	return &AnalyticsItem{
		DaoID:      pl.DaoID,
		CreatedAt:  time.Unix(int64(pl.Created), 0).UTC(),
		ProposalID: pl.ID,
		EventType:  et,
		Voter:      "",
		DaoNewVote: false,
	}
}

func (s *Service) ConvertVotesToAnalyticsItem(vp pevents.VotesPayload) []*AnalyticsItem {
	res := make([]*AnalyticsItem, len(vp))
	for i, item := range vp {
		res[i] = &AnalyticsItem{
			DaoID:      item.DaoID,
			CreatedAt:  time.Unix(int64(item.Created), 0).UTC(),
			ProposalID: item.ID,
			EventType:  VoteCreated,
			Voter:      item.Voter,
		}
	}

	return res
}
