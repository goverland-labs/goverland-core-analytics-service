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
	GetVoterBucketsByDaoId(id uuid.UUID) ([]*Bucket, error)
	GetExclusiveVoters(id uuid.UUID) (*ExclusiveVoters, error)
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

func (s *Service) GetVoterBuckets(id uuid.UUID) ([]*Bucket, error) {
	return s.repo.GetVoterBucketsByDaoId(id)
}

func (s *Service) GetExclusiveVoters(id uuid.UUID) (*ExclusiveVoters, error) {
	return s.repo.GetExclusiveVoters(id)
}

func (s *Service) ConvertDaoToAnalyticsItem(pl pevents.DaoPayload, et EventType) *AnalyticsItem {
	createdAt := pl.ActiveSince
	return &AnalyticsItem{
		DaoID:          pl.ID,
		EventType:      et,
		EventTime:      time.Now(),
		CreatedAt:      *createdAt,
		Network:        pl.Network,
		Strategies:     pl.Strategies,
		Categories:     pl.Categories,
		FollowersCount: pl.FollowersCount,
		ProposalsCount: pl.ProposalsCount,
	}
}

func (s *Service) ConvertProposalToAnalyticsItem(pl pevents.ProposalPayload, action string) *AnalyticsItem {
	return &AnalyticsItem{
		DaoID:         pl.DaoID,
		EventType:     EventTypeByAction[action],
		EventTime:     time.Now(),
		CreatedAt:     pl.Created,
		ProposalID:    pl.ID,
		Network:       pl.Network,
		Strategies:    pl.Strategies,
		Author:        pl.Author,
		Type:          pl.Type,
		Title:         pl.Title,
		Body:          pl.Body,
		Choices:       pl.Choices,
		Start:         pl.Start,
		End:           pl.End,
		Quorum:        pl.Quorum,
		State:         pl.State,
		Scores:        pl.Scores,
		ScoresState:   pl.ScoresState,
		ScoresTotal:   pl.ScoresTotal,
		ScoresUpdated: pl.ScoresUpdated,
		Votes:         pl.Votes,
	}
}

func (s *Service) ConvertVotesToAnalyticsItem(vp pevents.VotesPayload) []*AnalyticsItem {
	res := make([]*AnalyticsItem, len(vp))
	for i, item := range vp {
		res[i] = &AnalyticsItem{
			DaoID:        item.DaoID,
			EventType:    VoteCreated,
			EventTime:    time.Now(),
			CreatedAt:    item.Created,
			ProposalID:   item.ProposalID,
			Voter:        item.Voter,
			App:          item.App,
			Choice:       item.Choice,
			Vp:           item.Vp,
			VpByStrategy: item.VpByStrategy,
			VpState:      item.VpState,
		}
	}

	return res
}
