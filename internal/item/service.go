package item

import (
	"context"

	"github.com/google/uuid"
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

func (s *Service) GetMonthlyActiveUsers(id uuid.UUID) ([]*MonthlyActiveUser, error) {
	return s.repo.GetMonthlyActiveUsersByDaoId(id)
}

func (s *Service) GetVoterBuckets(id uuid.UUID) ([]*Bucket, error) {
	return s.repo.GetVoterBucketsByDaoId(id)
}

func (s *Service) GetExclusiveVoters(id uuid.UUID) (*ExclusiveVoters, error) {
	return s.repo.GetExclusiveVoters(id)
}
