package item

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/goverland-labs/analytics-api/protobuf/internalapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

type Server struct {
	internalapi.UnimplementedAnalyticsServer

	service *Service
}

func NewServer(service *Service) *Server {
	return &Server{
		service: service,
	}
}

func (s *Server) GetMonthlyActiveUsers(_ context.Context, req *internalapi.MonthlyActiveUsersRequest) (*internalapi.MonthlyActiveUsersResponse, error) {
	id, err := getDaoUuid(req.GetDaoId())
	if err != nil {
		return nil, err
	}

	users, err := s.service.GetMonthlyActiveUsers(id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no users for this dao ID")
	}

	return &internalapi.MonthlyActiveUsersResponse{
		MonthlyActiveUsers: convertMonthlyActiveUsersToAPI(users),
	}, nil
}

func (s *Server) GetVoterBuckets(_ context.Context, req *internalapi.VoterBucketsRequest) (*internalapi.VoterBucketsResponse, error) {
	id, err := getDaoUuid(req.GetDaoId())
	if err != nil {
		return nil, err
	}

	buckets, err := s.service.GetVoterBuckets(id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no votes for this dao ID")
	}

	return &internalapi.VoterBucketsResponse{
		Groups: convertBucketsToAPI(buckets),
	}, nil
}

func getDaoUuid(daoId string) (uuid.UUID, error) {
	if daoId == "" {
		return uuid.UUID{}, status.Error(codes.InvalidArgument, "invalid dao ID")
	}

	id, err := uuid.Parse(daoId)
	if err != nil {
		return uuid.UUID{}, status.Error(codes.InvalidArgument, "invalid dao ID format")
	}
	return id, nil
}

func convertMonthlyActiveUsersToAPI(users []*MonthlyActiveUser) []*internalapi.MonthlyActiveUsers {
	res := make([]*internalapi.MonthlyActiveUsers, len(users))
	for i, musers := range users {
		res[i] = &internalapi.MonthlyActiveUsers{
			PeriodStarted:  timestamppb.New(musers.PeriodStarted),
			ActiveUsers:    musers.ActiveUsers,
			NewActiveUsers: musers.NewActiveUsers,
		}
	}

	return res
}

func convertBucketsToAPI(buckets []*Bucket) []*internalapi.VoterGroup {
	res := make([]*internalapi.VoterGroup, len(buckets))
	for i, bucket := range buckets {
		res[i] = &internalapi.VoterGroup{
			MinVotes: BucketMinVotes[bucket.GroupId],
			Voters:   bucket.Voters,
		}
	}

	return res
}
