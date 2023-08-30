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

func (s *Server) GetMonthlyActiveUsers(ctx context.Context, req *internalapi.MonthlyActiveUsersRequest) (*internalapi.MonthlyActiveUsersResponse, error) {
	if req.GetDaoId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid dao ID")
	}

	id, err := uuid.Parse(req.GetDaoId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dao ID format")
	}

	users, err := s.service.GetMonthlyActiveUsers(id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no users for this dao ID")
	}

	return &internalapi.MonthlyActiveUsersResponse{
		MonthlyActiveUsers: convertMonthlyActiveUsersToAPI(users),
	}, nil
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
