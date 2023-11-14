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

func (s *Server) GetExclusiveVoters(_ context.Context, req *internalapi.ExclusiveVotersRequest) (*internalapi.ExclusiveVotersResponse, error) {
	id, err := getDaoUuid(req.GetDaoId())
	if err != nil {
		return nil, err
	}

	ev, err := s.service.GetExclusiveVoters(id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no votes for this dao ID")
	}

	return &internalapi.ExclusiveVotersResponse{
		Exclusive: ev.Exclusive,
		Total:     ev.Total,
	}, nil
}

func (s *Server) GetMonthlyNewProposals(_ context.Context, req *internalapi.MonthlyNewProposalsRequest) (*internalapi.MonthlyNewProposalsResponse, error) {
	id, err := getDaoUuid(req.GetDaoId())
	if err != nil {
		return nil, err
	}

	proposals, err := s.service.GetMonthlyNewProposals(id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no proposals for this dao ID")
	}

	return &internalapi.MonthlyNewProposalsResponse{
		ProposalsByMonth: convertMonthlyNewProposalsToAPI(proposals),
	}, nil
}

func (s *Server) GetSucceededProposalsCount(_ context.Context, req *internalapi.SucceededProposalsCountRequest) (*internalapi.SucceededProposalsCountResponse, error) {
	id, err := getDaoUuid(req.GetDaoId())
	if err != nil {
		return nil, err
	}

	spc, err := s.service.GetSucceededProposalsCount(id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no finished proposals for this dao ID")
	}

	return &internalapi.SucceededProposalsCountResponse{
		Succeeded: spc.Succeeded,
		Finished:  spc.Finished,
	}, nil
}

func (s *Server) GetTopVotersByVp(_ context.Context, req *internalapi.TopVotersByVpRequest) (*internalapi.TopVotersByVpResponse, error) {
	id, err := getDaoUuid(req.GetDaoId())
	if err != nil {
		return nil, err
	}

	voters, err := s.service.GetTopVotersByVp(id, req.Limit)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no users for this dao ID")
	}

	return &internalapi.TopVotersByVpResponse{
		VoterWithVp: convertVotersWithVpToAPI(voters),
	}, nil
}

func (s *Server) GetDaosVotersParticipateIn(_ context.Context, req *internalapi.DaosVotersParticipateInRequest) (*internalapi.DaosVotersParticipateInResponse, error) {
	id, err := getDaoUuid(req.GetDaoId())
	if err != nil {
		return nil, err
	}

	daos, err := s.service.GetMutualDaos(id, req.GetLimit())
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no daos")
	}

	return &internalapi.DaosVotersParticipateInResponse{
		DaoVotersParticipateIn: convertMutualDaoToAPI(daos),
	}, nil
}

func (s *Server) GetTotalsForLastPeriods(_ context.Context, req *internalapi.TotalsForLastPeriodsRequest) (*internalapi.TotalsForLastPeriodsResponse, error) {
	totals, err := s.service.GetTotalsForLastPeriods(req.GetPeriodInDays())

	return &internalapi.TotalsForLastPeriodsResponse{
		Daos: &internalapi.Totals{
			CurrentPeriodTotal:  totals.Daos.Current,
			PreviousPeriodTotal: totals.Daos.Previous,
		},
		Proposals: &internalapi.Totals{
			CurrentPeriodTotal:  totals.Proposals.Current,
			PreviousPeriodTotal: totals.Proposals.Previous,
		},
		Voters: &internalapi.Totals{
			CurrentPeriodTotal:  totals.Voters.Current,
			PreviousPeriodTotal: totals.Voters.Previous,
		},
		Votes: &internalapi.Totals{
			CurrentPeriodTotal:  totals.Votes.Current,
			PreviousPeriodTotal: totals.Votes.Previous,
		},
	}, err
}

func (s *Server) GetMonthlyActive(_ context.Context, req *internalapi.MonthlyActiveRequest) (*internalapi.MonthlyActiveResponse, error) {
	var (
		mt  []*MonthlyTotal
		err error
	)
	switch req.Type {
	case internalapi.ObjectType_OBJECT_TYPE_DAO:
		mt, err = s.service.GetMonthlyDaos()
	case internalapi.ObjectType_OBJECT_TYPE_PROPOSAL:
		mt, err = s.service.GetMonthlyProposals()
	case internalapi.ObjectType_OBJECT_TYPE_VOTER:
		mt, err = s.service.GetMonthlyVoters()
	}
	if err != nil {
		return nil, err
	}

	return &internalapi.MonthlyActiveResponse{
		TotalsByMonth: convertMonthlyTotalsToAPI(mt),
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
			Votes:  BucketMinVotes[bucket.GroupId],
			Voters: bucket.Voters,
		}
	}

	return res
}

func convertMonthlyNewProposalsToAPI(proposals []*ProposalsByMonth) []*internalapi.ProposalsByMonth {
	res := make([]*internalapi.ProposalsByMonth, len(proposals))
	for i, mp := range proposals {
		res[i] = &internalapi.ProposalsByMonth{
			PeriodStarted:  timestamppb.New(mp.PeriodStarted),
			ProposalsCount: mp.ProposalsCount,
		}
	}

	return res
}

func convertVotersWithVpToAPI(voters []*VoterWithVp) []*internalapi.VoterWithVp {
	res := make([]*internalapi.VoterWithVp, len(voters))
	for i, voter := range voters {
		res[i] = &internalapi.VoterWithVp{
			Voter:      voter.Voter,
			VpAvg:      voter.VpAvg,
			VotesCount: voter.VotesCount,
		}
	}

	return res
}

func convertMutualDaoToAPI(daos []*MutualDao) []*internalapi.DaoVotersParticipateIn {
	res := make([]*internalapi.DaoVotersParticipateIn, len(daos))
	for i, dao := range daos {
		res[i] = &internalapi.DaoVotersParticipateIn{
			DaoId:         dao.DaoID.String(),
			VotersCount:   dao.VotersCount,
			PercentVoters: dao.VotersPercent,
		}
	}

	return res
}

func convertMonthlyTotalsToAPI(mt []*MonthlyTotal) []*internalapi.TotalsByMonth {
	res := make([]*internalapi.TotalsByMonth, len(mt))
	for i, t := range mt {
		res[i] = &internalapi.TotalsByMonth{
			PeriodStarted:   timestamppb.New(t.PeriodStarted),
			Total:           t.Total,
			NewObjectsTotal: t.TotalOfNew,
		}
	}

	return res
}
