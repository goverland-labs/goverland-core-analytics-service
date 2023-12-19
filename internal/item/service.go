package item

import (
	"context"
	"errors"
	pevents "github.com/goverland-labs/platform-events/events/core"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	"github.com/google/uuid"
)

const popularDaoIndexCalculationPeriod = 120

type Publisher interface {
	PublishJSON(ctx context.Context, subject string, obj any) error
}

type DataProvider interface {
	GetMonthlyActiveUsersByDaoId(id uuid.UUID) ([]*MonthlyActiveUser, error)
	GetVoterBucketsByDaoId(id uuid.UUID) ([]*Bucket, error)
	GetExclusiveVotersByDaoId(id uuid.UUID) (*ExclusiveVoters, error)
	GetMonthlyNewProposalsByDaoId(id uuid.UUID) ([]*ProposalsByMonth, error)
	GetProposalsCountByDaoId(id uuid.UUID) (*FinalProposalCounts, error)
	GetMutualDaos(id uuid.UUID, limit uint64) ([]*DaoVoters, error)
	GetTopVotersByVp(id uuid.UUID, limit uint64) ([]*VoterWithVp, error)
	GetVoterTotalsForPeriods(periodInDays uint32) (*VoterTotals, error)
	GetDaoProposalTotalsForPeriods(periodInDays uint32) (*ActiveDaoProposalTotals, error)
	GetMonthlyDaos() ([]*MonthlyTotal, error)
	GetMonthlyProposals() ([]*MonthlyTotal, error)
	GetMonthlyVoters() ([]*MonthlyTotal, error)
	GetDaoProposalForPeriod(period uint8) (map[uuid.UUID]float64, error)
	GetDaoVotersForPeriod(period uint8) (map[uuid.UUID]float64, error)
	GetDaoNewVotersForPeriod(period uint8) (map[uuid.UUID]float64, error)
	GetDaos() ([]uuid.UUID, error)
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
	return s.repo.GetExclusiveVotersByDaoId(id)
}

func (s *Service) GetMonthlyNewProposals(id uuid.UUID) ([]*ProposalsByMonth, error) {
	return s.repo.GetMonthlyNewProposalsByDaoId(id)
}

func (s *Service) GetSucceededProposalsCount(id uuid.UUID) (*FinalProposalCounts, error) {
	return s.repo.GetProposalsCountByDaoId(id)
}

func (s *Service) GetMutualDaos(id uuid.UUID, limit uint64) ([]*MutualDao, error) {
	daos, err := s.repo.GetMutualDaos(id, limit+1)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, status.Error(codes.InvalidArgument, "no daos")
	}
	if len(daos) == 0 {
		return nil, err
	}
	dcount := daos[0].VotersCount
	res := make([]*MutualDao, len(daos)-1)
	for i, dao := range daos[1:] {
		res[i] = &MutualDao{
			DaoID:         dao.DaoID,
			VotersCount:   dao.VotersCount,
			VotersPercent: float32(dao.VotersCount) / float32(dcount) * 100.0,
		}
	}
	return res, nil
}

func (s *Service) GetTopVotersByVp(id uuid.UUID, limit uint64) ([]*VoterWithVp, error) {
	return s.repo.GetTopVotersByVp(id, limit)
}

func (s *Service) GetTotalsForLastPeriods(period uint32) (*EcosystemTotals, error) {
	dp, _ := s.repo.GetDaoProposalTotalsForPeriods(period)
	vv, _ := s.repo.GetVoterTotalsForPeriods(period)
	return &EcosystemTotals{
		Daos: TotalsForTwoPeriods{
			Current:  dp.DaoTotal,
			Previous: dp.DaoTotalPrevPeriod,
		},
		Proposals: TotalsForTwoPeriods{
			Current:  dp.ProposalTotal,
			Previous: dp.ProposalTotalPrevPeriod,
		},
		Voters: TotalsForTwoPeriods{
			Current:  vv.VoterTotal,
			Previous: vv.VoterTotalPrevPeriod,
		},
		Votes: TotalsForTwoPeriods{
			Current:  vv.VotesTotal,
			Previous: vv.VotesTotalPrevPeriod,
		},
	}, nil
}

func (s *Service) GetMonthlyDaos() ([]*MonthlyTotal, error) {
	return s.repo.GetMonthlyDaos()
}

func (s *Service) GetMonthlyProposals() ([]*MonthlyTotal, error) {
	return s.repo.GetMonthlyProposals()
}

func (s *Service) GetMonthlyVoters() ([]*MonthlyTotal, error) {
	return s.repo.GetMonthlyVoters()
}

func (s *Service) processPopularityIndexCalculation(ctx context.Context) error {
	daos, err := s.repo.GetDaos()
	if err != nil {
		return err
	}

	dp, err := s.repo.GetDaoProposalForPeriod(popularDaoIndexCalculationPeriod)
	if err != nil {
		return err
	}

	dv, err := s.repo.GetDaoVotersForPeriod(popularDaoIndexCalculationPeriod)
	if err != nil {
		return err
	}

	dnv, err := s.repo.GetDaoNewVotersForPeriod(popularDaoIndexCalculationPeriod)
	if err != nil {
		return err
	}

	dpt, err := s.repo.GetDaoProposalTotalsForPeriods(popularDaoIndexCalculationPeriod)
	if err != nil {
		return err
	}
	proposalTotal := float64(dpt.ProposalTotal)
	vt, err := s.repo.GetVoterTotalsForPeriods(popularDaoIndexCalculationPeriod)
	if err != nil {
		return err
	}
	voterTotal := float64(vt.VoterTotal)

	for _, dao := range daos {
		// Experimental calculation that can be updated not once
		// Index is based on proposal and voter counts.
		// Number of voters has paramount importance(so the coefficient for voters > the coefficient for proposals).
		// Also 'old' voters has more significance than new voters(that's why we have dv[dao] + (dv[dao]-dnv[dao])).
		index := 900*dp[dao]/proposalTotal + 1000*(2*dv[dao]-dnv[dao])/voterTotal
		if err = s.events.PublishJSON(ctx, pevents.SubjectPopularityIndexUpdated,
			pevents.DaoPayload{ID: dao, PopularityIndex: &index}); err != nil {
			log.Error().Err(err).Msgf("publish dao event #%s", dao)
		}
	}
	return err
}
