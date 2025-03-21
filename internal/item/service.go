package item

import (
	"context"
	"errors"
	pevents "github.com/goverland-labs/goverland-platform-events/events/core"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"math"

	"github.com/google/uuid"
)

const popularDaoIndexCalculationPeriod = 90

type Publisher interface {
	PublishJSON(ctx context.Context, subject string, obj any) error
}

type DataProvider interface {
	GetMonthlyActiveUsersByDaoId(id uuid.UUID, period uint32) ([]*MonthlyActiveUser, error)
	GetVoterBucketsByDaoId(id uuid.UUID) ([]*Bucket, error)
	GetVotesGroupsByDaoId(id uuid.UUID) ([]*Bucket, error)
	GetExclusiveVotersByDaoId(id uuid.UUID) (*ExclusiveVoters, error)
	GetMonthlyNewProposalsByDaoId(id uuid.UUID, period uint32) ([]*ProposalsByMonth, error)
	GetProposalsCountByDaoId(id uuid.UUID) (*FinalProposalCounts, error)
	GetMutualDaos(id uuid.UUID, limit uint64) ([]*DaoVoters, error)
	GetTopVotersByVp(id uuid.UUID, limit int, offset int, period uint32) ([]*VoterWithVp, error)
	GetTotalVpAvgForActiveVoters(id uuid.UUID, period uint32) (*VpAvgTotal, error)
	GetVoterTotalsForPeriods(periodInDays uint32) (*VoterTotals, error)
	GetDaoProposalTotalsForPeriods(periodInDays uint32) (*ActiveDaoProposalTotals, error)
	GetMonthlyDaos() ([]*MonthlyTotal, error)
	GetMonthlyProposals() ([]*MonthlyTotal, error)
	GetMonthlyVoters() ([]*MonthlyTotal, error)
	GetDaoProposalForPeriod(period uint8) (map[uuid.UUID]float64, error)
	GetDaoVotersForPeriod(period uint8) (map[uuid.UUID]float64, error)
	GetDaoVotesForPeriod(period uint8) (map[uuid.UUID]float64, error)
	GetGoverlandIndexAdditives() (map[uuid.UUID]float64, error)
	GetDaos() ([]uuid.UUID, error)
	GetVpAvgList(id uuid.UUID, period uint32, price float32) ([]float32, error)
	GetTokenPrice(id uuid.UUID) (float32, error)
	GetTopDaos(category string, interval string, pricePeriod string) ([]*TopDao, error)
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

func (s *Service) GetMonthlyActiveUsers(id uuid.UUID, period uint32) ([]*MonthlyActiveUser, error) {
	return s.repo.GetMonthlyActiveUsersByDaoId(id, period)
}

func (s *Service) GetVoterBuckets(id uuid.UUID) ([]*Bucket, error) {
	return s.repo.GetVoterBucketsByDaoId(id)
}

func (s *Service) GetVotesGroups(id uuid.UUID) ([]*Bucket, error) {
	return s.repo.GetVotesGroupsByDaoId(id)
}

func (s *Service) GetExclusiveVoters(id uuid.UUID) (*ExclusiveVoters, error) {
	return s.repo.GetExclusiveVotersByDaoId(id)
}

func (s *Service) GetMonthlyNewProposals(id uuid.UUID, period uint32) ([]*ProposalsByMonth, error) {
	return s.repo.GetMonthlyNewProposalsByDaoId(id, period)
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

func (s *Service) GetTopVotersByVp(id uuid.UUID, offset uint32, limit uint32, period uint32) ([]*VoterWithVp, error) {
	return s.repo.GetTopVotersByVp(id, int(offset), int(limit), period)
}

func (s *Service) GetTotalVpAvg(id uuid.UUID, period uint32) (*VpAvgTotal, error) {
	return s.repo.GetTotalVpAvgForActiveVoters(id, period)
}

func (s *Service) GetVpAvgList(id uuid.UUID, period uint32, minBalance float32) (*VpHistogram, error) {
	price, err := s.repo.GetTokenPrice(id)
	if err != nil || price <= 0 {
		return nil, err
	}
	list, _ := s.repo.GetVpAvgList(id, period, price)
	var avpTotal float32 = 0
	voterCutted := 0
	for _, vp := range list {
		if vp < minBalance {
			voterCutted++
			avpTotal += vp
		} else {
			break
		}
	}
	avpTotalCutted := avpTotal

	vps := list[voterCutted:]
	bins := make([]Bin, 0)
	if len(vps) > 0 {
		var minValue float64 = 0
		if vps[0] > 1 {
			minValue = math.Log2(float64(vps[0]))
		}
		var maxValue float64 = 0
		if vps[len(vps)-1] > 1 {
			maxValue = math.Log2(float64(vps[len(vps)-1]))
		}
		numberBins := int(math.Floor(maxValue-minValue) + 1)
		bins = make([]Bin, numberBins)
		inputIndex := 0
		for i := 0; i < numberBins; i++ {
			ub := math.Floor(math.Pow(2, minValue+float64(i+1)))
			binCount := 0
			var binAvpTotal float32 = 0
			for j := inputIndex; j < len(vps); j++ {
				if float64(vps[j]) < ub {
					binCount++
					binAvpTotal += vps[j]
				} else {
					bins[i] = Bin{UpperBound: float32(ub), Count: uint32(binCount), TotalAvp: binAvpTotal}
					avpTotal += binAvpTotal
					inputIndex = j
					binCount = 0
					binAvpTotal = 0
					break
				}
			}
			if binCount != 0 {
				bins[i] = Bin{UpperBound: float32(ub), Count: uint32(binCount), TotalAvp: binAvpTotal}
				avpTotal += binAvpTotal
			}

		}
	}

	return &VpHistogram{
		VpValue:        price,
		VotersTotal:    uint32(len(list)),
		VotersCutted:   uint32(voterCutted),
		AvpTotal:       avpTotal,
		AvpTotalCutted: avpTotalCutted,
		Bins:           bins,
	}, nil
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

func (s *Service) GetTopDaos(category string, interval string, pricePeriod string) ([]*TopDao, error) {
	return s.repo.GetTopDaos(category, interval, pricePeriod)
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

	dvo, err := s.repo.GetDaoVotersForPeriod(0)
	if err != nil {
		return err
	}

	dvs, err := s.repo.GetDaoVotesForPeriod(popularDaoIndexCalculationPeriod)
	if err != nil {
		return err
	}

	dvso, err := s.repo.GetDaoVotesForPeriod(0)
	if err != nil {
		return err
	}

	dadditives, err := s.repo.GetGoverlandIndexAdditives()
	if err != nil {
		return err
	}

	for _, dao := range daos {
		// Experimental calculation that can be updated not once
		// Index is based on proposal, voter, votes counts.
		index := 5*math.Log(max(dp[dao], math.E)) + math.Log2(max(dvs[dao], 1)) + 3*math.Log2(max(dv[dao], 1)) + 0.3*(math.Log2(max(dvso[dao], 1))+3*math.Log2(max(dvo[dao], 1))) + dadditives[dao]
		if err = s.events.PublishJSON(ctx, pevents.SubjectPopularityIndexUpdated,
			pevents.DaoPayload{ID: dao, PopularityIndex: &index}); err != nil {
			log.Error().Err(err).Msgf("publish dao event #%s", dao)
		}
	}
	return err
}
