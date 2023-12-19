package item

import (
	"context"
	"github.com/rs/zerolog/log"
	"time"
)

const (
	popularityIndexCheckDelay = 12 * time.Hour
)

type PopularityWorker struct {
	service *Service
}

func NewPopularityWorker(s *Service) *PopularityWorker {
	return &PopularityWorker{
		service: s,
	}
}

func (w *PopularityWorker) Process(ctx context.Context) error {
	for {
		err := w.service.processPopularityIndexCalculation(ctx)
		if err != nil {
			log.Error().Err(err).Msg("process popularity index calculation")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(popularityIndexCheckDelay):
		}
	}
}
