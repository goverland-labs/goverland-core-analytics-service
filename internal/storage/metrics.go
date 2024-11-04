package storage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goverland-labs/goverland-core-analytics-service/internal/metrics"
)

const subsystem = "ch_storage"

var (
	txesCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: subsystem,
			Name:      "txes",
			Help:      "Count of committed txes to clickhouse",
		},
		[]string{"source"},
	)

	histCommitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: subsystem,
		Name:      "commit_duration_milliseconds",
		Help:      "Time taken to batch commit to clickhouse",
		Buckets:   []float64{20, 50, 100, 500, 800, 1000, 1500, 2000, 8000, 12000, 16000, 25000},
	}, []string{"source"})

	histNewTxDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: subsystem,
		Name:      "new_tx_duration_milliseconds",
		Help:      "Time taken to create new tx in clickhouse",
		Buckets:   []float64{20, 50, 100, 500, 800, 1000, 1500, 2000, 8000, 12000, 16000, 25000},
	}, []string{"source"})
)
