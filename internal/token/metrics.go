package token

import (
	"github.com/goverland-labs/analytics-service/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var metricHandleHistogram = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: "token",
		Name:      "handle_duration_seconds",
		Help:      "Handle token event duration seconds",
		Buckets:   []float64{.001, .005, .01, .025, .05, .1, .5, 1, 2.5, 5, 10},
	}, []string{"type", "error"},
)
