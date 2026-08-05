package internal

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for the server-side latency gate (p99 < 5ms) and operational
// observability (degraded-mode activation). Prometheus scrapes /metrics.
var (
	checkDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rlas_check_duration_seconds",
		Help:    "End-to-end duration of /v1/check handling.",
		Buckets: prometheus.ExponentialBuckets(0.00025, 2, 16), // 0.25ms .. 16s
	}, []string{"allowed", "degraded"})

	checksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rlas_checks_total",
		Help: "Total /v1/check requests by outcome.",
	}, []string{"allowed", "degraded"})

	breakerOpen = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rlas_circuit_breaker_open",
		Help: "1 while the check-path circuit breaker is open (Redis unreachable).",
	})
)

func observeCheck(dur time.Duration, o CheckOutcome) {
	labels := prometheus.Labels{
		"allowed":  boolStr(o.Allowed),
		"degraded": boolStr(o.Degraded),
	}
	checkDuration.With(labels).Observe(dur.Seconds())
	checksTotal.With(labels).Inc()
}

func setBreakerOpen(v bool) {
	if v {
		breakerOpen.Set(1)
	} else {
		breakerOpen.Set(0)
	}
}

func boolStr(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
