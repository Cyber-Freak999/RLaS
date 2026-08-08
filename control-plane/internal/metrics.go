package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Streams-consumer lag gauges. The consumer polls these once per cycle; the
// ops dashboard graphs them so a consumer falling behind is visible in
// Grafana, not just as the >50k log warning (constraint 10's "warn before the
// trim drops events" signal gets an alertable metric counterpart).
var (
	streamEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rlas_stream_entries",
		Help: "Current length of the approved-requests stream (XLEN).",
	})
	streamPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rlas_stream_pending",
		Help: "Delivered-but-unacked entries in the consumer group (XPENDING count); rising values mean the consumer is falling behind.",
	})
)
