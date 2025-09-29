package autostart

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	AutostartLatencySeconds prometheus.HistogramVec
	AutostartErrorsTotal    prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		AutostartLatencySeconds: *prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "coderd",
			Subsystem: "scaletest",
			Name:      "autostart_latency_seconds",
			Help:      "Time from when the workspace is scheduled to be autostarted to when the autostart build has finished.",
		}, []string{"username", "workspace_name"}),
		AutostartErrorsTotal: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "coderd",
			Subsystem: "scaletest",
			Name:      "autostart_errors_total",
			Help:      "Total number of autostart errors",
		}, []string{"username", "action"}),
	}

	reg.MustRegister(m.AutostartLatencySeconds)
	reg.MustRegister(m.AutostartErrorsTotal)
	return m
}

func (m *Metrics) RecordCompletion(elapsed time.Duration, username string, workspace string) {
	m.AutostartLatencySeconds.WithLabelValues(username, workspace).Observe(elapsed.Seconds())
}

func (m *Metrics) AddError(username string, action string) {
	m.AutostartErrorsTotal.WithLabelValues(username, action).Inc()
}
