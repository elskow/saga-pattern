package observability

import (
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricSagaDuration  = "saga_framework_duration_seconds"
	metricStepDuration  = "saga_framework_step_duration_seconds"
	metricCompStarted   = "saga_framework_compensation_started_total"
	metricCompCompleted = "saga_framework_compensation_completed_total"
	labelSagaType       = "saga_type"
	labelResult         = "result"
	labelStep           = "step"
	labelDirection      = "direction"
)

var suiteLabel = os.Getenv("SUITE_LABEL")

var sagaDurationBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 75, 100, 120}

func constLabels() prometheus.Labels {
	return prometheus.Labels{"suite_label": suiteLabel}
}

type Metrics struct {
	registry              *prometheus.Registry
	sagaDuration          *prometheus.HistogramVec
	stepDuration          *prometheus.HistogramVec
	compensationStarted   *prometheus.CounterVec
	compensationCompleted *prometheus.CounterVec
}

func NewMetrics(registry *prometheus.Registry) (*Metrics, error) {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	metrics := &Metrics{
		registry: registry,
		sagaDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        metricSagaDuration,
			Help:        "Duration of orchestration sagas in seconds.",
			Buckets:     sagaDurationBuckets,
			ConstLabels: constLabels(),
		}, []string{labelSagaType, labelResult}),
		stepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        metricStepDuration,
			Help:        "Duration of orchestration steps in seconds.",
			Buckets:     sagaDurationBuckets,
			ConstLabels: constLabels(),
		}, []string{labelSagaType, labelStep, labelDirection, labelResult}),
		compensationStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        metricCompStarted,
			Help:        "Count of compensation commands started.",
			ConstLabels: constLabels(),
		}, []string{labelSagaType, labelStep}),
		compensationCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        metricCompCompleted,
			Help:        "Count of compensation commands completed.",
			ConstLabels: constLabels(),
		}, []string{labelSagaType, labelStep}),
	}
	registered := make([]prometheus.Collector, 0, 4)
	for _, collector := range []prometheus.Collector{metrics.sagaDuration, metrics.stepDuration, metrics.compensationStarted, metrics.compensationCompleted} {
		if err := registry.Register(collector); err != nil {
			for _, registeredCollector := range registered {
				registry.Unregister(registeredCollector)
			}
			return nil, err
		}
		registered = append(registered, collector)
	}
	return metrics, nil
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *Metrics) RecordSagaDuration(sagaType string, result string, duration time.Duration) {
	m.sagaDuration.WithLabelValues(sagaType, result).Observe(duration.Seconds())
}

func (m *Metrics) RecordStepDuration(sagaType string, step string, direction string, result string, duration time.Duration) {
	m.stepDuration.WithLabelValues(sagaType, step, direction, result).Observe(duration.Seconds())
}

func (m *Metrics) RecordCompensationStarted(sagaType string, step string) {
	m.compensationStarted.WithLabelValues(sagaType, step).Inc()
}

func (m *Metrics) RecordCompensationCompleted(sagaType string, step string) {
	m.compensationCompleted.WithLabelValues(sagaType, step).Inc()
}

func (m *Metrics) InitCompensationMetrics(sagaType string, steps []string) {
	for _, step := range steps {
		m.compensationStarted.WithLabelValues(sagaType, step).Add(0)
		m.compensationCompleted.WithLabelValues(sagaType, step).Add(0)
	}
}
