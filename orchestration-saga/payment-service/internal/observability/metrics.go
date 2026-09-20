package observability

import (
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricStepPaymentDuration = "saga_step_payment_duration_seconds"
	metricCompensations       = "saga_compensations_payment_total"

	labelPattern = "pattern"
	labelService = "service"

	labelValuePattern = "orchestration"
	labelValueService = "orchestration"
)

var suiteLabel = os.Getenv("SUITE_LABEL")

func constLabels() prometheus.Labels {
	return prometheus.Labels{"suite_label": suiteLabel}
}

type Metrics struct {
	registry      *prometheus.Registry
	stepDuration  prometheus.Observer
	compensations prometheus.Counter
}

func NewMetrics(registry *prometheus.Registry) (*Metrics, error) {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	labels := prometheus.Labels{labelPattern: labelValuePattern, labelService: labelValueService}
	stepDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: metricStepPaymentDuration, Help: "Payment step duration in seconds.", Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 75, 100, 120}, ConstLabels: constLabels()},
		[]string{labelPattern, labelService},
	)
	compensations := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: metricCompensations, Help: "Total payment compensations.", ConstLabels: constLabels()},
		[]string{labelPattern, labelService},
	)

	registered := make([]prometheus.Collector, 0, 2)
	for _, collector := range []prometheus.Collector{stepDuration, compensations} {
		if err := registry.Register(collector); err != nil {
			for _, registeredCollector := range registered {
				registry.Unregister(registeredCollector)
			}
			return nil, err
		}
		registered = append(registered, collector)
	}

	metrics := &Metrics{
		registry:      registry,
		stepDuration:  stepDuration.With(labels),
		compensations: compensations.With(labels),
	}
	metrics.compensations.Add(0)
	return metrics, nil
}

func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

func (m *Metrics) RecordPaymentStep(duration time.Duration) {
	m.stepDuration.Observe(duration.Seconds())
}

func (m *Metrics) RecordPaymentCompensation(duration time.Duration) {
	m.compensations.Inc()
	m.stepDuration.Observe(duration.Seconds())
}
