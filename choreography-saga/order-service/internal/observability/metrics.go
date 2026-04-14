package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricOrdersCreated   = "saga_orders_created_total"
	metricOrdersCompleted = "saga_orders_completed_total"
	metricOrdersFailed    = "saga_orders_failed_total"
	metricCompensations   = "saga_compensations_total"
	metricProcessingTime  = "saga_order_processing_time_seconds"

	labelPattern = "pattern"
	labelService = "service"

	labelValuePattern = "choreography"
	labelValueService = "choreography"
)

type Metrics struct {
	registry        *prometheus.Registry
	ordersCreated   prometheus.Counter
	ordersCompleted prometheus.Counter
	ordersFailed    prometheus.Counter
	compensations   prometheus.Counter
	processingTime  prometheus.Observer
}

func NewMetrics(registry *prometheus.Registry) (*Metrics, error) {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	labels := prometheus.Labels{labelPattern: labelValuePattern, labelService: labelValueService}
	created := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricOrdersCreated, Help: "Total orders created."}, []string{labelPattern, labelService})
	completed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricOrdersCompleted, Help: "Total orders completed."}, []string{labelPattern, labelService})
	failed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricOrdersFailed, Help: "Total orders failed."}, []string{labelPattern, labelService})
	compensations := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricCompensations, Help: "Total saga compensations."}, []string{labelPattern, labelService})
	processing := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: metricProcessingTime, Help: "Saga order processing time in seconds.", Buckets: prometheus.DefBuckets}, []string{labelPattern, labelService})

	for _, collector := range []prometheus.Collector{created, completed, failed, compensations, processing} {
		if err := registry.Register(collector); err != nil {
			return nil, err
		}
	}

	return &Metrics{
		registry:        registry,
		ordersCreated:   created.With(labels),
		ordersCompleted: completed.With(labels),
		ordersFailed:    failed.With(labels),
		compensations:   compensations.With(labels),
		processingTime:  processing.With(labels),
	}, nil
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *Metrics) RecordOrderCreated() {
	m.ordersCreated.Inc()
}

func (m *Metrics) RecordOrderCompleted(duration time.Duration) {
	m.ordersCompleted.Inc()
	m.processingTime.Observe(duration.Seconds())
}

func (m *Metrics) RecordOrderFailed(duration time.Duration) {
	m.ordersFailed.Inc()
	m.compensations.Inc()
	m.processingTime.Observe(duration.Seconds())
}
