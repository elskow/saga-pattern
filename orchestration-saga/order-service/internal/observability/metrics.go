package observability

import (
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricOrdersCreated          = "saga_orders_created_total"
	metricOrdersCompleted        = "saga_orders_completed_total"
	metricOrdersFailed           = "saga_orders_failed_total"
	metricSagaTotalDuration      = "saga_total_duration_seconds"
	metricCompensationsTotal     = "saga_compensations_total"
	metricCompensationsPayment   = "saga_compensations_payment_total"
	metricCompensationsInventory = "saga_compensations_inventory_total"
	metricCompensationsShipping  = "saga_compensations_shipping_total"
	labelPattern                 = "pattern"
	labelService                 = "service"
	labelValuePattern            = "orchestration"
	labelValueService            = "orchestration"
)

var suiteLabel = os.Getenv("SUITE_LABEL")

func constLabels() prometheus.Labels {
	return prometheus.Labels{"suite_label": suiteLabel}
}

type Metrics struct {
	registry               *prometheus.Registry
	ordersCreated          prometheus.Counter
	ordersCompleted        prometheus.Counter
	ordersFailed           prometheus.Counter
	totalDuration          prometheus.Observer
	compensationsTotal     prometheus.Counter
	compensationsPayment   prometheus.Counter
	compensationsInventory prometheus.Counter
	compensationsShipping  prometheus.Counter
}

func NewMetrics(registry *prometheus.Registry) (*Metrics, error) {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	labels := prometheus.Labels{labelPattern: labelValuePattern, labelService: labelValueService}
	created := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricOrdersCreated, Help: "Total orders created.", ConstLabels: constLabels()}, []string{labelPattern, labelService})
	completed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricOrdersCompleted, Help: "Total orders completed.", ConstLabels: constLabels()}, []string{labelPattern, labelService})
	failed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricOrdersFailed, Help: "Total orders failed.", ConstLabels: constLabels()}, []string{labelPattern, labelService})
	totalDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: metricSagaTotalDuration, Help: "End-to-end orchestration saga duration in seconds.", Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 75, 100, 120}, ConstLabels: constLabels()}, []string{labelPattern, labelService})
	compTotal := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricCompensationsTotal, Help: "Total compensated orchestration sagas.", ConstLabels: constLabels()}, []string{labelPattern, labelService})
	compPayment := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricCompensationsPayment, Help: "Total payment compensations.", ConstLabels: constLabels()}, []string{labelPattern, labelService})
	compInventory := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricCompensationsInventory, Help: "Total inventory compensations.", ConstLabels: constLabels()}, []string{labelPattern, labelService})
	compShipping := prometheus.NewCounterVec(prometheus.CounterOpts{Name: metricCompensationsShipping, Help: "Total shipping compensations.", ConstLabels: constLabels()}, []string{labelPattern, labelService})

	registered := make([]prometheus.Collector, 0, 8)
	for _, collector := range []prometheus.Collector{created, completed, failed, totalDuration, compTotal, compPayment, compInventory, compShipping} {
		if err := registry.Register(collector); err != nil {
			for _, registeredCollector := range registered {
				registry.Unregister(registeredCollector)
			}
			return nil, err
		}
		registered = append(registered, collector)
	}

	metrics := &Metrics{
		registry:               registry,
		ordersCreated:          created.With(labels),
		ordersCompleted:        completed.With(labels),
		ordersFailed:           failed.With(labels),
		totalDuration:          totalDuration.With(labels),
		compensationsTotal:     compTotal.With(labels),
		compensationsPayment:   compPayment.With(labels),
		compensationsInventory: compInventory.With(labels),
		compensationsShipping:  compShipping.With(labels),
	}
	metrics.compensationsPayment.Add(0)
	metrics.compensationsInventory.Add(0)
	metrics.compensationsShipping.Add(0)
	metrics.compensationsTotal.Add(0)
	return metrics, nil
}

func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

func (m *Metrics) RecordOrderCreated() { m.ordersCreated.Inc() }

func (m *Metrics) RecordOrderCompleted(duration time.Duration) {
	m.ordersCompleted.Inc()
	m.totalDuration.Observe(duration.Seconds())
}

func (m *Metrics) RecordOrderFailed(duration time.Duration) {
	m.ordersFailed.Inc()
	m.compensationsTotal.Inc()
	m.totalDuration.Observe(duration.Seconds())
}

func (m *Metrics) RecordPaymentCompensation() { m.compensationsPayment.Inc() }

func (m *Metrics) RecordInventoryCompensation() { m.compensationsInventory.Inc() }

func (m *Metrics) RecordShippingCompensation() { m.compensationsShipping.Inc() }
