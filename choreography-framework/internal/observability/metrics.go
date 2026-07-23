package observability

import (
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricEventsConsumed        = "choreography_framework_events_consumed_total"
	metricEventsPublished       = "choreography_framework_events_published_total"
	metricOutboxPublishFailed   = "choreography_framework_outbox_publish_failed_total"
	metricOutboxAttempts        = "choreography_framework_outbox_attempts"
	metricOutboxSendDuration    = "choreography_framework_outbox_send_duration_seconds"
	metricEventHandleDuration   = "choreography_framework_event_handle_duration_seconds"
	metricCompensationStarted   = "choreography_framework_compensation_started_total"
	metricCompensationCompleted = "choreography_framework_compensation_completed_total"
	labelService                = "service"
	labelTopic                  = "topic"
	labelEventType              = "event_type"
	labelResult                 = "result"
)

// suiteLabel is captured once at process start (empty string is a valid value).
var suiteLabel = os.Getenv("SUITE_LABEL")

// sagaDurationBuckets extends prometheus.DefBuckets past 10s to cover
// choreography compensation cascades (empirically ~65–70s, headroom to 120s).
var sagaDurationBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 75, 100, 120}

func constLabels() prometheus.Labels {
	return prometheus.Labels{"suite_label": suiteLabel}
}

type Metrics struct {
	registry              *prometheus.Registry
	eventsConsumed        *prometheus.CounterVec
	eventsPublished       *prometheus.CounterVec
	outboxPublishFailed   *prometheus.CounterVec
	outboxAttempts        *prometheus.HistogramVec
	outboxSendDuration    *prometheus.HistogramVec
	eventHandleDuration   *prometheus.HistogramVec
	compensationStarted   *prometheus.CounterVec
	compensationCompleted *prometheus.CounterVec
}

func NewMetrics(registry *prometheus.Registry) (*Metrics, error) {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	metrics := &Metrics{
		registry: registry,
		eventsConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        metricEventsConsumed,
			Help:        "Number of choreography events consumed by the framework.",
			ConstLabels: constLabels(),
		}, []string{labelService, labelTopic, labelEventType, labelResult}),
		eventsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        metricEventsPublished,
			Help:        "Number of choreography events published from the outbox.",
			ConstLabels: constLabels(),
		}, []string{labelService, labelTopic, labelEventType}),
		outboxPublishFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        metricOutboxPublishFailed,
			Help:        "Number of outbox publish failures.",
			ConstLabels: constLabels(),
		}, []string{labelService, labelTopic, labelEventType}),
		outboxAttempts: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        metricOutboxAttempts,
			Help:        "Number of attempts made before an outbox row was sent.",
			Buckets:     []float64{1, 2, 3, 4, 5, 10},
			ConstLabels: constLabels(),
		}, []string{labelService, labelTopic, labelEventType}),
		outboxSendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        metricOutboxSendDuration,
			Help:        "Latency of outbox publish operations in seconds.",
			Buckets:     sagaDurationBuckets,
			ConstLabels: constLabels(),
		}, []string{labelService, labelTopic, labelEventType, labelResult}),
		eventHandleDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        metricEventHandleDuration,
			Help:        "Latency of choreography event handler execution in seconds.",
			Buckets:     sagaDurationBuckets,
			ConstLabels: constLabels(),
		}, []string{labelService, labelTopic, labelEventType, labelResult}),
		compensationStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        metricCompensationStarted,
			Help:        "Count of compensation events started.",
			ConstLabels: constLabels(),
		}, []string{labelService, labelEventType}),
		compensationCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        metricCompensationCompleted,
			Help:        "Count of compensation events completed.",
			ConstLabels: constLabels(),
		}, []string{labelService, labelEventType}),
	}
	registered := make([]prometheus.Collector, 0, 8)
	for _, collector := range []prometheus.Collector{
		metrics.eventsConsumed,
		metrics.eventsPublished,
		metrics.outboxPublishFailed,
		metrics.outboxAttempts,
		metrics.outboxSendDuration,
		metrics.eventHandleDuration,
		metrics.compensationStarted,
		metrics.compensationCompleted,
	} {
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

func (m *Metrics) RecordEventConsumed(service, topic, eventType, result string) {
	m.eventsConsumed.WithLabelValues(service, topic, eventType, result).Inc()
}

func (m *Metrics) RecordEventPublished(service, topic, eventType string) {
	m.eventsPublished.WithLabelValues(service, topic, eventType).Inc()
}

func (m *Metrics) RecordOutboxPublishFailed(service, topic, eventType string) {
	m.outboxPublishFailed.WithLabelValues(service, topic, eventType).Inc()
}

func (m *Metrics) RecordOutboxAttempts(service, topic, eventType string, attempts int) {
	m.outboxAttempts.WithLabelValues(service, topic, eventType).Observe(float64(attempts))
}

func (m *Metrics) RecordOutboxSendDuration(service, topic, eventType, result string, duration time.Duration) {
	m.outboxSendDuration.WithLabelValues(service, topic, eventType, result).Observe(duration.Seconds())
}

func (m *Metrics) RecordEventHandleDuration(service, topic, eventType, result string, duration time.Duration) {
	m.eventHandleDuration.WithLabelValues(service, topic, eventType, result).Observe(duration.Seconds())
}

func (m *Metrics) RecordCompensationStarted(service, eventType string) {
	m.compensationStarted.WithLabelValues(service, eventType).Inc()
}

func (m *Metrics) RecordCompensationCompleted(service, eventType string) {
	m.compensationCompleted.WithLabelValues(service, eventType).Inc()
}
