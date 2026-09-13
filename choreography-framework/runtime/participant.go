package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	internalkafka "saga-pattern/choreography-framework/internal/kafka"
	"saga-pattern/choreography-framework/internal/loops"
	"saga-pattern/choreography-framework/internal/model"
	"saga-pattern/choreography-framework/internal/observability"
	"saga-pattern/choreography-framework/internal/store"
	"saga-pattern/common/events"
	commontracing "saga-pattern/common/tracing"
)

type Participant struct {
	config      Config
	registry    EventRegistry
	allowed     map[string]map[string]struct{}
	handler     EventHandler
	store       store.Store
	publisher   Publisher
	metrics     *observability.Metrics
	clock       func() time.Time
	idGenerator func() string
	workerID    string
	outboxLoop  *loops.OutboxLoop
	outboxMu    sync.Mutex
	outboxTrigger chan struct{}
	cleanupLoop *loops.CleanupLoop
}

type publisherAdapter struct{ publisher Publisher }

var defaultOutboxIDSequence atomic.Uint64

func (a publisherAdapter) Publish(ctx context.Context, message internalkafka.Message) error {
	return a.publisher.Publish(ctx, Message{
		Topic:     message.Topic,
		Key:       message.Key,
		EventType: message.EventType,
		Payload:   message.Payload,
	})
}

// New constructs a Participant from a caller-provided store.
// Most application code should prefer NewPostgres.
func New(deps Dependencies) (*Participant, error) {
	if err := deps.Registry.Validate(); err != nil {
		return nil, err
	}
	if deps.Handler == nil {
		return nil, fmt.Errorf("event handler is required")
	}
	if deps.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if deps.Publisher == nil {
		return nil, fmt.Errorf("publisher is required")
	}
	config := deps.Config.withDefaults()
	if config.ServiceName == "" {
		return nil, fmt.Errorf("config.ServiceName is required")
	}
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := deps.IDGenerator
	if idGenerator == nil {
		idGenerator = func() string {
			return fmt.Sprintf("outbox-%d-%d", clock().UnixNano(), defaultOutboxIDSequence.Add(1))
		}
	}
	workerID := config.WorkerID
	if workerID == "" {
		workerID = defaultWorkerID(config.ServiceName)
	}
	leaseName := config.OutboxLeaseName
	if leaseName == "" {
		leaseName = config.ServiceName + "-outbox"
	}
	metrics, err := observability.NewMetrics(deps.MetricsRegistry)
	if err != nil {
		return nil, err
	}
	p := &Participant{
		config:      config,
		registry:    deps.Registry,
		allowed:     deps.Registry.allowedMap(),
		handler:     deps.Handler,
		store:       deps.Store,
		publisher:   deps.Publisher,
		metrics:     metrics,
		clock:       clock,
		idGenerator: idGenerator,
		workerID:    workerID,
		outboxTrigger: make(chan struct{}, 1),
	}
	p.outboxLoop = &loops.OutboxLoop{
		ServiceName: config.ServiceName,
		LeaseName:   leaseName,
		LeaseTTL:    config.OutboxLeaseTTL,
		RetryDelay:  config.OutboxRetryDelay,
		BatchSize:   config.OutboxBatchSize,
		SendTimeout: config.KafkaSendTimeout,
		Store:       p.store,
		Publisher:   publisherAdapter{publisher: deps.Publisher},
		WorkerID:    workerID,
		Metrics:     metrics,
		OnPublishFailed: func(d time.Duration) {
			p.scheduleOutboxRetry(d)
		},
	}
	p.cleanupLoop = &loops.CleanupLoop{
		Store:                   p.store,
		ProcessedEventRetention: config.ProcessedEventRetention,
		OutboxRetention:         config.OutboxRetention,
	}
	return p, nil
}

func NewPostgres(db *sql.DB, registry EventRegistry, handler EventHandler, publisher Publisher, config Config) (*Participant, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres db is required")
	}
	return New(Dependencies{
		Registry:  registry,
		Handler:   handler,
		Store:     store.NewPostgresStore(db),
		Publisher: publisher,
		Config:    config,
	})
}

func (p *Participant) Registry() *prometheus.Registry {
	return p.metrics.Registry()
}

func (p *Participant) Topics() []string {
	return p.registry.Topics()
}

func (p *Participant) WorkerID() string {
	return p.workerID
}

// Suitable for use as a kafka subscriber callback.
func (p *Participant) Consume(ctx context.Context, envelope internalkafka.Envelope) error {
	ctx, span := commontracing.Tracer("choreography-framework/consume").Start(ctx, "choreography.event.consume",
		trace.WithAttributes(
			attribute.String("service.name", p.config.ServiceName),
			attribute.String("topic", envelope.Topic),
			attribute.String("messaging.kafka.key", envelope.Key),
		),
	)
	defer span.End()

	allowedTypes, ok := p.allowed[envelope.Topic]
	if !ok {
		if p.registry.OnUnknownEvent == RejectUnknown {
			err := fmt.Errorf("unsupported topic %q", envelope.Topic)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			p.metrics.RecordEventConsumed(p.config.ServiceName, envelope.Topic, "", "unknown_topic")
			return err
		}
		span.AddEvent("topic_ignored")
		p.metrics.RecordEventConsumed(p.config.ServiceName, envelope.Topic, "", "ignored")
		return nil
	}
	event, err := events.DecodeChoreographyEvent(bytes.NewReader(envelope.Value))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.metrics.RecordEventConsumed(p.config.ServiceName, envelope.Topic, "", "decode_error")
		return fmt.Errorf("decode choreography event: %w", err)
	}
	eventType := event.EventType()
	span.SetAttributes(attribute.String("event.type", eventType))
	if _, ok := allowedTypes[eventType]; !ok {
		if p.registry.OnUnknownEvent == RejectUnknown {
			err := fmt.Errorf("event type %q is not allowed on topic %q", eventType, envelope.Topic)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			p.metrics.RecordEventConsumed(p.config.ServiceName, envelope.Topic, eventType, "unknown_type")
			return err
		}
		span.AddEvent("event_type_ignored")
		p.metrics.RecordEventConsumed(p.config.ServiceName, envelope.Topic, eventType, "ignored")
		return nil
	}
	ctx = events.ContextWithMetadata(ctx, event)
	start := p.clock()
	err = p.handler.HandleEvent(ctx, event)
	duration := p.clock().Sub(start)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.metrics.RecordEventConsumed(p.config.ServiceName, envelope.Topic, eventType, "error")
		p.metrics.RecordEventHandleDuration(p.config.ServiceName, envelope.Topic, eventType, "error", duration)
		return err
	}
	p.metrics.RecordEventConsumed(p.config.ServiceName, envelope.Topic, eventType, "success")
	p.metrics.RecordEventHandleDuration(p.config.ServiceName, envelope.Topic, eventType, "success", duration)
	return nil
}

func (p *Participant) ConsumeRaw(ctx context.Context, topic string, key string, value []byte) error {
	return p.Consume(ctx, internalkafka.Envelope{Topic: topic, Key: key, Value: value})
}

// Choreography analog of orchestration's transactional command enqueue.
func (p *Participant) EnqueueEvent(ctx context.Context, tx store.Tx, topic, key, eventType string, payload any) error {
	if topic == "" {
		return fmt.Errorf("topic is required")
	}
	if eventType == "" {
		return fmt.Errorf("event type is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	now := p.clock().UTC()
	headers := commontracing.InjectHeaders(ctx, map[string]string{})
	row := model.OutboxRow{
		ID:           p.idGenerator(),
		Topic:        topic,
		Key:          key,
		EventType:    eventType,
		Payload:      body,
		Status:       model.OutboxStatusPending,
		AvailableAt:  now,
		CreatedAt:    now,
		UpdatedAt:    now,
		AttemptCount: 0,
		MaxAttempts:  p.config.OutboxMaxAttempts,
		TraceHeaders: headers,
	}
	if err := tx.InsertOutbox(ctx, row); err != nil {
		return err
	}
	return nil
}

// Analogous to orchestration-framework's InsertProcessedReply.
func (p *Participant) TryMarkProcessedEvent(ctx context.Context, tx store.Tx, key string) (bool, error) {
	return tx.TryMarkProcessedEvent(ctx, key)
}

func (p *Participant) DeleteProcessedEvent(ctx context.Context, tx store.Tx, key string) error {
	return tx.DeleteProcessedEvent(ctx, key)
}

// For services that also touch domain tables, use WrapSQLTx instead.
func (p *Participant) WithinTx(ctx context.Context, fn func(store.Tx) error) error {
	return p.store.WithinTx(ctx, fn)
}

// Primary integration point for transactional outbox on top of domain writes.
func WrapSQLTx(tx *sql.Tx) store.Tx {
	return store.WrapSQLTx(tx)
}

func (p *Participant) RunWorkers(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.runOutboxLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		p.runTickerLoop(ctx, p.config.CleanupInterval, "choreography cleanup worker failed", func(now time.Time) error {
			return p.cleanupLoop.RunOnce(ctx, now)
		})
	}()
	<-ctx.Done()
	wg.Wait()
}

func (p *Participant) runOutboxLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.outboxTrigger:
			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Default().Error("choreography outbox worker recovered from panic", "panic", r)
					}
				}()
				if err := p.PublishPending(ctx); err != nil {
					slog.Default().Error("choreography outbox worker failed", "error", err)
				}
			}()
		}
	}
}

func (p *Participant) runTickerLoop(ctx context.Context, interval time.Duration, logMessage string, run func(time.Time) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := run(p.clock().UTC()); err != nil {
				slog.Default().Error(logMessage, "error", err)
			}
		}
	}
}

func (p *Participant) PublishPending(ctx context.Context) error {
	p.outboxMu.Lock()
	defer p.outboxMu.Unlock()
	return p.outboxLoop.RunOnce(ctx, p.clock().UTC())
}

func (p *Participant) scheduleOutboxRetry(d time.Duration) {
	time.AfterFunc(d, func() {
		select {
		case p.outboxTrigger <- struct{}{}:
		default:
		}
	})
}

func (p *Participant) TriggerImmediatePublish(ctx context.Context) {
	if !p.config.ImmediateOutboxPublish {
		return
	}
	select {
	case p.outboxTrigger <- struct{}{}:
	default:
	}
}

func (p *Participant) Cleanup(ctx context.Context) error {
	return p.cleanupLoop.RunOnce(ctx, p.clock().UTC())
}

func (p *Participant) RecordCompensationStarted(eventType string) {
	p.metrics.RecordCompensationStarted(p.config.ServiceName, eventType)
}

func (p *Participant) RecordCompensationCompleted(eventType string) {
	p.metrics.RecordCompensationCompleted(p.config.ServiceName, eventType)
}

func defaultWorkerID(serviceName string) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%s-%s", serviceName, hostname, strconv.Itoa(os.Getpid()))
}
