package runtime

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	commontracing "saga-pattern/common/tracing"
	internalkafka "saga-pattern/orchestration-framework/internal/kafka"
	"saga-pattern/orchestration-framework/internal/loops"
	"saga-pattern/orchestration-framework/internal/observability"
	"saga-pattern/orchestration-framework/internal/store"
)

type Runtime[D any] struct {
	definition          Definition[D]
	store               store.Store
	publisher           Publisher
	metrics             *observability.Metrics
	clock               func() time.Time
	idGenerator         func() string
	workerID            string
	config              Config
	sagaTimeoutOverride atomic.Int64 // nanoseconds; 0 means use config default
	outboxLoop          *loops.OutboxLoop
	outboxMu            sync.Mutex
	outboxTrigger       chan struct{}
	timeoutLoop         *loops.TimeoutLoop
	cleanupLoop         *loops.CleanupLoop
}

type publisherAdapter struct{ publisher Publisher }

func (a publisherAdapter) Publish(ctx context.Context, message internalkafka.Message) error {
	return a.publisher.Publish(ctx, Message{
		Topic:       message.Topic,
		Key:         message.Key,
		MessageType: message.MessageType,
		Payload:     message.Payload,
		SagaID:      message.SagaID,
		Step:        message.Step,
		Direction:   string(message.Direction),
	})
}

func newRuntimeID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// Preserve availability if the OS random source fails while retaining
		// process-local uniqueness for concurrent calls.
		return fmt.Sprintf("saga-%d-%d", time.Now().UnixNano(), fallbackIDSequence.Add(1))
	}
	return hex.EncodeToString(buf)
}

var fallbackIDSequence atomic.Uint64

// New constructs a runtime from a caller-provided store.
// Most application code should prefer NewPostgres.
func New[D any](def Definition[D], deps Dependencies) (*Runtime[D], error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}
	if deps.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if deps.Publisher == nil {
		return nil, fmt.Errorf("publisher is required")
	}
	config := deps.Config
	if config == (Config{}) {
		config = DefaultConfig()
	}
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := deps.IDGenerator
	if idGenerator == nil {
		idGenerator = newRuntimeID
	}
	metrics, err := observability.NewMetrics(deps.MetricsRegistry)
	if err != nil {
		return nil, err
	}
	stepNames := make([]string, 0, len(def.Steps))
	for _, step := range def.Steps {
		stepNames = append(stepNames, step.Name)
	}
	metrics.InitCompensationMetrics(def.SagaType, stepNames)
	workerID := deps.WorkerID
	if workerID == "" {
		workerID = idGenerator()
	}
	r := &Runtime[D]{
		definition:  def,
		store:       deps.Store,
		publisher:   deps.Publisher,
		metrics:     metrics,
		clock:       clock,
		idGenerator: idGenerator,
		workerID:    workerID,
		config:      config,
		outboxTrigger: make(chan struct{}, 1),
	}
	internalPublisher := publisherAdapter{publisher: deps.Publisher}
	r.outboxLoop = &loops.OutboxLoop{
		LeaseName:   "orchestration-framework-outbox",
		LeaseTTL:    config.LeaseTTL,
		RetryDelay:  config.OutboxRetryDelay,
		BatchSize:   config.OutboxBatchSize,
		SendTimeout: config.KafkaSendTimeout,
		Store:       r.store,
		Publisher:   internalPublisher,
		WorkerID:    workerID,
		OnPublishFailed: func(d time.Duration) {
			r.scheduleOutboxRetry(d)
		},
	}
	r.timeoutLoop = &loops.TimeoutLoop{Store: r.store, Handler: r, Limit: config.OutboxBatchSize}
	r.cleanupLoop = &loops.CleanupLoop{Store: r.store, ProcessedReplyRetention: config.ProcessedReplyRetention, OutboxRetention: config.OutboxRetention}
	return r, nil
}

func NewWithStore[D any](def Definition[D], deps AdvancedDependencies) (*Runtime[D], error) {
	return New(def, deps)
}

func NewPostgres[D any](def Definition[D], db *sql.DB, deps PostgresDependencies) (*Runtime[D], error) {
	if db == nil {
		return nil, fmt.Errorf("postgres db is required")
	}
	return New(def, Dependencies{
		Store:           store.NewPostgresStore(db),
		Publisher:       deps.Publisher,
		MetricsRegistry: deps.MetricsRegistry,
		Clock:           deps.Clock,
		IDGenerator:     deps.IDGenerator,
		WorkerID:        deps.WorkerID,
		Config:          deps.Config,
	})
}

func (r *Runtime[D]) Registry() *prometheus.Registry {
	return r.metrics.Registry()
}

func (r *Runtime[D]) RunWorkers(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		r.runOutboxLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		r.runTickerLoop(ctx, r.config.TimeoutCheckInterval, "orchestration timeout worker failed", func(now time.Time) error {
			return r.timeoutLoop.RunOnce(ctx, now)
		})
	}()
	go func() {
		defer wg.Done()
		r.runTickerLoop(ctx, r.config.CleanupInterval, "orchestration cleanup worker failed", func(now time.Time) error {
			return r.cleanupLoop.RunOnce(ctx, now)
		})
	}()
	<-ctx.Done()
	wg.Wait()
}

func (r *Runtime[D]) runOutboxLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.outboxTrigger:
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						slog.Default().Error("orchestration outbox worker recovered from panic", "panic", rec)
					}
				}()
				if err := r.PublishPending(ctx); err != nil {
					slog.Default().Error("orchestration outbox worker failed", "error", err)
				}
			}()
		}
	}
}

func (r *Runtime[D]) runTickerLoop(ctx context.Context, interval time.Duration, logMessage string, run func(time.Time) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := run(r.clock().UTC()); err != nil {
				slog.Default().Error(logMessage, "error", err)
			}
		}
	}
}

func (r *Runtime[D]) PublishPending(ctx context.Context) error {
	r.outboxMu.Lock()
	defer r.outboxMu.Unlock()
	return r.outboxLoop.RunOnce(ctx, r.clock().UTC())
}

func (r *Runtime[D]) scheduleOutboxRetry(d time.Duration) {
	time.AfterFunc(d, func() {
		select {
		case r.outboxTrigger <- struct{}{}:
		default:
		}
	})
}

func (r *Runtime[D]) triggerOutboxPublish(_ context.Context, _ trace.Span) {
	if !r.config.ImmediateOutboxPublish {
		return
	}
	select {
	case r.outboxTrigger <- struct{}{}:
	default:
	}
}

func (r *Runtime[D]) RecoverTimeouts(ctx context.Context) error {
	return r.timeoutLoop.RunOnce(ctx, r.clock().UTC())
}

func (r *Runtime[D]) Cleanup(ctx context.Context) error {
	return r.cleanupLoop.RunOnce(ctx, r.clock().UTC())
}

// SetSagaTimeout overrides the saga timeout for new sagas at runtime.
func (r *Runtime[D]) SetSagaTimeout(d time.Duration) {
	r.sagaTimeoutOverride.Store(int64(d))
}

// GetSagaTimeout returns the effective saga timeout.
func (r *Runtime[D]) GetSagaTimeout() time.Duration {
	if override := r.sagaTimeoutOverride.Load(); override > 0 {
		return time.Duration(override)
	}
	return r.config.SagaTimeout
}

func (r *Runtime[D]) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return commontracing.Tracer("orchestration-framework/runtime").Start(ctx, name, trace.WithAttributes(attrs...))
}
