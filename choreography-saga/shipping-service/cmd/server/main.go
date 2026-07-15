package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"

	choreoruntime "saga-pattern/choreography-framework/runtime"
	"saga-pattern/choreography-saga/internal/serverutil"
	serviceconfig "saga-pattern/choreography-saga/shipping-service/internal/config"
	"saga-pattern/choreography-saga/shipping-service/internal/httpapi"
	"saga-pattern/choreography-saga/shipping-service/internal/observability"
	"saga-pattern/choreography-saga/shipping-service/internal/repository"
	"saga-pattern/choreography-saga/shipping-service/internal/shipping"
	"saga-pattern/common/events"
	"saga-pattern/common/httpcompat"
	commonkafka "saga-pattern/common/kafka"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (err error) {
	cfg, err := serviceconfig.Load()
	if err != nil {
		return err
	}

	resources, err := serverutil.BootstrapParticipantWithExtraMigrations(
		cfg,
		"choreography-shipping-service",
		"choreography-saga/shipping-service/db/migrations",
		[]serverutil.ExtraMigration{
			{Scope: "choreography-framework", Dir: "choreography-framework/db/migrations"},
		},
	)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resources.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close participant resources: %w", closeErr)
		}
	}()
	repo, err := repository.NewPostgresRepository(resources.DB)
	if err != nil {
		return fmt.Errorf("create shipping postgres repository: %w", err)
	}
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}

	handlerHolder := &eventHandlerHolder{}
	participant, err := choreoruntime.NewPostgres(
		resources.DB,
		choreoruntime.EventRegistry{
			Subscriptions: []choreoruntime.TopicSubscription{
				{Topic: commonkafka.DefaultOrderEventsTopic, EventTypes: []string{events.TypeOrderCreated}},
				{Topic: commonkafka.DefaultInventoryEventsTopic, EventTypes: []string{events.TypeInventoryReserved, events.TypeInventoryReleased}},
				{Topic: commonkafka.DefaultPaymentEventsTopic, EventTypes: []string{events.TypePaymentRefunded}},
			},
			OnUnknownEvent: choreoruntime.IgnoreUnknown,
		},
		handlerHolder,
		newFrameworkPublisher(resources.RuntimePublisher),
		choreoruntime.Config{ServiceName: cfg.ServiceName},
	)
	if err != nil {
		return fmt.Errorf("create choreography framework participant: %w", err)
	}

	shippingParticipant := &shippingParticipant{participant: participant, db: resources.DB}
	service, err := shipping.NewService(repo, shippingParticipant, metrics)
	if err != nil {
		return fmt.Errorf("create shipping service: %w", err)
	}
	handlerHolder.handler = service

	consumerGroup, err := commonkafka.NewSubscriberGroup(
		cfg.Runtime.KafkaBrokers, cfg.ServiceName, participant.Topics(), resources.Logger,
		participant.ConsumeRaw,
	)
	if err != nil {
		return fmt.Errorf("create kafka subscriber group: %w", err)
	}

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()
	go participant.RunWorkers(workerCtx)

	handler := httpapi.NewHandler(httpapi.HandlerDependencies{Config: cfg, Logger: resources.Logger, Registry: metrics.Registry(), Repo: repo, Service: service, HealthProvider: httpcompat.NewDBAndKafkaHealthProvider(resources.DB, cfg.Runtime.KafkaBrokers)})
	return serverutil.RunParticipantServer(cfg, resources.Logger, handler, participant.Topics(), consumerGroup)
}

type shippingParticipant struct {
	participant *choreoruntime.Participant
	db          *sql.DB
}

func (p *shippingParticipant) EnqueueEvent(ctx context.Context, tx *sql.Tx, topic, key, eventType string, payload any) error {
	if tx != nil {
		return p.participant.EnqueueEvent(ctx, choreoruntime.WrapSQLTx(tx), topic, key, eventType, payload)
	}
	ownTx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for standalone enqueue: %w", err)
	}
	defer ownTx.Rollback()
	if err := p.participant.EnqueueEvent(ctx, choreoruntime.WrapSQLTx(ownTx), topic, key, eventType, payload); err != nil {
		return err
	}
	return ownTx.Commit()
}

func (p *shippingParticipant) TriggerImmediatePublish(ctx context.Context) {
	p.participant.TriggerImmediatePublish(ctx)
}

type eventHandlerHolder struct {
	handler choreoruntime.EventHandler
}

func (h *eventHandlerHolder) HandleEvent(ctx context.Context, event events.ChoreographyEvent) error {
	if h.handler == nil {
		return fmt.Errorf("event handler not wired yet")
	}
	return h.handler.HandleEvent(ctx, event)
}

type frameworkPublisher struct {
	inner *commonkafka.RuntimePublisher
}

func newFrameworkPublisher(inner *commonkafka.RuntimePublisher) frameworkPublisher {
	return frameworkPublisher{inner: inner}
}

func (p frameworkPublisher) Publish(ctx context.Context, msg choreoruntime.Message) error {
	return p.inner.PublishRaw(ctx, msg.Topic, msg.Key, msg.Payload)
}
