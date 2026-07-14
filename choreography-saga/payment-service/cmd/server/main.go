package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"

	choreoruntime "saga-pattern/choreography-framework/runtime"
	"saga-pattern/choreography-saga/internal/serverutil"
	serviceconfig "saga-pattern/choreography-saga/payment-service/internal/config"
	"saga-pattern/choreography-saga/payment-service/internal/httpapi"
	"saga-pattern/choreography-saga/payment-service/internal/observability"
	"saga-pattern/choreography-saga/payment-service/internal/payments"
	"saga-pattern/choreography-saga/payment-service/internal/repository"
	"saga-pattern/common/events"
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
		"choreography-payment-service",
		"choreography-saga/payment-service/db/migrations",
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
		return fmt.Errorf("create payment postgres repository: %w", err)
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
				{Topic: commonkafka.DefaultInventoryEventsTopic, EventTypes: []string{events.TypeInventoryReservationFailed}},
				{Topic: commonkafka.DefaultShippingEventsTopic, EventTypes: []string{events.TypeShippingFailed}},
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

	paymentParticipant := &paymentParticipant{participant: participant}
	service, err := payments.NewService(repo, paymentParticipant, metrics)
	if err != nil {
		return fmt.Errorf("create payment service: %w", err)
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

	handler := httpapi.NewHandler(httpapi.HandlerDependencies{Config: cfg, Logger: resources.Logger, Registry: metrics.Registry(), Repo: repo, Balancer: service})
	return serverutil.RunParticipantServer(cfg, resources.Logger, handler, participant.Topics(), consumerGroup)
}

type paymentParticipant struct {
	participant *choreoruntime.Participant
}

func (p *paymentParticipant) EnqueueEvent(ctx context.Context, tx *sql.Tx, topic, key, eventType string, payload any) error {
	return p.participant.EnqueueEvent(ctx, choreoruntime.WrapSQLTx(tx), topic, key, eventType, payload)
}

func (p *paymentParticipant) TriggerImmediatePublish(ctx context.Context) {
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
