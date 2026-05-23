package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/lib/pq"

	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/orchestration-saga/internal/healthutil"
	"saga-pattern/orchestration-saga/internal/serverutil"
	serviceconfig "saga-pattern/orchestration-saga/payment-service/internal/config"
	"saga-pattern/orchestration-saga/payment-service/internal/httpapi"
	"saga-pattern/orchestration-saga/payment-service/internal/messaging"
	"saga-pattern/orchestration-saga/payment-service/internal/observability"
	"saga-pattern/orchestration-saga/payment-service/internal/payments"
	"saga-pattern/orchestration-saga/payment-service/internal/repository"
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

	resources, err := serverutil.BootstrapParticipant(cfg, "orchestration-payment-service", "orchestration-saga/payment-service/db/migrations")
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
	metrics, err := observability.NewMetrics(resources.Registry)
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}
	replyPublisher := messaging.NewReplyPublisher(resources.RuntimePublisher)
	service, err := payments.NewService(repo, replyPublisher, metrics)
	if err != nil {
		return fmt.Errorf("create payment service: %w", err)
	}
	consumer, err := messaging.NewCommandConsumer(service)
	if err != nil {
		return fmt.Errorf("create payment command consumer: %w", err)
	}
	consumerGroup, err := commonkafka.NewSubscriberGroup(cfg.Runtime.KafkaBrokers, cfg.ServiceName, consumer.Topics(), resources.Logger, func(ctx context.Context, topic string, key string, value []byte) error {
		return consumer.Consume(ctx, messaging.CommandEnvelope{Topic: topic, Key: key, Value: value})
	})
	if err != nil {
		return fmt.Errorf("create kafka subscriber group: %w", err)
	}
	handler := httpapi.NewHandler(httpapi.HandlerDependencies{
		Config:         cfg,
		Logger:         resources.Logger,
		Registry:       metrics.Registry(),
		HealthProvider: healthutil.ParticipantHealthProvider(service, cfg.Runtime.KafkaBrokers),
		Repo:           repo,
		Balancer:       service,
	})
	return serverutil.RunParticipantServer(cfg, resources.Logger, handler, consumer.Topics(), replyPublisher.Topic(), consumerGroup)
}
