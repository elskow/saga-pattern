package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/lib/pq"

	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/orchestration-saga/internal/healthutil"
	"saga-pattern/orchestration-saga/internal/serverutil"
	serviceconfig "saga-pattern/orchestration-saga/inventory-service/internal/config"
	"saga-pattern/orchestration-saga/inventory-service/internal/httpapi"
	"saga-pattern/orchestration-saga/inventory-service/internal/inventory"
	"saga-pattern/orchestration-saga/inventory-service/internal/messaging"
	"saga-pattern/orchestration-saga/inventory-service/internal/observability"
	"saga-pattern/orchestration-saga/inventory-service/internal/repository"
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

	resources, err := serverutil.BootstrapParticipant(cfg, "orchestration-inventory-service", "orchestration-saga/inventory-service/db/migrations")
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
		return fmt.Errorf("create inventory postgres repository: %w", err)
	}
	metrics, err := observability.NewMetrics(resources.Registry)
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}
	replyPublisher := messaging.NewReplyPublisher(resources.RuntimePublisher)
	service, err := inventory.NewService(repo, replyPublisher, metrics)
	if err != nil {
		return fmt.Errorf("create inventory service: %w", err)
	}
	consumer, err := messaging.NewCommandConsumer(service)
	if err != nil {
		return fmt.Errorf("create inventory command consumer: %w", err)
	}
	consumerGroup, err := commonkafka.NewSubscriberGroup(cfg.Runtime.KafkaBrokers, cfg.ServiceName, consumer.Topics(), resources.Logger, func(ctx context.Context, topic, key string, value []byte) error {
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
		Service:        service,
	})
	return serverutil.RunParticipantServer(cfg, resources.Logger, handler, consumer.Topics(), replyPublisher.Topic(), consumerGroup)
}
