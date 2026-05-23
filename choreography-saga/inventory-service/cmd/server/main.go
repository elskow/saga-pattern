package main

import (
	"context"
	"fmt"
	"os"

	_ "github.com/lib/pq"

	"saga-pattern/choreography-saga/internal/serverutil"
	serviceconfig "saga-pattern/choreography-saga/inventory-service/internal/config"
	"saga-pattern/choreography-saga/inventory-service/internal/httpapi"
	"saga-pattern/choreography-saga/inventory-service/internal/inventory"
	"saga-pattern/choreography-saga/inventory-service/internal/messaging"
	"saga-pattern/choreography-saga/inventory-service/internal/observability"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
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

	resources, err := serverutil.BootstrapParticipant(cfg, "choreography-inventory-service", "choreography-saga/inventory-service/db/migrations")
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
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}
	service, err := inventory.NewService(repo, messaging.NewInventoryTopicPublisher(resources.RuntimePublisher), metrics)
	if err != nil {
		return fmt.Errorf("create inventory service: %w", err)
	}
	consumer, err := messaging.NewDownstreamConsumer(service, resources.Logger)
	if err != nil {
		return fmt.Errorf("create downstream consumer: %w", err)
	}
	consumerGroup, err := commonkafka.NewSubscriberGroup(cfg.Runtime.KafkaBrokers, cfg.ServiceName, consumer.Topics(), resources.Logger, func(ctx context.Context, topic string, key string, value []byte) error {
		return consumer.Consume(ctx, messaging.DownstreamEnvelope{Topic: topic, Key: key, Value: value})
	})
	if err != nil {
		return fmt.Errorf("create kafka subscriber group: %w", err)
	}
	handler := httpapi.NewHandler(httpapi.HandlerDependencies{Config: cfg, Logger: resources.Logger, Registry: metrics.Registry(), Repo: repo, Service: service})
	return serverutil.RunParticipantServer(cfg, resources.Logger, handler, consumer.Topics(), consumerGroup)
}
