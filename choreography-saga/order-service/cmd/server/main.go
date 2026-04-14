package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	_ "github.com/lib/pq"

	serviceconfig "saga-pattern/choreography-saga/order-service/internal/config"
	"saga-pattern/choreography-saga/order-service/internal/httpapi"
	"saga-pattern/choreography-saga/order-service/internal/messaging"
	"saga-pattern/choreography-saga/order-service/internal/observability"
	"saga-pattern/choreography-saga/order-service/internal/orders"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	commonkafka "saga-pattern/common/kafka"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := serviceconfig.Load()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	db, err := sql.Open("postgres", cfg.Runtime.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres connection: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping postgres connection: %w", err)
	}
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		return fmt.Errorf("create order postgres repository: %w", err)
	}
	runtimePublisher, err := commonkafka.NewRuntimePublisher(cfg.Runtime.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("create runtime publisher: %w", err)
	}
	defer runtimePublisher.Close()
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}
	orders, err := orders.NewService(repo, messaging.NewOrderTopicPublisher(runtimePublisher), metrics)
	if err != nil {
		return fmt.Errorf("create order service: %w", err)
	}
	consumer, err := messaging.NewDownstreamConsumer(orders, logger)
	if err != nil {
		return fmt.Errorf("create downstream consumer: %w", err)
	}
	consumerGroup, err := commonkafka.NewSubscriberGroup(cfg.Runtime.KafkaBrokers, cfg.ServiceName, consumer.Topics(), logger, func(ctx context.Context, topic string, key string, value []byte) error {
		return consumer.Consume(ctx, messaging.DownstreamEnvelope{Topic: topic, Key: key, Value: value})
	})
	if err != nil {
		return fmt.Errorf("create kafka subscriber group: %w", err)
	}
	defer consumerGroup.Close()
	consumerGroup.Start(context.Background())
	handler := httpapi.NewHandler(httpapi.HandlerDependencies{Config: cfg, Logger: logger, Orders: orders, Registry: metrics.Registry()})
	server := &http.Server{Addr: cfg.Address(), Handler: handler}

	logger.Info("starting service", "service", cfg.ServiceName, "pattern", cfg.Pattern, "addr", cfg.Address(), "consumerTopics", consumer.Topics())
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}

	return nil
}
