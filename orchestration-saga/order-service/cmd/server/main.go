package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"

	commonkafka "saga-pattern/common/kafka"
	frameworkruntime "saga-pattern/orchestration-framework/runtime"
	serviceconfig "saga-pattern/orchestration-saga/order-service/internal/config"
	"saga-pattern/orchestration-saga/order-service/internal/httpapi"
	"saga-pattern/orchestration-saga/order-service/internal/messaging"
	"saga-pattern/orchestration-saga/order-service/internal/observability"
	ordersvc "saga-pattern/orchestration-saga/order-service/internal/orders"
	"saga-pattern/orchestration-saga/order-service/internal/repository"
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

	registry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	runtimePublisher, err := commonkafka.NewRuntimePublisher(cfg.Runtime.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("create runtime publisher: %w", err)
	}
	defer runtimePublisher.Close()
	publisher, err := messaging.NewKafkaRuntimePublisher(runtimePublisher)
	if err != nil {
		return fmt.Errorf("create framework publisher adapter: %w", err)
	}
	runtime, err := frameworkruntime.NewPostgres(frameworkruntime.OrderDefinition(), db, frameworkruntime.PostgresDependencies{
		Publisher:       publisher,
		MetricsRegistry: registry,
	})
	if err != nil {
		return fmt.Errorf("create postgres runtime: %w", err)
	}
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		return fmt.Errorf("create postgres order repository: %w", err)
	}
	service, err := ordersvc.NewService(repo, runtime, metrics)
	if err != nil {
		return fmt.Errorf("new order service: %w", err)
	}
	consumer, err := messaging.NewReplyConsumer(service, []string{
		commonkafka.DefaultPaymentRepliesTopic,
		commonkafka.DefaultInventoryRepliesTopic,
		commonkafka.DefaultShippingRepliesTopic,
	})
	if err != nil {
		return fmt.Errorf("reply consumer: %w", err)
	}
	consumerGroup, err := commonkafka.NewSubscriberGroup(cfg.Runtime.KafkaBrokers, cfg.ServiceName, consumer.Topics(), logger, func(ctx context.Context, topic string, key string, value []byte) error {
		sum := sha256.Sum256(append(append([]byte(topic+":"+key+":"), value...), byte(len(value)%251)))
		return consumer.Consume(ctx, messaging.ReplyEnvelope{Topic: topic, SagaID: key, ReplyID: fmt.Sprintf("%x", sum), Payload: value, ReceivedAt: time.Now().UTC()})
	})
	if err != nil {
		return fmt.Errorf("create reply subscriber group: %w", err)
	}
	defer consumerGroup.Close()
	ctx := context.Background()
	consumerGroup.Start(ctx)
	go runtime.RunWorkers(ctx)
	handler := httpapi.NewHandler(httpapi.HandlerDependencies{Config: cfg, Logger: logger, Orders: service, Registry: metrics.Registry()})
	server := &http.Server{Addr: cfg.Address(), Handler: handler}

	logger.Info("starting orchestration order service", "service", cfg.ServiceName, "pattern", cfg.Pattern, "addr", cfg.Address(), "consumerTopics", consumer.Topics())
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}

	return nil
}
