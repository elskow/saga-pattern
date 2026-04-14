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
	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/common/httpcompat"
	commonkafka "saga-pattern/common/kafka"
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
		return fmt.Errorf("create inventory postgres repository: %w", err)
	}
	registry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}
	runtimePublisher, err := commonkafka.NewRuntimePublisher(cfg.Runtime.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("create runtime publisher: %w", err)
	}
	defer runtimePublisher.Close()
	replyPublisher := messaging.NewReplyPublisher(runtimePublisher)
	service, err := inventory.NewService(repo, replyPublisher, metrics)
	if err != nil {
		return fmt.Errorf("create inventory service: %w", err)
	}
	consumer, err := messaging.NewCommandConsumer(service)
	if err != nil {
		return fmt.Errorf("create inventory command consumer: %w", err)
	}
	consumerGroup, err := commonkafka.NewSubscriberGroup(cfg.Runtime.KafkaBrokers, cfg.ServiceName, consumer.Topics(), logger, func(ctx context.Context, topic, key string, value []byte) error {
		return consumer.Consume(ctx, messaging.CommandEnvelope{Topic: topic, Key: key, Value: value})
	})
	if err != nil {
		return fmt.Errorf("create kafka subscriber group: %w", err)
	}
	defer consumerGroup.Close()
	consumerGroup.Start(context.Background())
	handler := httpapi.NewHandler(httpapi.HandlerDependencies{
		Config:   cfg,
		Logger:   logger,
		Registry: metrics.Registry(),
		HealthProvider: func(ctx context.Context) httpcompat.HealthResponse {
			response := httpcompat.HealthResponse{Status: httpcompat.StatusUp, Components: map[string]httpcompat.HealthComponent{
				"db":    {Status: httpcompat.StatusUp},
				"kafka": {Status: httpcompat.StatusUp, Details: map[string]any{"brokers": cfg.Runtime.KafkaBrokers}},
			}}
			if err := service.HealthStatus(ctx); err != nil {
				response.Status = "DOWN"
				response.Components["db"] = httpcompat.HealthComponent{Status: "DOWN", Details: map[string]any{"error": err.Error()}}
			}
			return response
		},
	})
	server := &http.Server{Addr: cfg.Address(), Handler: handler}

	logger.Info("starting orchestration inventory service", "service", cfg.ServiceName, "pattern", cfg.Pattern, "addr", cfg.Address(), "consumerTopics", consumer.Topics(), "replyTopic", replyPublisher.Topic())
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}

	return nil
}
