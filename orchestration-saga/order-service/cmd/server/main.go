package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"

	commonconfig "saga-pattern/common/config"
	commondbobs "saga-pattern/common/dbobservability"
	commonhttpobs "saga-pattern/common/httpobservability"
	"saga-pattern/common/inventorycatalog"
	commonkafka "saga-pattern/common/kafka"
	commonlogging "saga-pattern/common/logging"
	"saga-pattern/common/migrations"
	commonshutdown "saga-pattern/common/shutdown"
	commontracing "saga-pattern/common/tracing"

	sagaRuntime "saga-pattern/orchestration-framework/runtime"
	serviceconfig "saga-pattern/orchestration-saga/order-service/internal/config"
	"saga-pattern/orchestration-saga/order-service/internal/httpapi"
	"saga-pattern/orchestration-saga/order-service/internal/messaging"
	"saga-pattern/orchestration-saga/order-service/internal/observability"
	ordersvc "saga-pattern/orchestration-saga/order-service/internal/orders"
	"saga-pattern/orchestration-saga/order-service/internal/repository"
	ordersaga "saga-pattern/orchestration-saga/order-service/internal/saga"
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

	logger := commonlogging.NewTextLogger(os.Stdout, cfg.Runtime.LogLevel)

	tracingShutdown, err := commontracing.Init(context.Background(), cfg.ServiceName, cfg.Runtime.OTLPEndpoint)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		if closeErr := tracingShutdown(context.Background()); err == nil && closeErr != nil {
			err = fmt.Errorf("shutdown tracing: %w", closeErr)
		}
	}()

	observedDB, err := commondbobs.OpenPostgres(cfg.Runtime.DatabaseURL, cfg.ServiceName)
	if err != nil {
		return fmt.Errorf("open postgres connection: %w", err)
	}
	db := observedDB.DB
	configureDatabasePool(db)
	defer func() {
		if closeErr := observedDB.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close database: %w", closeErr)
		}
	}()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping postgres connection: %w", err)
	}

	if err := migrations.Run(context.Background(), db, "orchestration-framework", "orchestration-framework/db/migrations"); err != nil {
		return fmt.Errorf("run orchestration framework migrations: %w", err)
	}

	if err := migrations.Run(context.Background(), db, "orchestration-order-service", "orchestration-saga/order-service/db/migrations"); err != nil {
		return fmt.Errorf("run orchestration order migrations: %w", err)
	}

	metricsRegistry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(metricsRegistry)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}

	runtimePublisher, err := commonkafka.NewRuntimePublisher(cfg.Runtime.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("create runtime publisher: %w", err)
	}
	defer func() {
		if closeErr := runtimePublisher.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close runtime publisher: %w", closeErr)
		}
	}()

	definition := ordersaga.Definition(commonkafka.DefaultTopics())

	publisher, err := messaging.NewKafkaRuntimePublisher(runtimePublisher)
	if err != nil {
		return fmt.Errorf("create framework publisher adapter: %w", err)
	}

	runtime, err := sagaRuntime.NewPostgres(definition, db, sagaRuntime.PostgresDependencies{
		Publisher:       publisher,
		MetricsRegistry: metricsRegistry,
		Config:          runtimeConfig(cfg),
	})
	if err != nil {
		return fmt.Errorf("create postgres runtime: %w", err)
	}

	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		return fmt.Errorf("create postgres order repository: %w", err)
	}

	catalogClient, err := inventorycatalog.NewClient(cfg.Runtime.InventoryServiceURL, nil)
	if err != nil {
		return fmt.Errorf("create inventory catalog client: %w", err)
	}

	service, err := ordersvc.NewService(repo, runtime, catalogClient, metrics)
	if err != nil {
		return fmt.Errorf("new order service: %w", err)
	}

	replyConsumer, err := messaging.NewReplyConsumer(service, definition.ReplyTopics())
	if err != nil {
		return fmt.Errorf("create reply consumer: %w", err)
	}

	consumerGroup, err := commonkafka.NewSubscriberGroup(
		cfg.Runtime.KafkaBrokers,
		cfg.ServiceName,
		replyConsumer.Topics(),
		logger,
		func(ctx context.Context, topic string, key string, value []byte) error {
			return replyConsumer.Consume(ctx, topic, key, value, time.Now().UTC())
		},
	)
	if err != nil {
		return fmt.Errorf("create reply subscriber group: %w", err)
	}
	defer func() {
		if closeErr := consumerGroup.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close reply subscriber group: %w", closeErr)
		}
	}()

	ctx, stop := commonshutdown.NotifyContext()
	defer stop()

	consumerGroup.Start(ctx)
	go runtime.RunWorkers(ctx)

	handler := commontracing.WrapHTTP(cfg.ServiceName, commonhttpobs.Wrap(httpapi.NewHandler(httpapi.HandlerDependencies{
		Config:   cfg,
		Logger:   logger,
		Orders:   service,
		Registry: metrics.Registry(),
	}), commonhttpobs.Options{ServiceName: cfg.ServiceName, Pattern: cfg.Pattern, Logger: logger}))

	server := &http.Server{Addr: cfg.Address(), Handler: handler}

	logger.Info("starting orchestration order service",
		"service", cfg.ServiceName,
		"pattern", cfg.Pattern,
		"addr", cfg.Address(),
		"consumerTopics", definition.ReplyTopics(),
	)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("listen and serve: %w", err)
		}
		return nil
	case <-ctx.Done():
		logger.Info("shutdown signal received", "service", cfg.ServiceName)
	}

	if err := commonshutdown.HTTPServer(server, commonshutdown.DefaultHTTPTimeout); err != nil {
		return fmt.Errorf("shutdown http server: %w", err)
	}

	if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}

	return nil
}

func runtimeConfig(cfg commonconfig.ServiceConfig) sagaRuntime.Config {
	runtimeConfig := sagaRuntime.DefaultConfig()
	if cfg.Runtime.SagaStepTimeout > 0 {
		runtimeConfig.PendingCommandRetryDelay = cfg.Runtime.SagaStepTimeout
	}
	if cfg.Runtime.SagaTimeout > 0 {
		runtimeConfig.SagaTimeout = cfg.Runtime.SagaTimeout
	}
	return runtimeConfig
}

func configureDatabasePool(db *sql.DB) {
	db.SetMaxOpenConns(40)
	db.SetMaxIdleConns(20)
	db.SetConnMaxIdleTime(30 * time.Second)
	db.SetConnMaxLifetime(5 * time.Minute)
}
