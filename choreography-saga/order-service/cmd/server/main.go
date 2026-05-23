package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/lib/pq"

	"saga-pattern/choreography-saga/internal/serverutil"
	serviceconfig "saga-pattern/choreography-saga/order-service/internal/config"
	"saga-pattern/choreography-saga/order-service/internal/httpapi"
	"saga-pattern/choreography-saga/order-service/internal/messaging"
	"saga-pattern/choreography-saga/order-service/internal/observability"
	ordersvc "saga-pattern/choreography-saga/order-service/internal/orders"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	commonconfig "saga-pattern/common/config"
	"saga-pattern/common/inventorycatalog"
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

	resources, err := serverutil.BootstrapParticipant(cfg, "choreography-order-service", "choreography-saga/order-service/db/migrations")
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resources.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close participant resources: %w", closeErr)
		}
	}()
	app, err := buildOrderApp(cfg, resources.Logger, resources.DB, resources.RuntimePublisher)
	if err != nil {
		return err
	}
	outboxCtx, stopOutbox := context.WithCancel(context.Background())
	defer stopOutbox()
	go app.runOrderOutbox(outboxCtx, resources.Logger)
	handler := httpapi.NewHandler(httpapi.HandlerDependencies{Config: cfg, Logger: resources.Logger, Orders: app.orders, Registry: app.metrics.Registry()})
	return serverutil.RunParticipantServer(cfg, resources.Logger, handler, app.consumer.Topics(), app.consumerGroup)
}

type orderApp struct {
	orders        *ordersvc.Service
	metrics       *observability.Metrics
	consumer      *messaging.DownstreamConsumer
	consumerGroup *commonkafka.SubscriberGroup
}

func buildOrderApp(cfg commonconfig.ServiceConfig, logger *slog.Logger, db *sql.DB, runtimePublisher *commonkafka.RuntimePublisher) (*orderApp, error) {
	repo, err := repository.NewPostgresRepository(db)
	if err != nil {
		return nil, fmt.Errorf("create order postgres repository: %w", err)
	}
	catalogClient, err := inventorycatalog.NewClient(cfg.Runtime.InventoryServiceURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create inventory catalog client: %w", err)
	}
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		return nil, fmt.Errorf("create metrics: %w", err)
	}
	orders, err := ordersvc.NewService(repo, messaging.NewOrderTopicPublisher(runtimePublisher), catalogClient, metrics)
	if err != nil {
		return nil, fmt.Errorf("create order service: %w", err)
	}
	consumer, err := messaging.NewDownstreamConsumer(orders, logger)
	if err != nil {
		return nil, fmt.Errorf("create downstream consumer: %w", err)
	}
	consumerGroup, err := commonkafka.NewSubscriberGroup(cfg.Runtime.KafkaBrokers, cfg.ServiceName, consumer.Topics(), logger, func(ctx context.Context, topic string, key string, value []byte) error {
		return consumer.Consume(ctx, messaging.DownstreamEnvelope{Topic: topic, Key: key, Value: value})
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka subscriber group: %w", err)
	}
	return &orderApp{orders: orders, metrics: metrics, consumer: consumer, consumerGroup: consumerGroup}, nil
}

func (a *orderApp) runOrderOutbox(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.orders.PublishPendingOrderEvents(ctx); err != nil {
				logger.Warn("publish pending order outbox", "error", err)
			}
		}
	}
}
