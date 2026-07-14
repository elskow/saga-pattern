package main

import (
    "context"
    "database/sql"
    "fmt"
    "log/slog"
    "os"

    _ "github.com/lib/pq"

    choreoruntime "saga-pattern/choreography-framework/runtime"
    "saga-pattern/choreography-saga/internal/serverutil"
    serviceconfig "saga-pattern/choreography-saga/order-service/internal/config"
    "saga-pattern/choreography-saga/order-service/internal/httpapi"
    "saga-pattern/choreography-saga/order-service/internal/observability"
    ordersvc "saga-pattern/choreography-saga/order-service/internal/orders"
    "saga-pattern/choreography-saga/order-service/internal/repository"
    commonconfig "saga-pattern/common/config"
    "saga-pattern/common/events"
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

    resources, err := serverutil.BootstrapParticipantWithExtraMigrations(
        cfg,
        "choreography-order-service",
        "choreography-saga/order-service/db/migrations",
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

    app, err := buildOrderApp(cfg, resources.Logger, resources.DB, resources.RuntimePublisher)
    if err != nil {
        return err
    }

    workerCtx, stopWorkers := context.WithCancel(context.Background())
    defer stopWorkers()
    go app.participant.RunWorkers(workerCtx)
    handler := httpapi.NewHandler(httpapi.HandlerDependencies{Config: cfg, Logger: resources.Logger, Orders: app.orders, Registry: app.metrics.Registry()})
    return serverutil.RunParticipantServer(cfg, resources.Logger, handler, app.participant.Topics(), app.consumerGroup)
}

type orderApp struct {
    orders        *ordersvc.Service
    metrics       *observability.Metrics
    participant   *choreoruntime.Participant
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
    handlerHolder := &eventHandlerHolder{}
    participant, err := choreoruntime.NewPostgres(
        db,
        choreoruntime.EventRegistry{
            Subscriptions: []choreoruntime.TopicSubscription{
                {Topic: commonkafka.DefaultPaymentEventsTopic, EventTypes: []string{events.TypePaymentCompleted, events.TypePaymentFailed, events.TypePaymentRefunded}},
                {Topic: commonkafka.DefaultInventoryEventsTopic, EventTypes: []string{events.TypeInventoryReserved, events.TypeInventoryReservationFailed, events.TypeInventoryReleased}},
                {Topic: commonkafka.DefaultShippingEventsTopic, EventTypes: []string{events.TypeShippingScheduled, events.TypeShippingFailed, events.TypeShippingCancelled}},
            },
            OnUnknownEvent: choreoruntime.RejectUnknown,
        },
        handlerHolder,
        newFrameworkPublisher(runtimePublisher),
        choreoruntime.Config{ServiceName: cfg.ServiceName},
    )
    if err != nil {
        return nil, fmt.Errorf("create choreography framework participant: %w", err)
    }
    frameworkParticipant := ordersvc.NewFrameworkParticipant(participant)
    orders, err := ordersvc.NewService(repo, frameworkParticipant, catalogClient, metrics)
    if err != nil {
        return nil, fmt.Errorf("create order service: %w", err)
    }
    handlerHolder.handler = orders
    consumerGroup, err := commonkafka.NewSubscriberGroup(cfg.Runtime.KafkaBrokers, cfg.ServiceName, participant.Topics(), logger, participant.ConsumeRaw)
    if err != nil {
        return nil, fmt.Errorf("create kafka subscriber group: %w", err)
    }
    return &orderApp{orders: orders, metrics: metrics, participant: participant, consumerGroup: consumerGroup}, nil
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

func (p frameworkPublisher) Publish(ctx context.Context, message choreoruntime.Message) error {
    return p.inner.PublishRaw(ctx, message.Topic, message.Key, message.Payload)
}
