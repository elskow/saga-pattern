package serverutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	commonconfig "saga-pattern/common/config"
	commondbobs "saga-pattern/common/dbobservability"
	commonhttpobs "saga-pattern/common/httpobservability"
	commonkafka "saga-pattern/common/kafka"
	commonlogging "saga-pattern/common/logging"
	"saga-pattern/common/migrations"
	commonshutdown "saga-pattern/common/shutdown"
	commontracing "saga-pattern/common/tracing"
)

type ParticipantResources struct {
	Logger           *slog.Logger
	DB               *sql.DB
	RuntimePublisher *commonkafka.RuntimePublisher
	cleanup          func() error
}

func BootstrapParticipant(cfg commonconfig.ServiceConfig, migrationScope string, migrationDir string) (*ParticipantResources, error) {
	logger := commonlogging.NewTextLogger(os.Stdout, cfg.Runtime.LogLevel)
	tracingShutdown, err := commontracing.Init(context.Background(), cfg.ServiceName, cfg.Runtime.OTLPEndpoint)
	if err != nil {
		return nil, fmt.Errorf("init tracing: %w", err)
	}
	cleanupStartup := func(db interface{ Close() error }) error {
		var closeErr error
		if db != nil {
			closeErr = errors.Join(closeErr, db.Close())
		}
		closeErr = errors.Join(closeErr, tracingShutdown(context.Background()))
		return closeErr
	}
	observedDB, err := commondbobs.OpenPostgres(cfg.Runtime.DatabaseURL, cfg.ServiceName)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open postgres connection: %w", err), cleanupStartup(nil))
	}
	db := observedDB.DB
	configureDatabasePool(db)
	if err := db.Ping(); err != nil {
		return nil, errors.Join(fmt.Errorf("ping postgres connection: %w", err), cleanupStartup(observedDB))
	}
	if err := migrations.Run(context.Background(), db, migrationScope, migrationDir); err != nil {
		return nil, errors.Join(fmt.Errorf("run %s migrations: %w", migrationScope, err), cleanupStartup(observedDB))
	}
	runtimePublisher, err := commonkafka.NewRuntimePublisher(cfg.Runtime.KafkaBrokers)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create runtime publisher: %w", err), cleanupStartup(observedDB))
	}
	resources := &ParticipantResources{
		Logger:           logger,
		DB:               db,
		RuntimePublisher: runtimePublisher,
	}
	resources.cleanup = func() error {
		var firstErr error
		if err := runtimePublisher.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := observedDB.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := tracingShutdown(context.Background()); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return resources, nil
}

func configureDatabasePool(db *sql.DB) {
	db.SetMaxOpenConns(40)
	db.SetMaxIdleConns(20)
	db.SetConnMaxIdleTime(30 * time.Second)
	db.SetConnMaxLifetime(5 * time.Minute)
}

func (r *ParticipantResources) Close() error {
	if r == nil || r.cleanup == nil {
		return nil
	}
	return r.cleanup()
}

func RunParticipantServer(cfg commonconfig.ServiceConfig, logger *slog.Logger, handler http.Handler, consumerTopics []string, subscriber *commonkafka.SubscriberGroup) (err error) {
	defer func() {
		if closeErr := subscriber.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close subscriber group: %w", closeErr)
		}
	}()
	ctx, stop := commonshutdown.NotifyContext()
	defer stop()
	subscriber.Start(ctx)
	handler = commontracing.WrapHTTP(
		cfg.ServiceName,
		commonhttpobs.Wrap(
			handler,
			commonhttpobs.Options{
				ServiceName: cfg.ServiceName, Pattern: cfg.Pattern, Logger: logger,
			},
		),
	)

	server := &http.Server{Addr: cfg.Address(), Handler: handler}

	logger.Info("starting choreography participant service", "service", cfg.ServiceName, "pattern", cfg.Pattern, "addr", cfg.Address(), "consumerTopics", consumerTopics)
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
