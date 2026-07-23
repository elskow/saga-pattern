package orders

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"saga-pattern/choreography-saga/order-service/internal/domain"
	"saga-pattern/choreography-saga/order-service/internal/observability"
	"saga-pattern/choreography-saga/order-service/internal/repository"
	"saga-pattern/common/events"
)

var (
	SagaTimeoutThreshold atomic.Int64 // milliseconds, default 60000
	SagaTimeoutInterval  atomic.Int64 // milliseconds, default 30000
)

func init() {
	SagaTimeoutThreshold.Store(60000)
	SagaTimeoutInterval.Store(30000)
	initFromEnv()
}

func initFromEnv() {
	if v := os.Getenv("SAGA_TIMEOUT_THRESHOLD_MS"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
			SagaTimeoutThreshold.Store(ms)
		}
	}
	if v := os.Getenv("SAGA_TIMEOUT_SCAN_INTERVAL_MS"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
			SagaTimeoutInterval.Store(ms)
		}
	}
}

// RunTimeoutScanner periodically scans for orders stuck in non-terminal states
// and cancels them with an OrderCancelledEvent emitted atomically. Blocks until
// ctx is cancelled.
func RunTimeoutScanner(ctx context.Context, logger *slog.Logger, repo repository.Repository, participant participantAdapter, metrics *observability.Metrics) {
	interval := time.Duration(SagaTimeoutInterval.Load()) * time.Millisecond
	logger.Info("timeout scanner started",
		"thresholdMs", SagaTimeoutThreshold.Load(),
		"intervalMs", SagaTimeoutInterval.Load())
	for {
		select {
		case <-ctx.Done():
			logger.Info("timeout scanner stopped")
			return
		case <-time.After(interval):
			scanAndCancel(ctx, logger, repo, participant, metrics)
			interval = time.Duration(SagaTimeoutInterval.Load()) * time.Millisecond
		}
	}
}

func scanAndCancel(ctx context.Context, logger *slog.Logger, repo repository.Repository, participant participantAdapter, metrics *observability.Metrics) {
	threshold := time.Duration(SagaTimeoutThreshold.Load()) * time.Millisecond
	cutoff := time.Now().UTC().Add(-threshold)

	stuck, err := repo.FindStuckOrders(ctx, cutoff)
	if err != nil {
		logger.Error("timeout scanner: failed to find stuck orders", "error", err)
		return
	}
	if len(stuck) == 0 {
		return
	}
	logger.Info("timeout scanner: found stuck orders", "count", len(stuck))

	for i := range stuck {
		cancelTimeoutOrder(ctx, logger, repo, participant, metrics, &stuck[i])
	}
}

func cancelTimeoutOrder(ctx context.Context, logger *slog.Logger, repo repository.Repository, participant participantAdapter, metrics *observability.Metrics, order *domain.Order) {
	now := time.Now().UTC()
	previousStatus := order.Status
	order.MarkCancelled("saga timeout", now)

	correlationID := order.CorrelationID
	if correlationID == "" {
		correlationID = order.OrderID
	}
	evt := events.NewOrderCancelledEvent(order.OrderID, "saga timeout", now, correlationID)

	hook := func(hctx context.Context, tx *sql.Tx) error {
		return participant.EnqueueOrderCancelled(hctx, tx, order.OrderID, evt)
	}

	if err := repo.SaveWithHook(ctx, *order, repository.TxHook(hook)); err != nil {
		logger.Error("timeout scanner: failed to cancel order", "orderId", order.OrderID, "error", err)
		return
	}
	participant.TriggerImmediatePublish(ctx)
	// Match foldTerminalFailure: timeout cancel is still a terminal saga failure for thesis metrics.
	if metrics != nil {
		metrics.RecordOrderFailed(order.UpdatedAt.Sub(order.CreatedAt))
	}

	logger.Info("timeout scanner: cancelled stuck order",
		"orderId", order.OrderID,
		"previousStatus", string(previousStatus))
}
