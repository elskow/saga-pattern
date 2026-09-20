package inventory

import (
	"context"
	"database/sql"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

func (s *Service) buildReserveHooks(
	sourceEvent events.PaymentCompletedEvent,
	reservedEvent *events.InventoryReservedEvent,
	failedEvent events.InventoryReservationFailedEvent,
	_ /* reservedAt */ interface{}, // unused, kept for readability at call site
) (onReserve repository.TxHook, onFail repository.TxHook) {
	if reservedEvent != nil {
		ev := *reservedEvent
		onReserve = func(hctx context.Context, tx *sql.Tx, resID string, _ error) error {
			actual := events.NewInventoryReservedEvent(resID, ev.OrderID, ev.ReservedItems, ev.ReservedAt, ev.CorrelationID, ev.CreatedAt)
			return s.participant.EnqueueEvent(hctx, tx, commonkafka.DefaultInventoryEventsTopic, sourceEvent.OrderID, actual.EventType(), actual)
		}
	}
	onFail = func(hctx context.Context, tx *sql.Tx, _ string, cause error) error {
		ev := failedEvent
		if cause != nil {
			ev = s.buildReservationFailedEvent(sourceEvent, cause, ev.FailedAt)
		}
		return s.participant.EnqueueEvent(hctx, tx, commonkafka.DefaultInventoryEventsTopic, sourceEvent.OrderID, ev.EventType(), ev)
	}
	return onReserve, onFail
}

func (s *Service) buildReleaseHook(orderID, correlationID string) repository.TxHook {
	return func(hctx context.Context, tx *sql.Tx, reservationID string, _ error) error {
		now := s.now()
		releasedEvent := events.NewInventoryReleasedEvent(reservationID, orderID, now, correlationID, now)
		return s.participant.EnqueueEvent(hctx, tx, commonkafka.DefaultInventoryEventsTopic, orderID, releasedEvent.EventType(), releasedEvent)
	}
}

func (s *Service) buildReservationFailedEvent(event events.PaymentCompletedEvent, cause error, _ interface{}) events.InventoryReservationFailedEvent {
	productID := ""
	reason := "unknown"
	if cause != nil {
		reason = cause.Error()
		switch e := cause.(type) {
		case domain.ProductNotFoundError:
			productID = e.ProductID
		case domain.InsufficientStockError:
			productID = e.ProductID
		}
	}
	now := s.now()
	return events.NewInventoryReservationFailedEvent(event.OrderID, productID, reason, now, event.CorrelationID, now)
}

func buildReservedItems(pendingItems []domain.PendingOrderItem) []events.InventoryReservedItem {
	items := make([]events.InventoryReservedItem, 0, len(pendingItems))
	for _, item := range pendingItems {
		items = append(items, events.InventoryReservedItem{ProductID: item.ProductID, Quantity: item.Quantity})
	}
	return items
}
