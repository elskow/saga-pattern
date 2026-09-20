package shipping

import (
	"context"
	"database/sql"
	"time"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
	"saga-pattern/choreography-saga/shipping-service/internal/repository"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

func (s *Service) buildScheduledShippingHook(correlationID string) func(domain.Shipment) repository.TxHook {
	return func(shipment domain.Shipment) repository.TxHook {
		scheduledEvent := events.NewShippingScheduledEvent(
			shipment.ShippingID,
			shipment.OrderID,
			shipment.TrackingNumber,
			shipment.ShippingAddress,
			shipment.EstimatedDelivery,
			shipment.UpdatedAt,
			correlationID,
			shipment.CreatedAt,
		)
		return func(hctx context.Context, tx *sql.Tx) error {
			return s.participant.EnqueueEvent(hctx, tx, commonkafka.DefaultShippingEventsTopic, shipment.OrderID, scheduledEvent.EventType(), scheduledEvent)
		}
	}
}

func (s *Service) buildCancelledShippingHook(correlationID string) func(domain.Shipment) repository.TxHook {
	return func(shipment domain.Shipment) repository.TxHook {
		cancelledEvent := events.NewShippingCancelledEvent(shipment.ShippingID, shipment.OrderID, shipment.UpdatedAt, correlationID, shipment.UpdatedAt)
		return func(hctx context.Context, tx *sql.Tx) error {
			return s.participant.EnqueueEvent(hctx, tx, commonkafka.DefaultShippingEventsTopic, shipment.OrderID, cancelledEvent.EventType(), cancelledEvent)
		}
	}
}

func (s *Service) buildFailedShippingEvent(orderID, reason string, failedAt time.Time, correlationID string) events.ShippingFailedEvent {
	return events.NewShippingFailedEvent(orderID, reason, failedAt, correlationID, failedAt)
}
