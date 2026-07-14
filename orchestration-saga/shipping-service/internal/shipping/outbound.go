package shipping

import (
	"context"

	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-saga/shipping-service/internal/domain"
)

func (s *Service) publishReply(ctx context.Context, orderID string, reply commonreplies.SagaReply) error {
	if err := reply.Validate(); err != nil {
		return err
	}
	return s.publisher.PublishReply(ctx, orderID, reply)
}

func existingScheduleReply(shipment domain.Shipment, orderID string) commonreplies.SagaReply {
	switch shipment.Status {
	case domain.ShipmentStatusScheduled:
		return commonreplies.NewShippingScheduledReply(shipment.ShippingID, orderID, shipment.TrackingNumber)
	case domain.ShipmentStatusFailed:
		reason := shipment.FailureReason
		if reason == "" {
			reason = "shipment previously failed"
		} else {
			reason = "Shipment previously failed: " + reason
		}
		return commonreplies.NewShippingFailedReply(shipment.ShippingID, orderID, reason)
	case domain.ShipmentStatusCancelled:
		return commonreplies.NewShippingFailedReply(shipment.ShippingID, orderID, "Shipment was already cancelled")
	default:
		return commonreplies.NewShippingFailedReply(shipment.ShippingID, orderID, "Shipment status is unsupported")
	}
}
