package inventory

import (
	"context"

	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-saga/inventory-service/internal/domain"
)

func (s *Service) publishReply(ctx context.Context, orderID string, reply commonreplies.SagaReply) error {
	if err := reply.Validate(); err != nil {
		return err
	}
	return s.publisher.PublishReply(ctx, orderID, reply)
}

func existingReserveReply(reservation domain.Reservation, orderID string) commonreplies.SagaReply {
	switch reservation.Status {
	case domain.ReservationStatusReserved:
		return commonreplies.NewInventoryReservedReply(reservation.ReservationID, orderID)
	case domain.ReservationStatusFailed:
		reason := reservation.FailureReason
		if reason == "" {
			reason = "inventory reservation previously failed"
		} else {
			reason = "Reservation previously failed: " + reason
		}
		return commonreplies.NewInventoryFailedReply(reservation.ReservationID, orderID, reason)
	case domain.ReservationStatusReleased:
		return commonreplies.NewInventoryFailedReply(reservation.ReservationID, orderID, "Reservation was already released")
	default:
		return commonreplies.NewInventoryFailedReply(reservation.ReservationID, orderID, "Reservation status is unsupported")
	}
}
