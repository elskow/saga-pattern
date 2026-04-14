package inventory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"saga-pattern/common/commands"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-saga/inventory-service/internal/domain"
	"saga-pattern/orchestration-saga/inventory-service/internal/messaging"
	"saga-pattern/orchestration-saga/inventory-service/internal/observability"
	"saga-pattern/orchestration-saga/inventory-service/internal/repository"
)

type Clock func() time.Time

type Service struct {
	repo      repository.Repository
	publisher messaging.ReplyPublisher
	metrics   *observability.Metrics
	clock     Clock
}

func NewService(repo repository.Repository, publisher messaging.ReplyPublisher, metrics *observability.Metrics) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics are required")
	}
	if publisher.Topic() == "" {
		return nil, fmt.Errorf("reply publisher is required")
	}
	return &Service{repo: repo, publisher: publisher, metrics: metrics, clock: time.Now}, nil
}

func (s *Service) WithClock(clock Clock) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *Service) HandleReserveInventory(ctx context.Context, command commands.ReserveInventoryCommand) error {
	startedAt := s.now()
	existing, ok, err := s.repo.GetReservation(ctx, command.ReservationID)
	if err != nil {
		return err
	}
	if ok {
		return s.publishReply(ctx, command.OrderID, existingReserveReply(existing, command.OrderID))
	}

	_, err = s.repo.ReserveInventory(ctx, command.ReservationID, command.OrderID, command.Items, startedAt)
	if err == nil {
		s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryReservedReply(command.ReservationID, command.OrderID))
	}

	if !isInventoryReservationFailure(err) {
		return s.publishExistingOrError(ctx, command.OrderID, command.ReservationID, err, existingReserveReply)
	}

	reason := err.Error()
	if _, saveErr := s.repo.SaveFailedReservation(ctx, command.ReservationID, command.OrderID, command.Items, reason, startedAt); saveErr != nil {
		return s.publishExistingOrError(ctx, command.OrderID, command.ReservationID, saveErr, existingReserveReply)
	}
	s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
	return s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryFailedReply(command.ReservationID, command.OrderID, reason))
}

func (s *Service) HandleReleaseInventory(ctx context.Context, command commands.ReleaseInventoryCommand) error {
	startedAt := s.now()
	reservation, ok, err := s.repo.GetReservation(ctx, command.ReservationID)
	if err != nil {
		return err
	}
	if !ok {
		return s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryReleasedReply(command.ReservationID, command.OrderID, false, "Release failed"))
	}
	if reservation.Status == domain.ReservationStatusReleased {
		return s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryReleasedReply(command.ReservationID, command.OrderID, true, ""))
	}

	if _, err := s.repo.ReleaseReservation(ctx, command.ReservationID, startedAt, repository.CompensationReleaseReason()); err != nil {
		return err
	}
	s.metrics.RecordInventoryCompensation(s.now().Sub(startedAt))
	return s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryReleasedReply(command.ReservationID, command.OrderID, true, ""))
}

func (s *Service) HealthStatus(ctx context.Context) error {
	return s.repo.PingContext(ctx)
}

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

func isInventoryReservationFailure(err error) bool {
	if err == nil {
		return false
	}
	var productNotFound domain.ProductNotFoundError
	if errors.As(err, &productNotFound) {
		return true
	}
	var insufficientStock domain.InsufficientStockError
	return errors.As(err, &insufficientStock)
}

func (s *Service) publishExistingOrError(ctx context.Context, orderID, reservationID string, original error, replyFor func(domain.Reservation, string) commonreplies.SagaReply) error {
	existing, ok, err := s.repo.GetReservation(ctx, reservationID)
	if err != nil {
		return err
	}
	if !ok {
		return original
	}
	return s.publishReply(ctx, orderID, replyFor(existing, orderID))
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
