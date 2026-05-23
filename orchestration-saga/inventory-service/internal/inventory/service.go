package inventory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/common/commands"
	"saga-pattern/common/faultinjection"
	commonreplies "saga-pattern/common/replies"
	commontracing "saga-pattern/common/tracing"
	"saga-pattern/orchestration-saga/inventory-service/internal/domain"
	"saga-pattern/orchestration-saga/inventory-service/internal/messaging"
	"saga-pattern/orchestration-saga/inventory-service/internal/observability"
	"saga-pattern/orchestration-saga/inventory-service/internal/repository"
)

type Clock func() time.Time

type Service struct {
	repo        repository.Repository
	publisher   messaging.ReplyPublisher
	metrics     *observability.Metrics
	clock       Clock
	failureMode faultinjection.Controller
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

func (s *Service) FailureModeEnabled() bool {
	return s.failureMode.Snapshot(s.now()).Enabled
}

func (s *Service) SetFailureModeEnabled(enabled bool) {
	s.failureMode.SetEnabled(enabled, s.now())
}

func (s *Service) FailureModeState() faultinjection.Snapshot {
	return s.failureMode.Snapshot(s.now())
}

func (s *Service) ConfigureFailureMode(config faultinjection.Config) faultinjection.Snapshot {
	return s.failureMode.Configure(config, s.now())
}

func (s *Service) HandleReserveInventory(ctx context.Context, command commands.ReserveInventoryCommand) (err error) {
	ctx, span := commontracing.Tracer("orchestration/inventory-service").Start(ctx, "orchestration.participant.inventory.reserve_inventory",
		trace.WithAttributes(
			attribute.String("saga.participant", "inventory"),
			attribute.String("saga.command.type", commands.CommandReserveInventory),
			attribute.String("order.id", command.OrderID),
			attribute.String("reservation.id", command.ReservationID),
			attribute.Int("item.count", len(command.Items)),
		))
	defer finishParticipantSpan(span, &err)
	startedAt := s.now()
	existing, ok, err := s.repo.GetReservation(ctx, command.ReservationID)
	if err != nil {
		return err
	}
	if ok {
		span.SetAttributes(attribute.String("inventory.result", "existing"))
		return s.publishReply(ctx, command.OrderID, existingReserveReply(existing, command.OrderID))
	}

	// Failure mode injection
	if shouldFail, failureState := s.failureMode.ShouldFail(s.now()); shouldFail {
		reason := "inventory failure mode enabled — reservation forced to fail"
		span.SetAttributes(attribute.String("inventory.result", "failed"), attribute.String("failure.type", "failure_mode"), attribute.String("failure.run_label", failureState.RunLabel), attribute.Int("failure.remaining", failureState.Remaining))
		s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryFailedReply(command.ReservationID, command.OrderID, reason))
	}

	_, err = s.repo.ReserveInventory(ctx, command.ReservationID, command.OrderID, command.Items, startedAt)
	if err == nil {
		s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
		span.SetAttributes(attribute.String("inventory.result", "reserved"))
		err = s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryReservedReply(command.ReservationID, command.OrderID))
		if err == nil {
			span.AddEvent("inventory.reply.published", trace.WithAttributes(attribute.String("reply.type", commonreplies.TypeInventoryReserved)))
		}
		return err
	}

	if !isInventoryReservationFailure(err) {
		return s.publishExistingOrError(ctx, command.OrderID, command.ReservationID, err, existingReserveReply)
	}

	reason := err.Error()
	span.SetAttributes(attribute.String("inventory.result", "failed"), attribute.String("failure.type", "reservation_failure"))
	if _, saveErr := s.repo.SaveFailedReservation(ctx, command.ReservationID, command.OrderID, command.Items, reason, startedAt); saveErr != nil {
		return s.publishExistingOrError(ctx, command.OrderID, command.ReservationID, saveErr, existingReserveReply)
	}
	s.metrics.RecordInventoryStep(s.now().Sub(startedAt))
	err = s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryFailedReply(command.ReservationID, command.OrderID, reason))
	if err == nil {
		span.AddEvent("inventory.reply.published", trace.WithAttributes(attribute.String("reply.type", commonreplies.TypeInventoryFailed)))
	}
	return err
}

func (s *Service) HandleCommitInventory(ctx context.Context, command commands.CommitInventoryCommand) (err error) {
	ctx, span := commontracing.Tracer("orchestration/inventory-service").Start(ctx, "orchestration.participant.inventory.commit_inventory",
		trace.WithAttributes(
			attribute.String("saga.participant", "inventory"),
			attribute.String("saga.command.type", commands.CommandCommitInventory),
			attribute.String("order.id", command.OrderID),
			attribute.String("reservation.id", command.ReservationID),
		))
	defer finishParticipantSpan(span, &err)
	_, changed, err := s.repo.CommitReservation(ctx, command.ReservationID, s.now())
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Bool("inventory.changed", changed), attribute.String("inventory.result", "committed"))
	err = s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryCommittedReply(command.ReservationID, command.OrderID))
	if err == nil {
		span.AddEvent("inventory.reply.published", trace.WithAttributes(attribute.String("reply.type", commonreplies.TypeInventoryCommitted)))
	}
	return err
}

func (s *Service) HandleReleaseInventory(ctx context.Context, command commands.ReleaseInventoryCommand) (err error) {
	ctx, span := commontracing.Tracer("orchestration/inventory-service").Start(ctx, "orchestration.participant.inventory.release_inventory",
		trace.WithAttributes(
			attribute.String("saga.participant", "inventory"),
			attribute.String("saga.command.type", commands.CommandReleaseInventory),
			attribute.String("order.id", command.OrderID),
			attribute.String("reservation.id", command.ReservationID),
			attribute.String("saga.pending.direction", "compensation"),
		))
	defer finishParticipantSpan(span, &err)
	startedAt := s.now()
	reservation, ok, err := s.repo.GetReservation(ctx, command.ReservationID)
	if err != nil {
		return err
	}
	if !ok {
		span.SetAttributes(attribute.String("inventory.result", "not_found"))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryReleasedReply(command.ReservationID, command.OrderID, false, "Release failed"))
	}
	if reservation.Status == domain.ReservationStatusReleased {
		span.SetAttributes(attribute.String("inventory.result", "already_released"))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryReleasedReply(command.ReservationID, command.OrderID, true, ""))
	}

	if _, err := s.repo.ReleaseReservation(ctx, command.ReservationID, startedAt, repository.CompensationReleaseReason()); err != nil {
		return err
	}
	s.metrics.RecordInventoryCompensation(s.now().Sub(startedAt))
	span.SetAttributes(attribute.String("inventory.result", "released"))
	err = s.publishReply(ctx, command.OrderID, commonreplies.NewInventoryReleasedReply(command.ReservationID, command.OrderID, true, ""))
	if err == nil {
		span.AddEvent("inventory.reply.published", trace.WithAttributes(attribute.String("reply.type", commonreplies.TypeInventoryReleased)))
	}
	return err
}

func finishParticipantSpan(span trace.Span, err *error) {
	if err != nil && *err != nil {
		span.RecordError(*err)
		span.SetStatus(codes.Error, (*err).Error())
	}
	span.End()
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
