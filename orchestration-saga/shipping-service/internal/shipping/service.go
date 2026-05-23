package shipping

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/common/commands"
	"saga-pattern/common/faultinjection"
	commonreplies "saga-pattern/common/replies"
	commontracing "saga-pattern/common/tracing"
	"saga-pattern/orchestration-saga/shipping-service/internal/domain"
	"saga-pattern/orchestration-saga/shipping-service/internal/messaging"
	"saga-pattern/orchestration-saga/shipping-service/internal/observability"
	"saga-pattern/orchestration-saga/shipping-service/internal/repository"
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

func (s *Service) HandleScheduleShipping(ctx context.Context, command commands.ScheduleShippingCommand) (err error) {
	ctx, span := commontracing.Tracer("orchestration/shipping-service").Start(ctx, "orchestration.participant.shipping.schedule_shipping",
		trace.WithAttributes(
			attribute.String("saga.participant", "shipping"),
			attribute.String("saga.command.type", commands.CommandScheduleShipping),
			attribute.String("order.id", command.OrderID),
			attribute.String("shipping.id", command.ShippingID),
		))
	defer finishParticipantSpan(span, &err)
	startedAt := s.now()
	existing, ok, err := s.repo.GetByShippingID(ctx, command.ShippingID)
	if err != nil {
		return err
	}
	if ok {
		span.SetAttributes(attribute.String("shipping.result", "existing"))
		return s.publishReply(ctx, command.OrderID, existingScheduleReply(existing, command.OrderID))
	}

	// Failure mode injection
	if shouldFail, failureState := s.failureMode.ShouldFail(s.now()); shouldFail {
		reason := "shipping failure mode enabled — scheduling forced to fail"
		span.SetAttributes(attribute.String("shipping.result", "failed"), attribute.String("failure.type", "failure_mode"), attribute.String("failure.run_label", failureState.RunLabel), attribute.Int("failure.remaining", failureState.Remaining))
		shipment, err := domain.NewFailedShipment(command.ShippingID, command.OrderID, command.ShippingAddress, reason, startedAt)
		if err != nil {
			return err
		}
		if _, err := s.repo.Create(ctx, shipment); err != nil {
			return err
		}
		s.metrics.RecordShippingStep(s.now().Sub(startedAt))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewShippingFailedReply(command.ShippingID, command.OrderID, reason))
	}

	span.SetAttributes(attribute.String("shipping.result", "scheduled"))
	shipment, err := domain.NewScheduledShipment(command.ShippingID, command.OrderID, command.ShippingAddress, startedAt)
	if err != nil {
		return err
	}
	reply := commonreplies.NewShippingScheduledReply(command.ShippingID, command.OrderID, shipment.TrackingNumber)

	if _, err := s.repo.Create(ctx, shipment); err != nil {
		existing, ok, getErr := s.repo.GetByShippingID(ctx, command.ShippingID)
		if getErr != nil {
			return getErr
		}
		if !ok {
			return err
		}
		return s.publishReply(ctx, command.OrderID, existingScheduleReply(existing, command.OrderID))
	}

	s.metrics.RecordShippingStep(s.now().Sub(startedAt))
	err = s.publishReply(ctx, command.OrderID, reply)
	if err == nil {
		span.AddEvent("shipping.reply.published", trace.WithAttributes(attribute.String("reply.type", reply.ReplyType())))
	}
	return err
}

func (s *Service) HandleCancelShipping(ctx context.Context, command commands.CancelShippingCommand) (err error) {
	ctx, span := commontracing.Tracer("orchestration/shipping-service").Start(ctx, "orchestration.participant.shipping.cancel_shipping",
		trace.WithAttributes(
			attribute.String("saga.participant", "shipping"),
			attribute.String("saga.command.type", commands.CommandCancelShipping),
			attribute.String("order.id", command.OrderID),
			attribute.String("shipping.id", command.ShippingID),
			attribute.String("saga.pending.direction", "compensation"),
		))
	defer finishParticipantSpan(span, &err)
	startedAt := s.now()
	shipment, ok, err := s.repo.GetByShippingID(ctx, command.ShippingID)
	if err != nil {
		return err
	}
	if !ok {
		span.SetAttributes(attribute.String("shipping.result", "not_found"))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewShippingCancelledReply(command.ShippingID, command.OrderID, true, ""))
	}
	if shipment.Status == domain.ShipmentStatusCancelled {
		span.SetAttributes(attribute.String("shipping.result", "already_cancelled"))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewShippingCancelledReply(command.ShippingID, command.OrderID, true, ""))
	}
	if err := shipment.Cancel(repository.CompensationCancellationReason, s.now()); err != nil {
		return err
	}
	if err := s.repo.Save(ctx, shipment); err != nil {
		return err
	}
	s.metrics.RecordShippingCompensation(s.now().Sub(startedAt))
	span.SetAttributes(attribute.String("shipping.result", "cancelled"))
	err = s.publishReply(ctx, command.OrderID, commonreplies.NewShippingCancelledReply(command.ShippingID, command.OrderID, true, ""))
	if err == nil {
		span.AddEvent("shipping.reply.published", trace.WithAttributes(attribute.String("reply.type", commonreplies.TypeShippingCancelled)))
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

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
