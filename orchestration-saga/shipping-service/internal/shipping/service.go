package shipping

import (
	"context"
	"fmt"
	"strings"
	"time"

	"saga-pattern/common/commands"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-saga/shipping-service/internal/domain"
	"saga-pattern/orchestration-saga/shipping-service/internal/messaging"
	"saga-pattern/orchestration-saga/shipping-service/internal/observability"
	"saga-pattern/orchestration-saga/shipping-service/internal/repository"
)

const failureTriggerToken = "FAIL_SHIPPING"

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

func (s *Service) HandleScheduleShipping(ctx context.Context, command commands.ScheduleShippingCommand) error {
	startedAt := s.now()
	existing, ok, err := s.repo.GetByShippingID(ctx, command.ShippingID)
	if err != nil {
		return err
	}
	if ok {
		return s.publishReply(ctx, command.OrderID, existingScheduleReply(existing, command.OrderID))
	}

	var reply commonreplies.SagaReply
	var shipment domain.Shipment
	if shippingShouldFail(command.ShippingAddress) {
		reason := shippingFailureReason(command.ShippingAddress)
		shipment, err = domain.NewFailedShipment(command.ShippingID, command.OrderID, command.ShippingAddress, reason, startedAt)
		if err != nil {
			return err
		}
		reply = commonreplies.NewShippingFailedReply(command.ShippingID, command.OrderID, reason)
	} else {
		shipment, err = domain.NewScheduledShipment(command.ShippingID, command.OrderID, command.ShippingAddress, startedAt)
		if err != nil {
			return err
		}
		reply = commonreplies.NewShippingScheduledReply(command.ShippingID, command.OrderID)
	}

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
	return s.publishReply(ctx, command.OrderID, reply)
}

func (s *Service) HandleCancelShipping(ctx context.Context, command commands.CancelShippingCommand) error {
	startedAt := s.now()
	shipment, ok, err := s.repo.GetByShippingID(ctx, command.ShippingID)
	if err != nil {
		return err
	}
	if !ok {
		return s.publishReply(ctx, command.OrderID, commonreplies.NewShippingCancelledReply(command.ShippingID, command.OrderID, true, ""))
	}
	if shipment.Status == domain.ShipmentStatusCancelled {
		return s.publishReply(ctx, command.OrderID, commonreplies.NewShippingCancelledReply(command.ShippingID, command.OrderID, true, ""))
	}
	if err := shipment.Cancel(repository.CompensationCancellationReason, s.now()); err != nil {
		return err
	}
	if err := s.repo.Save(ctx, shipment); err != nil {
		return err
	}
	s.metrics.RecordShippingCompensation(s.now().Sub(startedAt))
	return s.publishReply(ctx, command.OrderID, commonreplies.NewShippingCancelledReply(command.ShippingID, command.OrderID, true, ""))
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
		return commonreplies.NewShippingScheduledReply(shipment.ShippingID, orderID)
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

func shippingShouldFail(address string) bool {
	return strings.Contains(strings.ToUpper(address), failureTriggerToken)
}

func shippingFailureReason(_ string) string {
	return "shipping simulation requested failure"
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
