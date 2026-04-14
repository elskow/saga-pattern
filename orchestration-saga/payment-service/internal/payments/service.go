package payments

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"saga-pattern/common/commands"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
	"saga-pattern/orchestration-saga/payment-service/internal/messaging"
	"saga-pattern/orchestration-saga/payment-service/internal/observability"
	"saga-pattern/orchestration-saga/payment-service/internal/repository"
)

const maxPaymentAmount = "10000.00"

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

func (s *Service) HandleProcessPayment(ctx context.Context, command commands.ProcessPaymentCommand) error {
	startedAt := s.now()
	existing, ok, err := s.repo.GetByPaymentID(ctx, command.PaymentID)
	if err != nil {
		return err
	}
	if ok {
		return s.publishReply(ctx, command.OrderID, existingProcessReply(existing, command))
	}

	payment, err := domain.NewPayment(command.PaymentID, command.OrderID, command.CustomerID, command.Amount, startedAt)
	if err != nil {
		return err
	}

	var reply commonreplies.SagaReply
	if paymentShouldFail(command.Amount) {
		reason := fmt.Sprintf("Payment amount exceeds maximum allowed limit of %s", maxPaymentAmount)
		if err := payment.MarkFailed(reason, s.now()); err != nil {
			return err
		}
		reply = commonreplies.NewPaymentFailedReply(command.PaymentID, command.OrderID, reason)
	} else {
		if err := payment.MarkCompleted(s.now()); err != nil {
			return err
		}
		reply = commonreplies.NewPaymentCompletedReply(command.PaymentID, command.OrderID)
	}

	if _, err := s.repo.Create(ctx, payment); err != nil {
		existing, ok, getErr := s.repo.GetByPaymentID(ctx, command.PaymentID)
		if getErr != nil {
			return getErr
		}
		if !ok {
			return err
		}
		return s.publishReply(ctx, command.OrderID, existingProcessReply(existing, command))
	}

	s.metrics.RecordPaymentStep(s.now().Sub(startedAt))
	return s.publishReply(ctx, command.OrderID, reply)
}

func (s *Service) HandleRefundPayment(ctx context.Context, command commands.RefundPaymentCommand) error {
	startedAt := s.now()
	payment, ok, err := s.repo.GetByPaymentID(ctx, command.PaymentID)
	if err != nil {
		return err
	}
	if !ok {
		return s.publishReply(ctx, command.OrderID, commonreplies.NewPaymentRefundedReply(command.PaymentID, command.OrderID, true, ""))
	}
	if payment.Status == domain.PaymentStatusRefunded {
		return s.publishReply(ctx, command.OrderID, commonreplies.NewPaymentRefundedReply(command.PaymentID, command.OrderID, true, ""))
	}
	if err := payment.MarkRefunded("Saga compensation", s.now()); err != nil {
		return err
	}
	if err := s.repo.Save(ctx, payment); err != nil {
		return err
	}
	s.metrics.RecordPaymentCompensation(s.now().Sub(startedAt))
	return s.publishReply(ctx, command.OrderID, commonreplies.NewPaymentRefundedReply(command.PaymentID, command.OrderID, true, ""))
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

func existingProcessReply(payment domain.Payment, command commands.ProcessPaymentCommand) commonreplies.SagaReply {
	switch payment.Status {
	case domain.PaymentStatusCompleted, domain.PaymentStatusPending:
		return commonreplies.NewPaymentCompletedReply(command.PaymentID, command.OrderID)
	case domain.PaymentStatusFailed:
		reason := payment.FailureReason
		if reason == "" {
			reason = "payment previously failed"
		} else {
			reason = "Payment previously failed: " + reason
		}
		return commonreplies.NewPaymentFailedReply(command.PaymentID, command.OrderID, reason)
	case domain.PaymentStatusRefunded:
		return commonreplies.NewPaymentFailedReply(command.PaymentID, command.OrderID, "Payment was already refunded")
	default:
		return commonreplies.NewPaymentFailedReply(command.PaymentID, command.OrderID, "Payment status is unsupported")
	}
}

func paymentShouldFail(amount json.Number) bool {
	value, ok := new(big.Rat).SetString(amount.String())
	if !ok {
		return false
	}
	limit, ok := new(big.Rat).SetString(maxPaymentAmount)
	if !ok {
		return false
	}
	return value.Cmp(limit) >= 0
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
