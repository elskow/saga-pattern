package payments

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/common/commands"
	commonreplies "saga-pattern/common/replies"
	commontracing "saga-pattern/common/tracing"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
	"saga-pattern/orchestration-saga/payment-service/internal/messaging"
	"saga-pattern/orchestration-saga/payment-service/internal/observability"
	"saga-pattern/orchestration-saga/payment-service/internal/repository"
)

type Clock func() time.Time

type Service struct {
	repo           repository.Repository
	publisher      messaging.ReplyPublisher
	metrics        *observability.Metrics
	clock          Clock
	processMu      sync.Mutex
	depositMu      sync.Mutex
	depositBalance *big.Rat // nil means unlimited
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

func (s *Service) GetDepositBalance() *big.Rat {
	s.depositMu.Lock()
	defer s.depositMu.Unlock()
	if s.depositBalance == nil {
		return nil
	}
	return new(big.Rat).Set(s.depositBalance)
}

func (s *Service) SetDepositBalance(balance *big.Rat) {
	s.depositMu.Lock()
	defer s.depositMu.Unlock()
	if balance != nil {
		s.depositBalance = new(big.Rat).Set(balance)
	} else {
		s.depositBalance = nil
	}
}

func (s *Service) CheckAndDeductBalance(amount *big.Rat) bool {
	s.depositMu.Lock()
	defer s.depositMu.Unlock()
	if s.depositBalance == nil {
		return true
	}
	if s.depositBalance.Cmp(amount) < 0 {
		return false
	}
	s.depositBalance = new(big.Rat).Sub(s.depositBalance, amount)
	return true
}

func (s *Service) RefundBalance(amount *big.Rat) {
	s.depositMu.Lock()
	defer s.depositMu.Unlock()
	if s.depositBalance == nil {
		return
	}
	s.depositBalance = new(big.Rat).Add(s.depositBalance, amount)
}

func (s *Service) HandleProcessPayment(ctx context.Context, command commands.ProcessPaymentCommand) (err error) {
	s.processMu.Lock()
	defer s.processMu.Unlock()

	ctx, span := commontracing.Tracer("orchestration/payment-service").Start(ctx, "orchestration.participant.payment.process_payment",
		trace.WithAttributes(
			attribute.String("saga.participant", "payment"),
			attribute.String("saga.command.type", commands.CommandProcessPayment),
			attribute.String("order.id", command.OrderID),
			attribute.String("payment.id", command.PaymentID),
		))
	defer finishParticipantSpan(span, &err)
	startedAt := s.now()
	existing, ok, err := s.repo.GetByPaymentID(ctx, command.PaymentID)
	if err != nil {
		return err
	}
	if ok {
		span.SetAttributes(attribute.String("payment.result", "existing"))
		return s.publishReply(ctx, command.OrderID, existingProcessReply(existing, command))
	}

	payment, err := domain.NewPayment(command.PaymentID, command.OrderID, command.CustomerID, command.Amount, startedAt)
	if err != nil {
		return err
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

	var reply commonreplies.SagaReply
	amountRat, ok := new(big.Rat).SetString(command.Amount.String())
	if !ok {
		amountRat = big.NewRat(0, 1)
	}
	if !s.CheckAndDeductBalance(amountRat) {
		reason := "insufficient deposit balance"
		span.SetAttributes(attribute.String("payment.result", "failed"), attribute.String("failure.type", "insufficient_balance"))
		if err := payment.MarkFailed(reason, s.now()); err != nil {
			return err
		}
		reply = commonreplies.NewPaymentFailedReply(command.PaymentID, command.OrderID, reason)
	} else {
		span.SetAttributes(attribute.String("payment.result", "completed"))
		if err := payment.MarkCompleted(s.now()); err != nil {
			return err
		}
		reply = commonreplies.NewPaymentCompletedReply(command.PaymentID, command.OrderID)
	}

	if err := s.repo.Save(ctx, payment); err != nil {
		if payment.Status == domain.PaymentStatusCompleted {
			s.RefundBalance(amountRat)
		}
		return err
	}

	s.metrics.RecordPaymentStep(s.now().Sub(startedAt))
	err = s.publishReply(ctx, command.OrderID, reply)
	if err == nil {
		span.AddEvent("payment.reply.published", trace.WithAttributes(attribute.String("reply.type", reply.ReplyType())))
	}
	return err
}

func (s *Service) HandleRefundPayment(ctx context.Context, command commands.RefundPaymentCommand) (err error) {
	ctx, span := commontracing.Tracer("orchestration/payment-service").Start(ctx, "orchestration.participant.payment.refund_payment",
		trace.WithAttributes(
			attribute.String("saga.participant", "payment"),
			attribute.String("saga.command.type", commands.CommandRefundPayment),
			attribute.String("order.id", command.OrderID),
			attribute.String("payment.id", command.PaymentID),
			attribute.String("saga.pending.direction", "compensation"),
		))
	defer finishParticipantSpan(span, &err)
	startedAt := s.now()
	payment, ok, err := s.repo.GetByPaymentID(ctx, command.PaymentID)
	if err != nil {
		return err
	}
	if !ok {
		span.SetAttributes(attribute.String("payment.result", "not_found"))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewPaymentRefundedReply(command.PaymentID, command.OrderID, true, ""))
	}
	if payment.Status == domain.PaymentStatusRefunded {
		span.SetAttributes(attribute.String("payment.result", "already_refunded"))
		return s.publishReply(ctx, command.OrderID, commonreplies.NewPaymentRefundedReply(command.PaymentID, command.OrderID, true, ""))
	}
	amountRat, shouldRefundBalance := new(big.Rat).SetString(payment.Amount.String())
	shouldRefundBalance = shouldRefundBalance && payment.Status == domain.PaymentStatusCompleted
	if err := payment.MarkRefunded("Saga compensation", s.now()); err != nil {
		return err
	}
	if err := s.repo.Save(ctx, payment); err != nil {
		return err
	}
	if shouldRefundBalance {
		s.RefundBalance(amountRat)
	}
	s.metrics.RecordPaymentCompensation(s.now().Sub(startedAt))
	span.SetAttributes(attribute.String("payment.result", "refunded"))
	err = s.publishReply(ctx, command.OrderID, commonreplies.NewPaymentRefundedReply(command.PaymentID, command.OrderID, true, ""))
	if err == nil {
		span.AddEvent("payment.reply.published", trace.WithAttributes(attribute.String("reply.type", commonreplies.TypePaymentRefunded)))
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

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
