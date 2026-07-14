package payments

import (
	"context"
	"math/big"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"saga-pattern/common/commands"
	commonreplies "saga-pattern/common/replies"
	commontracing "saga-pattern/common/tracing"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
)

func (s *Service) HandleProcessPayment(ctx context.Context, command commands.ProcessPaymentCommand) (err error) {
	if delay := domain.SimulatedDelayMs.Load(); delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
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
	if delay := domain.SimulatedDelayMs.Load(); delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
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
