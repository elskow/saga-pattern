package payments

import (
	"context"

	"saga-pattern/common/commands"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
)

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
