package domain

import (
	"encoding/json"
	"fmt"
	"time"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/validate"
)

type PaymentStatus string

const (
	PaymentStatusPending   PaymentStatus = "PENDING"
	PaymentStatusCompleted PaymentStatus = "COMPLETED"
	PaymentStatusFailed    PaymentStatus = "FAILED"
	PaymentStatusRefunded  PaymentStatus = "REFUNDED"
)

type Payment struct {
	PaymentID     string
	OrderID       string
	CustomerID    string
	Amount        json.Number
	TransactionID string
	Status        PaymentStatus
	FailureReason string
	CorrelationID string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func NewPayment(paymentID, orderID, customerID string, amount json.Number, correlationID string, now time.Time) (Payment, error) {
	now = now.UTC()
	if err := validate.NonBlank(paymentID, "paymentId"); err != nil {
		return Payment{}, err
	}
	if err := validate.NonBlank(orderID, "orderId"); err != nil {
		return Payment{}, err
	}
	if err := validate.NonBlank(customerID, "customerId"); err != nil {
		return Payment{}, err
	}
	if err := validate.PositiveNumber(amount, "amount"); err != nil {
		return Payment{}, err
	}
	if now.IsZero() {
		return Payment{}, fmt.Errorf("created time cannot be zero")
	}

	return Payment{
		PaymentID:     paymentID,
		OrderID:       orderID,
		CustomerID:    customerID,
		Amount:        amount,
		Status:        PaymentStatusPending,
		CorrelationID: commoncontext.ResolveCorrelationID(correlationID),
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

func (p *Payment) MarkCompleted(transactionID string, now time.Time) error {
	if err := validate.NonBlank(transactionID, "transactionId"); err != nil {
		return err
	}
	if p.Status != PaymentStatusPending {
		return fmt.Errorf("payment %s cannot complete from status %s", p.PaymentID, p.Status)
	}
	p.TransactionID = transactionID
	p.Status = PaymentStatusCompleted
	p.FailureReason = ""
	p.UpdatedAt = now.UTC()
	return nil
}

func (p *Payment) MarkFailed(reason string, now time.Time) error {
	if err := validate.NonBlank(reason, "reason"); err != nil {
		return err
	}
	if p.Status != PaymentStatusPending {
		return fmt.Errorf("payment %s cannot fail from status %s", p.PaymentID, p.Status)
	}
	p.Status = PaymentStatusFailed
	p.FailureReason = reason
	p.UpdatedAt = now.UTC()
	return nil
}

func (p *Payment) MarkRefunded(now time.Time) error {
	if p.Status != PaymentStatusCompleted {
		return fmt.Errorf("payment %s cannot refund from status %s", p.PaymentID, p.Status)
	}
	p.Status = PaymentStatusRefunded
	p.UpdatedAt = now.UTC()
	return nil
}
