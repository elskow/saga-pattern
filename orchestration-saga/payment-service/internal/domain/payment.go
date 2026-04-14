package domain

import (
	"encoding/json"
	"fmt"
	"time"

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
	Status        PaymentStatus
	FailureReason string
	RefundReason  string
	CreatedAt     time.Time
	ProcessedAt   time.Time
	RefundedAt    time.Time
}

func NewPayment(paymentID, orderID, customerID string, amount json.Number, now time.Time) (Payment, error) {
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
		PaymentID:  paymentID,
		OrderID:    orderID,
		CustomerID: customerID,
		Amount:     amount,
		Status:     PaymentStatusPending,
		CreatedAt:  now,
	}, nil
}

func (p *Payment) MarkCompleted(now time.Time) error {
	if p.Status != PaymentStatusPending {
		return fmt.Errorf("payment %s cannot complete from status %s", p.PaymentID, p.Status)
	}
	p.Status = PaymentStatusCompleted
	p.ProcessedAt = now.UTC()
	p.FailureReason = ""
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
	p.ProcessedAt = now.UTC()
	return nil
}

func (p *Payment) MarkRefunded(reason string, now time.Time) error {
	if err := validate.NonBlank(reason, "reason"); err != nil {
		return err
	}
	if p.Status == PaymentStatusRefunded {
		return fmt.Errorf("payment %s already refunded", p.PaymentID)
	}
	p.Status = PaymentStatusRefunded
	p.RefundReason = reason
	p.RefundedAt = now.UTC()
	return nil
}
