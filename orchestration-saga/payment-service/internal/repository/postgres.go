package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"saga-pattern/orchestration-saga/payment-service/internal/domain"
	"time"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres db is required")
	}
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) Create(ctx context.Context, payment domain.Payment) (domain.Payment, error) {
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO payments (payment_id, order_id, customer_id, amount, status, failure_reason, refund_reason, created_at, processed_at, refunded_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		payment.PaymentID, payment.OrderID, payment.CustomerID, payment.Amount.String(), payment.Status,
		nullableString(payment.FailureReason), nullableString(payment.RefundReason), payment.CreatedAt,
		nullableTime(payment.ProcessedAt), nullableTime(payment.RefundedAt),
	)
	if err != nil {
		return domain.Payment{}, err
	}
	return payment, nil
}

func (r *PostgresRepository) Save(ctx context.Context, payment domain.Payment) error {
	result, err := r.db.ExecContext(ctx, `
	UPDATE payments
	SET order_id = $2,
	    customer_id = $3,
	    amount = $4,
	    status = $5,
	    failure_reason = $6,
	    refund_reason = $7,
	    created_at = $8,
	    processed_at = $9,
	    refunded_at = $10
	WHERE payment_id = $1`,
		payment.PaymentID, payment.OrderID, payment.CustomerID, payment.Amount.String(), payment.Status,
		nullableString(payment.FailureReason), nullableString(payment.RefundReason), payment.CreatedAt,
		nullableTime(payment.ProcessedAt), nullableTime(payment.RefundedAt),
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("payment %s not found", payment.PaymentID)
	}
	return nil
}

func (r *PostgresRepository) GetByPaymentID(ctx context.Context, paymentID string) (domain.Payment, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT payment_id, order_id, customer_id, amount, status, failure_reason, refund_reason, created_at, processed_at, refunded_at
	FROM payments
	WHERE payment_id = $1`, paymentID)

	var payment domain.Payment
	var amount string
	var failureReason sql.NullString
	var refundReason sql.NullString
	var processedAt sql.NullTime
	var refundedAt sql.NullTime
	err := row.Scan(
		&payment.PaymentID,
		&payment.OrderID,
		&payment.CustomerID,
		&amount,
		&payment.Status,
		&failureReason,
		&refundReason,
		&payment.CreatedAt,
		&processedAt,
		&refundedAt,
	)
	if err == sql.ErrNoRows {
		return domain.Payment{}, false, nil
	}
	if err != nil {
		return domain.Payment{}, false, err
	}
	payment.Amount = json.Number(amount)
	payment.FailureReason = failureReason.String
	payment.RefundReason = refundReason.String
	if processedAt.Valid {
		payment.ProcessedAt = processedAt.Time.UTC()
	}
	if refundedAt.Valid {
		payment.RefundedAt = refundedAt.Time.UTC()
	}
	payment.CreatedAt = payment.CreatedAt.UTC()
	return payment, true, nil
}

func (r *PostgresRepository) PingContext(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func (r *PostgresRepository) ListPayments(ctx context.Context) ([]domain.Payment, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT payment_id, order_id, customer_id, amount, status, failure_reason, refund_reason, created_at, processed_at, refunded_at
	FROM payments
	ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var payments []domain.Payment
	for rows.Next() {
		var p domain.Payment
		var amount string
		var failureReason sql.NullString
		var refundReason sql.NullString
		var processedAt sql.NullTime
		var refundedAt sql.NullTime
		if err := rows.Scan(&p.PaymentID, &p.OrderID, &p.CustomerID, &amount, &p.Status, &failureReason, &refundReason, &p.CreatedAt, &processedAt, &refundedAt); err != nil {
			return nil, err
		}
		p.Amount = json.Number(amount)
		p.FailureReason = failureReason.String
		p.RefundReason = refundReason.String
		p.CreatedAt = p.CreatedAt.UTC()
		if processedAt.Valid {
			p.ProcessedAt = processedAt.Time.UTC()
		}
		if refundedAt.Valid {
			p.RefundedAt = refundedAt.Time.UTC()
		}
		payments = append(payments, p)
	}
	return payments, rows.Err()
}

var _ Repository = (*PostgresRepository)(nil)
