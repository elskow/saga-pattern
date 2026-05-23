package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"saga-pattern/choreography-saga/payment-service/internal/domain"
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
	INSERT INTO payments (payment_id, order_id, customer_id, amount, transaction_id, status, failure_reason, correlation_id, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		payment.PaymentID, payment.OrderID, payment.CustomerID, payment.Amount.String(), nullableString(payment.TransactionID), payment.Status,
		nullableString(payment.FailureReason), payment.CorrelationID, payment.CreatedAt.UTC(), payment.UpdatedAt.UTC(),
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
	    transaction_id = $5,
	    status = $6,
	    failure_reason = $7,
	    correlation_id = $8,
	    updated_at = $9
	WHERE payment_id = $1`,
		payment.PaymentID, payment.OrderID, payment.CustomerID, payment.Amount.String(), nullableString(payment.TransactionID), payment.Status,
		nullableString(payment.FailureReason), payment.CorrelationID, payment.UpdatedAt.UTC(),
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

func (r *PostgresRepository) GetByOrderID(ctx context.Context, orderID string) (domain.Payment, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT payment_id, order_id, customer_id, amount, transaction_id, status, failure_reason, correlation_id, created_at, updated_at
	FROM payments
	WHERE order_id = $1`, orderID)

	var payment domain.Payment
	var amount string
	var transactionID sql.NullString
	var failureReason sql.NullString
	err := row.Scan(
		&payment.PaymentID,
		&payment.OrderID,
		&payment.CustomerID,
		&amount,
		&transactionID,
		&payment.Status,
		&failureReason,
		&payment.CorrelationID,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return domain.Payment{}, false, nil
	}
	if err != nil {
		return domain.Payment{}, false, err
	}
	payment.Amount = json.Number(amount)
	payment.TransactionID = transactionID.String
	payment.FailureReason = failureReason.String
	payment.CreatedAt = payment.CreatedAt.UTC()
	payment.UpdatedAt = payment.UpdatedAt.UTC()
	return payment, true, nil
}

func (r *PostgresRepository) TryMarkProcessedEvent(ctx context.Context, key string) (bool, error) {
	result, err := r.db.ExecContext(ctx, `
	INSERT INTO processed_events (event_key)
	VALUES ($1)
	ON CONFLICT (event_key) DO NOTHING`, key)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

func (r *PostgresRepository) DeleteProcessedEvent(ctx context.Context, key string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM processed_events WHERE event_key = $1`, key)
	return err
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (r *PostgresRepository) ListPayments(ctx context.Context) ([]domain.Payment, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT payment_id, order_id, customer_id, amount, transaction_id, status, failure_reason, correlation_id, created_at, updated_at
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
		var transactionID sql.NullString
		var failureReason sql.NullString
		if err := rows.Scan(&p.PaymentID, &p.OrderID, &p.CustomerID, &amount, &transactionID, &p.Status, &failureReason, &p.CorrelationID, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		p.Amount = json.Number(amount)
		p.TransactionID = transactionID.String
		p.FailureReason = failureReason.String
		p.CreatedAt = p.CreatedAt.UTC()
		p.UpdatedAt = p.UpdatedAt.UTC()
		payments = append(payments, p)
	}
	return payments, rows.Err()
}

var _ Repository = (*PostgresRepository)(nil)
