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
	repo := &PostgresRepository{db: db}
	if err := repo.initSchema(context.Background()); err != nil {
		return nil, err
	}
	return repo, nil
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

func (r *PostgresRepository) initSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS payments (
	    payment_id VARCHAR(255) PRIMARY KEY,
	    order_id VARCHAR(255) NOT NULL UNIQUE,
	    customer_id VARCHAR(255) NOT NULL,
	    amount DECIMAL(19,2) NOT NULL,
	    transaction_id VARCHAR(255),
	    status VARCHAR(50) NOT NULL,
	    failure_reason TEXT,
	    correlation_id VARCHAR(255) NOT NULL,
	    created_at TIMESTAMP NOT NULL,
	    updated_at TIMESTAMP NOT NULL
	);
	CREATE TABLE IF NOT EXISTS processed_events (
	    event_key VARCHAR(255) PRIMARY KEY,
	    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
	);`)
	if err != nil {
		return fmt.Errorf("init choreography payments schema: %w", err)
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var _ Repository = (*PostgresRepository)(nil)
