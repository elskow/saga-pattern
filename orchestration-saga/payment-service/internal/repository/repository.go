package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"saga-pattern/orchestration-saga/payment-service/internal/domain"
)

type Repository interface {
	Create(context.Context, domain.Payment) (domain.Payment, error)
	Save(context.Context, domain.Payment) error
	GetByPaymentID(context.Context, string) (domain.Payment, bool, error)
	PingContext(context.Context) error
}

type MemoryRepository struct {
	mu       sync.RWMutex
	payments map[string]domain.Payment
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{payments: make(map[string]domain.Payment)}
}

func (r *MemoryRepository) Create(_ context.Context, payment domain.Payment) (domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.payments[payment.PaymentID]; exists {
		return domain.Payment{}, fmt.Errorf("payment %s already exists", payment.PaymentID)
	}
	r.payments[payment.PaymentID] = payment
	return payment, nil
}

func (r *MemoryRepository) Save(_ context.Context, payment domain.Payment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.payments[payment.PaymentID]; !exists {
		return fmt.Errorf("payment %s not found", payment.PaymentID)
	}
	r.payments[payment.PaymentID] = payment
	return nil
}

func (r *MemoryRepository) GetByPaymentID(_ context.Context, paymentID string) (domain.Payment, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	payment, ok := r.payments[paymentID]
	return payment, ok, nil
}

func (r *MemoryRepository) PingContext(context.Context) error { return nil }

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

func (r *PostgresRepository) initSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS payments (
	    payment_id VARCHAR(255) PRIMARY KEY,
	    order_id VARCHAR(255) NOT NULL,
	    customer_id VARCHAR(255) NOT NULL,
	    amount DECIMAL(19,2) NOT NULL,
	    status VARCHAR(50) NOT NULL,
	    failure_reason TEXT,
	    refund_reason TEXT,
	    created_at TIMESTAMP NOT NULL,
	    processed_at TIMESTAMP,
	    refunded_at TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);
	CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);`)
	if err != nil {
		return fmt.Errorf("init payments schema: %w", err)
	}
	return nil
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

var _ Repository = (*MemoryRepository)(nil)
var _ Repository = (*PostgresRepository)(nil)
