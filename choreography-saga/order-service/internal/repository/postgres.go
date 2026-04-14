package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"saga-pattern/choreography-saga/order-service/internal/domain"
	"saga-pattern/common/dto"
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

func (r *PostgresRepository) Create(ctx context.Context, order domain.Order) (domain.Order, error) {
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO orders (order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13, ''),$14,$15)`,
		order.OrderID, order.CustomerID, order.ShippingAddress, order.Status, marshalItems(order.Items), order.TotalAmount.String(),
		nullableString(order.PaymentID), nullableString(order.ReservationID), nullableString(order.ShippingID), nullableString(order.TrackingNumber), nullableString(order.FailureReason),
		order.CorrelationID, order.IdempotencyKey, order.CreatedAt.UTC(), order.UpdatedAt.UTC(),
	)
	if err != nil {
		return domain.Order{}, err
	}
	return order, nil
}

func (r *PostgresRepository) CreateIfAbsent(ctx context.Context, idempotencyKey string, order domain.Order) (domain.Order, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Order{}, false, err
	}
	defer tx.Rollback()

	if idempotencyKey != "" {
		existing, ok, err := scanOrderRow(tx.QueryRowContext(ctx, `
		SELECT order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at
		FROM orders
		WHERE idempotency_key = $1`, idempotencyKey))
		if err != nil {
			return domain.Order{}, false, err
		}
		if ok {
			if err := tx.Commit(); err != nil {
				return domain.Order{}, false, err
			}
			return existing, false, nil
		}
	}

	if _, err := tx.ExecContext(ctx, `
	INSERT INTO orders (order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13, ''),$14,$15)`,
		order.OrderID, order.CustomerID, order.ShippingAddress, order.Status, marshalItems(order.Items), order.TotalAmount.String(),
		nullableString(order.PaymentID), nullableString(order.ReservationID), nullableString(order.ShippingID), nullableString(order.TrackingNumber), nullableString(order.FailureReason),
		order.CorrelationID, idempotencyKey, order.CreatedAt.UTC(), order.UpdatedAt.UTC(),
	); err != nil {
		return domain.Order{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return domain.Order{}, false, err
	}
	return order, true, nil
}

func (r *PostgresRepository) Get(ctx context.Context, orderID string) (domain.Order, bool, error) {
	return scanOrderRow(r.db.QueryRowContext(ctx, `
	SELECT order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at
	FROM orders
	WHERE order_id = $1`, orderID))
}

func (r *PostgresRepository) Save(ctx context.Context, order domain.Order) error {
	result, err := r.db.ExecContext(ctx, `
	UPDATE orders
	SET customer_id = $2,
	    shipping_address = $3,
	    status = $4,
	    items_json = $5,
	    total_amount = $6,
	    payment_id = $7,
	    reservation_id = $8,
	    shipping_id = $9,
	    tracking_number = $10,
	    failure_reason = $11,
	    correlation_id = $12,
	    idempotency_key = NULLIF($13, ''),
	    updated_at = $14
	WHERE order_id = $1`,
		order.OrderID, order.CustomerID, order.ShippingAddress, order.Status, marshalItems(order.Items), order.TotalAmount.String(),
		nullableString(order.PaymentID), nullableString(order.ReservationID), nullableString(order.ShippingID), nullableString(order.TrackingNumber), nullableString(order.FailureReason),
		order.CorrelationID, order.IdempotencyKey, order.UpdatedAt.UTC(),
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("order %s not found", order.OrderID)
	}
	return nil
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
	CREATE TABLE IF NOT EXISTS orders (
	    order_id VARCHAR(255) PRIMARY KEY,
	    customer_id VARCHAR(255) NOT NULL,
	    shipping_address TEXT NOT NULL,
	    status VARCHAR(50) NOT NULL,
	    items_json TEXT NOT NULL,
	    total_amount DECIMAL(19,2) NOT NULL,
	    payment_id VARCHAR(255),
	    reservation_id VARCHAR(255),
	    shipping_id VARCHAR(255),
	    tracking_number VARCHAR(255),
	    failure_reason TEXT,
	    correlation_id VARCHAR(255) NOT NULL,
	    idempotency_key VARCHAR(255) UNIQUE,
	    created_at TIMESTAMP NOT NULL,
	    updated_at TIMESTAMP NOT NULL
	);
	CREATE TABLE IF NOT EXISTS processed_events (
	    event_key VARCHAR(255) PRIMARY KEY,
	    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);`)
	if err != nil {
		return fmt.Errorf("init choreography orders schema: %w", err)
	}
	return nil
}

type orderScanner interface{ Scan(dest ...any) error }

func scanOrderRow(scanner orderScanner) (domain.Order, bool, error) {
	var order domain.Order
	var itemsJSON string
	var totalAmount string
	var paymentID sql.NullString
	var reservationID sql.NullString
	var shippingID sql.NullString
	var trackingNumber sql.NullString
	var failureReason sql.NullString
	var idempotencyKey sql.NullString
	err := scanner.Scan(
		&order.OrderID,
		&order.CustomerID,
		&order.ShippingAddress,
		&order.Status,
		&itemsJSON,
		&totalAmount,
		&paymentID,
		&reservationID,
		&shippingID,
		&trackingNumber,
		&failureReason,
		&order.CorrelationID,
		&idempotencyKey,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return domain.Order{}, false, nil
	}
	if err != nil {
		return domain.Order{}, false, err
	}
	if err := json.Unmarshal([]byte(itemsJSON), &order.Items); err != nil {
		return domain.Order{}, false, err
	}
	order.TotalAmount = json.Number(totalAmount)
	order.PaymentID = paymentID.String
	order.ReservationID = reservationID.String
	order.ShippingID = shippingID.String
	order.TrackingNumber = trackingNumber.String
	order.FailureReason = failureReason.String
	order.IdempotencyKey = idempotencyKey.String
	order.CreatedAt = order.CreatedAt.UTC()
	order.UpdatedAt = order.UpdatedAt.UTC()
	return order, true, nil
}

func marshalItems(items []dto.OrderItemResponse) string {
	data, _ := json.Marshal(items)
	return string(data)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var _ Repository = (*PostgresRepository)(nil)
