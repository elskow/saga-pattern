package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	frameworkruntime "saga-pattern/orchestration-framework/runtime"
	"saga-pattern/orchestration-saga/order-service/internal/domain"
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

func (r *PostgresRepository) SaveAccepted(context.Context, domain.Order) error {
	return nil
}

func (r *PostgresRepository) GetVisible(ctx context.Context, orderID string) (domain.Order, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT order_id, customer_id, total_amount, shipping_address, items_json, status, payment_id, reservation_id, shipment_id,
	       failure_reason, created_at, updated_at
	FROM orders
	WHERE order_id = $1`, orderID)
	return scanOrder(row)
}

func (r *PostgresRepository) FinalizeFromSnapshot(ctx context.Context, snapshot frameworkruntime.Snapshot, now time.Time) (domain.Order, error) {
	itemsJSON, err := json.Marshal(snapshot.Items)
	if err != nil {
		return domain.Order{}, err
	}
	order := domain.FromSnapshot(snapshot, now)
	_, err = r.db.ExecContext(ctx, `
	INSERT INTO orders (order_id, customer_id, total_amount, shipping_address, items_json, status, payment_id, reservation_id, shipment_id, failure_reason, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	ON CONFLICT (order_id) DO UPDATE SET
	customer_id = EXCLUDED.customer_id,
	total_amount = EXCLUDED.total_amount,
	shipping_address = EXCLUDED.shipping_address,
	items_json = EXCLUDED.items_json,
	status = EXCLUDED.status,
	payment_id = EXCLUDED.payment_id,
	reservation_id = EXCLUDED.reservation_id,
	shipment_id = EXCLUDED.shipment_id,
	failure_reason = EXCLUDED.failure_reason,
	updated_at = EXCLUDED.updated_at`,
		order.OrderID, order.CustomerID, order.TotalAmount.String(), order.ShippingAddress, string(itemsJSON), order.Status,
		order.PaymentID, order.ReservationID, order.ShippingID, order.FailureReason, order.CreatedAt, order.UpdatedAt)
	if err != nil {
		return domain.Order{}, err
	}
	return order, nil
}

func (r *PostgresRepository) ListVisible(ctx context.Context) ([]domain.Order, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT order_id, customer_id, total_amount, shipping_address, items_json, status, payment_id, reservation_id, shipment_id,
	       failure_reason, created_at, updated_at
	FROM orders
	ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

func (r *PostgresRepository) ListVisibleByCustomer(ctx context.Context, customerID string) ([]domain.Order, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT order_id, customer_id, total_amount, shipping_address, items_json, status, payment_id, reservation_id, shipment_id,
	       failure_reason, created_at, updated_at
	FROM orders
	WHERE customer_id = $1
	ORDER BY created_at`, customerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

type orderScanner interface{ Scan(dest ...any) error }

func scanOrder(scanner orderScanner) (domain.Order, bool, error) {
	var order domain.Order
	var itemsJSON string
	var totalAmount string
	err := scanner.Scan(
		&order.OrderID,
		&order.CustomerID,
		&totalAmount,
		&order.ShippingAddress,
		&itemsJSON,
		&order.Status,
		&order.PaymentID,
		&order.ReservationID,
		&order.ShippingID,
		&order.FailureReason,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return domain.Order{}, false, nil
	}
	if err != nil {
		return domain.Order{}, false, err
	}
	order.TotalAmount = json.Number(totalAmount)
	order.Visible = true
	if itemsJSON != "" {
		if err := json.Unmarshal([]byte(itemsJSON), &order.Items); err != nil {
			return domain.Order{}, false, err
		}
	}
	return order, true, nil
}

func scanOrders(rows *sql.Rows) ([]domain.Order, error) {
	orders := make([]domain.Order, 0)
	for rows.Next() {
		order, ok, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		if ok {
			orders = append(orders, order)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return orders, nil
}

func (r *PostgresRepository) initSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS orders (
	    order_id VARCHAR(255) PRIMARY KEY,
	    customer_id VARCHAR(255) NOT NULL,
	    total_amount DECIMAL(19,2) NOT NULL,
	    shipping_address VARCHAR(255) NOT NULL,
	    items_json TEXT,
	    status VARCHAR(50) NOT NULL,
	    payment_id VARCHAR(255),
	    reservation_id VARCHAR(255),
	    shipment_id VARCHAR(255),
	    failure_reason TEXT,
	    created_at TIMESTAMP,
	    updated_at TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
	CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
	CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);`)
	if err != nil {
		return fmt.Errorf("init orders schema: %w", err)
	}
	return nil
}

var _ Repository = (*PostgresRepository)(nil)
