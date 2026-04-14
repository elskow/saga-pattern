package repository

import (
	"context"
	"database/sql"
	"fmt"

	"saga-pattern/choreography-saga/shipping-service/internal/domain"
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

func (r *PostgresRepository) StorePendingAddress(ctx context.Context, orderID string, shippingAddress string) error {
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO pending_addresses (order_id, shipping_address)
	VALUES ($1,$2)
	ON CONFLICT (order_id) DO UPDATE SET shipping_address = EXCLUDED.shipping_address`, orderID, shippingAddress)
	return err
}

func (r *PostgresRepository) PendingAddress(ctx context.Context, orderID string) (string, bool, error) {
	row := r.db.QueryRowContext(ctx, `SELECT shipping_address FROM pending_addresses WHERE order_id = $1`, orderID)
	var address string
	if err := row.Scan(&address); err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	return address, true, nil
}

func (r *PostgresRepository) DeletePendingAddress(ctx context.Context, orderID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM pending_addresses WHERE order_id = $1`, orderID)
	return err
}

func (r *PostgresRepository) SaveShipment(ctx context.Context, shipment domain.Shipment) error {
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO shipments (shipping_id, order_id, tracking_number, shipping_address, status, estimated_delivery, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	ON CONFLICT (shipping_id) DO UPDATE SET
	order_id = EXCLUDED.order_id,
	tracking_number = EXCLUDED.tracking_number,
	shipping_address = EXCLUDED.shipping_address,
	status = EXCLUDED.status,
	estimated_delivery = EXCLUDED.estimated_delivery,
	updated_at = EXCLUDED.updated_at`,
		shipment.ShippingID, shipment.OrderID, shipment.TrackingNumber, shipment.ShippingAddress, shipment.Status, shipment.EstimatedDelivery.UTC(), shipment.CreatedAt.UTC(), shipment.UpdatedAt.UTC(),
	)
	return err
}

func (r *PostgresRepository) GetShipmentByOrderID(ctx context.Context, orderID string) (domain.Shipment, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT shipping_id, order_id, tracking_number, shipping_address, status, estimated_delivery, created_at, updated_at
	FROM shipments
	WHERE order_id = $1`, orderID)
	var shipment domain.Shipment
	err := row.Scan(&shipment.ShippingID, &shipment.OrderID, &shipment.TrackingNumber, &shipment.ShippingAddress, &shipment.Status, &shipment.EstimatedDelivery, &shipment.CreatedAt, &shipment.UpdatedAt)
	if err == sql.ErrNoRows {
		return domain.Shipment{}, false, nil
	}
	if err != nil {
		return domain.Shipment{}, false, err
	}
	shipment.EstimatedDelivery = shipment.EstimatedDelivery.UTC()
	shipment.CreatedAt = shipment.CreatedAt.UTC()
	shipment.UpdatedAt = shipment.UpdatedAt.UTC()
	return shipment, true, nil
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
	CREATE TABLE IF NOT EXISTS pending_addresses (
	    order_id VARCHAR(255) PRIMARY KEY,
	    shipping_address TEXT NOT NULL
	);
	CREATE TABLE IF NOT EXISTS shipments (
	    shipping_id VARCHAR(255) PRIMARY KEY,
	    order_id VARCHAR(255) NOT NULL UNIQUE,
	    tracking_number VARCHAR(255) NOT NULL,
	    shipping_address TEXT NOT NULL,
	    status VARCHAR(50) NOT NULL,
	    estimated_delivery TIMESTAMP NOT NULL,
	    created_at TIMESTAMP NOT NULL,
	    updated_at TIMESTAMP NOT NULL
	);
	CREATE TABLE IF NOT EXISTS processed_events (
	    event_key VARCHAR(255) PRIMARY KEY,
	    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
	);`)
	if err != nil {
		return fmt.Errorf("init choreography shipping schema: %w", err)
	}
	return nil
}

var _ Repository = (*PostgresRepository)(nil)
