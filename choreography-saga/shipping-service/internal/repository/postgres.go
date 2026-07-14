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
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) SavePendingShippingAddress(ctx context.Context, orderID string, shippingAddress string) error {
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO pending_addresses (order_id, shipping_address)
	VALUES ($1,$2)
	ON CONFLICT (order_id) DO UPDATE SET shipping_address = EXCLUDED.shipping_address`, orderID, shippingAddress)
	return err
}

func (r *PostgresRepository) LoadPendingShippingAddress(ctx context.Context, orderID string) (string, bool, error) {
	row := r.db.QueryRowContext(ctx, `SELECT shipping_address FROM pending_addresses WHERE order_id = $1`, orderID)
	var address string
	if err := row.Scan(&address); err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	return address, true, nil
}

func (r *PostgresRepository) ClearPendingShippingAddress(ctx context.Context, orderID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM pending_addresses WHERE order_id = $1`, orderID)
	return err
}

func (r *PostgresRepository) SaveShipment(ctx context.Context, shipment domain.Shipment, hook TxHook) error {
	if hook == nil {
		return r.saveShipment(ctx, r.db, shipment)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := r.saveShipment(ctx, tx, shipment); err != nil {
		return err
	}
	if err := hook(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *PostgresRepository) saveShipment(ctx context.Context, db interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}, shipment domain.Shipment) error {
	_, err := db.ExecContext(ctx, `
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

func (r *PostgresRepository) DeleteProcessedEvent(ctx context.Context, key string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM processed_events WHERE event_key = $1`, key)
	return err
}

func (r *PostgresRepository) ListShipments(ctx context.Context) ([]domain.Shipment, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT shipping_id, order_id, tracking_number, shipping_address, status, estimated_delivery, created_at, updated_at
	FROM shipments
	ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var shipments []domain.Shipment
	for rows.Next() {
		var s domain.Shipment
		if err := rows.Scan(&s.ShippingID, &s.OrderID, &s.TrackingNumber, &s.ShippingAddress, &s.Status, &s.EstimatedDelivery, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		s.EstimatedDelivery = s.EstimatedDelivery.UTC()
		s.CreatedAt = s.CreatedAt.UTC()
		s.UpdatedAt = s.UpdatedAt.UTC()
		shipments = append(shipments, s)
	}
	return shipments, rows.Err()
}

func (r *PostgresRepository) UpdateStatus(ctx context.Context, shippingID string, status domain.ShipmentStatus) error {
	_, err := r.db.ExecContext(ctx, `UPDATE shipments SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE shipping_id = $2`, status, shippingID)
	return err
}

var _ Repository = (*PostgresRepository)(nil)
