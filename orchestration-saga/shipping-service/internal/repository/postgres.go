package repository

import (
	"context"
	"database/sql"
	"fmt"
	"saga-pattern/orchestration-saga/shipping-service/internal/domain"
	"time"
)

const CompensationCancellationReason = "Saga compensation"

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres db is required")
	}
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) Create(ctx context.Context, shipment domain.Shipment) (domain.Shipment, error) {
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO shipments (shipping_id, order_id, tracking_number, shipping_address, status, failure_reason, cancellation_reason, created_at, scheduled_at, cancelled_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		shipment.ShippingID,
		shipment.OrderID,
		shipment.TrackingNumber,
		shipment.ShippingAddress,
		shipment.Status,
		nullableString(shipment.FailureReason),
		nullableString(shipment.CancellationReason),
		shipment.CreatedAt.UTC(),
		nullableTime(shipment.ScheduledAt),
		nullableTime(shipment.CancelledAt),
		shipment.UpdatedAt.UTC(),
	)
	if err != nil {
		return domain.Shipment{}, err
	}
	return shipment, nil
}

func (r *PostgresRepository) Save(ctx context.Context, shipment domain.Shipment) error {
	result, err := r.db.ExecContext(ctx, `
	UPDATE shipments
	SET order_id = $2,
	    tracking_number = $3,
	    shipping_address = $4,
	    status = $5,
	    failure_reason = $6,
	    cancellation_reason = $7,
	    created_at = $8,
	    scheduled_at = $9,
	    cancelled_at = $10,
	    updated_at = $11
	WHERE shipping_id = $1`,
		shipment.ShippingID,
		shipment.OrderID,
		shipment.TrackingNumber,
		shipment.ShippingAddress,
		shipment.Status,
		nullableString(shipment.FailureReason),
		nullableString(shipment.CancellationReason),
		shipment.CreatedAt.UTC(),
		nullableTime(shipment.ScheduledAt),
		nullableTime(shipment.CancelledAt),
		shipment.UpdatedAt.UTC(),
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("shipment %s not found", shipment.ShippingID)
	}
	return nil
}

func (r *PostgresRepository) GetByShippingID(ctx context.Context, shippingID string) (domain.Shipment, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT shipping_id, order_id, tracking_number, shipping_address, status, failure_reason, cancellation_reason, created_at, scheduled_at, cancelled_at, updated_at
	FROM shipments
	WHERE shipping_id = $1`, shippingID)

	var shipment domain.Shipment
	var failureReason sql.NullString
	var cancellationReason sql.NullString
	var scheduledAt sql.NullTime
	var cancelledAt sql.NullTime
	err := row.Scan(
		&shipment.ShippingID,
		&shipment.OrderID,
		&shipment.TrackingNumber,
		&shipment.ShippingAddress,
		&shipment.Status,
		&failureReason,
		&cancellationReason,
		&shipment.CreatedAt,
		&scheduledAt,
		&cancelledAt,
		&shipment.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return domain.Shipment{}, false, nil
	}
	if err != nil {
		return domain.Shipment{}, false, err
	}
	shipment.FailureReason = failureReason.String
	shipment.CancellationReason = cancellationReason.String
	shipment.CreatedAt = shipment.CreatedAt.UTC()
	shipment.UpdatedAt = shipment.UpdatedAt.UTC()
	if scheduledAt.Valid {
		shipment.ScheduledAt = scheduledAt.Time.UTC()
	}
	if cancelledAt.Valid {
		shipment.CancelledAt = cancelledAt.Time.UTC()
	}
	return shipment, true, nil
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

func (r *PostgresRepository) ListShipments(ctx context.Context) ([]domain.Shipment, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT shipping_id, order_id, tracking_number, shipping_address, status, failure_reason, cancellation_reason, created_at, scheduled_at, cancelled_at, updated_at
	FROM shipments
	ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var shipments []domain.Shipment
	for rows.Next() {
		var s domain.Shipment
		var failureReason sql.NullString
		var cancellationReason sql.NullString
		var scheduledAt sql.NullTime
		var cancelledAt sql.NullTime
		if err := rows.Scan(&s.ShippingID, &s.OrderID, &s.TrackingNumber, &s.ShippingAddress, &s.Status, &failureReason, &cancellationReason, &s.CreatedAt, &scheduledAt, &cancelledAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		s.FailureReason = failureReason.String
		s.CancellationReason = cancellationReason.String
		s.CreatedAt = s.CreatedAt.UTC()
		s.UpdatedAt = s.UpdatedAt.UTC()
		if scheduledAt.Valid {
			s.ScheduledAt = scheduledAt.Time.UTC()
		}
		if cancelledAt.Valid {
			s.CancelledAt = cancelledAt.Time.UTC()
		}
		shipments = append(shipments, s)
	}
	return shipments, rows.Err()
}

func (r *PostgresRepository) UpdateStatus(ctx context.Context, shippingID string, status domain.ShipmentStatus) error {
	_, err := r.db.ExecContext(ctx, `UPDATE shipments SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE shipping_id = $2`, status, shippingID)
	return err
}

var _ Repository = (*PostgresRepository)(nil)
