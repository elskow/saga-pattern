package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"saga-pattern/orchestration-saga/shipping-service/internal/domain"
)

const CompensationCancellationReason = "Saga compensation"

type Repository interface {
	Create(context.Context, domain.Shipment) (domain.Shipment, error)
	Save(context.Context, domain.Shipment) error
	GetByShippingID(context.Context, string) (domain.Shipment, bool, error)
	PingContext(context.Context) error
}

type MemoryRepository struct {
	mu        sync.RWMutex
	shipments map[string]domain.Shipment
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{shipments: make(map[string]domain.Shipment)}
}

func (r *MemoryRepository) Create(_ context.Context, shipment domain.Shipment) (domain.Shipment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.shipments[shipment.ShippingID]; exists {
		return domain.Shipment{}, fmt.Errorf("shipment %s already exists", shipment.ShippingID)
	}
	r.shipments[shipment.ShippingID] = shipment
	return shipment, nil
}

func (r *MemoryRepository) Save(_ context.Context, shipment domain.Shipment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.shipments[shipment.ShippingID]; !exists {
		return fmt.Errorf("shipment %s not found", shipment.ShippingID)
	}
	r.shipments[shipment.ShippingID] = shipment
	return nil
}

func (r *MemoryRepository) GetByShippingID(_ context.Context, shippingID string) (domain.Shipment, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	shipment, ok := r.shipments[shippingID]
	return shipment, ok, nil
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

func (r *PostgresRepository) Create(ctx context.Context, shipment domain.Shipment) (domain.Shipment, error) {
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO shipments (shipping_id, order_id, shipping_address, status, failure_reason, cancellation_reason, created_at, scheduled_at, cancelled_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		shipment.ShippingID,
		shipment.OrderID,
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
	    shipping_address = $3,
	    status = $4,
	    failure_reason = $5,
	    cancellation_reason = $6,
	    created_at = $7,
	    scheduled_at = $8,
	    cancelled_at = $9,
	    updated_at = $10
	WHERE shipping_id = $1`,
		shipment.ShippingID,
		shipment.OrderID,
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
	SELECT shipping_id, order_id, shipping_address, status, failure_reason, cancellation_reason, created_at, scheduled_at, cancelled_at, updated_at
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

func (r *PostgresRepository) initSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS shipments (
	    shipping_id VARCHAR(255) PRIMARY KEY,
	    order_id VARCHAR(255) NOT NULL,
	    shipping_address TEXT NOT NULL,
	    status VARCHAR(50) NOT NULL,
	    failure_reason TEXT,
	    cancellation_reason TEXT,
	    created_at TIMESTAMP NOT NULL,
	    scheduled_at TIMESTAMP,
	    cancelled_at TIMESTAMP,
	    updated_at TIMESTAMP NOT NULL
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_shipments_order_id ON shipments(order_id);
	CREATE INDEX IF NOT EXISTS idx_shipments_status ON shipments(status);`)
	if err != nil {
		return fmt.Errorf("init shipments schema: %w", err)
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
