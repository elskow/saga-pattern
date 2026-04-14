package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lib/pq"

	"saga-pattern/common/dto"
	"saga-pattern/orchestration-saga/inventory-service/internal/domain"
)

const compensationReleaseReason = "Order cancelled - saga compensation"

type Repository interface {
	GetReservation(context.Context, string) (domain.Reservation, bool, error)
	ReserveInventory(context.Context, string, string, []dto.OrderItemRequest, time.Time) (domain.Reservation, error)
	SaveFailedReservation(context.Context, string, string, []dto.OrderItemRequest, string, time.Time) (domain.Reservation, error)
	ReleaseReservation(context.Context, string, time.Time, string) (domain.Reservation, error)
	Product(context.Context, string) (domain.Product, bool, error)
	PingContext(context.Context) error
}

type MemoryRepository struct {
	mu           sync.RWMutex
	products     map[string]domain.Product
	reservations map[string]domain.Reservation
}

func NewMemoryRepository() *MemoryRepository {
	products := make(map[string]domain.Product)
	for _, product := range domain.DefaultProducts() {
		products[product.ProductID] = product
	}
	return &MemoryRepository{
		products:     products,
		reservations: make(map[string]domain.Reservation),
	}
}

func (r *MemoryRepository) GetReservation(_ context.Context, reservationID string) (domain.Reservation, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reservation, ok := r.reservations[reservationID]
	return reservation.Clone(), ok, nil
}

func (r *MemoryRepository) ReserveInventory(_ context.Context, reservationID, orderID string, items []dto.OrderItemRequest, at time.Time) (domain.Reservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.reservations[reservationID]; exists {
		return domain.Reservation{}, fmt.Errorf("reservation %s already exists", reservationID)
	}

	products := make(map[string]domain.Product, len(items))
	for _, item := range items {
		product, ok := r.products[item.ProductID]
		if !ok {
			return domain.Reservation{}, domain.ProductNotFoundError{ProductID: item.ProductID}
		}
		products[item.ProductID] = product
	}
	for _, item := range items {
		product := products[item.ProductID]
		if err := product.Reserve(item.Quantity, at); err != nil {
			return domain.Reservation{}, err
		}
		products[item.ProductID] = product
	}
	for productID, product := range products {
		r.products[productID] = product
	}

	reservation := domain.Reservation{
		ReservationID: reservationID,
		OrderID:       orderID,
		Items:         cloneItems(items),
		Status:        domain.ReservationStatusReserved,
		CreatedAt:     at.UTC(),
		ReservedAt:    at.UTC(),
	}
	r.reservations[reservationID] = reservation
	return reservation.Clone(), nil
}

func (r *MemoryRepository) SaveFailedReservation(_ context.Context, reservationID, orderID string, items []dto.OrderItemRequest, reason string, at time.Time) (domain.Reservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.reservations[reservationID]; exists {
		return domain.Reservation{}, fmt.Errorf("reservation %s already exists", reservationID)
	}
	reservation := domain.Reservation{
		ReservationID: reservationID,
		OrderID:       orderID,
		Items:         cloneItems(items),
		Status:        domain.ReservationStatusFailed,
		FailureReason: reason,
		CreatedAt:     at.UTC(),
	}
	r.reservations[reservationID] = reservation
	return reservation.Clone(), nil
}

func (r *MemoryRepository) ReleaseReservation(_ context.Context, reservationID string, at time.Time, reason string) (domain.Reservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reservation, ok := r.reservations[reservationID]
	if !ok {
		return domain.Reservation{}, fmt.Errorf("reservation %s not found", reservationID)
	}
	if reservation.Status == domain.ReservationStatusReserved {
		for _, item := range reservation.Items {
			product, ok := r.products[item.ProductID]
			if !ok {
				return domain.Reservation{}, domain.ProductNotFoundError{ProductID: item.ProductID}
			}
			if err := product.Release(item.Quantity, at); err != nil {
				return domain.Reservation{}, err
			}
			r.products[item.ProductID] = product
		}
	}
	reservation.Release(at, reason)
	r.reservations[reservationID] = reservation
	return reservation.Clone(), nil
}

func (r *MemoryRepository) Product(_ context.Context, productID string) (domain.Product, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	product, ok := r.products[productID]
	return product, ok, nil
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
	if err := repo.seedProducts(context.Background()); err != nil {
		return nil, err
	}
	return repo, nil
}

func (r *PostgresRepository) GetReservation(ctx context.Context, reservationID string) (domain.Reservation, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT reservation_id, order_id, items_json, status, failure_reason, release_reason, created_at, reserved_at, released_at
	FROM inventory_reservations
	WHERE reservation_id = $1`, reservationID)

	reservation, err := scanReservation(row)
	if err == sql.ErrNoRows {
		return domain.Reservation{}, false, nil
	}
	if err != nil {
		return domain.Reservation{}, false, err
	}
	return reservation, true, nil
}

func (r *PostgresRepository) ReserveInventory(ctx context.Context, reservationID, orderID string, items []dto.OrderItemRequest, at time.Time) (domain.Reservation, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Reservation{}, err
	}
	defer tx.Rollback()

	products, err := loadProductsForUpdate(ctx, tx, productIDs(items))
	if err != nil {
		return domain.Reservation{}, err
	}
	for _, item := range items {
		product, ok := products[item.ProductID]
		if !ok {
			return domain.Reservation{}, domain.ProductNotFoundError{ProductID: item.ProductID}
		}
		if err := product.Reserve(item.Quantity, at); err != nil {
			return domain.Reservation{}, err
		}
		products[item.ProductID] = product
	}
	for _, productID := range sortedProductKeys(products) {
		product := products[productID]
		if _, err := tx.ExecContext(ctx, `
		UPDATE products
		SET reserved_quantity = $2
		WHERE product_id = $1`, product.ProductID, product.ReservedQuantity); err != nil {
			return domain.Reservation{}, err
		}
	}

	itemsJSON, err := json.Marshal(cloneItems(items))
	if err != nil {
		return domain.Reservation{}, fmt.Errorf("marshal reservation items: %w", err)
	}
	reservation := domain.Reservation{
		ReservationID: reservationID,
		OrderID:       orderID,
		Items:         cloneItems(items),
		Status:        domain.ReservationStatusReserved,
		CreatedAt:     at.UTC(),
		ReservedAt:    at.UTC(),
	}
	if _, err := tx.ExecContext(ctx, `
	INSERT INTO inventory_reservations (reservation_id, order_id, items_json, status, created_at, reserved_at)
	VALUES ($1,$2,$3,$4,$5,$6)`, reservation.ReservationID, reservation.OrderID, string(itemsJSON), reservation.Status, reservation.CreatedAt, reservation.ReservedAt); err != nil {
		return domain.Reservation{}, err
	}

	if err := tx.Commit(); err != nil {
		return domain.Reservation{}, err
	}
	return reservation, nil
}

func (r *PostgresRepository) SaveFailedReservation(ctx context.Context, reservationID, orderID string, items []dto.OrderItemRequest, reason string, at time.Time) (domain.Reservation, error) {
	itemsJSON, err := json.Marshal(cloneItems(items))
	if err != nil {
		return domain.Reservation{}, fmt.Errorf("marshal failed reservation items: %w", err)
	}
	reservation := domain.Reservation{
		ReservationID: reservationID,
		OrderID:       orderID,
		Items:         cloneItems(items),
		Status:        domain.ReservationStatusFailed,
		FailureReason: reason,
		CreatedAt:     at.UTC(),
	}
	if _, err := r.db.ExecContext(ctx, `
	INSERT INTO inventory_reservations (reservation_id, order_id, items_json, status, failure_reason, created_at)
	VALUES ($1,$2,$3,$4,$5,$6)`, reservation.ReservationID, reservation.OrderID, string(itemsJSON), reservation.Status, reservation.FailureReason, reservation.CreatedAt); err != nil {
		return domain.Reservation{}, err
	}
	return reservation, nil
}

func (r *PostgresRepository) ReleaseReservation(ctx context.Context, reservationID string, at time.Time, reason string) (domain.Reservation, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Reservation{}, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
	SELECT reservation_id, order_id, items_json, status, failure_reason, release_reason, created_at, reserved_at, released_at
	FROM inventory_reservations
	WHERE reservation_id = $1
	FOR UPDATE`, reservationID)
	reservation, err := scanReservation(row)
	if err != nil {
		return domain.Reservation{}, err
	}

	if reservation.Status == domain.ReservationStatusReserved {
		products, err := loadProductsForUpdate(ctx, tx, productIDs(reservation.Items))
		if err != nil {
			return domain.Reservation{}, err
		}
		for _, item := range reservation.Items {
			product, ok := products[item.ProductID]
			if !ok {
				return domain.Reservation{}, domain.ProductNotFoundError{ProductID: item.ProductID}
			}
			if err := product.Release(item.Quantity, at); err != nil {
				return domain.Reservation{}, err
			}
			products[item.ProductID] = product
		}
		for _, productID := range sortedProductKeys(products) {
			product := products[productID]
			if _, err := tx.ExecContext(ctx, `
			UPDATE products
			SET reserved_quantity = $2
			WHERE product_id = $1`, product.ProductID, product.ReservedQuantity); err != nil {
				return domain.Reservation{}, err
			}
		}
	}

	reservation.Release(at, reason)
	if _, err := tx.ExecContext(ctx, `
	UPDATE inventory_reservations
	SET status = $2, release_reason = $3, released_at = $4
	WHERE reservation_id = $1`, reservation.ReservationID, reservation.Status, nullableString(reservation.ReleaseReason), nullableTime(reservation.ReleasedAt)); err != nil {
		return domain.Reservation{}, err
	}

	if err := tx.Commit(); err != nil {
		return domain.Reservation{}, err
	}
	return reservation, nil
}

func (r *PostgresRepository) Product(ctx context.Context, productID string) (domain.Product, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT product_id, name, quantity, reserved_quantity
	FROM products
	WHERE product_id = $1`, productID)
	product, err := scanProduct(row)
	if err == sql.ErrNoRows {
		return domain.Product{}, false, nil
	}
	if err != nil {
		return domain.Product{}, false, err
	}
	return product, true, nil
}

func (r *PostgresRepository) PingContext(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

func (r *PostgresRepository) initSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS products (
	    product_id VARCHAR(255) PRIMARY KEY,
	    name VARCHAR(255) NOT NULL,
	    quantity INT NOT NULL,
	    reserved_quantity INT NOT NULL DEFAULT 0
	);
	CREATE TABLE IF NOT EXISTS inventory_reservations (
	    reservation_id VARCHAR(255) PRIMARY KEY,
	    order_id VARCHAR(255) NOT NULL,
	    items_json TEXT NOT NULL,
	    status VARCHAR(50) NOT NULL,
	    failure_reason TEXT,
	    release_reason TEXT,
	    created_at TIMESTAMP NOT NULL,
	    reserved_at TIMESTAMP,
	    released_at TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_reservations_order_id ON inventory_reservations(order_id);
	CREATE INDEX IF NOT EXISTS idx_inventory_reservations_status ON inventory_reservations(status);`)
	if err != nil {
		return fmt.Errorf("init inventory schema: %w", err)
	}
	return nil
}

func (r *PostgresRepository) seedProducts(ctx context.Context) error {
	for _, product := range domain.DefaultProducts() {
		if _, err := r.db.ExecContext(ctx, `
		INSERT INTO products (product_id, name, quantity, reserved_quantity)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (product_id) DO NOTHING`, product.ProductID, product.ProductName, product.Quantity, product.ReservedQuantity); err != nil {
			return fmt.Errorf("seed product %s: %w", product.ProductID, err)
		}
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanReservation(row rowScanner) (domain.Reservation, error) {
	var reservation domain.Reservation
	var itemsJSON string
	var failureReason sql.NullString
	var releaseReason sql.NullString
	var reservedAt sql.NullTime
	var releasedAt sql.NullTime
	err := row.Scan(
		&reservation.ReservationID,
		&reservation.OrderID,
		&itemsJSON,
		&reservation.Status,
		&failureReason,
		&releaseReason,
		&reservation.CreatedAt,
		&reservedAt,
		&releasedAt,
	)
	if err != nil {
		return domain.Reservation{}, err
	}
	if itemsJSON != "" {
		if err := json.Unmarshal([]byte(itemsJSON), &reservation.Items); err != nil {
			return domain.Reservation{}, fmt.Errorf("decode reservation items: %w", err)
		}
	}
	reservation.FailureReason = failureReason.String
	reservation.ReleaseReason = releaseReason.String
	reservation.CreatedAt = reservation.CreatedAt.UTC()
	if reservedAt.Valid {
		reservation.ReservedAt = reservedAt.Time.UTC()
	}
	if releasedAt.Valid {
		reservation.ReleasedAt = releasedAt.Time.UTC()
	}
	reservation.Items = cloneItems(reservation.Items)
	return reservation, nil
}

func scanProduct(row rowScanner) (domain.Product, error) {
	var product domain.Product
	err := row.Scan(&product.ProductID, &product.ProductName, &product.Quantity, &product.ReservedQuantity)
	if err != nil {
		return domain.Product{}, err
	}
	return product, nil
}

func loadProductsForUpdate(ctx context.Context, tx *sql.Tx, ids []string) (map[string]domain.Product, error) {
	products := make(map[string]domain.Product, len(ids))
	if len(ids) == 0 {
		return products, nil
	}
	rows, err := tx.QueryContext(ctx, `
	SELECT product_id, name, quantity, reserved_quantity
	FROM products
	WHERE product_id = ANY($1)
	FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		product, err := scanProduct(rows)
		if err != nil {
			return nil, err
		}
		products[product.ProductID] = product
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return products, nil
}

func productIDs(items []dto.OrderItemRequest) []string {
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		set[item.ProductID] = struct{}{}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func sortedProductKeys(products map[string]domain.Product) []string {
	keys := make([]string, 0, len(products))
	for key := range products {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func cloneItems(items []dto.OrderItemRequest) []dto.OrderItemRequest {
	cloned := make([]dto.OrderItemRequest, len(items))
	copy(cloned, items)
	return cloned
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

func CompensationReleaseReason() string {
	return compensationReleaseReason
}

var _ Repository = (*MemoryRepository)(nil)
var _ Repository = (*PostgresRepository)(nil)
