package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"saga-pattern/common/dto"
	"saga-pattern/orchestration-saga/inventory-service/internal/domain"
	"sort"
	"time"

	"github.com/lib/pq"
)

const compensationReleaseReason = "Order cancelled - saga compensation"

func CompensationReleaseReason() string {
	return compensationReleaseReason
}

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres db is required")
	}
	return &PostgresRepository{db: db}, nil
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

func (r *PostgresRepository) CommitReservation(ctx context.Context, reservationID string, at time.Time) (domain.Reservation, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Reservation{}, false, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
	SELECT reservation_id, order_id, items_json, status, failure_reason, release_reason, created_at, reserved_at, released_at
	FROM inventory_reservations
	WHERE reservation_id = $1
	FOR UPDATE`, reservationID)
	reservation, err := scanReservation(row)
	if err != nil {
		return domain.Reservation{}, false, err
	}
	if reservation.Status != domain.ReservationStatusReserved {
		return reservation, false, tx.Commit()
	}
	products, err := loadProductsForUpdate(ctx, tx, productIDs(reservation.Items))
	if err != nil {
		return domain.Reservation{}, false, err
	}
	for _, item := range reservation.Items {
		product, ok := products[item.ProductID]
		if !ok {
			return domain.Reservation{}, false, domain.ProductNotFoundError{ProductID: item.ProductID}
		}
		product.Quantity -= item.Quantity
		product.ReservedQuantity -= item.Quantity
		products[item.ProductID] = product
	}
	for _, productID := range sortedProductKeys(products) {
		product := products[productID]
		if _, err := tx.ExecContext(ctx, `
		UPDATE products
		SET quantity = $2, reserved_quantity = $3
		WHERE product_id = $1`, product.ProductID, product.Quantity, product.ReservedQuantity); err != nil {
			return domain.Reservation{}, false, err
		}
	}
	reservation.Commit()
	if _, err := tx.ExecContext(ctx, `
	UPDATE inventory_reservations
	SET status = $2
	WHERE reservation_id = $1`, reservation.ReservationID, reservation.Status); err != nil {
		return domain.Reservation{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Reservation{}, false, err
	}
	return reservation, true, nil
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
	SELECT product_id, name, description, price::text, image, category, visible, quantity, reserved_quantity, last_restocked_at
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

func (r *PostgresRepository) ListProducts(ctx context.Context) ([]domain.Product, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT product_id, name, description, price::text, image, category, visible, quantity, reserved_quantity, last_restocked_at
	FROM products
	ORDER BY product_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var products []domain.Product
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	return products, rows.Err()
}

func (r *PostgresRepository) UpdateTotalStock(ctx context.Context, productID string, total int, at time.Time) (domain.Product, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Product{}, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
	SELECT product_id, name, description, price::text, image, category, visible, quantity, reserved_quantity, last_restocked_at
	FROM products
	WHERE product_id = $1
	FOR UPDATE`, productID)
	product, err := scanProduct(row)
	if err != nil {
		return domain.Product{}, err
	}
	if err := product.SetTotalStock(total, at); err != nil {
		return domain.Product{}, err
	}
	if _, err := tx.ExecContext(ctx, `
	UPDATE products
	SET quantity = $2, reserved_quantity = $3, last_restocked_at = $4
	WHERE product_id = $1`, product.ProductID, product.Quantity, product.ReservedQuantity, nullableTime(product.LastRestockedAt)); err != nil {
		return domain.Product{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Product{}, err
	}
	return product, nil
}

func (r *PostgresRepository) UpdateVisibility(ctx context.Context, productID string, visible bool) (domain.Product, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Product{}, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
	SELECT product_id, name, description, price::text, image, category, visible, quantity, reserved_quantity, last_restocked_at
	FROM products
	WHERE product_id = $1
	FOR UPDATE`, productID)
	product, err := scanProduct(row)
	if err != nil {
		return domain.Product{}, err
	}
	product.Visible = visible
	if _, err := tx.ExecContext(ctx, `
	UPDATE products
	SET visible = $2
	WHERE product_id = $1`, product.ProductID, product.Visible); err != nil {
		return domain.Product{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Product{}, err
	}
	return product, nil
}

func (r *PostgresRepository) ListReservations(ctx context.Context) ([]domain.Reservation, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT reservation_id, order_id, items_json, status, failure_reason, release_reason, created_at, reserved_at, released_at
	FROM inventory_reservations
	ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reservations []domain.Reservation
	for rows.Next() {
		r, err := scanReservation(rows)
		if err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, rows.Err()
}

func (r *PostgresRepository) PingContext(ctx context.Context) error {
	return r.db.PingContext(ctx)
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
	var price string
	var lastRestocked sql.NullTime
	err := row.Scan(&product.ProductID, &product.ProductName, &product.Description, &price, &product.Image, &product.Category, &product.Visible, &product.Quantity, &product.ReservedQuantity, &lastRestocked)
	if err != nil {
		return domain.Product{}, err
	}
	product.Price = json.Number(price)
	if lastRestocked.Valid {
		product.LastRestockedAt = lastRestocked.Time.UTC()
	}
	return product, nil
}

func loadProductsForUpdate(ctx context.Context, tx *sql.Tx, ids []string) (map[string]domain.Product, error) {
	products := make(map[string]domain.Product, len(ids))
	if len(ids) == 0 {
		return products, nil
	}
	rows, err := tx.QueryContext(ctx, `
	SELECT product_id, name, description, price::text, image, category, visible, quantity, reserved_quantity, last_restocked_at
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

var _ Repository = (*PostgresRepository)(nil)
