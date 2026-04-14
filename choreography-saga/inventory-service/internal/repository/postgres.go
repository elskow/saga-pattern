package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
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
	if err := repo.seedProducts(context.Background()); err != nil {
		return nil, err
	}
	return repo, nil
}

func (r *PostgresRepository) StorePendingItems(ctx context.Context, orderID string, items []domain.PendingOrderItem) error {
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO pending_items (order_id, items_json)
	VALUES ($1,$2)
	ON CONFLICT (order_id) DO UPDATE SET items_json = EXCLUDED.items_json`, orderID, marshalPendingItems(items))
	return err
}

func (r *PostgresRepository) PendingItems(ctx context.Context, orderID string) ([]domain.PendingOrderItem, error) {
	row := r.db.QueryRowContext(ctx, `SELECT items_json FROM pending_items WHERE order_id = $1`, orderID)
	var itemsJSON string
	if err := row.Scan(&itemsJSON); err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var items []domain.PendingOrderItem
	if err := json.Unmarshal([]byte(itemsJSON), &items); err != nil {
		return nil, err
	}
	return items, nil
}

func (r *PostgresRepository) DeletePendingItems(ctx context.Context, orderID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM pending_items WHERE order_id = $1`, orderID)
	return err
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

func (r *PostgresRepository) ReserveInventory(ctx context.Context, orderID string, reservationID string, items []domain.PendingOrderItem, at time.Time) ([]domain.Reservation, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	products, err := r.loadProductsForUpdate(ctx, tx, productIDs(items))
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		product, ok := products[item.ProductID]
		if !ok {
			return nil, domain.ProductNotFoundError{ProductID: item.ProductID}
		}
		if err := product.Reserve(item.Quantity, at); err != nil {
			return nil, err
		}
		products[item.ProductID] = product
	}
	for _, productID := range sortedProductKeys(products) {
		product := products[productID]
		if _, err := tx.ExecContext(ctx, `
		UPDATE products
		SET quantity_available = $2, quantity_reserved = $3, last_reservation_at = $4
		WHERE product_id = $1`, product.ProductID, product.QuantityAvailable, product.QuantityReserved, nullableTime(product.LastReservationAt)); err != nil {
			return nil, err
		}
	}

	reservations := make([]domain.Reservation, 0, len(items))
	for _, item := range items {
		reservation := domain.Reservation{ReservationID: reservationID, OrderID: orderID, ProductID: item.ProductID, Quantity: item.Quantity, Status: domain.ReservationStatusReserved, CreatedAt: at.UTC()}
		if _, err := tx.ExecContext(ctx, `
		INSERT INTO inventory_reservations (order_id, reservation_id, product_id, quantity, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)`, orderID, reservationID, item.ProductID, item.Quantity, reservation.Status, reservation.CreatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, reservation)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return reservations, nil
}

func (r *PostgresRepository) ReleaseInventory(ctx context.Context, orderID string, at time.Time) (string, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
	SELECT reservation_id, product_id, quantity, status
	FROM inventory_reservations
	WHERE order_id = $1
	FOR UPDATE`, orderID)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()

	type reservationRow struct {
		ReservationID string
		ProductID     string
		Quantity      int
		Status        domain.ReservationStatus
	}
	var reservationID string
	var reservations []reservationRow
	for rows.Next() {
		var row reservationRow
		if err := rows.Scan(&row.ReservationID, &row.ProductID, &row.Quantity, &row.Status); err != nil {
			return "", false, err
		}
		if reservationID == "" {
			reservationID = row.ReservationID
		}
		reservations = append(reservations, row)
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	if len(reservations) == 0 {
		return "", false, nil
	}

	updated := false
	productIDs := make([]string, 0, len(reservations))
	seenProductIDs := make(map[string]struct{}, len(reservations))
	for _, reservation := range reservations {
		if _, ok := seenProductIDs[reservation.ProductID]; ok {
			continue
		}
		seenProductIDs[reservation.ProductID] = struct{}{}
		productIDs = append(productIDs, reservation.ProductID)
	}
	sort.Strings(productIDs)
	products, err := r.loadProductsForUpdate(ctx, tx, productIDs)
	if err != nil {
		return "", false, err
	}
	for _, reservation := range reservations {
		if reservation.Status != domain.ReservationStatusReserved {
			continue
		}
		product, ok := products[reservation.ProductID]
		if !ok {
			return "", false, domain.ProductNotFoundError{ProductID: reservation.ProductID}
		}
		if err := product.Release(reservation.Quantity, at); err != nil {
			return "", false, err
		}
		products[reservation.ProductID] = product
		updated = true
	}
	if !updated {
		return reservationID, false, tx.Commit()
	}

	for _, productID := range sortedProductKeys(products) {
		product := products[productID]
		if _, err := tx.ExecContext(ctx, `
		UPDATE products
		SET quantity_available = $2, quantity_reserved = $3, last_release_at = $4
		WHERE product_id = $1`, product.ProductID, product.QuantityAvailable, product.QuantityReserved, nullableTime(product.LastReleaseAt)); err != nil {
			return "", false, err
		}
	}
	if _, err := tx.ExecContext(ctx, `
	UPDATE inventory_reservations
	SET status = $2, released_at = $3
	WHERE order_id = $1 AND status = $4`, orderID, domain.ReservationStatusReleased, at.UTC(), domain.ReservationStatusReserved); err != nil {
		return "", false, err
	}

	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return reservationID, true, nil
}

func (r *PostgresRepository) Product(ctx context.Context, productID string) (domain.Product, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT product_id, product_name, quantity_available, quantity_reserved, last_reservation_at, last_release_at
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

func (r *PostgresRepository) initSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS products (
	    product_id VARCHAR(255) PRIMARY KEY,
	    product_name VARCHAR(255) NOT NULL,
	    quantity_available INT NOT NULL,
	    quantity_reserved INT NOT NULL DEFAULT 0,
	    last_reservation_at TIMESTAMP,
	    last_release_at TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS pending_items (
	    order_id VARCHAR(255) PRIMARY KEY,
	    items_json TEXT NOT NULL
	);
	CREATE TABLE IF NOT EXISTS inventory_reservations (
	    order_id VARCHAR(255) NOT NULL,
	    reservation_id VARCHAR(255) NOT NULL,
	    product_id VARCHAR(255) NOT NULL,
	    quantity INT NOT NULL,
	    status VARCHAR(50) NOT NULL,
	    created_at TIMESTAMP NOT NULL,
	    released_at TIMESTAMP,
	    PRIMARY KEY (order_id, product_id)
	);
	CREATE TABLE IF NOT EXISTS processed_events (
	    event_key VARCHAR(255) PRIMARY KEY,
	    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
	);`)
	if err != nil {
		return fmt.Errorf("init choreography inventory schema: %w", err)
	}
	return nil
}

func (r *PostgresRepository) seedProducts(ctx context.Context) error {
	for _, product := range domain.DefaultProducts() {
		if _, err := r.db.ExecContext(ctx, `
		INSERT INTO products (product_id, product_name, quantity_available, quantity_reserved)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (product_id) DO NOTHING`, product.ProductID, product.ProductName, product.QuantityAvailable, product.QuantityReserved); err != nil {
			return err
		}
	}
	return nil
}

func (r *PostgresRepository) loadProductsForUpdate(ctx context.Context, tx *sql.Tx, ids []string) (map[string]domain.Product, error) {
	products := make(map[string]domain.Product, len(ids))
	for _, id := range ids {
		row := tx.QueryRowContext(ctx, `
		SELECT product_id, product_name, quantity_available, quantity_reserved, last_reservation_at, last_release_at
		FROM products
		WHERE product_id = $1
		FOR UPDATE`, id)
		product, err := scanProduct(row)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, err
		}
		products[id] = product
	}
	return products, nil
}

func scanProduct(scanner interface{ Scan(dest ...any) error }) (domain.Product, error) {
	var product domain.Product
	var lastReservation sql.NullTime
	var lastRelease sql.NullTime
	err := scanner.Scan(&product.ProductID, &product.ProductName, &product.QuantityAvailable, &product.QuantityReserved, &lastReservation, &lastRelease)
	if err != nil {
		return domain.Product{}, err
	}
	if lastReservation.Valid {
		product.LastReservationAt = lastReservation.Time.UTC()
	}
	if lastRelease.Valid {
		product.LastReleaseAt = lastRelease.Time.UTC()
	}
	return product, nil
}

func marshalPendingItems(items []domain.PendingOrderItem) string {
	data, _ := json.Marshal(items)
	return string(data)
}

func productIDs(items []domain.PendingOrderItem) []string {
	ids := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, ok := seen[item.ProductID]; ok {
			continue
		}
		seen[item.ProductID] = struct{}{}
		ids = append(ids, item.ProductID)
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

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

var _ Repository = (*PostgresRepository)(nil)
