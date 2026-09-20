package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/lib/pq"

	"saga-pattern/choreography-saga/inventory-service/internal/domain"
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

func (r *PostgresRepository) SavePendingReservationItems(ctx context.Context, orderID string, items []domain.PendingOrderItem) error {
	itemsJSON, err := marshalPendingItems(items)
	if err != nil {
		return fmt.Errorf("marshal pending items: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
	INSERT INTO pending_items (order_id, items_json)
	VALUES ($1,$2)
	ON CONFLICT (order_id) DO UPDATE SET items_json = EXCLUDED.items_json`, orderID, itemsJSON)
	return err
}

func (r *PostgresRepository) LoadPendingReservationItems(ctx context.Context, orderID string) ([]domain.PendingOrderItem, error) {
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

func (r *PostgresRepository) ClearPendingReservationItems(ctx context.Context, orderID string) error {
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

func (r *PostgresRepository) DeleteProcessedEvent(ctx context.Context, key string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM processed_events WHERE event_key = $1`, key)
	return err
}

func (r *PostgresRepository) ReserveInventory(ctx context.Context, orderID string, reservationID string, items []domain.PendingOrderItem, at time.Time, onReserve TxHook, onFail TxHook) ([]domain.Reservation, error) {
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
			if onFail != nil {
				if hookErr := onFail(ctx, tx, "", domain.ProductNotFoundError{ProductID: item.ProductID}); hookErr != nil {
					return nil, hookErr
				}
			}
			return nil, domain.ProductNotFoundError{ProductID: item.ProductID}
		}
		if err := product.Reserve(item.Quantity, at); err != nil {
			if onFail != nil {
				if hookErr := onFail(ctx, tx, "", err); hookErr != nil {
					return nil, hookErr
				}
			}
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

	if onReserve != nil {
		if err := onReserve(ctx, tx, reservationID, nil); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return reservations, nil
}

func (r *PostgresRepository) SaveFailedReservation(ctx context.Context, reservationID, orderID string, items []domain.PendingOrderItem, reason string, at time.Time) error {
	for _, item := range items {
		if _, err := r.db.ExecContext(ctx, `
		INSERT INTO inventory_reservations (order_id, reservation_id, product_id, quantity, status, failure_reason, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, orderID, reservationID, item.ProductID, item.Quantity, domain.ReservationStatusFailed, reason, at.UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (r *PostgresRepository) ReleaseInventory(ctx context.Context, orderID string, at time.Time, hook TxHook) (string, bool, error) {
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

	if hook != nil {
		if err := hook(ctx, tx, reservationID, nil); err != nil {
			return "", false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return reservationID, true, nil
}

func (r *PostgresRepository) CommitInventory(ctx context.Context, orderID string) (string, bool, error) {
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

	var reservationID string
	type reservationRow struct {
		ReservationID string
		ProductID     string
		Quantity      int
		Status        domain.ReservationStatus
	}
	var reservations []reservationRow
	committed := false
	for rows.Next() {
		var row reservationRow
		if err := rows.Scan(&row.ReservationID, &row.ProductID, &row.Quantity, &row.Status); err != nil {
			return "", false, err
		}
		if reservationID == "" {
			reservationID = row.ReservationID
		}
		if row.Status == domain.ReservationStatusReserved {
			committed = true
		}
		reservations = append(reservations, row)
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	if reservationID == "" {
		return "", false, nil
	}
	if !committed {
		return reservationID, false, tx.Commit()
	}
	productIDs := make([]string, 0, len(reservations))
	seenProductIDs := make(map[string]struct{}, len(reservations))
	for _, reservation := range reservations {
		if reservation.Status != domain.ReservationStatusReserved {
			continue
		}
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
		product.QuantityReserved -= reservation.Quantity
		products[reservation.ProductID] = product
	}
	for _, productID := range sortedProductKeys(products) {
		product := products[productID]
		if _, err := tx.ExecContext(ctx, `
		UPDATE products
		SET quantity_reserved = $2
		WHERE product_id = $1`, product.ProductID, product.QuantityReserved); err != nil {
			return "", false, err
		}
	}
	if _, err := tx.ExecContext(ctx, `
	UPDATE inventory_reservations
	SET status = $2
	WHERE order_id = $1 AND status = $3`, orderID, domain.ReservationStatusCommitted, domain.ReservationStatusReserved); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return reservationID, true, nil
}

func (r *PostgresRepository) Product(ctx context.Context, productID string) (domain.Product, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT product_id, product_name, description, price::text, image, category, visible, quantity_available, quantity_reserved, last_restocked_at, last_reservation_at, last_release_at
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

func (r *PostgresRepository) loadProductsForUpdate(ctx context.Context, tx *sql.Tx, ids []string) (map[string]domain.Product, error) {
	products := make(map[string]domain.Product, len(ids))
	if len(ids) == 0 {
		return products, nil
	}
	rows, err := tx.QueryContext(ctx, `
	SELECT product_id, product_name, description, price::text, image, category, visible, quantity_available, quantity_reserved, last_restocked_at, last_reservation_at, last_release_at
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

func scanProduct(scanner interface{ Scan(dest ...any) error }) (domain.Product, error) {
	var product domain.Product
	var price string
	var lastReservation sql.NullTime
	var lastRelease sql.NullTime
	var lastRestocked sql.NullTime
	err := scanner.Scan(&product.ProductID, &product.ProductName, &product.Description, &price, &product.Image, &product.Category, &product.Visible, &product.QuantityAvailable, &product.QuantityReserved, &lastRestocked, &lastReservation, &lastRelease)
	if err != nil {
		return domain.Product{}, err
	}
	product.Price = json.Number(price)
	if lastRestocked.Valid {
		product.LastRestockedAt = lastRestocked.Time.UTC()
	}
	if lastReservation.Valid {
		product.LastReservationAt = lastReservation.Time.UTC()
	}
	if lastRelease.Valid {
		product.LastReleaseAt = lastRelease.Time.UTC()
	}
	return product, nil
}

func marshalPendingItems(items []domain.PendingOrderItem) (string, error) {
	data, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(data), nil
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

func (r *PostgresRepository) ListProducts(ctx context.Context) ([]domain.Product, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT product_id, product_name, description, price::text, image, category, visible, quantity_available, quantity_reserved, last_restocked_at, last_reservation_at, last_release_at
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
	SELECT product_id, product_name, description, price::text, image, category, visible, quantity_available, quantity_reserved, last_restocked_at, last_reservation_at, last_release_at
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
	SET quantity_available = $2, quantity_reserved = $3, last_restocked_at = $4
	WHERE product_id = $1`, product.ProductID, product.QuantityAvailable, product.QuantityReserved, nullableTime(product.LastRestockedAt)); err != nil {
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
	SELECT product_id, product_name, description, price::text, image, category, visible, quantity_available, quantity_reserved, last_restocked_at, last_reservation_at, last_release_at
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
	SELECT reservation_id, order_id, product_id, quantity, status, COALESCE(failure_reason, '') as failure_reason, created_at, COALESCE(released_at, '0001-01-01') as released_at
	FROM inventory_reservations
	ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reservations []domain.Reservation
	for rows.Next() {
		var res domain.Reservation
		if err := rows.Scan(&res.ReservationID, &res.OrderID, &res.ProductID, &res.Quantity, &res.Status, &res.FailureReason, &res.CreatedAt, &res.ReleasedAt); err != nil {
			return nil, err
		}
		res.CreatedAt = res.CreatedAt.UTC()
		res.ReleasedAt = res.ReleasedAt.UTC()
		reservations = append(reservations, res)
	}
	return reservations, rows.Err()
}

func (r *PostgresRepository) CreateProduct(ctx context.Context, p domain.Product) (domain.Product, error) {
	var nextID string
	row := r.db.QueryRowContext(ctx, `
	SELECT COALESCE(
		'PROD-' || LPAD((MAX(CAST(SUBSTRING(product_id FROM 6) AS INTEGER)) + 1)::text, 3, '0'),
		'PROD-001'
	) FROM products WHERE product_id ~ '^PROD-[0-9]+$'`)
	if err := row.Scan(&nextID); err != nil {
		return domain.Product{}, fmt.Errorf("generate product id: %w", err)
	}
	p.ProductID = nextID
	p.Visible = true
	_, err := r.db.ExecContext(ctx, `
	INSERT INTO products (product_id, product_name, description, price, image, category, visible, quantity_available, quantity_reserved)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0)`,
		p.ProductID, p.ProductName, p.Description, p.Price.String(), p.Image, p.Category, p.Visible, p.QuantityAvailable)
	if err != nil {
		return domain.Product{}, fmt.Errorf("insert product: %w", err)
	}
	return p, nil
}

func (r *PostgresRepository) UpdateProductMeta(ctx context.Context, productId, name, description, category, image string, price json.Number) (domain.Product, error) {
	_, err := r.db.ExecContext(ctx, `
	UPDATE products
	SET product_name = $2, description = $3, price = $4, image = $5, category = $6
	WHERE product_id = $1`, productId, name, description, price.String(), image, category)
	if err != nil {
		return domain.Product{}, fmt.Errorf("update product meta: %w", err)
	}
	product, found, err := r.Product(ctx, productId)
	if err != nil {
		return domain.Product{}, err
	}
	if !found {
		return domain.Product{}, domain.ProductNotFoundError{ProductID: productId}
	}
	return product, nil
}

func (r *PostgresRepository) DeleteProduct(ctx context.Context, productId string) error {
	var reserved int
	row := r.db.QueryRowContext(ctx, `SELECT quantity_reserved FROM products WHERE product_id = $1`, productId)
	if err := row.Scan(&reserved); err == sql.ErrNoRows {
		return domain.ProductNotFoundError{ProductID: productId}
	} else if err != nil {
		return fmt.Errorf("check reserved: %w", err)
	}
	if reserved > 0 {
		return fmt.Errorf("cannot delete product %s: %d units are currently reserved", productId, reserved)
	}
	_, err := r.db.ExecContext(ctx, `DELETE FROM products WHERE product_id = $1`, productId)
	return err
}

var _ Repository = (*PostgresRepository)(nil)
