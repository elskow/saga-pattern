package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"saga-pattern/choreography-saga/order-service/internal/domain"
	"saga-pattern/common/dto"
)

var ErrIdempotencyConflict = errors.New("idempotency key reused with different order payload")

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres db is required")
	}
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) Create(ctx context.Context, order domain.Order) (domain.Order, error) {
	itemsJSON, err := marshalItems(order.Items)
	if err != nil {
		return domain.Order{}, fmt.Errorf("marshal order items: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Order{}, err
	}
	defer tx.Rollback()

	if err := insertOrder(ctx, tx, order, order.IdempotencyKey, itemsJSON); err != nil {
		return domain.Order{}, err
	}
	if err := insertOrderCreatedOutbox(ctx, tx, order); err != nil {
		return domain.Order{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Order{}, err
	}
	return order, nil
}

func insertOrder(ctx context.Context, tx *sql.Tx, order domain.Order, idempotencyKey string, itemsJSON string) error {
	_, err := tx.ExecContext(ctx, `
	INSERT INTO orders (order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13, ''),$14,$15)`,
		order.OrderID, order.CustomerID, order.ShippingAddress, order.Status, itemsJSON, order.TotalAmount.String(),
		nullableString(order.PaymentID), nullableString(order.ReservationID), nullableString(order.ShippingID), nullableString(order.TrackingNumber), nullableString(order.FailureReason),
		order.CorrelationID, idempotencyKey, order.CreatedAt.UTC(), order.UpdatedAt.UTC(),
	)
	return err
}

func (r *PostgresRepository) CreateIfAbsent(ctx context.Context, idempotencyKey string, order domain.Order) (domain.Order, bool, error) {
	itemsJSON, err := marshalItems(order.Items)
	if err != nil {
		return domain.Order{}, false, fmt.Errorf("marshal order items: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Order{}, false, err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
	INSERT INTO orders (order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13, ''),$14,$15)
	ON CONFLICT (idempotency_key) DO NOTHING`,
		order.OrderID, order.CustomerID, order.ShippingAddress, order.Status, itemsJSON, order.TotalAmount.String(),
		nullableString(order.PaymentID), nullableString(order.ReservationID), nullableString(order.ShippingID), nullableString(order.TrackingNumber), nullableString(order.FailureReason),
		order.CorrelationID, idempotencyKey, order.CreatedAt.UTC(), order.UpdatedAt.UTC(),
	)
	if err != nil {
		return domain.Order{}, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return domain.Order{}, false, err
	}
	if rows == 0 {
		existing, ok, err := scanOrderRow(tx.QueryRowContext(ctx, `
		SELECT order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at
		FROM orders
		WHERE idempotency_key = $1`, idempotencyKey))
		if err != nil {
			return domain.Order{}, false, err
		}
		if !ok {
			return domain.Order{}, false, fmt.Errorf("order with idempotency key %q not found after conflict", idempotencyKey)
		}
		if !sameOrderPayload(existing, order) {
			return domain.Order{}, false, ErrIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return domain.Order{}, false, err
		}
		return existing, false, nil
	}
	if err := insertOrderCreatedOutbox(ctx, tx, order); err != nil {
		return domain.Order{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return domain.Order{}, false, err
	}
	return order, true, nil
}

func sameOrderPayload(existing domain.Order, incoming domain.Order) bool {
	if existing.CustomerID != incoming.CustomerID || existing.ShippingAddress != incoming.ShippingAddress || existing.TotalAmount.String() != incoming.TotalAmount.String() || len(existing.Items) != len(incoming.Items) {
		return false
	}
	for i := range existing.Items {
		left := existing.Items[i]
		right := incoming.Items[i]
		if left.ProductID != right.ProductID || left.ProductName != right.ProductName || left.Quantity != right.Quantity || left.Price.String() != right.Price.String() {
			return false
		}
	}
	return true
}

func (r *PostgresRepository) Get(ctx context.Context, orderID string) (domain.Order, bool, error) {
	return scanOrderRow(r.db.QueryRowContext(ctx, `
	SELECT order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at
	FROM orders
	WHERE order_id = $1`, orderID))
}

func (r *PostgresRepository) List(ctx context.Context) ([]domain.Order, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT order_id, customer_id, shipping_address, status, items_json, total_amount, payment_id, reservation_id, shipping_id, tracking_number, failure_reason, correlation_id, idempotency_key, created_at, updated_at
	FROM orders
	ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	orders := make([]domain.Order, 0)
	for rows.Next() {
		order, ok, err := scanOrderRow(rows)
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

func (r *PostgresRepository) Save(ctx context.Context, order domain.Order) error {
	itemsJSON, err := marshalItems(order.Items)
	if err != nil {
		return fmt.Errorf("marshal order items: %w", err)
	}

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
		order.OrderID, order.CustomerID, order.ShippingAddress, order.Status, itemsJSON, order.TotalAmount.String(),
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

func (r *PostgresRepository) DeleteProcessedEvent(ctx context.Context, key string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM processed_events WHERE event_key = $1`, key)
	return err
}

func (r *PostgresRepository) ClaimPendingOrderEvents(ctx context.Context, limit int) ([]OrderOutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
	UPDATE order_outbox_messages
	SET status = 'publishing', updated_at = NOW()
	WHERE id IN (
		SELECT id
		FROM order_outbox_messages
		WHERE status = 'pending'
		   OR (status = 'publishing' AND updated_at < NOW() - INTERVAL '30 seconds')
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	)
	RETURNING id, order_id, event_type, payload_json, attempt_count`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]OrderOutboxMessage, 0)
	for rows.Next() {
		var message OrderOutboxMessage
		if err := rows.Scan(&message.ID, &message.OrderID, &message.EventType, &message.PayloadJSON, &message.Attempts); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (r *PostgresRepository) pendingOrderEventsForTest(ctx context.Context, limit int) ([]OrderOutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
	SELECT id, order_id, event_type, payload_json, attempt_count
	FROM order_outbox_messages
	WHERE status IN ('pending', 'publishing')
	ORDER BY created_at ASC
	LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]OrderOutboxMessage, 0)
	for rows.Next() {
		var message OrderOutboxMessage
		if err := rows.Scan(&message.ID, &message.OrderID, &message.EventType, &message.PayloadJSON, &message.Attempts); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func (r *PostgresRepository) MarkOrderEventPublished(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `
	UPDATE order_outbox_messages
	SET status = 'sent', updated_at = $2
	WHERE id = $1`, id, time.Now().UTC())
	return err
}

func (r *PostgresRepository) MarkOrderEventPublishFailed(ctx context.Context, id int64, errMessage string) error {
	_, err := r.db.ExecContext(ctx, `
	UPDATE order_outbox_messages
	SET status = 'pending', attempt_count = attempt_count + 1, last_error = $2, updated_at = $3
	WHERE id = $1`, id, errMessage, time.Now().UTC())
	return err
}

func insertOrderCreatedOutbox(ctx context.Context, tx *sql.Tx, order domain.Order) error {
	payload, err := json.Marshal(order.ToOrderCreatedEvent(order.CreatedAt))
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
	INSERT INTO order_outbox_messages (order_id, event_type, payload_json, status, created_at, updated_at)
	VALUES ($1, $2, $3, 'pending', $4, $4)`, order.OrderID, "OrderCreated", string(payload), order.CreatedAt.UTC())
	return err
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

func marshalItems(items []dto.OrderItemResponse) (string, error) {
	data, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var _ Repository = (*PostgresRepository)(nil)
