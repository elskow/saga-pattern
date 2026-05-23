package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	sagaRuntime "saga-pattern/orchestration-framework/runtime"
	"saga-pattern/orchestration-saga/order-service/internal/domain"
	ordersaga "saga-pattern/orchestration-saga/order-service/internal/saga"
	"time"
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

// AcknowledgeAcceptedLifecycle keeps the accepted-vs-finalized lifecycle explicit
// without creating a public orders row. The runtime remains the source of truth
// until terminal state is materialized into the finalized order projection.
func (r *PostgresRepository) AcknowledgeAcceptedLifecycle(context.Context) error {
	return nil
}

func (r *PostgresRepository) GetFinalized(ctx context.Context, orderID string) (domain.Order, bool, error) {
	row := r.db.QueryRowContext(ctx, `
	SELECT order_id, customer_id, total_amount, shipping_address, items_json, status, payment_id, reservation_id, shipment_id, tracking_number,
	       failure_reason, failure_step, compensated_steps_json, created_at, updated_at
	FROM orders
	WHERE order_id = $1`, orderID)
	return scanOrder(row)
}

func (r *PostgresRepository) UpsertFinalizedFromRuntimeView(ctx context.Context, runtimeView sagaRuntime.View[ordersaga.Data], now time.Time) (domain.Order, error) {
	itemsJSON, err := json.Marshal(runtimeView.Data.Items)
	if err != nil {
		return domain.Order{}, err
	}
	finalizedOrder := domain.FinalizedFromRuntimeView(runtimeView, now)
	compensatedStepsJSON, err := json.Marshal(finalizedOrder.CompensatedSteps)
	if err != nil {
		return domain.Order{}, err
	}
	_, err = r.db.ExecContext(ctx, `
	INSERT INTO orders (order_id, customer_id, total_amount, shipping_address, items_json, status, payment_id, reservation_id, shipment_id, tracking_number, failure_reason, failure_step, compensated_steps_json, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	ON CONFLICT (order_id) DO UPDATE SET
	customer_id = EXCLUDED.customer_id,
	total_amount = EXCLUDED.total_amount,
	shipping_address = EXCLUDED.shipping_address,
	items_json = EXCLUDED.items_json,
	status = EXCLUDED.status,
	payment_id = EXCLUDED.payment_id,
	reservation_id = EXCLUDED.reservation_id,
	shipment_id = EXCLUDED.shipment_id,
	tracking_number = EXCLUDED.tracking_number,
	failure_reason = EXCLUDED.failure_reason,
	failure_step = EXCLUDED.failure_step,
	compensated_steps_json = EXCLUDED.compensated_steps_json,
	updated_at = EXCLUDED.updated_at`,
		finalizedOrder.OrderID, finalizedOrder.CustomerID, finalizedOrder.TotalAmount.String(), finalizedOrder.ShippingAddress, string(itemsJSON), finalizedOrder.Status,
		finalizedOrder.PaymentID, finalizedOrder.ReservationID, finalizedOrder.ShippingID, finalizedOrder.TrackingNumber, finalizedOrder.FailureReason, finalizedOrder.FailureStep, string(compensatedStepsJSON), finalizedOrder.CreatedAt, finalizedOrder.UpdatedAt)
	if err != nil {
		return domain.Order{}, err
	}
	return finalizedOrder, nil
}

func (r *PostgresRepository) ListFinalized(ctx context.Context) ([]domain.Order, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT order_id, customer_id, total_amount, shipping_address, items_json, status, payment_id, reservation_id, shipment_id, tracking_number,
	       failure_reason, failure_step, compensated_steps_json, created_at, updated_at
	FROM orders
	ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

func (r *PostgresRepository) ListFinalizedByCustomer(ctx context.Context, customerID string) ([]domain.Order, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT order_id, customer_id, total_amount, shipping_address, items_json, status, payment_id, reservation_id, shipment_id, tracking_number,
	       failure_reason, failure_step, compensated_steps_json, created_at, updated_at
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
	var failureStep sql.NullString
	var compensatedStepsJSON sql.NullString
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
		&order.TrackingNumber,
		&order.FailureReason,
		&failureStep,
		&compensatedStepsJSON,
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
	if itemsJSON != "" {
		if err := json.Unmarshal([]byte(itemsJSON), &order.Items); err != nil {
			return domain.Order{}, false, err
		}
	}
	order.FailureStep = failureStep.String
	if compensatedStepsJSON.String != "" {
		if err := json.Unmarshal([]byte(compensatedStepsJSON.String), &order.CompensatedSteps); err != nil {
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

var _ Repository = (*PostgresRepository)(nil)
