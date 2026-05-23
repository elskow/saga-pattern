package domain

import (
	"encoding/json"
	"testing"
	"time"

	"saga-pattern/common/dto"
	sagaRuntime "saga-pattern/orchestration-framework/runtime"
	ordersaga "saga-pattern/orchestration-saga/order-service/internal/saga"
)

func TestFinalizedFromRuntimeViewBuildsVisibleTerminalProjection(t *testing.T) {
	now := time.Date(2026, 4, 14, 13, 0, 0, 0, time.UTC)
	runtimeView := sagaRuntime.View[ordersaga.Data]{
		SagaID:    "ORDER-1",
		SagaType:  "order-saga",
		State:     StatusCompleted,
		StartedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Second),
		Data: ordersaga.Data{
			OrderID:         "ORDER-1",
			CustomerID:      "CUST-1",
			TotalAmount:     json.Number("1599000"),
			ShippingAddress: "Jl. Ketintang Wiyata, Surabaya 60231",
			PaymentID:       "PAY-1",
			ReservationID:   "RES-1",
			ShippingID:      "SHIP-1",
			Items: []dto.OrderItemRequest{{
				ProductID:   "PROD-1",
				ProductName: "Widget",
				Quantity:    1,
				Price:       json.Number("1599000"),
			}},
		},
	}

	finalized := FinalizedFromRuntimeView(runtimeView, now)
	if finalized.Status != StatusCompleted {
		t.Fatalf("finalized status = %q, want %q", finalized.Status, StatusCompleted)
	}
	if finalized.CreatedAt != runtimeView.StartedAt {
		t.Fatalf("finalized created_at = %v, want %v", finalized.CreatedAt, runtimeView.StartedAt)
	}
}
