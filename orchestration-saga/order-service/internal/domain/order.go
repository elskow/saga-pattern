package domain

import (
	"encoding/json"
	"time"

	"saga-pattern/common/dto"
	frameworkruntime "saga-pattern/orchestration-framework/runtime"
)

const (
	StatusCreated   = "CREATED"
	StatusCompleted = "COMPLETED"
	StatusCancelled = "CANCELLED"
	StatusFailed    = "FAILED"
)

type Order struct {
	OrderID         string
	CustomerID      string
	TotalAmount     json.Number
	ShippingAddress string
	Items           []dto.OrderItemRequest
	PaymentID       string
	ReservationID   string
	ShippingID      string
	Status          string
	FailureReason   string
	Visible         bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func NewAccepted(orderID string, request dto.OrchestrationCreateOrderRequest, paymentID string, reservationID string, shippingID string, now time.Time) Order {
	items := append([]dto.OrderItemRequest(nil), request.Items...)
	return Order{
		OrderID:         orderID,
		CustomerID:      request.CustomerID,
		TotalAmount:     request.TotalAmount,
		ShippingAddress: request.ShippingAddress,
		Items:           items,
		PaymentID:       paymentID,
		ReservationID:   reservationID,
		ShippingID:      shippingID,
		Status:          StatusCreated,
		Visible:         false,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func FromSnapshot(snapshot frameworkruntime.Snapshot, now time.Time) Order {
	return Order{
		OrderID:         snapshot.OrderID,
		CustomerID:      snapshot.CustomerID,
		TotalAmount:     snapshot.TotalAmount,
		ShippingAddress: snapshot.ShippingAddress,
		Items:           append([]dto.OrderItemRequest(nil), snapshot.Items...),
		PaymentID:       snapshot.PaymentID,
		ReservationID:   snapshot.ReservationID,
		ShippingID:      snapshot.ShippingID,
		Status:          terminalStatus(snapshot),
		FailureReason:   snapshot.LastError,
		Visible:         true,
		CreatedAt:       snapshot.StartedAt,
		UpdatedAt:       now,
	}
}

func (o Order) Response() dto.OrderResponse {
	items := make([]dto.OrderItemResponse, 0, len(o.Items))
	for _, item := range o.Items {
		items = append(items, dto.OrderItemResponse{
			ProductID:   item.ProductID,
			ProductName: item.ProductName,
			Quantity:    item.Quantity,
			Price:       item.Price,
		})
	}
	return dto.OrderResponse{
		OrderID:     o.OrderID,
		CustomerID:  o.CustomerID,
		Status:      o.Status,
		Items:       items,
		TotalAmount: o.TotalAmount,
		CreatedAt:   o.CreatedAt,
		UpdatedAt:   o.UpdatedAt,
	}
}

func terminalStatus(snapshot frameworkruntime.Snapshot) string {
	switch snapshot.State {
	case StatusCompleted:
		return StatusCompleted
	case StatusCancelled:
		return StatusCancelled
	default:
		return StatusFailed
	}
}
