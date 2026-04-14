package domain

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/events"
)

type Order struct {
	OrderID         string
	CustomerID      string
	ShippingAddress string
	Status          dto.OrderStatus
	Items           []dto.OrderItemResponse
	TotalAmount     json.Number
	PaymentID       string
	ReservationID   string
	ShippingID      string
	TrackingNumber  string
	FailureReason   string
	CorrelationID   string
	IdempotencyKey  string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func NewOrderFromRequest(orderID string, request dto.ChoreographyCreateOrderRequest, idempotencyKey string, now time.Time) (Order, error) {
	items := make([]dto.OrderItemResponse, 0, len(request.Items))
	total := new(big.Rat)
	for _, item := range request.Items {
		items = append(items, dto.OrderItemResponse{
			ProductID:   item.ProductID,
			ProductName: item.ProductName,
			Quantity:    item.Quantity,
			Price:       item.Price,
		})

		price := new(big.Rat)
		if _, ok := price.SetString(item.Price.String()); !ok {
			return Order{}, fmt.Errorf("parse item price %q", item.Price.String())
		}
		lineTotal := new(big.Rat).Mul(price, big.NewRat(int64(item.Quantity), 1))
		total.Add(total, lineTotal)
	}

	correlationID := commoncontext.ResolveCorrelationID("")
	return Order{
		OrderID:         orderID,
		CustomerID:      request.CustomerID,
		ShippingAddress: request.ShippingAddress,
		Status:          dto.OrderStatusPending,
		Items:           items,
		TotalAmount:     json.Number(total.FloatString(2)),
		CorrelationID:   correlationID,
		IdempotencyKey:  idempotencyKey,
		CreatedAt:       now.UTC(),
		UpdatedAt:       now.UTC(),
	}, nil
}

func (o Order) Response() dto.OrderResponse {
	items := make([]dto.OrderItemResponse, len(o.Items))
	copy(items, o.Items)
	return dto.OrderResponse{
		OrderID:     o.OrderID,
		CustomerID:  o.CustomerID,
		Status:      string(o.Status),
		Items:       items,
		TotalAmount: o.TotalAmount,
		CreatedAt:   o.CreatedAt,
		UpdatedAt:   o.UpdatedAt,
	}
}

func (o Order) ToOrderCreatedEvent(createdAt time.Time) events.OrderCreatedEvent {
	items := make([]dto.OrderItemRequest, 0, len(o.Items))
	for _, item := range o.Items {
		items = append(items, dto.OrderItemRequest{
			ProductID:   item.ProductID,
			ProductName: item.ProductName,
			Quantity:    item.Quantity,
			Price:       item.Price,
		})
	}
	return events.NewOrderCreatedEvent(o.OrderID, o.CustomerID, o.ShippingAddress, o.CorrelationID, items, o.TotalAmount, createdAt.UTC())
}

func (o *Order) MarkPaymentCompleted(paymentID string, now time.Time) {
	o.PaymentID = paymentID
	o.Status = dto.OrderStatusPaymentCompleted
	o.UpdatedAt = now.UTC()
}

func (o *Order) MarkInventoryReserved(reservationID string, now time.Time) {
	o.ReservationID = reservationID
	o.Status = dto.OrderStatusInventoryReserved
	o.UpdatedAt = now.UTC()
}

func (o *Order) MarkCompleted(shippingID string, trackingNumber string, now time.Time) {
	o.ShippingID = shippingID
	o.TrackingNumber = trackingNumber
	o.Status = dto.OrderStatusCompleted
	o.UpdatedAt = now.UTC()
}

func (o *Order) MarkCancelled(reason string, now time.Time) {
	o.FailureReason = reason
	o.Status = dto.OrderStatusCancelled
	o.UpdatedAt = now.UTC()
}
