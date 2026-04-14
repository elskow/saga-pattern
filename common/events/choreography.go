package events

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"saga-pattern/common/dto"
	"saga-pattern/common/validate"
)

const (
	TypeOrderCreated               = "ORDER_CREATED"
	TypeOrderCompleted             = "ORDER_COMPLETED"
	TypeOrderCancelled             = "ORDER_CANCELLED"
	TypePaymentCompleted           = "PAYMENT_COMPLETED"
	TypePaymentFailed              = "PAYMENT_FAILED"
	TypePaymentRefunded            = "PAYMENT_REFUNDED"
	TypeInventoryReserved          = "INVENTORY_RESERVED"
	TypeInventoryReservationFailed = "INVENTORY_RESERVATION_FAILED"
	TypeInventoryReleased          = "INVENTORY_RELEASED"
	TypeShippingScheduled          = "SHIPPING_SCHEDULED"
	TypeShippingFailed             = "SHIPPING_FAILED"
	TypeShippingCancelled          = "SHIPPING_CANCELLED"
)

type ChoreographyEvent interface {
	EventType() string
	Validate() error
}

type OrderCreatedEvent struct {
	Type            string                 `json:"type"`
	OrderID         string                 `json:"orderId"`
	CustomerID      string                 `json:"customerId"`
	ShippingAddress string                 `json:"shippingAddress"`
	CorrelationID   string                 `json:"correlationId"`
	Items           []dto.OrderItemRequest `json:"items"`
	TotalAmount     json.Number            `json:"totalAmount"`
	CreatedAt       time.Time              `json:"createdAt"`
}

func NewOrderCreatedEvent(orderID, customerID, shippingAddress, correlationID string, items []dto.OrderItemRequest, totalAmount json.Number, createdAt time.Time) OrderCreatedEvent {
	return OrderCreatedEvent{Type: TypeOrderCreated, OrderID: orderID, CustomerID: customerID, ShippingAddress: shippingAddress, CorrelationID: correlationID, Items: items, TotalAmount: totalAmount, CreatedAt: createdAt}
}

func (e OrderCreatedEvent) EventType() string { return e.Type }
func (e OrderCreatedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeOrderCreated, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CustomerID, "customerId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.ShippingAddress, "shippingAddress"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if len(e.Items) == 0 {
		return fmt.Errorf("items cannot be empty")
	}
	for idx, item := range e.Items {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("items[%d]: %w", idx, err)
		}
	}
	if err := validate.PositiveNumber(e.TotalAmount, "totalAmount"); err != nil {
		return err
	}
	return nil
}

type OrderCompletedEvent struct {
	Type          string    `json:"type"`
	OrderID       string    `json:"orderId"`
	CompletedAt   time.Time `json:"completedAt"`
	CorrelationID string    `json:"correlationId"`
}

func NewOrderCompletedEvent(orderID string, completedAt time.Time, correlationID string) OrderCompletedEvent {
	return OrderCompletedEvent{Type: TypeOrderCompleted, OrderID: orderID, CompletedAt: completedAt, CorrelationID: correlationID}
}
func (e OrderCompletedEvent) EventType() string { return e.Type }
func (e OrderCompletedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeOrderCompleted, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.CompletedAt.IsZero() {
		return fmt.Errorf("completedAt cannot be zero")
	}
	return nil
}

type OrderCancelledEvent struct {
	Type          string    `json:"type"`
	OrderID       string    `json:"orderId"`
	Reason        string    `json:"reason"`
	CancelledAt   time.Time `json:"cancelledAt"`
	CorrelationID string    `json:"correlationId"`
}

func NewOrderCancelledEvent(orderID, reason string, cancelledAt time.Time, correlationID string) OrderCancelledEvent {
	return OrderCancelledEvent{Type: TypeOrderCancelled, OrderID: orderID, Reason: reason, CancelledAt: cancelledAt, CorrelationID: correlationID}
}
func (e OrderCancelledEvent) EventType() string { return e.Type }
func (e OrderCancelledEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeOrderCancelled, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.Reason, "reason"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.CancelledAt.IsZero() {
		return fmt.Errorf("cancelledAt cannot be zero")
	}
	return nil
}

type PaymentCompletedEvent struct {
	Type          string      `json:"type"`
	PaymentID     string      `json:"paymentId"`
	OrderID       string      `json:"orderId"`
	Amount        json.Number `json:"amount"`
	TransactionID string      `json:"transactionId"`
	CompletedAt   time.Time   `json:"completedAt"`
	CorrelationID string      `json:"correlationId"`
	CreatedAt     time.Time   `json:"createdAt"`
}

func NewPaymentCompletedEvent(paymentID, orderID string, amount json.Number, transactionID string, completedAt time.Time, correlationID string, createdAt time.Time) PaymentCompletedEvent {
	return PaymentCompletedEvent{Type: TypePaymentCompleted, PaymentID: paymentID, OrderID: orderID, Amount: amount, TransactionID: transactionID, CompletedAt: completedAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e PaymentCompletedEvent) EventType() string { return e.Type }
func (e PaymentCompletedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypePaymentCompleted, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.PaymentID, "paymentId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.PositiveNumber(e.Amount, "amount"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.TransactionID, "transactionId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.CompletedAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("payment timestamps cannot be zero")
	}
	return nil
}

type PaymentFailedEvent struct {
	Type          string    `json:"type"`
	PaymentID     string    `json:"paymentId,omitempty"`
	OrderID       string    `json:"orderId"`
	Reason        string    `json:"reason"`
	FailedAt      time.Time `json:"failedAt"`
	CorrelationID string    `json:"correlationId"`
	CreatedAt     time.Time `json:"createdAt"`
}

func NewPaymentFailedEvent(paymentID, orderID, reason string, failedAt time.Time, correlationID string, createdAt time.Time) PaymentFailedEvent {
	return PaymentFailedEvent{Type: TypePaymentFailed, PaymentID: paymentID, OrderID: orderID, Reason: reason, FailedAt: failedAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e PaymentFailedEvent) EventType() string { return e.Type }
func (e PaymentFailedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypePaymentFailed, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.Reason, "reason"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.FailedAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("payment failure timestamps cannot be zero")
	}
	return nil
}

type PaymentRefundedEvent struct {
	Type          string      `json:"type"`
	PaymentID     string      `json:"paymentId"`
	OrderID       string      `json:"orderId"`
	RefundAmount  json.Number `json:"refundAmount"`
	RefundedAt    time.Time   `json:"refundedAt"`
	CorrelationID string      `json:"correlationId"`
	CreatedAt     time.Time   `json:"createdAt"`
}

func NewPaymentRefundedEvent(paymentID, orderID string, refundAmount json.Number, refundedAt time.Time, correlationID string, createdAt time.Time) PaymentRefundedEvent {
	return PaymentRefundedEvent{Type: TypePaymentRefunded, PaymentID: paymentID, OrderID: orderID, RefundAmount: refundAmount, RefundedAt: refundedAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e PaymentRefundedEvent) EventType() string { return e.Type }
func (e PaymentRefundedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypePaymentRefunded, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.PaymentID, "paymentId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.PositiveNumber(e.RefundAmount, "refundAmount"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.RefundedAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("payment refund timestamps cannot be zero")
	}
	return nil
}

type InventoryReservedItem struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

func (i InventoryReservedItem) Validate() error {
	if err := validate.NonBlank(i.ProductID, "productId"); err != nil {
		return err
	}
	if err := validate.PositiveInt(i.Quantity, "quantity"); err != nil {
		return err
	}
	return nil
}

type InventoryReservedEvent struct {
	Type          string                  `json:"type"`
	ReservationID string                  `json:"reservationId"`
	OrderID       string                  `json:"orderId"`
	ReservedItems []InventoryReservedItem `json:"reservedItems"`
	ReservedAt    time.Time               `json:"reservedAt"`
	CorrelationID string                  `json:"correlationId"`
	CreatedAt     time.Time               `json:"createdAt"`
}

func NewInventoryReservedEvent(reservationID, orderID string, reservedItems []InventoryReservedItem, reservedAt time.Time, correlationID string, createdAt time.Time) InventoryReservedEvent {
	return InventoryReservedEvent{Type: TypeInventoryReserved, ReservationID: reservationID, OrderID: orderID, ReservedItems: reservedItems, ReservedAt: reservedAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e InventoryReservedEvent) EventType() string { return e.Type }
func (e InventoryReservedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeInventoryReserved, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.ReservationID, "reservationId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if len(e.ReservedItems) == 0 {
		return fmt.Errorf("reservedItems cannot be empty")
	}
	for idx, item := range e.ReservedItems {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("reservedItems[%d]: %w", idx, err)
		}
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.ReservedAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("inventory reserved timestamps cannot be zero")
	}
	return nil
}

type InventoryReservationFailedEvent struct {
	Type          string    `json:"type"`
	OrderID       string    `json:"orderId"`
	ProductID     string    `json:"productId,omitempty"`
	Reason        string    `json:"reason"`
	FailedAt      time.Time `json:"failedAt"`
	CorrelationID string    `json:"correlationId"`
	CreatedAt     time.Time `json:"createdAt"`
}

func NewInventoryReservationFailedEvent(orderID, productID, reason string, failedAt time.Time, correlationID string, createdAt time.Time) InventoryReservationFailedEvent {
	return InventoryReservationFailedEvent{Type: TypeInventoryReservationFailed, OrderID: orderID, ProductID: productID, Reason: reason, FailedAt: failedAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e InventoryReservationFailedEvent) EventType() string { return e.Type }
func (e InventoryReservationFailedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeInventoryReservationFailed, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.Reason, "reason"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.FailedAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("inventory failure timestamps cannot be zero")
	}
	return nil
}

type InventoryReleasedEvent struct {
	Type          string    `json:"type"`
	ReservationID string    `json:"reservationId"`
	OrderID       string    `json:"orderId"`
	ReleasedAt    time.Time `json:"releasedAt"`
	CorrelationID string    `json:"correlationId"`
	CreatedAt     time.Time `json:"createdAt"`
}

func NewInventoryReleasedEvent(reservationID, orderID string, releasedAt time.Time, correlationID string, createdAt time.Time) InventoryReleasedEvent {
	return InventoryReleasedEvent{Type: TypeInventoryReleased, ReservationID: reservationID, OrderID: orderID, ReleasedAt: releasedAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e InventoryReleasedEvent) EventType() string { return e.Type }
func (e InventoryReleasedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeInventoryReleased, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.ReservationID, "reservationId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.ReleasedAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("inventory released timestamps cannot be zero")
	}
	return nil
}

type ShippingScheduledEvent struct {
	Type              string    `json:"type"`
	ShippingID        string    `json:"shippingId"`
	OrderID           string    `json:"orderId"`
	TrackingNumber    string    `json:"trackingNumber"`
	Address           string    `json:"address"`
	EstimatedDelivery time.Time `json:"estimatedDelivery"`
	ScheduledAt       time.Time `json:"scheduledAt"`
	CorrelationID     string    `json:"correlationId"`
	CreatedAt         time.Time `json:"createdAt"`
}

func NewShippingScheduledEvent(shippingID, orderID, trackingNumber, address string, estimatedDelivery, scheduledAt time.Time, correlationID string, createdAt time.Time) ShippingScheduledEvent {
	return ShippingScheduledEvent{Type: TypeShippingScheduled, ShippingID: shippingID, OrderID: orderID, TrackingNumber: trackingNumber, Address: address, EstimatedDelivery: estimatedDelivery, ScheduledAt: scheduledAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e ShippingScheduledEvent) EventType() string { return e.Type }
func (e ShippingScheduledEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeShippingScheduled, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.ShippingID, "shippingId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.TrackingNumber, "trackingNumber"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.Address, "address"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.EstimatedDelivery.IsZero() || e.ScheduledAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("shipping scheduled timestamps cannot be zero")
	}
	return nil
}

type ShippingFailedEvent struct {
	Type          string    `json:"type"`
	OrderID       string    `json:"orderId"`
	Reason        string    `json:"reason"`
	FailedAt      time.Time `json:"failedAt"`
	CorrelationID string    `json:"correlationId"`
	CreatedAt     time.Time `json:"createdAt"`
}

func NewShippingFailedEvent(orderID, reason string, failedAt time.Time, correlationID string, createdAt time.Time) ShippingFailedEvent {
	return ShippingFailedEvent{Type: TypeShippingFailed, OrderID: orderID, Reason: reason, FailedAt: failedAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e ShippingFailedEvent) EventType() string { return e.Type }
func (e ShippingFailedEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeShippingFailed, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.Reason, "reason"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.FailedAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("shipping failure timestamps cannot be zero")
	}
	return nil
}

type ShippingCancelledEvent struct {
	Type          string    `json:"type"`
	ShippingID    string    `json:"shippingId"`
	OrderID       string    `json:"orderId"`
	CancelledAt   time.Time `json:"cancelledAt"`
	CorrelationID string    `json:"correlationId"`
	CreatedAt     time.Time `json:"createdAt"`
}

func NewShippingCancelledEvent(shippingID, orderID string, cancelledAt time.Time, correlationID string, createdAt time.Time) ShippingCancelledEvent {
	return ShippingCancelledEvent{Type: TypeShippingCancelled, ShippingID: shippingID, OrderID: orderID, CancelledAt: cancelledAt, CorrelationID: correlationID, CreatedAt: createdAt}
}
func (e ShippingCancelledEvent) EventType() string { return e.Type }
func (e ShippingCancelledEvent) Validate() error {
	if err := validate.Expected(e.Type, TypeShippingCancelled, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.ShippingID, "shippingId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(e.CorrelationID, "correlationId"); err != nil {
		return err
	}
	if e.CancelledAt.IsZero() || e.CreatedAt.IsZero() {
		return fmt.Errorf("shipping cancelled timestamps cannot be zero")
	}
	return nil
}

func DecodeChoreographyEvent(r io.Reader) (ChoreographyEvent, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}

	switch envelope.Type {
	case TypeOrderCreated:
		var event OrderCreatedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypeOrderCompleted:
		var event OrderCompletedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypeOrderCancelled:
		var event OrderCancelledEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypePaymentCompleted:
		var event PaymentCompletedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypePaymentFailed:
		var event PaymentFailedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypePaymentRefunded:
		var event PaymentRefundedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypeInventoryReserved:
		var event InventoryReservedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypeInventoryReservationFailed:
		var event InventoryReservationFailedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypeInventoryReleased:
		var event InventoryReleasedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypeShippingScheduled:
		var event ShippingScheduledEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypeShippingFailed:
		var event ShippingFailedEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	case TypeShippingCancelled:
		var event ShippingCancelledEvent
		if err := validate.UnmarshalStrictJSON(data, &event); err != nil {
			return nil, err
		}
		return event, event.Validate()
	default:
		return nil, fmt.Errorf("unknown choreography event type %q", envelope.Type)
	}
}
