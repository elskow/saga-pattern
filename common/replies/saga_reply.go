package replies

import (
	"encoding/json"
	"fmt"
	"io"

	"saga-pattern/common/validate"
)

const (
	TypePaymentCompleted  = "PAYMENT_COMPLETED"
	TypePaymentFailed     = "PAYMENT_FAILED"
	TypePaymentRefunded   = "PAYMENT_REFUNDED"
	TypeInventoryReserved = "INVENTORY_RESERVED"
	TypeInventoryFailed   = "INVENTORY_FAILED"
	TypeInventoryReleased = "INVENTORY_RELEASED"
	TypeShippingScheduled = "SHIPPING_SCHEDULED"
	TypeShippingFailed    = "SHIPPING_FAILED"
	TypeShippingCancelled = "SHIPPING_CANCELLED"
)

type SagaReply interface {
	ReplyType() string
	Validate() error
}

type PaymentCompletedReply struct {
	Type      string `json:"type"`
	PaymentID string `json:"paymentId"`
	OrderID   string `json:"orderId"`
}

func NewPaymentCompletedReply(paymentID, orderID string) PaymentCompletedReply {
	return PaymentCompletedReply{Type: TypePaymentCompleted, PaymentID: paymentID, OrderID: orderID}
}
func (r PaymentCompletedReply) ReplyType() string { return r.Type }
func (r PaymentCompletedReply) Validate() error {
	if err := validate.Expected(r.Type, TypePaymentCompleted, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.PaymentID, "paymentId"); err != nil {
		return err
	}
	return validate.NonBlank(r.OrderID, "orderId")
}

type PaymentFailedReply struct {
	Type      string `json:"type"`
	PaymentID string `json:"paymentId,omitempty"`
	OrderID   string `json:"orderId"`
	Reason    string `json:"reason"`
}

func NewPaymentFailedReply(paymentID, orderID, reason string) PaymentFailedReply {
	return PaymentFailedReply{Type: TypePaymentFailed, PaymentID: paymentID, OrderID: orderID, Reason: reason}
}
func (r PaymentFailedReply) ReplyType() string { return r.Type }
func (r PaymentFailedReply) Validate() error {
	if err := validate.Expected(r.Type, TypePaymentFailed, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.OrderID, "orderId"); err != nil {
		return err
	}
	return validate.NonBlank(r.Reason, "reason")
}

type PaymentRefundedReply struct {
	Type      string `json:"type"`
	PaymentID string `json:"paymentId"`
	OrderID   string `json:"orderId"`
	Success   bool   `json:"success"`
	Reason    string `json:"reason,omitempty"`
}

func NewPaymentRefundedReply(paymentID, orderID string, success bool, reason string) PaymentRefundedReply {
	return PaymentRefundedReply{Type: TypePaymentRefunded, PaymentID: paymentID, OrderID: orderID, Success: success, Reason: reason}
}
func (r PaymentRefundedReply) ReplyType() string { return r.Type }
func (r PaymentRefundedReply) Validate() error {
	if err := validate.Expected(r.Type, TypePaymentRefunded, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.PaymentID, "paymentId"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.OrderID, "orderId"); err != nil {
		return err
	}
	if !r.Success && r.Reason == "" {
		return fmt.Errorf("reason cannot be blank when success is false")
	}
	return nil
}

type InventoryReservedReply struct {
	Type          string `json:"type"`
	ReservationID string `json:"reservationId"`
	OrderID       string `json:"orderId"`
}

func NewInventoryReservedReply(reservationID, orderID string) InventoryReservedReply {
	return InventoryReservedReply{Type: TypeInventoryReserved, ReservationID: reservationID, OrderID: orderID}
}
func (r InventoryReservedReply) ReplyType() string { return r.Type }
func (r InventoryReservedReply) Validate() error {
	if err := validate.Expected(r.Type, TypeInventoryReserved, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.ReservationID, "reservationId"); err != nil {
		return err
	}
	return validate.NonBlank(r.OrderID, "orderId")
}

type InventoryFailedReply struct {
	Type          string `json:"type"`
	ReservationID string `json:"reservationId,omitempty"`
	OrderID       string `json:"orderId"`
	Reason        string `json:"reason"`
}

func NewInventoryFailedReply(reservationID, orderID, reason string) InventoryFailedReply {
	return InventoryFailedReply{Type: TypeInventoryFailed, ReservationID: reservationID, OrderID: orderID, Reason: reason}
}
func (r InventoryFailedReply) ReplyType() string { return r.Type }
func (r InventoryFailedReply) Validate() error {
	if err := validate.Expected(r.Type, TypeInventoryFailed, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.OrderID, "orderId"); err != nil {
		return err
	}
	return validate.NonBlank(r.Reason, "reason")
}

type InventoryReleasedReply struct {
	Type          string `json:"type"`
	ReservationID string `json:"reservationId"`
	OrderID       string `json:"orderId"`
	Success       bool   `json:"success"`
	Reason        string `json:"reason,omitempty"`
}

func NewInventoryReleasedReply(reservationID, orderID string, success bool, reason string) InventoryReleasedReply {
	return InventoryReleasedReply{Type: TypeInventoryReleased, ReservationID: reservationID, OrderID: orderID, Success: success, Reason: reason}
}
func (r InventoryReleasedReply) ReplyType() string { return r.Type }
func (r InventoryReleasedReply) Validate() error {
	if err := validate.Expected(r.Type, TypeInventoryReleased, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.ReservationID, "reservationId"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.OrderID, "orderId"); err != nil {
		return err
	}
	if !r.Success && r.Reason == "" {
		return fmt.Errorf("reason cannot be blank when success is false")
	}
	return nil
}

type ShippingScheduledReply struct {
	Type       string `json:"type"`
	ShippingID string `json:"shippingId"`
	OrderID    string `json:"orderId"`
}

func NewShippingScheduledReply(shippingID, orderID string) ShippingScheduledReply {
	return ShippingScheduledReply{Type: TypeShippingScheduled, ShippingID: shippingID, OrderID: orderID}
}
func (r ShippingScheduledReply) ReplyType() string { return r.Type }
func (r ShippingScheduledReply) Validate() error {
	if err := validate.Expected(r.Type, TypeShippingScheduled, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.ShippingID, "shippingId"); err != nil {
		return err
	}
	return validate.NonBlank(r.OrderID, "orderId")
}

type ShippingFailedReply struct {
	Type       string `json:"type"`
	ShippingID string `json:"shippingId,omitempty"`
	OrderID    string `json:"orderId"`
	Reason     string `json:"reason"`
}

func NewShippingFailedReply(shippingID, orderID, reason string) ShippingFailedReply {
	return ShippingFailedReply{Type: TypeShippingFailed, ShippingID: shippingID, OrderID: orderID, Reason: reason}
}
func (r ShippingFailedReply) ReplyType() string { return r.Type }
func (r ShippingFailedReply) Validate() error {
	if err := validate.Expected(r.Type, TypeShippingFailed, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.OrderID, "orderId"); err != nil {
		return err
	}
	return validate.NonBlank(r.Reason, "reason")
}

type ShippingCancelledReply struct {
	Type       string `json:"type"`
	ShippingID string `json:"shippingId"`
	OrderID    string `json:"orderId"`
	Success    bool   `json:"success"`
	Reason     string `json:"reason,omitempty"`
}

func NewShippingCancelledReply(shippingID, orderID string, success bool, reason string) ShippingCancelledReply {
	return ShippingCancelledReply{Type: TypeShippingCancelled, ShippingID: shippingID, OrderID: orderID, Success: success, Reason: reason}
}
func (r ShippingCancelledReply) ReplyType() string { return r.Type }
func (r ShippingCancelledReply) Validate() error {
	if err := validate.Expected(r.Type, TypeShippingCancelled, "type"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.ShippingID, "shippingId"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.OrderID, "orderId"); err != nil {
		return err
	}
	if !r.Success && r.Reason == "" {
		return fmt.Errorf("reason cannot be blank when success is false")
	}
	return nil
}

func DecodeSagaReply(r io.Reader) (SagaReply, error) {
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
	case TypePaymentCompleted:
		var reply PaymentCompletedReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	case TypePaymentFailed:
		var reply PaymentFailedReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	case TypePaymentRefunded:
		var reply PaymentRefundedReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	case TypeInventoryReserved:
		var reply InventoryReservedReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	case TypeInventoryFailed:
		var reply InventoryFailedReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	case TypeInventoryReleased:
		var reply InventoryReleasedReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	case TypeShippingScheduled:
		var reply ShippingScheduledReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	case TypeShippingFailed:
		var reply ShippingFailedReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	case TypeShippingCancelled:
		var reply ShippingCancelledReply
		if err := validate.UnmarshalStrictJSON(data, &reply); err != nil {
			return nil, err
		}
		return reply, reply.Validate()
	default:
		return nil, fmt.Errorf("unknown saga reply type %q", envelope.Type)
	}
}
