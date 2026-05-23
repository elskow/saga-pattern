package commands

import (
	"encoding/json"

	"saga-pattern/common/dto"
	"saga-pattern/common/validate"
)

const (
	CommandProcessPayment   = "PROCESS_PAYMENT"
	CommandRefundPayment    = "REFUND_PAYMENT"
	CommandReserveInventory = "RESERVE_INVENTORY"
	CommandCommitInventory  = "COMMIT_INVENTORY"
	CommandReleaseInventory = "RELEASE_INVENTORY"
	CommandScheduleShipping = "SCHEDULE_SHIPPING"
	CommandCancelShipping   = "CANCEL_SHIPPING"
)

type ProcessPaymentCommand struct {
	CommandType string                 `json:"commandType"`
	PaymentID   string                 `json:"paymentId"`
	OrderID     string                 `json:"orderId"`
	CustomerID  string                 `json:"customerId"`
	Amount      json.Number            `json:"amount"`
	Items       []dto.OrderItemRequest `json:"items"`
}

func NewProcessPaymentCommand(paymentID, orderID, customerID string, amount json.Number, items []dto.OrderItemRequest) ProcessPaymentCommand {
	return ProcessPaymentCommand{
		CommandType: CommandProcessPayment,
		PaymentID:   paymentID,
		OrderID:     orderID,
		CustomerID:  customerID,
		Amount:      amount,
		Items:       items,
	}
}
func (c ProcessPaymentCommand) Validate() error {
	if err := validate.Expected(c.CommandType, CommandProcessPayment, "commandType"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.PaymentID, "paymentId"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.OrderID, "orderId"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.CustomerID, "customerId"); err != nil {
		return err
	}
	if err := validate.PositiveNumber(c.Amount, "amount"); err != nil {
		return err
	}
	for _, item := range c.Items {
		if err := item.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type RefundPaymentCommand struct {
	CommandType string `json:"commandType"`
	PaymentID   string `json:"paymentId"`
	OrderID     string `json:"orderId"`
}

func NewRefundPaymentCommand(paymentID, orderID string) RefundPaymentCommand {
	return RefundPaymentCommand{CommandType: CommandRefundPayment, PaymentID: paymentID, OrderID: orderID}
}
func (c RefundPaymentCommand) Validate() error {
	if err := validate.Expected(c.CommandType, CommandRefundPayment, "commandType"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.PaymentID, "paymentId"); err != nil {
		return err
	}
	return validate.NonBlank(c.OrderID, "orderId")
}

type ReserveInventoryCommand struct {
	CommandType   string                 `json:"commandType"`
	ReservationID string                 `json:"reservationId"`
	OrderID       string                 `json:"orderId"`
	Items         []dto.OrderItemRequest `json:"items"`
}

func NewReserveInventoryCommand(reservationID, orderID string, items []dto.OrderItemRequest) ReserveInventoryCommand {
	return ReserveInventoryCommand{CommandType: CommandReserveInventory, ReservationID: reservationID, OrderID: orderID, Items: items}
}
func (c ReserveInventoryCommand) Validate() error {
	if err := validate.Expected(c.CommandType, CommandReserveInventory, "commandType"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.ReservationID, "reservationId"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.OrderID, "orderId"); err != nil {
		return err
	}
	if len(c.Items) == 0 {
		return validate.NonBlank("", "items")
	}
	for _, item := range c.Items {
		if err := item.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type ReleaseInventoryCommand struct {
	CommandType   string `json:"commandType"`
	ReservationID string `json:"reservationId"`
	OrderID       string `json:"orderId"`
}

type CommitInventoryCommand struct {
	CommandType   string `json:"commandType"`
	ReservationID string `json:"reservationId"`
	OrderID       string `json:"orderId"`
}

func NewCommitInventoryCommand(reservationID, orderID string) CommitInventoryCommand {
	return CommitInventoryCommand{CommandType: CommandCommitInventory, ReservationID: reservationID, OrderID: orderID}
}
func (c CommitInventoryCommand) Validate() error {
	if err := validate.Expected(c.CommandType, CommandCommitInventory, "commandType"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.ReservationID, "reservationId"); err != nil {
		return err
	}
	return validate.NonBlank(c.OrderID, "orderId")
}

func NewReleaseInventoryCommand(reservationID, orderID string) ReleaseInventoryCommand {
	return ReleaseInventoryCommand{CommandType: CommandReleaseInventory, ReservationID: reservationID, OrderID: orderID}
}
func (c ReleaseInventoryCommand) Validate() error {
	if err := validate.Expected(c.CommandType, CommandReleaseInventory, "commandType"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.ReservationID, "reservationId"); err != nil {
		return err
	}
	return validate.NonBlank(c.OrderID, "orderId")
}

type ScheduleShippingCommand struct {
	CommandType     string `json:"commandType"`
	ShippingID      string `json:"shippingId"`
	OrderID         string `json:"orderId"`
	ShippingAddress string `json:"shippingAddress"`
}

func NewScheduleShippingCommand(shippingID, orderID, shippingAddress string) ScheduleShippingCommand {
	return ScheduleShippingCommand{CommandType: CommandScheduleShipping, ShippingID: shippingID, OrderID: orderID, ShippingAddress: shippingAddress}
}
func (c ScheduleShippingCommand) Validate() error {
	if err := validate.Expected(c.CommandType, CommandScheduleShipping, "commandType"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.ShippingID, "shippingId"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.OrderID, "orderId"); err != nil {
		return err
	}
	return validate.NonBlank(c.ShippingAddress, "shippingAddress")
}

type CancelShippingCommand struct {
	CommandType string `json:"commandType"`
	ShippingID  string `json:"shippingId"`
	OrderID     string `json:"orderId"`
}

func NewCancelShippingCommand(shippingID, orderID string) CancelShippingCommand {
	return CancelShippingCommand{
		CommandType: CommandCancelShipping,
		ShippingID:  shippingID,
		OrderID:     orderID,
	}
}
func (c CancelShippingCommand) Validate() error {
	if err := validate.Expected(c.CommandType, CommandCancelShipping, "commandType"); err != nil {
		return err
	}
	if err := validate.NonBlank(c.ShippingID, "shippingId"); err != nil {
		return err
	}
	return validate.NonBlank(c.OrderID, "orderId")
}
