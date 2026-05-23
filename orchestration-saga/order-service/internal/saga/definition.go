package saga

import (
	"encoding/json"
	"fmt"

	"saga-pattern/common/commands"
	"saga-pattern/common/dto"
	commonkafka "saga-pattern/common/kafka"
	commonreplies "saga-pattern/common/replies"
	"saga-pattern/common/validate"
	sagaRuntime "saga-pattern/orchestration-framework/runtime"
)

const (
	StatePaymentPending         = "PAYMENT_PENDING"
	StateInventoryPending       = "INVENTORY_PENDING"
	StateShippingPending        = "SHIPPING_PENDING"
	StateInventoryCommitPending = "INVENTORY_COMMIT_PENDING"
	StateCompensating           = "COMPENSATING"
)

var (
	stepPayment         = sagaRuntime.StepRef{Name: "Payment", PendingState: StatePaymentPending}
	stepInventory       = sagaRuntime.StepRef{Name: "Inventory", PendingState: StateInventoryPending}
	stepShipping        = sagaRuntime.StepRef{Name: "Shipping", PendingState: StateShippingPending}
	stepInventoryCommit = sagaRuntime.StepRef{Name: "InventoryCommit", PendingState: StateInventoryCommitPending}
)

type Data struct {
	OrderID         string                 `json:"orderId"`
	CustomerID      string                 `json:"customerId"`
	PaymentID       string                 `json:"paymentId"`
	ReservationID   string                 `json:"reservationId"`
	ShippingID      string                 `json:"shippingId"`
	CorrelationID   string                 `json:"correlationId"`
	ShippingAddress string                 `json:"shippingAddress"`
	TotalAmount     json.Number            `json:"totalAmount"`
	Items           []dto.OrderItemRequest `json:"items"`
}

func (d Data) Validate() error {
	if d.OrderID == "" {
		return fmt.Errorf("order id is required")
	}
	if d.CustomerID == "" {
		return fmt.Errorf("customer id is required")
	}
	if d.PaymentID == "" {
		return fmt.Errorf("payment id is required")
	}
	if d.ReservationID == "" {
		return fmt.Errorf("reservation id is required")
	}
	if d.ShippingID == "" {
		return fmt.Errorf("shipping id is required")
	}
	if d.TotalAmount == "" {
		return fmt.Errorf("total amount is required")
	}
	if d.ShippingAddress == "" {
		return fmt.Errorf("shipping address is required")
	}
	if len(d.Items) == 0 {
		return fmt.Errorf("items are required")
	}
	for _, item := range d.Items {
		if err := item.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func Definition(topics commonkafka.Topics) sagaRuntime.Definition[Data] {
	return sagaRuntime.Define[Data]("OrderSaga").
		Codec(sagaRuntime.JSONCodec[Data]{}).
		Validate(func(data Data) error { return data.Validate() }).
		CompensatingState(StateCompensating).
		TerminalStates("COMPLETED", "CANCELLED").
		Step(
			sagaRuntime.StepDefRef[Data](stepPayment).
				Forward(sagaRuntime.Command(topics.PaymentCommands, commands.CommandProcessPayment, func(data Data) (any, error) {
					return commands.NewProcessPaymentCommand(data.PaymentID, data.OrderID, data.CustomerID, data.TotalAmount, data.Items), nil
				})).
				Compensation(sagaRuntime.Command(topics.PaymentCommands, commands.CommandRefundPayment, func(data Data) (any, error) {
					return commands.NewRefundPaymentCommand(data.PaymentID, data.OrderID), nil
				})).
				OnForward(
					sagaRuntime.On[Data, commonreplies.PaymentCompletedReply](topics.PaymentReplies, commonreplies.TypePaymentCompleted, decodeReply[commonreplies.PaymentCompletedReply]).ThenAdvanceTo(stepInventory),
					sagaRuntime.On[Data, commonreplies.PaymentFailedReply](topics.PaymentReplies, commonreplies.TypePaymentFailed, decodeReply[commonreplies.PaymentFailedReply]).ThenCancel("CANCELLED", func(reply commonreplies.PaymentFailedReply) string { return reply.Reason }),
				).
				OnCompensate(
					sagaRuntime.On[Data, commonreplies.PaymentRefundedReply](topics.PaymentReplies, commonreplies.TypePaymentRefunded, decodeReply[commonreplies.PaymentRefundedReply]).ThenCompensatedIf(func(reply commonreplies.PaymentRefundedReply) bool { return reply.Success }),
				).
				Build(),
		).
		Step(
			sagaRuntime.StepDefRef[Data](stepInventory).
				Forward(sagaRuntime.Command(topics.InventoryCommands, commands.CommandReserveInventory, func(data Data) (any, error) {
					return commands.NewReserveInventoryCommand(data.ReservationID, data.OrderID, data.Items), nil
				})).
				Compensation(sagaRuntime.Command(topics.InventoryCommands, commands.CommandReleaseInventory, func(data Data) (any, error) {
					return commands.NewReleaseInventoryCommand(data.ReservationID, data.OrderID), nil
				})).
				OnForward(
					sagaRuntime.On[Data, commonreplies.InventoryReservedReply](topics.InventoryReplies, commonreplies.TypeInventoryReserved, decodeReply[commonreplies.InventoryReservedReply]).ThenAdvanceTo(stepShipping),
					sagaRuntime.On[Data, commonreplies.InventoryFailedReply](topics.InventoryReplies, commonreplies.TypeInventoryFailed, decodeReply[commonreplies.InventoryFailedReply]).ThenBeginCompensation(func(reply commonreplies.InventoryFailedReply) string { return reply.Reason }),
				).
				OnCompensate(
					sagaRuntime.On[Data, commonreplies.InventoryReleasedReply](topics.InventoryReplies, commonreplies.TypeInventoryReleased, decodeReply[commonreplies.InventoryReleasedReply]).ThenCompensatedIf(func(reply commonreplies.InventoryReleasedReply) bool { return reply.Success }),
				).
				Build(),
		).
		Step(
			sagaRuntime.StepDefRef[Data](stepShipping).
				Forward(sagaRuntime.Command(topics.ShippingCommands, commands.CommandScheduleShipping, func(data Data) (any, error) {
					return commands.NewScheduleShippingCommand(data.ShippingID, data.OrderID, data.ShippingAddress), nil
				})).
				Compensation(sagaRuntime.Command(topics.ShippingCommands, commands.CommandCancelShipping, func(data Data) (any, error) {
					return commands.NewCancelShippingCommand(data.ShippingID, data.OrderID), nil
				})).
				OnForward(
					sagaRuntime.On[Data, commonreplies.ShippingScheduledReply](topics.ShippingReplies, commonreplies.TypeShippingScheduled, decodeReply[commonreplies.ShippingScheduledReply]).ThenAdvanceTo(stepInventoryCommit),
					sagaRuntime.On[Data, commonreplies.ShippingFailedReply](topics.ShippingReplies, commonreplies.TypeShippingFailed, decodeReply[commonreplies.ShippingFailedReply]).ThenBeginCompensation(func(reply commonreplies.ShippingFailedReply) string { return reply.Reason }),
				).
				OnCompensate(
					sagaRuntime.On[Data, commonreplies.ShippingCancelledReply](topics.ShippingReplies, commonreplies.TypeShippingCancelled, decodeReply[commonreplies.ShippingCancelledReply]).ThenCompensatedIf(func(reply commonreplies.ShippingCancelledReply) bool { return reply.Success }),
				).
				Build(),
		).
		Step(
			sagaRuntime.StepDefRef[Data](stepInventoryCommit).
				Forward(sagaRuntime.Command(topics.InventoryCommands, commands.CommandCommitInventory, func(data Data) (any, error) {
					return commands.NewCommitInventoryCommand(data.ReservationID, data.OrderID), nil
				})).
				Compensation(sagaRuntime.Command(topics.InventoryCommands, commands.CommandReleaseInventory, func(data Data) (any, error) {
					return commands.NewReleaseInventoryCommand(data.ReservationID, data.OrderID), nil
				})).
				OnForward(
					sagaRuntime.On[Data, commonreplies.InventoryCommittedReply](topics.InventoryReplies, commonreplies.TypeInventoryCommitted, decodeReply[commonreplies.InventoryCommittedReply]).ThenComplete("COMPLETED"),
				).
				OnCompensate(
					sagaRuntime.On[Data, commonreplies.InventoryReleasedReply](topics.InventoryReplies, commonreplies.TypeInventoryReleased, decodeReply[commonreplies.InventoryReleasedReply]).ThenCompensatedIf(func(reply commonreplies.InventoryReleasedReply) bool { return reply.Success }),
				).
				Build(),
		).
		Build()
}

func decodeReply[R interface{ Validate() error }](payload []byte) (R, error) {
	var reply R
	if err := validate.UnmarshalStrictJSON(payload, &reply); err != nil {
		return reply, err
	}
	return reply, reply.Validate()
}
