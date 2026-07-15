package domain

import (
	"encoding/json"
	"strings"
	"time"

	"saga-pattern/common/dto"
	"saga-pattern/common/tracking"
	sagaRuntime "saga-pattern/orchestration-framework/runtime"
	ordersaga "saga-pattern/orchestration-saga/order-service/internal/saga"
)

const (
	StatusCompleted    = "COMPLETED"
	StatusCancelled    = "CANCELLED"
	StatusFailed       = "FAILED"
	StatusCompensating = "COMPENSATING"
)

type Order struct {
	OrderID          string
	CustomerID       string
	TotalAmount      json.Number
	ShippingAddress  string
	Items            []dto.OrderItemRequest
	PaymentID        string
	ReservationID    string
	ShippingID       string
	TrackingNumber   string
	Status           string
	FailureReason    string
	FailureStep      string
	CompensatedSteps []string
	CurrentStep      string
	CompletedSteps   []string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// FinalizedFromRuntimeView materializes the public queryable order projection
// once the orchestration runtime reaches a terminal state.
func FinalizedFromRuntimeView(view sagaRuntime.View[ordersaga.Data], now time.Time) Order {
	return Order{
		OrderID:          view.Data.OrderID,
		CustomerID:       view.Data.CustomerID,
		TotalAmount:      view.Data.TotalAmount,
		ShippingAddress:  view.Data.ShippingAddress,
		Items:            append([]dto.OrderItemRequest(nil), view.Data.Items...),
		PaymentID:        view.Data.PaymentID,
		ReservationID:    view.Data.ReservationID,
		ShippingID:       view.Data.ShippingID,
		TrackingNumber:   tracking.NumberForShippingID(view.Data.ShippingID),
		Status:           finalizedStatusFromRuntimeView(view),
		FailureReason:    view.LastError,
		FailureStep:      failureStepFromRuntimeView(view),
		CompensatedSteps: compensatedStepsFromRuntimeView(view),
		CompletedSteps:   forwardCompletedStepsFromRuntimeView(view),
		CreatedAt:        view.StartedAt,
		UpdatedAt:        now,
	}
}

// InProgressFromRuntimeView builds an order projection for a saga that has not
// yet reached a terminal state. The status is mapped to choreography-equivalent
// values so API consumers see familiar intermediate states.
func InProgressFromRuntimeView(view sagaRuntime.View[ordersaga.Data], now time.Time) Order {
	return Order{
		OrderID:         view.Data.OrderID,
		CustomerID:      view.Data.CustomerID,
		TotalAmount:     view.Data.TotalAmount,
		ShippingAddress: view.Data.ShippingAddress,
		Items:           append([]dto.OrderItemRequest(nil), view.Data.Items...),
		PaymentID:       view.Data.PaymentID,
		ReservationID:   view.Data.ReservationID,
		ShippingID:      view.Data.ShippingID,
		TrackingNumber:  tracking.NumberForShippingID(view.Data.ShippingID),
		Status:          inProgressStatusFromRuntimeState(view.State),
		CurrentStep:     currentStepFromRuntimeView(view),
		CompletedSteps:  forwardCompletedStepsFromRuntimeView(view),
		CreatedAt:       view.StartedAt,
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
		OrderID:          o.OrderID,
		CustomerID:       o.CustomerID,
		ShippingAddress:  o.ShippingAddress,
		Status:           o.Status,
		Items:            items,
		TotalAmount:      o.TotalAmount,
		PaymentID:        o.PaymentID,
		ReservationID:    o.ReservationID,
		ShippingID:       o.ShippingID,
		TrackingNumber:   o.TrackingNumber,
		FailureReason:    o.FailureReason,
		FailureStep:      o.FailureStep,
		CompensatedSteps: append([]string(nil), o.CompensatedSteps...),
		CurrentStep:      o.CurrentStep,
		CompletedSteps:   append([]string(nil), o.CompletedSteps...),
		CreatedAt:        o.CreatedAt,
		UpdatedAt:        o.UpdatedAt,
	}
}

func failureStepFromRuntimeView(view sagaRuntime.View[ordersaga.Data]) string {
	for _, row := range view.StepHistory {
		if row.Direction == "forward" && row.Status == "failed" {
			return normalizeStep(row.Step)
		}
	}
	return ""
}

func compensatedStepsFromRuntimeView(view sagaRuntime.View[ordersaga.Data]) []string {
	seen := make(map[string]struct{})
	steps := make([]string, 0)
	for _, row := range view.StepHistory {
		if row.Direction != "compensation" || row.Status != "compensated" {
			continue
		}
		step := normalizeStep(row.Step)
		if step == "" {
			continue
		}
		if _, ok := seen[step]; ok {
			continue
		}
		seen[step] = struct{}{}
		steps = append(steps, step)
	}
	return steps
}

func normalizeStep(step string) string {
	switch strings.ToLower(step) {
	case "payment":
		return "payment"
	case "inventory", "inventorycommit":
		return "inventory"
	case "shipping":
		return "shipping"
	default:
		return ""
	}
}

func finalizedStatusFromRuntimeView(view sagaRuntime.View[ordersaga.Data]) string {
	switch view.State {
	case StatusCompleted:
		return StatusCompleted
	case StatusCancelled:
		return StatusCancelled
	default:
		return StatusFailed
	}
}

// inProgressStatusFromRuntimeState maps a non-terminal saga state to a
// choreography-equivalent order status so API consumers see familiar
// intermediate states.
func inProgressStatusFromRuntimeState(state string) string {
	switch state {
	case ordersaga.StatePaymentPending:
		return "PAYMENT_PENDING"
	case ordersaga.StateInventoryPending:
		return "PAYMENT_COMPLETED"
	case ordersaga.StateShippingPending:
		return "INVENTORY_RESERVED"
	case ordersaga.StateInventoryCommitPending:
		return "SHIPPING_SCHEDULED"
	case ordersaga.StateCompensating:
		return StatusCompensating
	default:
		return "SAGA_STARTED"
	}
}

// currentStepFromRuntimeView returns the normalized name of the step the saga
// is currently executing (forward or compensation).
func currentStepFromRuntimeView(view sagaRuntime.View[ordersaga.Data]) string {
	if view.CurrentStep == "" {
		return ""
	}
	return normalizeStep(view.CurrentStep)
}

// forwardCompletedStepsFromRuntimeView returns the normalized names of all
// forward steps that have succeeded (i.e., completed their forward action).
func forwardCompletedStepsFromRuntimeView(view sagaRuntime.View[ordersaga.Data]) []string {
	seen := make(map[string]struct{})
	steps := make([]string, 0)
	for _, row := range view.StepHistory {
		if row.Direction != "forward" || row.Status != "succeeded" {
			continue
		}
		step := normalizeStep(row.Step)
		if step == "" {
			continue
		}
		if _, ok := seen[step]; ok {
			continue
		}
		seen[step] = struct{}{}
		steps = append(steps, step)
	}
	return steps
}
