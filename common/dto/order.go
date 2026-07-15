package dto

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"saga-pattern/common/validate"
)

type OrderStatus string

const (
	OrderStatusPending           OrderStatus = "PENDING"
	OrderStatusPaymentPending    OrderStatus = "PAYMENT_PENDING"
	OrderStatusPaymentCompleted  OrderStatus = "PAYMENT_COMPLETED"
	OrderStatusPaymentFailed     OrderStatus = "PAYMENT_FAILED"
	OrderStatusInventoryReserved OrderStatus = "INVENTORY_RESERVED"
	OrderStatusInventoryFailed   OrderStatus = "INVENTORY_FAILED"
	OrderStatusShippingScheduled OrderStatus = "SHIPPING_SCHEDULED"
	OrderStatusShippingFailed    OrderStatus = "SHIPPING_FAILED"
	OrderStatusCompleted         OrderStatus = "COMPLETED"
	OrderStatusCancelled         OrderStatus = "CANCELLED"

	// OrchestrationAcceptedStatus is the immediate create-order acknowledgement for
	// an accepted orchestration request. It is distinct from the internal accepted
	// order lifecycle state and from the later terminal order response statuses.
	OrchestrationAcceptedStatus = "SAGA_STARTED"
)

type OrderItemRequest struct {
	ProductID   string      `json:"productId"`
	ProductName string      `json:"productName"`
	Quantity    int         `json:"quantity"`
	Price       json.Number `json:"price"`
}

func (i OrderItemRequest) Validate() error {
	if err := validate.NonBlank(i.ProductID, "productId"); err != nil {
		return err
	}
	if err := validate.NonBlank(i.ProductName, "productName"); err != nil {
		return err
	}
	if err := validate.PositiveInt(i.Quantity, "quantity"); err != nil {
		return err
	}
	if err := validate.PositiveNumber(i.Price, "price"); err != nil {
		return err
	}
	return nil
}

type ChoreographyCreateOrderRequest struct {
	CustomerID      string             `json:"customerId"`
	ShippingAddress string             `json:"shippingAddress"`
	Items           []OrderItemRequest `json:"items"`
}

func (r ChoreographyCreateOrderRequest) Validate() error {
	if err := validate.NonBlank(r.CustomerID, "customerId"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.ShippingAddress, "shippingAddress"); err != nil {
		return err
	}
	if len(r.Items) == 0 {
		return fmt.Errorf("items cannot be empty")
	}
	for idx, item := range r.Items {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("items[%d]: %w", idx, err)
		}
	}
	return nil
}

func DecodeChoreographyCreateOrderRequest(r io.Reader) (ChoreographyCreateOrderRequest, error) {
	var request ChoreographyCreateOrderRequest
	if err := validate.DecodeStrictJSON(r, &request); err != nil {
		return ChoreographyCreateOrderRequest{}, err
	}
	if err := request.Validate(); err != nil {
		return ChoreographyCreateOrderRequest{}, err
	}
	return request, nil
}

type OrchestrationCreateOrderRequest struct {
	CustomerID      string             `json:"customerId"`
	TotalAmount     json.Number        `json:"totalAmount"`
	ShippingAddress string             `json:"shippingAddress"`
	Items           []OrderItemRequest `json:"items"`
}

func (r OrchestrationCreateOrderRequest) Validate() error {
	if err := validate.NonBlank(r.CustomerID, "customerId"); err != nil {
		return err
	}
	if err := validate.PositiveNumber(r.TotalAmount, "totalAmount"); err != nil {
		return err
	}
	if err := validate.NonBlank(r.ShippingAddress, "shippingAddress"); err != nil {
		return err
	}
	if len(r.Items) == 0 {
		return fmt.Errorf("items cannot be empty")
	}
	for idx, item := range r.Items {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("items[%d]: %w", idx, err)
		}
	}
	return nil
}

func DecodeOrchestrationCreateOrderRequest(r io.Reader) (OrchestrationCreateOrderRequest, error) {
	var request OrchestrationCreateOrderRequest
	if err := validate.DecodeStrictJSON(r, &request); err != nil {
		return OrchestrationCreateOrderRequest{}, err
	}
	if err := request.Validate(); err != nil {
		return OrchestrationCreateOrderRequest{}, err
	}
	return request, nil
}

type OrderItemResponse struct {
	ProductID   string      `json:"productId"`
	ProductName string      `json:"productName"`
	Quantity    int         `json:"quantity"`
	Price       json.Number `json:"price"`
}

type OrderResponse struct {
	OrderID          string              `json:"orderId"`
	CustomerID       string              `json:"customerId"`
	ShippingAddress  string              `json:"shippingAddress"`
	Status           string              `json:"status"`
	Items            []OrderItemResponse `json:"items"`
	TotalAmount      json.Number         `json:"totalAmount"`
	PaymentID        string              `json:"paymentId,omitempty"`
	ReservationID    string              `json:"reservationId,omitempty"`
	ShippingID       string              `json:"shippingId,omitempty"`
	TrackingNumber   string              `json:"trackingNumber,omitempty"`
	FailureReason    string              `json:"failureReason,omitempty"`
	FailureStep      string              `json:"failureStep,omitempty"`
	CompensatedSteps []string            `json:"compensatedSteps,omitempty"`
	CurrentStep      string              `json:"currentStep,omitempty"`
	CompletedSteps   []string            `json:"completedSteps,omitempty"`
	CreatedAt        time.Time           `json:"createdAt"`
	UpdatedAt        time.Time           `json:"updatedAt"`
}

type OrchestrationCreateOrderAcceptedResponse struct {
	OrderID     string      `json:"orderId"`
	CustomerID  string      `json:"customerId"`
	TotalAmount json.Number `json:"totalAmount"`
	Status      string      `json:"status"`
}

func NewOrchestrationCreateOrderAcceptedResponse(orderID string, request OrchestrationCreateOrderRequest) OrchestrationCreateOrderAcceptedResponse {
	return OrchestrationCreateOrderAcceptedResponse{
		OrderID:     orderID,
		CustomerID:  request.CustomerID,
		TotalAmount: request.TotalAmount,
		Status:      OrchestrationAcceptedStatus,
	}
}
