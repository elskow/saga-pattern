package commands

import (
	"encoding/json"
	"testing"

	"saga-pattern/common/dto"
)

func TestProcessPaymentCommandValidateAcceptsMissingItems(t *testing.T) {
	command := NewProcessPaymentCommand("PAY-1", "ORDER-1", "CUST-1", json.Number("1599000"), nil)

	if err := command.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestProcessPaymentCommandValidateAcceptsItems(t *testing.T) {
	command := NewProcessPaymentCommand("PAY-1", "ORDER-1", "CUST-1", json.Number("1599000"), []dto.OrderItemRequest{{
		ProductID:   "PROD-001",
		ProductName: "Widget",
		Quantity:    1,
		Price:       json.Number("1599000"),
	}})

	if err := command.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
