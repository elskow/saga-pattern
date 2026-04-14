package dto_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"saga-pattern/common/dto"
)

func TestOrderPayloadVariants(t *testing.T) {
	t.Parallel()

	choreo := dto.ChoreographyCreateOrderRequest{
		CustomerID:      "CUST-001",
		ShippingAddress: "123 Main Street, City, Country",
		Items: []dto.OrderItemRequest{{
			ProductID:   "PROD-001",
			ProductName: "Sample Product",
			Quantity:    2,
			Price:       json.Number("49.99"),
		}},
	}
	orch := dto.OrchestrationCreateOrderRequest{
		CustomerID:      "CUST-001",
		TotalAmount:     json.Number("99.98"),
		ShippingAddress: "123 Main Street, City, Country",
		Items:           choreo.Items,
	}

	choreoJSON, err := json.Marshal(choreo)
	if err != nil {
		t.Fatalf("marshal choreography request: %v", err)
	}
	orchJSON, err := json.Marshal(orch)
	if err != nil {
		t.Fatalf("marshal orchestration request: %v", err)
	}

	var choreoMap map[string]any
	if err := json.Unmarshal(choreoJSON, &choreoMap); err != nil {
		t.Fatalf("decode choreography request: %v", err)
	}
	if _, ok := choreoMap["totalAmount"]; ok {
		t.Fatalf("choreography request unexpectedly includes totalAmount: %s", choreoJSON)
	}

	var orchMap map[string]any
	if err := json.Unmarshal(orchJSON, &orchMap); err != nil {
		t.Fatalf("decode orchestration request: %v", err)
	}
	if _, ok := orchMap["totalAmount"]; !ok {
		t.Fatalf("orchestration request missing totalAmount: %s", orchJSON)
	}

	response := dto.OrderResponse{
		OrderID:     "ORDER-123",
		CustomerID:  choreo.CustomerID,
		Status:      string(dto.OrderStatusPending),
		Items:       []dto.OrderItemResponse{{ProductID: "PROD-001", ProductName: "Sample Product", Quantity: 2, Price: json.Number("49.99")}},
		TotalAmount: json.Number("99.98"),
		CreatedAt:   time.Unix(1710000000, 0).UTC(),
		UpdatedAt:   time.Unix(1710000060, 0).UTC(),
	}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal order response: %v", err)
	}
	for _, field := range []string{"orderId", "customerId", "status", "items", "totalAmount", "createdAt", "updatedAt"} {
		if !bytes.Contains(responseJSON, []byte("\""+field+"\"")) {
			t.Fatalf("order response missing %s in %s", field, responseJSON)
		}
	}

	accepted := dto.NewOrchestrationCreateOrderAcceptedResponse("ORDER-999", orch)
	if accepted.Status != dto.OrchestrationAcceptedStatus {
		t.Fatalf("accepted response status = %q", accepted.Status)
	}
	acceptedJSON, err := json.Marshal(accepted)
	if err != nil {
		t.Fatalf("marshal accepted response: %v", err)
	}
	if strings.Contains(string(acceptedJSON), "shippingAddress") || strings.Contains(string(acceptedJSON), "items") {
		t.Fatalf("accepted response leaked full order shape: %s", acceptedJSON)
	}
}

func TestRejectsMalformedCreateOrderRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		decode  func(*strings.Reader) error
		payload string
		wantErr string
	}{
		{
			name: "choreography rejects totalAmount asymmetry break",
			decode: func(r *strings.Reader) error {
				_, err := dto.DecodeChoreographyCreateOrderRequest(r)
				return err
			},
			payload: `{"customerId":"CUST-001","totalAmount":99.98,"shippingAddress":"123 Main Street","items":[{"productId":"PROD-001","productName":"Sample Product","quantity":1,"price":49.99}]}`,
			wantErr: "unknown field",
		},
		{
			name: "orchestration requires totalAmount",
			decode: func(r *strings.Reader) error {
				_, err := dto.DecodeOrchestrationCreateOrderRequest(r)
				return err
			},
			payload: `{"customerId":"CUST-001","shippingAddress":"123 Main Street","items":[{"productId":"PROD-001","productName":"Sample Product","quantity":1,"price":49.99}]}`,
			wantErr: "totalAmount",
		},
		{
			name: "rejects blank shipping address",
			decode: func(r *strings.Reader) error {
				_, err := dto.DecodeChoreographyCreateOrderRequest(r)
				return err
			},
			payload: `{"customerId":"CUST-001","shippingAddress":"","items":[{"productId":"PROD-001","productName":"Sample Product","quantity":1,"price":49.99}]}`,
			wantErr: "shippingAddress",
		},
		{
			name: "rejects empty items",
			decode: func(r *strings.Reader) error {
				_, err := dto.DecodeChoreographyCreateOrderRequest(r)
				return err
			},
			payload: `{"customerId":"CUST-001","shippingAddress":"123 Main Street","items":[]}`,
			wantErr: "items",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.decode(strings.NewReader(tt.payload))
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}
