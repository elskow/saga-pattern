package inventorycatalog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"saga-pattern/common/dto"
)

func TestNormalizeOrderItemsNormalizesCatalogData(t *testing.T) {
	client := newCatalogTestClient(t, []CatalogItem{{ProductID: "PROD-001", Name: "Laptop", Price: json.Number("1299.99"), Available: 3}})

	items, total, err := client.NormalizeOrderItems(context.Background(), []dto.OrderItemRequest{{ProductID: "PROD-001", ProductName: "stale", Quantity: 2, Price: json.Number("1.00")}})
	if err != nil {
		t.Fatalf("NormalizeOrderItems error = %v", err)
	}
	if total != json.Number("2599.98") {
		t.Fatalf("total = %s, want 2599.98", total)
	}
	if got := items[0].ProductName; got != "Laptop" {
		t.Fatalf("product name = %q, want Laptop", got)
	}
	if got := items[0].Price; got != json.Number("1299.99") {
		t.Fatalf("price = %s, want 1299.99", got)
	}
}

func TestNormalizeOrderItemsRejectsInsufficientAvailability(t *testing.T) {
	client := newCatalogTestClient(t, []CatalogItem{{ProductID: "PROD-001", Name: "Laptop", Price: json.Number("1299.99"), Available: 0}})

	_, _, err := client.NormalizeOrderItems(context.Background(), []dto.OrderItemRequest{{ProductID: "PROD-001", Quantity: 1, Price: json.Number("1299.99")}})
	if err == nil {
		t.Fatal("NormalizeOrderItems error = nil, want insufficient availability")
	}
	var insufficient InsufficientAvailabilityError
	if !errors.As(err, &insufficient) {
		t.Fatalf("error = %T %[1]v, want InsufficientAvailabilityError", err)
	}
	if insufficient.ProductID != "PROD-001" || insufficient.Requested != 1 || insufficient.Available != 0 {
		t.Fatalf("insufficient error = %+v", insufficient)
	}
}

func TestNormalizeOrderItemsAggregatesDuplicateProductQuantities(t *testing.T) {
	client := newCatalogTestClient(t, []CatalogItem{{ProductID: "PROD-001", Name: "Laptop", Price: json.Number("10.00"), Available: 3}})

	_, _, err := client.NormalizeOrderItems(context.Background(), []dto.OrderItemRequest{
		{ProductID: "PROD-001", Quantity: 2, Price: json.Number("10.00")},
		{ProductID: "PROD-001", Quantity: 2, Price: json.Number("10.00")},
	})
	if err == nil {
		t.Fatal("NormalizeOrderItems error = nil, want insufficient availability")
	}
	var insufficient InsufficientAvailabilityError
	if !errors.As(err, &insufficient) {
		t.Fatalf("error = %T %[1]v, want InsufficientAvailabilityError", err)
	}
	if insufficient.Requested != 4 || insufficient.Available != 3 {
		t.Fatalf("insufficient error = %+v", insufficient)
	}
}

func newCatalogTestClient(t *testing.T, products []CatalogItem) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/catalog" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(products); err != nil {
			t.Fatalf("encode catalog: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}
