package repository

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"saga-pattern/choreography-saga/order-service/internal/domain"
	"saga-pattern/common/dto"
	"saga-pattern/common/testutil"
)

func TestCreateIfAbsentHandlesConcurrentSameIdempotencyKey(t *testing.T) {
	db := testutil.OpenPostgres(t, testutil.DefaultChoreographyOrderDatabaseURL, "choreography_order_repository_test", testutil.Migration{Scope: "choreography-order-service", Dir: "choreography-saga/order-service/db/migrations"})
	repo, err := NewPostgresRepository(db)
	if err != nil {
		t.Fatalf("new postgres repository: %v", err)
	}

	now := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
	orders := []domain.Order{
		newOrderForTest(t, "ORDER-1", "IDEM-CONCURRENT", now),
		newOrderForTest(t, "ORDER-2", "IDEM-CONCURRENT", now),
	}

	var wg sync.WaitGroup
	results := make(chan struct {
		order   domain.Order
		created bool
		err     error
	}, len(orders))
	for _, order := range orders {
		order := order
		wg.Add(1)
		go func() {
			defer wg.Done()
			stored, created, err := repo.CreateIfAbsent(context.Background(), order.IdempotencyKey, order, nil)
			results <- struct {
				order   domain.Order
				created bool
				err     error
			}{order: stored, created: created, err: err}
		}()
	}
	wg.Wait()
	close(results)

	createdCount := 0
	storedIDs := map[string]bool{}
	for result := range results {
		if result.err != nil {
			t.Fatalf("create if absent: %v", result.err)
		}
		if result.created {
			createdCount++
		}
		storedIDs[result.order.OrderID] = true
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}
	if len(storedIDs) != 1 {
		t.Fatalf("stored order ids = %v, want one stable id", storedIDs)
	}
}

func newOrderForTest(t *testing.T, orderID, idempotencyKey string, now time.Time) domain.Order {
	t.Helper()
	request := dto.ChoreographyCreateOrderRequest{
		CustomerID:      "CUST-001",
		ShippingAddress: "Jl. Ketintang Wiyata, Surabaya 60231",
		Items: []dto.OrderItemRequest{{
			ProductID:   "PROD-001",
			ProductName: "Widget",
			Quantity:    1,
			Price:       json.Number("799000"),
		}},
	}
	order, err := domain.NewOrderFromRequest(orderID, request, idempotencyKey, now)
	if err != nil {
		t.Fatalf("new order: %v", err)
	}
	return order
}
