package messaging

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/events"
	commonkafka "saga-pattern/common/kafka"
)

type recordingHandler struct {
	calls int
	data  commoncontext.Data
}

func (h *recordingHandler) HandleEvent(ctx context.Context, _ events.ChoreographyEvent) error {
	h.calls++
	h.data, _ = commoncontext.From(ctx)
	return nil
}

func TestDownstreamConsumerPassesEventMetadataToHandlerContext(t *testing.T) {
	handler := &recordingHandler{}
	consumer, err := NewDownstreamConsumer(handler, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new downstream consumer: %v", err)
	}
	now := time.Now().UTC()
	payload, err := json.Marshal(events.NewInventoryReservedEvent("RES-1", "ORDER-1", []events.InventoryReservedItem{{ProductID: "PROD-1", Quantity: 1}}, now, "corr-1", now))
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	ctx := commoncontext.With(context.Background(), commoncontext.Data{RequestID: "request-1", CorrelationID: "http-corr"})

	if err := consumer.Consume(ctx, DownstreamEnvelope{Topic: commonkafka.DefaultInventoryEventsTopic, Key: "ORDER-1", Value: payload}); err != nil {
		t.Fatalf("consume event: %v", err)
	}
	if handler.data.RequestID != "request-1" || handler.data.OrderID != "ORDER-1" || handler.data.CorrelationID != "corr-1" {
		t.Fatalf("handler context = %#v", handler.data)
	}
}

func TestDownstreamConsumerIgnoresKnownTopicMismatches(t *testing.T) {
	handler := &recordingHandler{}
	consumer, err := NewDownstreamConsumer(handler, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new downstream consumer: %v", err)
	}
	now := time.Now().UTC()
	payload, err := json.Marshal(events.NewInventoryReservationFailedEvent("ORDER-1", "PROD-1", "no stock", now, "corr-1", now))
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	err = consumer.Consume(context.Background(), DownstreamEnvelope{Topic: commonkafka.DefaultInventoryEventsTopic, Key: "ORDER-1", Value: payload})
	if err != nil {
		t.Fatalf("mismatch error = %v, want nil", err)
	}
	if handler.calls != 0 {
		t.Fatalf("handler calls = %d, want 0", handler.calls)
	}
}
