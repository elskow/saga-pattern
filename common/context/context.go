package context

import (
	stdcontext "context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
)

const (
	HeaderOrderID        = "X-Order-Id"
	HeaderCorrelationID  = "X-Correlation-Id"
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderSagaID         = "X-Saga-Id"
	HeaderSagaType       = "X-Saga-Type"
)

type Data struct {
	OrderID       string
	CorrelationID string
	SagaID        string
	SagaType      string
}

type contextKey struct{}

func ForChoreography(orderID string, correlationID string) Data {
	return normalize(Data{OrderID: orderID, CorrelationID: correlationID})
}

func ForOrchestration(orderID string, correlationID string, sagaID string, sagaType string) Data {
	return normalize(Data{OrderID: orderID, CorrelationID: correlationID, SagaID: sagaID, SagaType: sagaType})
}

func With(parent stdcontext.Context, data Data) stdcontext.Context {
	return stdcontext.WithValue(parent, contextKey{}, normalize(data))
}

func From(ctx stdcontext.Context) (Data, bool) {
	data, ok := ctx.Value(contextKey{}).(Data)
	if !ok {
		return Data{}, false
	}
	return normalize(data), true
}

func Current(ctx stdcontext.Context) Data {
	if data, ok := From(ctx); ok {
		return data
	}
	return normalize(Data{})
}

func ResolveCorrelationID(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return NewID()
}

func ResolveIdempotencyKey(value string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return NewID()
}

func NewID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "generated-id-unavailable"
	}
	return hex.EncodeToString(buf)
}

func ApplyHeaders(headers http.Header, data Data, idempotencyKey string) {
	data = normalize(data)
	headers.Set(HeaderOrderID, data.OrderID)
	headers.Set(HeaderCorrelationID, data.CorrelationID)
	if data.SagaID != "" {
		headers.Set(HeaderSagaID, data.SagaID)
	}
	if data.SagaType != "" {
		headers.Set(HeaderSagaType, data.SagaType)
	}
	if key := ResolveIdempotencyKey(idempotencyKey); key != "" {
		headers.Set(HeaderIdempotencyKey, key)
	}
}

func FromHeaders(headers http.Header) Data {
	return normalize(Data{
		OrderID:       headers.Get(HeaderOrderID),
		CorrelationID: headers.Get(HeaderCorrelationID),
		SagaID:        headers.Get(HeaderSagaID),
		SagaType:      headers.Get(HeaderSagaType),
	})
}

func normalize(data Data) Data {
	if strings.TrimSpace(data.OrderID) == "" {
		data.OrderID = "unknown"
	}
	data.CorrelationID = ResolveCorrelationID(data.CorrelationID)
	return data
}
