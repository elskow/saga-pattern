package context

import (
	stdcontext "context"
	"net/http"
	"testing"
)

func TestRequestIDResolutionAndContextStorage(t *testing.T) {
	if got := ResolveRequestID("", "request-1"); got != "request-1" {
		t.Fatalf("ResolveRequestID returned %q, want request-1", got)
	}
	if got := ResolveRequestID("", ""); got == "" {
		t.Fatal("ResolveRequestID returned empty generated id")
	}

	ctx := With(stdcontext.Background(), Data{OrderID: "order-1", RequestID: "request-2", CorrelationID: "correlation-1"})
	data, ok := From(ctx)
	if !ok {
		t.Fatal("From did not find context data")
	}
	if data.RequestID != "request-2" {
		t.Fatalf("RequestID = %q, want request-2", data.RequestID)
	}
	if data.CorrelationID != "correlation-1" {
		t.Fatalf("CorrelationID = %q, want correlation-1", data.CorrelationID)
	}
}

func TestApplyHeadersIncludesRequestIDAndBenchmarkMetadataWithoutChangingIdempotencyBehavior(t *testing.T) {
	headers := http.Header{}
	ApplyHeaders(headers, Data{OrderID: "order-1", RequestID: "request-1", CorrelationID: "correlation-1", BenchmarkRun: "run-1", BenchmarkScene: "failure", BenchmarkPhase: "create"}, "idempotency-1")

	if got := headers.Get(HeaderOrderID); got != "order-1" {
		t.Fatalf("%s = %q, want order-1", HeaderOrderID, got)
	}
	if got := headers.Get(HeaderRequestID); got != "request-1" {
		t.Fatalf("%s = %q, want request-1", HeaderRequestID, got)
	}
	if got := headers.Get(HeaderCorrelationID); got != "correlation-1" {
		t.Fatalf("%s = %q, want correlation-1", HeaderCorrelationID, got)
	}
	if got := headers.Get(HeaderBenchmarkRun); got != "run-1" {
		t.Fatalf("%s = %q, want run-1", HeaderBenchmarkRun, got)
	}
	if got := headers.Get(HeaderBenchmarkScene); got != "failure" {
		t.Fatalf("%s = %q, want failure", HeaderBenchmarkScene, got)
	}
	if got := headers.Get(HeaderBenchmarkPhase); got != "create" {
		t.Fatalf("%s = %q, want create", HeaderBenchmarkPhase, got)
	}
	if got := headers.Get(HeaderIdempotencyKey); got != "idempotency-1" {
		t.Fatalf("%s = %q, want idempotency-1", HeaderIdempotencyKey, got)
	}
}

func TestApplyHeadersDoesNotGenerateRequestID(t *testing.T) {
	headers := http.Header{}
	ApplyHeaders(headers, Data{OrderID: "order-1", CorrelationID: "correlation-1"}, "idempotency-1")

	if got := headers.Get(HeaderRequestID); got != "" {
		t.Fatalf("%s = %q, want empty", HeaderRequestID, got)
	}
	if got := headers.Get(HeaderIdempotencyKey); got != "idempotency-1" {
		t.Fatalf("%s = %q, want idempotency-1", HeaderIdempotencyKey, got)
	}
}

func TestFromHeadersExtractsRequestIDAndPreservesCorrelationResolution(t *testing.T) {
	headers := http.Header{}
	headers.Set(HeaderOrderID, "order-1")
	headers.Set(HeaderRequestID, " request-1 ")
	headers.Set(HeaderCorrelationID, "correlation-1")
	headers.Set(HeaderSagaID, "saga-1")
	headers.Set(HeaderSagaType, "orchestration")
	headers.Set(HeaderBenchmarkRun, " run-1 ")
	headers.Set(HeaderBenchmarkScene, "failure")
	headers.Set(HeaderBenchmarkPhase, "create")

	data := FromHeaders(headers)

	if data.OrderID != "order-1" {
		t.Fatalf("OrderID = %q, want order-1", data.OrderID)
	}
	if data.RequestID != "request-1" {
		t.Fatalf("RequestID = %q, want request-1", data.RequestID)
	}
	if data.CorrelationID != "correlation-1" {
		t.Fatalf("CorrelationID = %q, want correlation-1", data.CorrelationID)
	}
	if data.SagaID != "saga-1" {
		t.Fatalf("SagaID = %q, want saga-1", data.SagaID)
	}
	if data.SagaType != "orchestration" {
		t.Fatalf("SagaType = %q, want orchestration", data.SagaType)
	}
	if data.BenchmarkRun != "run-1" {
		t.Fatalf("BenchmarkRun = %q, want run-1", data.BenchmarkRun)
	}
	if data.BenchmarkScene != "failure" {
		t.Fatalf("BenchmarkScene = %q, want failure", data.BenchmarkScene)
	}
	if data.BenchmarkPhase != "create" {
		t.Fatalf("BenchmarkPhase = %q, want create", data.BenchmarkPhase)
	}
}
