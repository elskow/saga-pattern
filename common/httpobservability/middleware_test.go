package httpobservability_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/httpobservability"
)

func TestWrapLogsRequestSummaryAndPreservesResponse(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	handler := httpobservability.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := commoncontext.Current(r.Context())
		if data.RequestID != "request-1" {
			t.Fatalf("context RequestID = %q, want request-1", data.RequestID)
		}
		if data.CorrelationID != "correlation-1" {
			t.Fatalf("context CorrelationID = %q, want correlation-1", data.CorrelationID)
		}
		if data.BenchmarkRun != "run-1" {
			t.Fatalf("context BenchmarkRun = %q, want run-1", data.BenchmarkRun)
		}
		if got := w.Header().Get(commoncontext.HeaderRequestID); got != "request-1" {
			t.Fatalf("response header before write = %q, want request-1", got)
		}
		w.Header().Set("X-Test", "ok")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "created")
	}), httpobservability.Options{ServiceName: "checkout-service", Pattern: "orchestration", Logger: logger})

	request := httptest.NewRequest(http.MethodPost, "/api/orders?debug=true", nil)
	request.Header.Set(commoncontext.HeaderRequestID, "request-1")
	request.Header.Set(commoncontext.HeaderCorrelationID, "correlation-1")
	request.Header.Set(commoncontext.HeaderBenchmarkRun, "run-1")
	request.Header.Set(commoncontext.HeaderBenchmarkScene, "happy-path")
	request.Header.Set(commoncontext.HeaderBenchmarkPhase, "create")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusCreated)
	}
	if body := response.Body.String(); body != "created" {
		t.Fatalf("body = %q, want created", body)
	}
	if got := response.Header().Get("X-Test"); got != "ok" {
		t.Fatalf("X-Test = %q, want ok", got)
	}
	if got := response.Header().Get(commoncontext.HeaderRequestID); got != "request-1" {
		t.Fatalf("%s = %q, want request-1", commoncontext.HeaderRequestID, got)
	}

	logLine := logs.String()
	for _, want := range []string{
		"http request completed",
		"service=checkout-service",
		"pattern=orchestration",
		"method=POST",
		"route=/api/orders",
		"path=/api/orders",
		"status=201",
		"bytes=7",
		"request_id=request-1",
		"correlation_id=correlation-1",
		"benchmark_run_label=run-1",
		"benchmark_scenario=happy-path",
		"benchmark_phase=create",
	} {
		if !strings.Contains(logLine, want) {
			t.Fatalf("log line missing %q: %s", want, logLine)
		}
	}
	if strings.Contains(logLine, "debug=true") {
		t.Fatalf("log line includes query string: %s", logLine)
	}
}

func TestWrapGeneratesMissingRequestID(t *testing.T) {
	handler := httpobservability.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := commoncontext.Current(r.Context())
		if data.RequestID == "" {
			t.Fatal("handler context RequestID is empty")
		}
		if got := w.Header().Get(commoncontext.HeaderRequestID); got != data.RequestID {
			t.Fatalf("response request id = %q, want %q", got, data.RequestID)
		}
		w.WriteHeader(http.StatusNoContent)
	}), httpobservability.Options{ServiceName: "checkout-service", Pattern: "orchestration", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/orders", nil))

	if got := response.Header().Get(commoncontext.HeaderRequestID); got == "" {
		t.Fatal("response request id is empty")
	}
}

func TestWrapDefaultsStatusToOK(t *testing.T) {
	var logs bytes.Buffer
	handler := httpobservability.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), httpobservability.Options{
		ServiceName: "inventory-service",
		Pattern:     "choreography",
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
	})

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/actuator/health", nil))

	if logLine := logs.String(); !strings.Contains(logLine, "status=200") {
		t.Fatalf("log line missing default 200 status: %s", logLine)
	}
}

func TestWrapAnnotatesCurrentSpan(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	defer func() { _ = provider.Shutdown(t.Context()) }()

	ctx, span := provider.Tracer("test").Start(t.Context(), "incoming")
	handler := httpobservability.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}), httpobservability.Options{ServiceName: "order-service", Pattern: "orchestration", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})

	request := httptest.NewRequest(http.MethodPatch, "/api/orders/123", nil).WithContext(ctx)
	request.Header.Set(commoncontext.HeaderRequestID, "request-2")
	request.Header.Set(commoncontext.HeaderCorrelationID, "correlation-2")
	request.Header.Set(commoncontext.HeaderBenchmarkRun, "run-2")
	request.Header.Set(commoncontext.HeaderBenchmarkScene, "failure")
	request.Header.Set(commoncontext.HeaderBenchmarkPhase, "poll")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	span.End()

	spans := spanRecorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	attrs := map[attribute.Key]attribute.Value{}
	for _, attr := range spans[0].Attributes() {
		attrs[attr.Key] = attr.Value
	}
	for key, want := range map[attribute.Key]string{
		"service.name":        "order-service",
		"saga.pattern":        "orchestration",
		"http.request.method": "PATCH",
		"http.route":          "/api/orders/123",
		"request.id":          "request-2",
		"correlation.id":      "correlation-2",
		"benchmark.run_label": "run-2",
		"benchmark.scenario":  "failure",
		"benchmark.phase":     "poll",
	} {
		if got := attrs[key].AsString(); got != want {
			t.Fatalf("attribute %s = %q, want %q", key, got, want)
		}
	}
	if got := attrs["http.response.status_code"].AsInt64(); got != http.StatusAccepted {
		t.Fatalf("status attribute = %d, want %d", got, http.StatusAccepted)
	}
	if len(spans[0].Events()) != 1 || spans[0].Events()[0].Name != "http.request.completed" {
		t.Fatalf("events = %#v, want http.request.completed", spans[0].Events())
	}
}
