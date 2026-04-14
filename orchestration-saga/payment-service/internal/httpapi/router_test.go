package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/common/httpcompat"
	serviceconfig "saga-pattern/orchestration-saga/payment-service/internal/config"
	"saga-pattern/orchestration-saga/payment-service/internal/observability"
)

func TestHandlerExposesHealthAndPrometheus(t *testing.T) {
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	handler := NewHandler(HandlerDependencies{
		Config:   serviceconfig.Config{HealthPath: "/actuator/health", PrometheusPath: "/actuator/prometheus"},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Registry: metrics.Registry(),
		HealthProvider: func(_ context.Context) httpcompat.HealthResponse {
			return httpcompat.HealthResponse{Status: httpcompat.StatusUp}
		},
	})

	healthResp := httptest.NewRecorder()
	handler.ServeHTTP(healthResp, httptest.NewRequest(http.MethodGet, "/actuator/health", nil))
	if healthResp.Code != http.StatusOK {
		t.Fatalf("health status = %d", healthResp.Code)
	}
	var healthBody map[string]any
	if err := json.Unmarshal(healthResp.Body.Bytes(), &healthBody); err != nil {
		t.Fatalf("decode health body: %v", err)
	}
	if healthBody["status"] != "UP" {
		t.Fatalf("health body = %s", healthResp.Body.String())
	}

	promResp := httptest.NewRecorder()
	handler.ServeHTTP(promResp, httptest.NewRequest(http.MethodGet, "/actuator/prometheus", nil))
	if promResp.Code != http.StatusOK {
		t.Fatalf("prometheus status = %d", promResp.Code)
	}
	for _, metricName := range []string{"saga_step_payment_duration_seconds", "saga_compensations_payment_total"} {
		if !strings.Contains(promResp.Body.String(), metricName) {
			t.Fatalf("missing metric %s", metricName)
		}
	}
}
