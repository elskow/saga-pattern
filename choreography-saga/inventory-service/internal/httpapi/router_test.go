package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"saga-pattern/choreography-saga/inventory-service/internal/observability"
	commonconfig "saga-pattern/common/config"
)

func TestPrometheusEndpointExposesInventoryMetrics(t *testing.T) {
	metrics, err := observability.NewMetrics(nil)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	metrics.RecordInventoryStep(0)
	metrics.RecordInventoryCompensation(0)

	handler := NewHandler(HandlerDependencies{
		Config:   commonconfig.ServiceConfig{HealthPath: "/actuator/health", PrometheusPath: "/actuator/prometheus"},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Registry: metrics.Registry(),
	})

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/actuator/prometheus", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("prometheus status = %d", resp.Code)
	}
	body := resp.Body.String()
	for _, metricName := range []string{"saga_step_inventory_duration_seconds", "saga_compensations_inventory_total"} {
		if !strings.Contains(body, metricName) {
			t.Fatalf("metrics body missing %s", metricName)
		}
	}
}

func TestHealthEndpointReportsUp(t *testing.T) {
	handler := NewHandler(HandlerDependencies{
		Config: commonconfig.ServiceConfig{HealthPath: "/actuator/health", PrometheusPath: "/actuator/prometheus"},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/actuator/health", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("health status = %d", resp.Code)
	}
	if !strings.Contains(resp.Body.String(), "UP") {
		t.Fatalf("health body = %q, want UP status", resp.Body.String())
	}
}
