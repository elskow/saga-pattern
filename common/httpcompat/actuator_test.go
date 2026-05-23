package httpcompat_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"saga-pattern/common/httpcompat"
)

func TestActuatorCompatibilityHandlers(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_metric_total", Help: "test metric"})
	registry.MustRegister(counter)
	counter.Inc()

	mux := http.NewServeMux()
	mux.Handle(httpcompat.HealthPath, httpcompat.NewStaticHealthHandler(httpcompat.StatusUp))
	mux.Handle(httpcompat.PrometheusPath, httpcompat.NewPrometheusHandler(registry, promhttp.HandlerOpts{}))

	server := httptest.NewServer(mux)
	defer server.Close()

	healthResp, err := http.Get(server.URL + httpcompat.HealthPath)
	if err != nil {
		t.Fatalf("GET health: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", healthResp.StatusCode)
	}
	if got := healthResp.Header.Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("health content type = %q", got)
	}
	healthBody, _ := io.ReadAll(healthResp.Body)
	if !strings.Contains(string(healthBody), `"status":"UP"`) {
		t.Fatalf("unexpected health body: %s", healthBody)
	}

	metricsResp, err := http.Get(server.URL + httpcompat.PrometheusPath)
	if err != nil {
		t.Fatalf("GET metrics: %v", err)
	}
	defer metricsResp.Body.Close()
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d", metricsResp.StatusCode)
	}
	metricsBody, _ := io.ReadAll(metricsResp.Body)
	if !strings.Contains(string(metricsBody), "test_metric_total 1") {
		t.Fatalf("unexpected metrics body: %s", metricsBody)
	}
	if !strings.Contains(string(metricsBody), "process_cpu_seconds_total") {
		t.Fatalf("metrics body missing process collector: %s", metricsBody)
	}
	if !strings.Contains(string(metricsBody), "go_goroutines") {
		t.Fatalf("metrics body missing Go collector: %s", metricsBody)
	}
}
