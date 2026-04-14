package httpcompat

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	HealthPath     = "/actuator/health"
	PrometheusPath = "/actuator/prometheus"
	StatusUp       = "UP"
)

type HealthComponent struct {
	Status     string                     `json:"status"`
	Details    map[string]any             `json:"details,omitempty"`
	Components map[string]HealthComponent `json:"components,omitempty"`
}

type HealthResponse struct {
	Status     string                     `json:"status"`
	Components map[string]HealthComponent `json:"components,omitempty"`
}

func NewStaticHealthHandler(status string) http.Handler {
	if status == "" {
		status = StatusUp
	}
	return NewHealthHandler(func(*http.Request) HealthResponse {
		return HealthResponse{Status: status}
	})
}

func NewHealthHandler(provider func(*http.Request) HealthResponse) http.Handler {
	if provider == nil {
		provider = func(*http.Request) HealthResponse { return HealthResponse{Status: StatusUp} }
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := provider(r)
		if response.Status == "" {
			response.Status = StatusUp
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
}

func NewPrometheusHandler(reg prometheus.Gatherer, opts promhttp.HandlerOpts) http.Handler {
	if reg == nil {
		return promhttp.Handler()
	}
	return promhttp.HandlerFor(reg, opts)
}
