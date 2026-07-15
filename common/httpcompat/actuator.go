package httpcompat

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	HealthPath     = "/actuator/health"
	PrometheusPath = "/actuator/prometheus"
	StatusUp       = "UP"
	StatusDown     = "DOWN"
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

// NewDBPingHealthProvider returns a HealthProvider that pings the database
// on each health check request. Returns UP if ping succeeds, DOWN otherwise.
func NewDBPingHealthProvider(db *sql.DB) func(context.Context) HealthResponse {
	return func(ctx context.Context) HealthResponse {
		if err := db.PingContext(ctx); err != nil {
			return HealthResponse{
				Status: StatusDown,
				Components: map[string]HealthComponent{
					"db": {Status: StatusDown, Details: map[string]any{"error": err.Error()}},
				},
			}
		}
		return HealthResponse{
			Status: StatusUp,
			Components: map[string]HealthComponent{
				"db": {Status: StatusUp},
			},
		}
	}
}

// NewDBAndKafkaHealthProvider returns a HealthProvider that pings the database
// and reports Kafka broker connectivity status.
func NewDBAndKafkaHealthProvider(db *sql.DB, kafkaBrokers string) func(context.Context) HealthResponse {
	return func(ctx context.Context) HealthResponse {
		response := HealthResponse{
			Status: StatusUp,
			Components: map[string]HealthComponent{
				"db":    {Status: StatusUp},
				"kafka": {Status: StatusUp, Details: map[string]any{"brokers": kafkaBrokers}},
			},
		}
		if err := db.PingContext(ctx); err != nil {
			response.Status = StatusDown
			response.Components["db"] = HealthComponent{Status: StatusDown, Details: map[string]any{"error": err.Error()}}
		}
		return response
	}
}

func NewPrometheusHandler(reg prometheus.Gatherer, opts promhttp.HandlerOpts) http.Handler {
	if reg == nil {
		return promhttp.Handler()
	}
	runtimeRegistry := prometheus.NewRegistry()
	runtimeRegistry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return promhttp.HandlerFor(prometheus.Gatherers{reg, runtimeRegistry}, opts)
}
