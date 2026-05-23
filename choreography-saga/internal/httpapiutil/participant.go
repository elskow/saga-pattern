package httpapiutil

import (
	"context"
	"io"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	commonconfig "saga-pattern/common/config"
	"saga-pattern/common/httpcompat"
)

type ParticipantHandlerDependencies struct {
	Config         commonconfig.ServiceConfig
	Logger         *slog.Logger
	Registry       prometheus.Gatherer
	HealthProvider func(context.Context) httpcompat.HealthResponse
}

func NewParticipantHandler(deps ParticipantHandlerDependencies) http.Handler {
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	router := chi.NewRouter()
	router.Handle(deps.Config.HealthPath, httpcompat.NewHealthHandler(func(r *http.Request) httpcompat.HealthResponse {
		if deps.HealthProvider == nil {
			return httpcompat.HealthResponse{Status: httpcompat.StatusUp}
		}
		return deps.HealthProvider(r.Context())
	}))
	router.Handle(deps.Config.PrometheusPath, httpcompat.NewPrometheusHandler(deps.Registry, promhttp.HandlerOpts{}))
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
		logger.Debug("route not found", "path", r.URL.Path, "method", r.Method)
	})

	return router
}
