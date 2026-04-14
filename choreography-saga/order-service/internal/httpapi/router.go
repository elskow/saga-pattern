package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	ordersvc "saga-pattern/choreography-saga/order-service/internal/orders"
	commonconfig "saga-pattern/common/config"
	"saga-pattern/common/dto"
	"saga-pattern/common/httpcompat"
)

const idempotencyHeader = "X-Idempotency-Key"

type OrderService interface {
	CreateOrder(context.Context, dto.ChoreographyCreateOrderRequest, string) (dto.OrderResponse, bool, error)
	GetOrder(context.Context, string) (dto.OrderResponse, error)
}

type HandlerDependencies struct {
	Config   commonconfig.ServiceConfig
	Logger   *slog.Logger
	Orders   OrderService
	Registry prometheus.Gatherer
}

func NewHandler(deps HandlerDependencies) http.Handler {
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	router := chi.NewRouter()
	router.Handle(deps.Config.HealthPath, httpcompat.NewStaticHealthHandler(httpcompat.StatusUp))
	router.Handle(deps.Config.PrometheusPath, httpcompat.NewPrometheusHandler(deps.Registry, promhttp.HandlerOpts{}))
	router.Post("/api/orders", createOrderHandler(logger, deps.Orders))
	router.Get("/api/orders/{orderId}", getOrderHandler(logger, deps.Orders))
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		logger.Debug("route not found", "path", r.URL.Path, "method", r.Method)
	})

	return router
}

func createOrderHandler(logger *slog.Logger, service OrderService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if service == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order service unavailable"})
			return
		}

		request, err := dto.DecodeChoreographyCreateOrderRequest(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		response, created, err := service.CreateOrder(r.Context(), request, strings.TrimSpace(r.Header.Get(idempotencyHeader)))
		if err != nil {
			logger.Error("create order", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		writeJSON(w, status, response)
	}
}

func getOrderHandler(logger *slog.Logger, service OrderService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if service == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order service unavailable"})
			return
		}

		response, err := service.GetOrder(r.Context(), chi.URLParam(r, "orderId"))
		if err != nil {
			if errors.Is(err, ordersvc.ErrOrderNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "order not found"})
				return
			}
			logger.Error("get order", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, response)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
