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
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/httpcompat"
	"saga-pattern/common/inventorycatalog"
)

const idempotencyHeader = commoncontext.HeaderIdempotencyKey

type OrderService interface {
	CreateOrder(context.Context, dto.ChoreographyCreateOrderRequest, string) (dto.OrderResponse, bool, error)
	GetOrder(context.Context, string) (dto.OrderResponse, error)
	ListOrders(context.Context) ([]dto.OrderResponse, error)
	CancelOrder(context.Context, string) error
}

type HandlerDependencies struct {
	Config         commonconfig.ServiceConfig
	Logger         *slog.Logger
	Orders         OrderService
	Registry       prometheus.Gatherer
	HealthProvider func(context.Context) httpcompat.HealthResponse
}

func NewHandler(deps HandlerDependencies) http.Handler {
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
	router.Post("/api/orders", createOrderHandler(logger, deps.Orders))
	router.Get("/api/orders", listOrdersHandler(logger, deps.Orders))
	router.Get("/api/orders/{orderId}", getOrderHandler(logger, deps.Orders))
	router.Post("/api/orders/{orderId}/cancel", cancelOrderHandler(logger, deps.Orders))
	router.Put("/api/admin/saga-timeout", sagaTimeoutHandler(logger))
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		logger.Debug("route not found", "path", r.URL.Path, "method", r.Method)
	})

	return router
}

func listOrdersHandler(logger *slog.Logger, service OrderService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if service == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order service unavailable"})
			return
		}
		response, err := service.ListOrders(r.Context())
		if err != nil {
			logger.Error("list orders", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, response)
	}
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
			if errors.Is(err, ordersvc.ErrIdempotencyConflict) {
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
				return
			}
			if inventorycatalog.IsInsufficientAvailability(err) {
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
				return
			}
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

func cancelOrderHandler(logger *slog.Logger, service OrderService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if service == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order service unavailable"})
			return
		}
		err := service.CancelOrder(r.Context(), chi.URLParam(r, "orderId"))
		if err != nil {
			if errors.Is(err, ordersvc.ErrOrderNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "order not found"})
				return
			}
			logger.Error("cancel order", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func sagaTimeoutHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ThresholdMs *int64 `json:"thresholdMs"`
			IntervalMs  *int64 `json:"intervalMs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
		if body.ThresholdMs != nil {
			if *body.ThresholdMs <= 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "thresholdMs must be positive"})
				return
			}
			ordersvc.SagaTimeoutThreshold.Store(*body.ThresholdMs)
		}
		if body.IntervalMs != nil {
			if *body.IntervalMs <= 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "intervalMs must be positive"})
				return
			}
			ordersvc.SagaTimeoutInterval.Store(*body.IntervalMs)
		}
		logger.Info("saga timeout config updated",
			"thresholdMs", ordersvc.SagaTimeoutThreshold.Load(),
			"intervalMs", ordersvc.SagaTimeoutInterval.Load())
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
