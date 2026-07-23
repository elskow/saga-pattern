package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	commonconfig "saga-pattern/common/config"
	commoncontext "saga-pattern/common/context"
	"saga-pattern/common/dto"
	"saga-pattern/common/httpcompat"
	"saga-pattern/common/inventorycatalog"
	ordersvc "saga-pattern/orchestration-saga/order-service/internal/orders"
)

type HandlerDependencies struct {
	Config                commonconfig.ServiceConfig
	Logger                *slog.Logger
	Orders                OrderService
	Registry              prometheus.Gatherer
	SagaTimeoutConfigurer SagaTimeoutConfigurer
}

type OrderService interface {
	CreateOrder(context.Context, dto.OrchestrationCreateOrderRequest, string) (dto.OrchestrationCreateOrderAcceptedResponse, error)
	GetOrder(context.Context, string) (dto.OrderResponse, error)
	ListOrders(context.Context) ([]dto.OrderResponse, error)
	ListOrdersByCustomer(context.Context, string) ([]dto.OrderResponse, error)
	CancelOrder(context.Context, string) error
}

type SagaTimeoutConfigurer interface {
	SetSagaTimeout(time.Duration)
	GetSagaTimeout() time.Duration
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
	router.Get("/api/orders/customer/{customerId}", getOrdersByCustomerHandler(logger, deps.Orders))
	router.Post("/api/orders/{orderId}/cancel", cancelOrderHandler(logger, deps.Orders))
	router.Get("/api/orders", listOrdersHandler(logger, deps.Orders))
	router.Put("/api/admin/saga-timeout", sagaTimeoutHandler(logger, deps.SagaTimeoutConfigurer))
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		if !httpcompat.IsOpsProbePath(r.URL.Path) {
			logger.Debug("route not found", "path", r.URL.Path, "method", r.Method)
		}
	})

	return router
}

func createOrderHandler(logger *slog.Logger, service OrderService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if service == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order service unavailable"})
			return
		}
		request, err := dto.DecodeOrchestrationCreateOrderRequest(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		response, err := service.CreateOrder(r.Context(), request, strings.TrimSpace(r.Header.Get(commoncontext.HeaderIdempotencyKey)))
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
		writeJSON(w, http.StatusAccepted, response)
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

func getOrdersByCustomerHandler(logger *slog.Logger, service OrderService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if service == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order service unavailable"})
			return
		}
		responses, err := service.ListOrdersByCustomer(r.Context(), chi.URLParam(r, "customerId"))
		if err != nil {
			logger.Error("list orders by customer", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, responses)
	}
}

func listOrdersHandler(logger *slog.Logger, service OrderService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if service == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order service unavailable"})
			return
		}
		responses, err := service.ListOrders(r.Context())
		if err != nil {
			logger.Error("list orders", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, responses)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func sagaTimeoutHandler(logger *slog.Logger, configurer SagaTimeoutConfigurer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if configurer == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "saga timeout configurer unavailable"})
			return
		}
		var body struct {
			TimeoutMs *int64 `json:"timeoutMs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
		if body.TimeoutMs == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "timeoutMs is required"})
			return
		}
		if *body.TimeoutMs <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "timeoutMs must be positive"})
			return
		}
		configurer.SetSagaTimeout(time.Duration(*body.TimeoutMs) * time.Millisecond)
		logger.Info("saga timeout config updated", "timeoutMs", configurer.GetSagaTimeout().Milliseconds())
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
