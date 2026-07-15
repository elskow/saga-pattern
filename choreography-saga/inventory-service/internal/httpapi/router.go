package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/choreography-saga/internal/httpapiutil"
	"saga-pattern/choreography-saga/inventory-service/internal/domain"
	"saga-pattern/choreography-saga/inventory-service/internal/inventory"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
	commonconfig "saga-pattern/common/config"
	"saga-pattern/common/faultinjection"
	"saga-pattern/common/httpcompat"
)

type HandlerDependencies struct {
	Config         commonconfig.ServiceConfig
	Logger         *slog.Logger
	Registry       prometheus.Gatherer
	Repo           repository.Repository
	Service        *inventory.Service
	HealthProvider func(context.Context) httpcompat.HealthResponse
}

func NewHandler(deps HandlerDependencies) http.Handler {
	base := httpapiutil.NewParticipantHandler(httpapiutil.ParticipantHandlerDependencies{
		Config:         deps.Config,
		Logger:         deps.Logger,
		Registry:       deps.Registry,
		HealthProvider: deps.HealthProvider,
	})

	// If no repo provided (e.g. in tests), return the base handler only
	if deps.Repo == nil {
		return base
	}

	// Wrap with a mux that handles the new query endpoints
	return newMuxWithQueryRoutes(base, deps, deps.Service)
}

func newMuxWithQueryRoutes(base http.Handler, deps HandlerDependencies, svc failureModeTogglable) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/catalog", listCatalogHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("GET /api/products", listProductsHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("PATCH /api/products/{productId}/stock", updateStockHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("PATCH /api/products/{productId}/visibility", updateVisibilityHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("POST /api/products", createProductHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("PATCH /api/products/{productId}", updateProductMetaHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("DELETE /api/products/{productId}", deleteProductHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("GET /api/reservations", listReservationsHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("GET /api/admin/failure-mode", getFailureModeHandler(svc))
	mux.HandleFunc("PUT /api/admin/failure-mode", putFailureModeHandler(deps.Logger, svc))
	mux.HandleFunc("GET /api/admin/delay", getDelayHandler())
	mux.HandleFunc("PUT /api/admin/delay", putDelayHandler(deps.Logger))

	// Fall through everything else to the base handler
	mux.Handle("/", base)
	return mux
}

func listCatalogHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		products, err := repo.ListProducts(r.Context())
		if err != nil {
			logger.Error("list catalog", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		items := make([]map[string]any, 0, len(products))
		for _, product := range products {
			if !product.Visible {
				continue
			}
			items = append(items, map[string]any{
				"productId":   product.ProductID,
				"name":        product.ProductName,
				"description": product.Description,
				"price":       product.Price,
				"image":       product.Image,
				"category":    product.Category,
				"stock":       product.QuantityAvailable + product.QuantityReserved,
				"reserved":    product.QuantityReserved,
				"available":   product.QuantityAvailable,
			})
		}
		writeJSON(w, http.StatusOK, items)
	}
}

type updateStockRequest struct {
	TotalStock int `json:"totalStock"`
}

type updateVisibilityRequest struct {
	Visible bool `json:"visible"`
}

func updateStockHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		productID := r.PathValue("productId")
		var request updateStockRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		product, err := repo.UpdateTotalStock(r.Context(), productID, request.TotalStock, time.Now().UTC())
		if err != nil {
			logger.Error("update stock", "error", err)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, product)
	}
}

func updateVisibilityHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		productID := r.PathValue("productId")
		var request updateVisibilityRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		product, err := repo.UpdateVisibility(r.Context(), productID, request.Visible)
		if err != nil {
			logger.Error("update visibility", "error", err)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, product)
	}
}

func listProductsHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		products, err := repo.ListProducts(r.Context())
		if err != nil {
			logger.Error("list products", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, products)
	}
}

func listReservationsHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reservations, err := repo.ListReservations(r.Context())
		if err != nil {
			logger.Error("list reservations", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, reservations)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type failureModeTogglable interface {
	FailureModeEnabled() bool
	SetFailureModeEnabled(bool)
	FailureModeState() faultinjection.Snapshot
	ConfigureFailureMode(faultinjection.Config) faultinjection.Snapshot
}

func getFailureModeHandler(svc failureModeTogglable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, svc.FailureModeState())
	}
}

func putFailureModeHandler(logger *slog.Logger, svc failureModeTogglable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req faultinjection.Config
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		state := svc.ConfigureFailureMode(req)
		logger.Info("failure mode updated", "enabled", state.Enabled, "runLabel", state.RunLabel, "remaining", state.Remaining)
		writeJSON(w, http.StatusOK, state)
	}
}

type delayRequest struct {
	DelayMs int32 `json:"delay_ms"`
}

func getDelayHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		delay := domain.SimulatedDelayMs.Load()
		writeJSON(w, http.StatusOK, delayRequest{DelayMs: delay})
	}
}

func putDelayHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req delayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		domain.SimulatedDelayMs.Store(req.DelayMs)
		logger.Info("simulated delay updated", "delay_ms", req.DelayMs)
		writeJSON(w, http.StatusOK, req)
	}
}

type createProductRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Price       json.Number `json:"price"`
	Image       string      `json:"image"`
	Category    string      `json:"category"`
	Stock       int         `json:"stock"`
}

type updateProductMetaRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Price       json.Number `json:"price"`
	Image       string      `json:"image"`
	Category    string      `json:"category"`
}

func createProductHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createProductRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if req.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
			return
		}
		p := domain.Product{
			ProductName:       req.Name,
			Description:       req.Description,
			Price:             req.Price,
			Image:             req.Image,
			Category:          req.Category,
			QuantityAvailable: req.Stock,
		}
		created, err := repo.CreateProduct(r.Context(), p)
		if err != nil {
			logger.Error("create product", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, created)
	}
}

func updateProductMetaHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		productID := r.PathValue("productId")
		var req updateProductMetaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		updated, err := repo.UpdateProductMeta(r.Context(), productID, req.Name, req.Description, req.Category, req.Image, req.Price)
		if err != nil {
			logger.Error("update product meta", "error", err)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, updated)
	}
}

func deleteProductHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		productID := r.PathValue("productId")
		if err := repo.DeleteProduct(r.Context(), productID); err != nil {
			logger.Error("delete product", "error", err)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
