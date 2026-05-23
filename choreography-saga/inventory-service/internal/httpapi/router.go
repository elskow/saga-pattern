package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/choreography-saga/internal/httpapiutil"
	"saga-pattern/choreography-saga/inventory-service/internal/inventory"
	"saga-pattern/choreography-saga/inventory-service/internal/repository"
	commonconfig "saga-pattern/common/config"
	"saga-pattern/common/faultinjection"
)

type HandlerDependencies struct {
	Config   commonconfig.ServiceConfig
	Logger   *slog.Logger
	Registry prometheus.Gatherer
	Repo     repository.Repository
	Service  *inventory.Service
}

func NewHandler(deps HandlerDependencies) http.Handler {
	base := httpapiutil.NewParticipantHandler(httpapiutil.ParticipantHandlerDependencies{
		Config:   deps.Config,
		Logger:   deps.Logger,
		Registry: deps.Registry,
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
	mux.HandleFunc("GET /api/reservations", listReservationsHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("GET /api/admin/failure-mode", getFailureModeHandler(svc))
	mux.HandleFunc("PUT /api/admin/failure-mode", putFailureModeHandler(deps.Logger, svc))

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
