package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/choreography-saga/internal/httpapiutil"
	"saga-pattern/choreography-saga/shipping-service/internal/domain"
	"saga-pattern/choreography-saga/shipping-service/internal/repository"
	"saga-pattern/choreography-saga/shipping-service/internal/shipping"
	commonconfig "saga-pattern/common/config"
	"saga-pattern/common/faultinjection"
)

type HandlerDependencies struct {
	Config   commonconfig.ServiceConfig
	Logger   *slog.Logger
	Registry prometheus.Gatherer
	Repo     repository.Repository
	Service  *shipping.Service
}

func NewHandler(deps HandlerDependencies) http.Handler {
	base := httpapiutil.NewParticipantHandler(httpapiutil.ParticipantHandlerDependencies{
		Config:   deps.Config,
		Logger:   deps.Logger,
		Registry: deps.Registry,
	})
	if deps.Repo == nil {
		return base
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/shipments", listShipmentsHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("PATCH /api/shipments/{shippingId}/status", updateShipmentStatusHandler(deps.Logger, deps.Repo))
	mux.HandleFunc("GET /api/admin/failure-mode", getFailureModeHandler(deps.Service))
	mux.HandleFunc("PUT /api/admin/failure-mode", putFailureModeHandler(deps.Logger, deps.Service))
	mux.Handle("/", base)
	return mux
}

func listShipmentsHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		shipments, err := repo.ListShipments(r.Context())
		if err != nil {
			logger.Error("list shipments", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if shipments == nil {
			shipments = []domain.Shipment{}
		}
		writeJSON(w, http.StatusOK, shipments)
	}
}

func updateShipmentStatusHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		shippingID := r.PathValue("shippingId")
		if shippingID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing shippingId"})
			return
		}
		var req struct {
			Status string `json:"status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
			return
		}
		if err := repo.UpdateStatus(r.Context(), shippingID, domain.ShipmentStatus(req.Status)); err != nil {
			logger.Error("update shipment status", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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
