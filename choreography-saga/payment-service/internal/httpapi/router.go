package httpapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"saga-pattern/choreography-saga/internal/httpapiutil"
	commonconfig "saga-pattern/common/config"
	"saga-pattern/choreography-saga/payment-service/internal/repository"
)

type depositBalanceManager interface {
	GetDepositBalance() *big.Rat
	SetDepositBalance(*big.Rat)
}

type HandlerDependencies struct {
	Config   commonconfig.ServiceConfig
	Logger   *slog.Logger
	Registry prometheus.Gatherer
	Repo     repository.Repository
	Balancer depositBalanceManager
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
	mux.HandleFunc("GET /api/payments", listPaymentsHandler(deps.Logger, deps.Repo))
	if deps.Balancer != nil {
		mux.HandleFunc("GET /api/admin/deposit-balance", getDepositBalanceHandler(deps.Balancer))
		mux.HandleFunc("PUT /api/admin/deposit-balance", putDepositBalanceHandler(deps.Logger, deps.Balancer))
	}
	mux.Handle("/", base)
	return mux
}

func listPaymentsHandler(logger *slog.Logger, repo repository.Repository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		payments, err := repo.ListPayments(r.Context())
		if err != nil {
			logger.Error("list payments", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, payments)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type depositBalanceResponse struct {
	Balance   string `json:"balance"`
	Unlimited bool   `json:"unlimited"`
}

type depositBalanceRequest struct {
	Balance *string `json:"balance"`
}

func getDepositBalanceHandler(balancer depositBalanceManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bal := balancer.GetDepositBalance()
		if bal == nil {
			writeJSON(w, http.StatusOK, depositBalanceResponse{Balance: "", Unlimited: true})
			return
		}
		floatBal, _ := bal.Float64()
		writeJSON(w, http.StatusOK, depositBalanceResponse{
			Balance:   fmt.Sprintf("%.2f", floatBal),
			Unlimited: false,
		})
	}
}

func putDepositBalanceHandler(logger *slog.Logger, balancer depositBalanceManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req depositBalanceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if req.Balance == nil || *req.Balance == "" {
			balancer.SetDepositBalance(nil)
			logger.Info("deposit balance set to unlimited")
			writeJSON(w, http.StatusOK, depositBalanceResponse{Balance: "", Unlimited: true})
			return
		}
		rat, ok := new(big.Rat).SetString(*req.Balance)
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid balance value"})
			return
		}
		balancer.SetDepositBalance(rat)
		logger.Info("deposit balance updated", "balance", *req.Balance)
		writeJSON(w, http.StatusOK, depositBalanceResponse{Balance: *req.Balance, Unlimited: false})
	}
}
