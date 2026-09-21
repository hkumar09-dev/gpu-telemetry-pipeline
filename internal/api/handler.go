package api

import (
	"net/http"
)

type Handler struct {
	svc *Service
	mux http.Handler
}

func NewHandler(svc *Service) *Handler {
	h := &Handler{svc: svc}
	h.mux = h.routes()
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.svc.health)
	mux.HandleFunc("GET /healthz/{$}", h.svc.health)
	mux.HandleFunc("GET /livez", h.svc.health)
	mux.HandleFunc("GET /livez/{$}", h.svc.health)
	mux.HandleFunc("GET /openapi.yaml", h.svc.openapi)
	mux.HandleFunc("GET /api/openapi.yaml", h.svc.openapi)
	mux.HandleFunc("GET /api/v1/gpus", h.svc.listGPUs)
	mux.HandleFunc("GET /api/v1/gpus/{id}/telemetry", h.svc.queryTelemetry)
	mux.HandleFunc("POST /internal/v1/telemetry", h.svc.ingest)
	return logging(h.svc.log, mux)
}
