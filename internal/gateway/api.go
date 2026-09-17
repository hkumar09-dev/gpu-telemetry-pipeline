package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/domain"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/ports"
)

// API documents and serves telemetry query + collector ingest.
type API struct {
	repo ports.Repository
	log  *slog.Logger
}

func New(repo ports.Repository, log *slog.Logger) *API {
	if log == nil {
		log = slog.Default()
	}
	return &API{repo: repo, log: log}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("GET /livez", a.health)
	mux.HandleFunc("GET /openapi.yaml", a.openapi)
	mux.HandleFunc("GET /api/openapi.yaml", a.openapi)
	mux.HandleFunc("GET /api/v1/gpus", a.listGPUs)
	mux.HandleFunc("GET /api/v1/gpus/{id}/telemetry", a.queryTelemetry)
	mux.HandleFunc("POST /internal/v1/telemetry", a.ingest)
	return logging(a.log, mux)
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) listGPUs(w http.ResponseWriter, r *http.Request) {
	gpus, err := a.repo.ListGPUs(r.Context())
	if err != nil {
		a.fail(w, http.StatusInternalServerError, err)
		return
	}
	if gpus == nil {
		gpus = []domain.GPU{}
	}
	writeJSON(w, http.StatusOK, gpus)
}

func (a *API) queryTelemetry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if strings.TrimSpace(id) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "gpu id is required"})
		return
	}
	window, err := parseWindow(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	rows, err := a.repo.QueryByGPU(r.Context(), id, window)
	if err != nil {
		a.fail(w, http.StatusInternalServerError, err)
		return
	}
	if rows == nil {
		rows = []domain.Telemetry{}
	}
	writeJSON(w, http.StatusOK, rows)
}

func (a *API) ingest(w http.ResponseWriter, r *http.Request) {
	var t domain.Telemetry
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if err := t.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := a.repo.Save(r.Context(), t); err != nil {
		a.fail(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (a *API) openapi(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write([]byte(OpenAPISpec))
}

func (a *API) fail(w http.ResponseWriter, code int, err error) {
	a.log.Error("request failed", "err", err)
	writeJSON(w, code, map[string]string{"error": http.StatusText(code)})
}

func parseWindow(r *http.Request) (domain.TimeWindow, error) {
	var w domain.TimeWindow
	q := r.URL.Query()
	if v := q.Get("start_time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return w, err
		}
		u := t.UTC()
		w.Start = &u
	}
	if v := q.Get("end_time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return w, err
		}
		u := t.UTC()
		w.End = &u
	}
	return w, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Info("http", "method", r.Method, "path", r.URL.Path, "dur", time.Since(start))
	})
}
