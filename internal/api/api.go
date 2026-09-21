package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/domain"
	"github.com/gpu-telemetry-pipeline/internal/ports"
)

// Service documents and serves telemetry query + collector ingest.
type Service struct {
	repo ports.Repository
	log  *slog.Logger
	ctx  context.Context
}

type Svc interface {
}

func NewService(repo ports.Repository, log *slog.Logger) *Service {
	ctx := context.Background()
	if log == nil {
		log = slog.Default()
	}

	return &Service{repo: repo, log: log, ctx: ctx}
}

//func (a *API) Handler() http.Handler {
//	mux := http.NewServeMux()
//	mux.HandleFunc("GET /healthz", a.health)
//	mux.HandleFunc("GET /healthz/{$}", a.health)
//	mux.HandleFunc("GET /livez", a.health)
//	mux.HandleFunc("GET /livez/{$}", a.health)
//	mux.HandleFunc("GET /openapi.yaml", a.openapi)
//	mux.HandleFunc("GET /api/openapi.yaml", a.openapi)
//	mux.HandleFunc("GET /api/v1/gpus", a.listGPUs)
//	mux.HandleFunc("GET /api/v1/gpus/{id}/telemetry", a.queryTelemetry)
//	mux.HandleFunc("POST /internal/v1/telemetry", a.ingest)
//	return logging(a.log, mux)
//}

func (a *Service) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *Service) listGPUs(w http.ResponseWriter, r *http.Request) {
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

func (a *Service) queryTelemetry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if strings.TrimSpace(id) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "gpu id is required"})
		return
	}

	window, err := a.parseWindow(r)
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

func (a *Service) ingest(w http.ResponseWriter, r *http.Request) {
	var t domain.Telemetry
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		a.log.Error("error in decoding", "err", err)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if err := t.Validate(); err != nil {
		a.log.Error("error in validation", "err", err)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := a.repo.Save(r.Context(), t); err != nil {
		a.log.Error("error in saving", "err", err)
		a.fail(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (a *Service) openapi(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write([]byte(OpenAPISpec))
}

func (a *Service) fail(w http.ResponseWriter, code int, err error) {
	a.log.Error("request failed", "err", err)
	writeJSON(w, code, map[string]string{"error": http.StatusText(code)})
}

func (a *Service) parseWindow(r *http.Request) (domain.TimeWindow, error) {
	var w domain.TimeWindow
	q := r.URL.Query()
	if v := q.Get("start_time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			a.log.Error("failed to parse start_time", "err", err)
			return w, err
		}

		u := t.UTC()
		w.Start = &u
	}
	if v := q.Get("end_time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			a.log.Error("failed to parse end_time", "err", err)
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

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		attrs := []any{"method", r.Method, "path", r.URL.Path, "status", sw.code, "dur_ms", time.Since(start).Milliseconds()}
		switch {
		case sw.code >= 500:
			log.Error("http", attrs...)
		case sw.code >= 400:
			log.Warn("http", attrs...)
		case r.URL.Path == "/internal/v1/telemetry":
			log.Debug("http", attrs...)
		default:
			log.Info("http", attrs...)
		}
	})
}
