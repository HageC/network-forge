package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"networkforge/services/api/internal/sim"
	"networkforge/services/api/internal/store"
)

type server struct {
	store *store.Store
	jobs  *jobManager
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	url := os.Getenv("DATABASE_URL")
	var db *store.Store
	if url != "" {
		var err error
		for attempt := 0; attempt < 20; attempt++ {
			db, err = store.Open(ctx, url)
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		if err != nil {
			slog.Error("database unavailable", "error", err)
			os.Exit(1)
		}
		defer db.Close()
		if err = db.Migrate(ctx); err != nil {
			slog.Error("migration failed", "error", err)
			os.Exit(1)
		}
	}
	addr := os.Getenv("NETWORKFORGE_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	jobs := newJobManager(db)
	defer jobs.close()
	srv := &http.Server{Addr: addr, Handler: newHandlerWithJobs(db, jobs), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("api listening", "addr", addr, "persistence", db != nil)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func newHandler(db *store.Store) http.Handler {
	return newHandlerWithJobs(db, newJobManager(db))
}
func newHandlerWithJobs(db *store.Store, jobs *jobManager) http.Handler {
	s := server{store: db, jobs: jobs}
	if err := s.jobs.recover(); err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /api/v1/simulations", s.simulate)
	mux.HandleFunc("POST /api/v1/validate", s.validateModel)
	mux.HandleFunc("POST /api/v1/experiments", s.experiment)
	mux.HandleFunc("POST /api/v1/comparisons", s.compare)
	mux.HandleFunc("POST /api/v1/calibrations", s.calibrate)
	mux.HandleFunc("POST /api/v1/jobs", s.createJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}", s.getJob)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", s.cancelJob)
	mux.HandleFunc("GET /api/v1/projects", s.listProjects)
	mux.HandleFunc("POST /api/v1/projects", s.createProject)
	mux.HandleFunc("GET /api/v1/projects/{id}", s.getProject)
	mux.HandleFunc("PATCH /api/v1/projects/{id}", s.renameProject)
	mux.HandleFunc("DELETE /api/v1/projects/{id}", s.deleteProject)
	mux.HandleFunc("POST /api/v1/projects/{id}/versions", s.saveVersion)
	mux.HandleFunc("POST /api/v1/projects/{id}/runs", s.runProject)
	mux.HandleFunc("GET /api/v1/projects/{id}/runs", s.listRuns)
	mux.HandleFunc("GET /api/v1/projects/{id}/runs/{runId}/traces", s.getTraces)
	return mux
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func respondError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func (s server) requireStore(w http.ResponseWriter) bool {
	if s.store == nil {
		respondError(w, http.StatusServiceUnavailable, "database is not configured")
		return false
	}
	return true
}
func handleStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		respondError(w, http.StatusNotFound, "project not found")
	} else {
		respondError(w, http.StatusInternalServerError, "database operation failed")
		slog.Error("database operation failed", "error", err)
	}
}
func (s server) simulate(w http.ResponseWriter, r *http.Request) {
	var in sim.Input
	if err := decode(w, r, &in); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	result, err := sim.RunContext(r.Context(), in)
	if err != nil {
		respondError(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, result)
}
func (s server) validateModel(w http.ResponseWriter, r *http.Request) {
	var in sim.Input
	if err := decode(w, r, &in); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	if err := sim.Validate(in); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, map[string]bool{"valid": true})
}
func (s server) experiment(w http.ResponseWriter, r *http.Request) {
	var in sim.ExperimentInput
	if err := decode(w, r, &in); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := sim.Experiment(r.Context(), in)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}
func (s server) compare(w http.ResponseWriter, r *http.Request) {
	var in sim.ComparisonInput
	if err := decode(w, r, &in); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := sim.Compare(r.Context(), in)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}
func (s server) calibrate(w http.ResponseWriter, r *http.Request) {
	var in sim.CalibrationInput
	if err := decode(w, r, &in); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	result, err := sim.Calibrate(r.Context(), in)
	if err != nil {
		respondError(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, result)
}
func (s server) listProjects(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	items, err := s.store.List(r.Context())
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, 200, items)
}
func (s server) createProject(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	var body struct {
		Name         string             `json:"name"`
		Architecture store.Architecture `json:"architecture"`
	}
	if err := decode(w, r, &body); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	p, err := s.store.Create(r.Context(), strings.TrimSpace(body.Name), body.Architecture)
	if err != nil {
		respondError(w, 400, err.Error())
		return
	}
	writeJSON(w, 201, p)
}
func (s server) getProject(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	p, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, 200, p)
}
func (s server) renameProject(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := decode(w, r, &body); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	err := s.store.Rename(r.Context(), r.PathValue("id"), strings.TrimSpace(body.Name))
	if err != nil {
		if err.Error() == "invalid project name" {
			respondError(w, 400, err.Error())
		} else {
			handleStoreError(w, err)
		}
		return
	}
	s.getProject(w, r)
}
func (s server) deleteProject(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	if err := s.store.Delete(r.Context(), r.PathValue("id")); err != nil {
		handleStoreError(w, err)
		return
	}
	w.WriteHeader(204)
}
func (s server) saveVersion(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	var a store.Architecture
	if err := decode(w, r, &a); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	p, err := s.store.Save(r.Context(), r.PathValue("id"), a)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			handleStoreError(w, err)
		} else {
			respondError(w, 400, err.Error())
		}
		return
	}
	writeJSON(w, 201, p)
}
func (s server) runProject(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	run, err := s.store.Run(r.Context(), r.PathValue("id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, 201, run)
}
func (s server) listRuns(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	runs, err := s.store.Runs(r.Context(), r.PathValue("id"))
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, 200, runs)
}
func (s server) getTraces(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("runId"), 10, 64)
	if err != nil || id < 1 {
		respondError(w, http.StatusBadRequest, "invalid run ID")
		return
	}
	traces, err := s.store.Traces(r.Context(), r.PathValue("id"), id)
	if err != nil {
		handleStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, traces)
}
