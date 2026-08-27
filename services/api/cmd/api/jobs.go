package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"networkforge/services/api/internal/sim"
	"networkforge/services/api/internal/store"
)

type jobSnapshot struct {
	ID            string          `json:"id"`
	Kind          string          `json:"kind"`
	Status        string          `json:"status"`
	ProgressDone  int             `json:"progressDone"`
	ProgressTotal int             `json:"progressTotal"`
	CreatedAt     time.Time       `json:"createdAt"`
	UpdatedAt     time.Time       `json:"updatedAt"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         string          `json:"error,omitempty"`
}
type job struct {
	jobSnapshot
	cancel context.CancelFunc
}
type jobManager struct {
	mu    sync.Mutex
	jobs  map[string]*job
	slots chan struct{}
	store *store.Store
	owner string
	stop  chan struct{}
}

var errJobCapacity = errors.New("job capacity reached; wait for a running job to finish")

func newJobManager(db ...*store.Store) *jobManager {
	m := &jobManager{jobs: map[string]*job{}, slots: make(chan struct{}, 2), stop: make(chan struct{})}
	if len(db) > 0 {
		m.store = db[0]
	}
	owner := make([]byte, 16)
	if _, err := rand.Read(owner); err == nil {
		m.owner = hex.EncodeToString(owner)
	}
	if m.store != nil {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-m.stop:
					return
				case <-ticker.C:
					if err := m.recover(); err != nil {
						slog.Error("job recovery scan failed", "error", err)
					}
				}
			}
		}()
	}
	return m
}
func (m *jobManager) close() {
	close(m.stop)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		j.cancel()
		if m.store != nil {
			_ = m.store.ReleaseJob(context.Background(), j.ID, m.owner)
		}
	}
}
func (m *jobManager) start(kind string, total int, run func(context.Context, func(int, int)) (any, error)) (jobSnapshot, error) {
	return m.startPayload(kind, total, nil, run)
}
func (m *jobManager) startPayload(kind string, total int, payload json.RawMessage, run func(context.Context, func(int, int)) (any, error)) (jobSnapshot, error) {
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return jobSnapshot{}, err
	}
	id := hex.EncodeToString(idBytes)
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now().UTC()
	j := &job{jobSnapshot: jobSnapshot{ID: id, Kind: kind, Status: "queued", ProgressTotal: total, CreatedAt: now, UpdatedAt: now}, cancel: cancel}
	m.mu.Lock()
	if len(m.jobs) >= 32 {
		var oldest *job
		for _, existing := range m.jobs {
			if existing.Status == "queued" || existing.Status == "running" {
				continue
			}
			if oldest == nil || existing.UpdatedAt.Before(oldest.UpdatedAt) {
				oldest = existing
			}
		}
		if oldest == nil {
			m.mu.Unlock()
			cancel()
			return jobSnapshot{}, errJobCapacity
		}
		delete(m.jobs, oldest.ID)
	}
	m.jobs[id] = j
	snapshot := j.jobSnapshot
	m.mu.Unlock()
	if m.store != nil {
		if err := m.store.CreateJob(context.Background(), store.JobRecord{ID: id, Kind: kind, Input: payload, ProgressTotal: total, CreatedAt: now}); err != nil {
			m.mu.Lock()
			delete(m.jobs, id)
			m.mu.Unlock()
			cancel()
			return jobSnapshot{}, err
		}
	}
	go m.execute(ctx, j, run)
	return snapshot, nil
}
func (m *jobManager) execute(ctx context.Context, j *job, run func(context.Context, func(int, int)) (any, error)) {
	if m.store != nil {
		defer func() { m.mu.Lock(); delete(m.jobs, j.ID); m.mu.Unlock() }()
	}
	select {
	case m.slots <- struct{}{}:
		defer func() { <-m.slots }()
	case <-ctx.Done():
		return
	}
	if m.store != nil {
		claimed, err := m.store.ClaimJob(ctx, j.ID, m.owner)
		if err != nil {
			slog.Error("job claim failed", "error", err)
			return
		}
		if !claimed {
			return
		}
		leaseCtx, cancel := context.WithCancel(ctx)
		ctx = leaseCtx
		defer cancel()
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					ok, err := m.store.RenewJob(context.Background(), j.ID, m.owner)
					if err != nil || !ok {
						if err != nil {
							slog.Error("job lease renewal failed", "error", err)
						}
						cancel()
						return
					}
				}
			}
		}()
	}
	m.mu.Lock()
	if j.Status == "cancelled" {
		m.mu.Unlock()
		return
	}
	j.Status = "running"
	j.UpdatedAt = time.Now().UTC()
	m.mu.Unlock()
	progress := func(done, total int) {
		m.mu.Lock()
		if j.Status == "running" {
			j.ProgressDone = done
			j.ProgressTotal = total
			j.UpdatedAt = time.Now().UTC()
			if m.store != nil {
				if _, err := m.store.UpdateLeasedJob(context.Background(), j.ID, m.owner, j.Status, done, total, nil, ""); err != nil {
					slog.Error("job progress update failed", "error", err)
				}
			}
		}
		m.mu.Unlock()
	}
	var result any
	var err error
	if j.Kind == "project_run" && m.store != nil {
		var record store.JobRecord
		record, err = m.store.GetJob(ctx, j.ID)
		if err == nil {
			var request jobRequest
			err = json.Unmarshal(record.Input, &request)
			if err == nil && request.Project == nil {
				err = errors.New("project snapshot is missing")
			}
			if err == nil {
				result, err = m.store.RunSnapshotForJob(ctx, *request.Project, j.ID, m.owner, progress)
			}
		}
	} else {
		result, err = run(ctx, progress)
	}
	var data json.RawMessage
	if err == nil {
		data, err = json.Marshal(result)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if j.Status == "cancelled" {
		return
	}
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		j.Status = "cancelled"
	} else if err != nil {
		j.Status = "failed"
		j.Error = err.Error()
	} else {
		j.Status = "succeeded"
		j.ProgressDone = j.ProgressTotal
		j.Result = data
	}
	j.UpdatedAt = time.Now().UTC()
	if m.store != nil {
		if _, err := m.store.UpdateLeasedJob(context.Background(), j.ID, m.owner, j.Status, j.ProgressDone, j.ProgressTotal, j.Result, j.Error); err != nil {
			slog.Error("job result update failed", "error", err)
		}
	}
}
func (m *jobManager) get(id string) (jobSnapshot, bool) {
	if m.store != nil {
		record, err := m.store.GetJob(context.Background(), id)
		if err == nil {
			return snapshotFromRecord(record), true
		}
		return jobSnapshot{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return jobSnapshot{}, false
	}
	return j.jobSnapshot, true
}
func (m *jobManager) cancel(id string) (jobSnapshot, bool) {
	if m.store != nil {
		record, err := m.store.CancelJob(context.Background(), id)
		if err != nil {
			return jobSnapshot{}, false
		}
		m.mu.Lock()
		if j := m.jobs[id]; j != nil && record.Status == "cancelled" {
			j.Status = "cancelled"
			j.cancel()
		}
		m.mu.Unlock()
		return snapshotFromRecord(record), true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return jobSnapshot{}, false
	}
	if j.Status == "queued" || j.Status == "running" {
		j.Status = "cancelled"
		j.UpdatedAt = time.Now().UTC()
		j.cancel()
	}
	return j.jobSnapshot, true
}

func snapshotFromRecord(j store.JobRecord) jobSnapshot {
	return jobSnapshot{ID: j.ID, Kind: j.Kind, Status: j.Status, ProgressDone: j.ProgressDone, ProgressTotal: j.ProgressTotal, CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt, Result: j.Result, Error: j.Error}
}

type jobRequest struct {
	Kind       string               `json:"kind"`
	Simulation *sim.Input           `json:"simulation,omitempty"`
	Experiment *sim.ExperimentInput `json:"experiment,omitempty"`
	Comparison *sim.ComparisonInput `json:"comparison,omitempty"`
	ProjectID  string               `json:"projectId,omitempty"`
	Project    *store.Project       `json:"project,omitempty"`
}

func (m *jobManager) recover(idFilter ...string) error {
	if m.store == nil {
		return nil
	}
	records, err := m.store.RecoverJobs(context.Background(), idFilter...)
	if err != nil {
		return err
	}
	for _, record := range records {
		var request jobRequest
		if err := json.Unmarshal(record.Input, &request); err != nil {
			_ = m.store.UpdateJob(context.Background(), record.ID, "failed", record.ProgressDone, record.ProgressTotal, nil, "invalid saved job input")
			continue
		}
		_, run, err := prepareJob(request, m.store)
		if err != nil {
			_ = m.store.UpdateJob(context.Background(), record.ID, "failed", record.ProgressDone, record.ProgressTotal, nil, err.Error())
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		j := &job{jobSnapshot: snapshotFromRecord(record), cancel: cancel}
		m.mu.Lock()
		if m.jobs[j.ID] != nil {
			m.mu.Unlock()
			cancel()
			continue
		}
		m.jobs[j.ID] = j
		m.mu.Unlock()
		go m.execute(ctx, j, run)
	}
	return nil
}

func prepareJob(body jobRequest, db ...*store.Store) (int, func(context.Context, func(int, int)) (any, error), error) {
	var total int
	var run func(context.Context, func(int, int)) (any, error)
	switch body.Kind {
	case "simulation":
		if body.Simulation == nil {
			return 0, nil, errors.New("simulation is required")
		}
		if err := sim.Validate(*body.Simulation); err != nil {
			return 0, nil, err
		}
		total = body.Simulation.Workload.RequestsPerSecond * body.Simulation.Workload.DurationSeconds
		run = func(ctx context.Context, progress func(int, int)) (any, error) {
			return sim.RunContextWithProgress(ctx, *body.Simulation, progress)
		}
	case "experiment":
		if body.Experiment == nil {
			return 0, nil, errors.New("experiment is required")
		}
		if err := sim.Validate(body.Experiment.Simulation); err != nil {
			return 0, nil, err
		}
		total = body.Experiment.Repetitions
		run = func(ctx context.Context, progress func(int, int)) (any, error) {
			return sim.ExperimentWithProgress(ctx, *body.Experiment, progress)
		}
	case "comparison":
		if body.Comparison == nil {
			return 0, nil, errors.New("comparison is required")
		}
		if err := sim.Validate(body.Comparison.Baseline); err != nil {
			return 0, nil, err
		}
		if err := sim.Validate(body.Comparison.Candidate); err != nil {
			return 0, nil, err
		}
		total = 2 * body.Comparison.Repetitions
		run = func(ctx context.Context, progress func(int, int)) (any, error) {
			return sim.CompareWithProgress(ctx, *body.Comparison, progress)
		}
	case "project_run":
		if body.Project == nil || len(db) == 0 || db[0] == nil {
			return 0, nil, errors.New("project snapshot is required")
		}
		total = body.Project.Architecture.Workload.RequestsPerSecond * body.Project.Architecture.Workload.DurationSeconds
		run = func(ctx context.Context, progress func(int, int)) (any, error) {
			return db[0].RunSnapshotWithProgress(ctx, *body.Project, progress)
		}
	default:
		return 0, nil, errors.New("unknown job kind")
	}
	return total, run, nil
}

func (s server) createJob(w http.ResponseWriter, r *http.Request) {
	var body jobRequest
	if err := decode(w, r, &body); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	if body.Kind == "project_run" {
		if s.store == nil {
			respondError(w, 503, "database is not configured")
			return
		}
		if body.ProjectID == "" {
			respondError(w, 400, "projectId is required")
			return
		}
		project, err := s.store.Get(r.Context(), body.ProjectID)
		if err != nil {
			handleStoreError(w, err)
			return
		}
		body.Project = &project
	}
	total, run, err := prepareJob(body, s.store)
	if err != nil {
		respondError(w, 400, err.Error())
		return
	}
	payload, err := json.Marshal(body)
	if err != nil {
		respondError(w, 500, "could not encode job")
		return
	}
	job, err := s.jobs.startPayload(body.Kind, total, payload, run)
	if errors.Is(err, errJobCapacity) {
		respondError(w, 429, err.Error())
		return
	}
	if err != nil {
		respondError(w, 500, "could not start job")
		return
	}
	w.Header().Set("Location", "/api/v1/jobs/"+job.ID)
	writeJSON(w, 202, job)
}
func (s server) getJob(w http.ResponseWriter, r *http.Request) {
	job, ok := s.jobs.get(r.PathValue("id"))
	if !ok {
		respondError(w, 404, "job not found")
		return
	}
	writeJSON(w, 200, job)
}
func (s server) cancelJob(w http.ResponseWriter, r *http.Request) {
	job, ok := s.jobs.cancel(r.PathValue("id"))
	if !ok {
		respondError(w, 404, "job not found")
		return
	}
	writeJSON(w, 200, job)
}
