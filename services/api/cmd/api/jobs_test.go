package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"networkforge/services/api/internal/sim"
	"networkforge/services/api/internal/store"
)

func TestBackgroundExperimentSurvivesRequestCancellation(t *testing.T) {
	handler := newHandler(nil)
	body := `{"kind":"experiment","experiment":{"simulation":{"nodes":[{"id":"s","kind":"service","concurrency":1,"queueCapacity":1}],"edges":[],"workload":{"source":"s","requestsPerSecond":1,"durationSeconds":1}},"repetitions":3,"sloP95Us":1000}}`
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body)).WithContext(ctx)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != 202 {
		t.Fatalf("create: %d %s", recorder.Code, recorder.Body.String())
	}
	var created jobSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		get := httptest.NewRecorder()
		handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+created.ID, nil))
		var current jobSnapshot
		if err := json.Unmarshal(get.Body.Bytes(), &current); err != nil {
			t.Fatal(err)
		}
		if current.Status == "succeeded" {
			if current.ProgressDone != 3 || current.ProgressTotal != 3 || !strings.Contains(string(current.Result), `"repetitions":3`) {
				t.Fatalf("result: %+v", current)
			}
			return
		}
		if current.Status == "failed" || current.Status == "cancelled" {
			t.Fatalf("job did not survive disconnect: %+v", current)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("job did not finish")
}

func TestTwoWorkersClaimOneSavedRun(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to test multi-worker claims")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := db.Create(ctx, "Two-worker fixture", store.Architecture{Nodes: []store.PositionedNode{{Node: sim.Node{ID: "service", Kind: "service", Concurrency: 1}}}, Workload: sim.Workload{Source: "service", RequestsPerSecond: 2, DurationSeconds: 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Delete(context.Background(), p.ID) })
	id := "two-workers-" + p.ID
	payload, err := json.Marshal(jobRequest{Kind: "project_run", Project: &p})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateJob(ctx, store.JobRecord{ID: id, Kind: "project_run", Input: payload, ProgressTotal: 2, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.DeleteJob(context.Background(), id) })
	a, b := newJobManager(db), newJobManager(db)
	if err := a.recover(id); err != nil {
		t.Fatal(err)
	}
	if err := b.recover(id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := db.GetJob(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == "succeeded" {
			runs, err := db.Runs(ctx, p.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(runs) != 1 {
				t.Fatalf("two workers saved %d runs", len(runs))
			}
			return
		}
		if job.Status == "failed" {
			t.Fatalf("job failed: %s", job.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not finish")
}

func TestPersistedJobRecoversAfterManagerRestart(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to test durable job recovery")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := time.Now().Format("20060102150405.000000000")
	input := json.RawMessage(`{"kind":"simulation","simulation":{"nodes":[{"id":"s","kind":"service","concurrency":1}],"workload":{"source":"s","requestsPerSecond":2,"durationSeconds":1}}}`)
	if err := db.CreateJob(ctx, store.JobRecord{ID: id, Kind: "simulation", Input: input, ProgressTotal: 2, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.DeleteJob(context.Background(), id) })
	if ok, err := db.ClaimJob(ctx, id, "interrupted-worker"); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := db.ReleaseJob(ctx, id, "interrupted-worker"); err != nil {
		t.Fatal(err)
	}
	manager := newJobManager(db)
	if err := manager.recover(id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, ok := manager.get(id)
		if ok && job.Status == "succeeded" {
			var result struct {
				Generated int `json:"generated"`
			}
			if err := json.Unmarshal(job.Result, &result); err != nil {
				t.Fatal(err)
			}
			if job.ProgressDone != 2 || result.Generated != 2 {
				t.Fatalf("bad result: %+v", job)
			}
			fresh := newJobManager(db)
			loaded, ok := fresh.get(id)
			if !ok || loaded.Status != "succeeded" || string(loaded.Result) != string(job.Result) {
				t.Fatalf("result lost after restart: %+v", loaded)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("recovered job did not finish")
}

func TestJobCancellationStopsWorker(t *testing.T) {
	m := newJobManager()
	started := make(chan struct{})
	job, err := m.start("test", 1, func(ctx context.Context, _ func(int, int)) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	current, ok := m.cancel(job.ID)
	if !ok || current.Status != "cancelled" {
		t.Fatalf("cancel: %+v", current)
	}
	time.Sleep(5 * time.Millisecond)
	current, _ = m.get(job.ID)
	if current.Status != "cancelled" {
		t.Fatalf("cancel overwritten: %+v", current)
	}
}
