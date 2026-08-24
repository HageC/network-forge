package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"networkforge/services/api/internal/sim"
)

func TestSavedRunJobCommitIsAtomicAndReplaySafe(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a := Architecture{Nodes: []PositionedNode{{Node: sim.Node{ID: "service", Kind: "service", Concurrency: 1}}}, Workload: sim.Workload{Source: "service", RequestsPerSecond: 2, DurationSeconds: 1}}
	p, err := s.Create(ctx, "Atomic job fixture", a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Delete(context.Background(), p.ID) })
	id := "atomic-" + p.ID
	if err := s.CreateJob(ctx, JobRecord{ID: id, Kind: "project_run", Input: json.RawMessage(`{"kind":"project_run"}`), ProgressTotal: 2, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.DeleteJob(context.Background(), id) })
	claimed, err := s.ClaimJob(ctx, id, "worker-a")
	if err != nil || !claimed {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	first, err := s.RunSnapshotForJob(ctx, p, id, "worker-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "succeeded" || job.ProgressDone != 2 {
		t.Fatalf("job was not completed with run: %+v", job)
	}
	var saved RunSummary
	if err := json.Unmarshal(job.Result, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.ID != first.ID {
		t.Fatalf("job result points to run %d, want %d", saved.ID, first.ID)
	}
	second, err := s.RunSnapshotForJob(ctx, p, id, "worker-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay inserted run %d, want %d", second.ID, first.ID)
	}
	runs, err := s.Runs(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("replay left %d runs, want 1", len(runs))
	}
	if err := s.UpdateJob(ctx, id, "running", 0, 2, nil, ""); err != nil {
		t.Fatal(err)
	}
	job, err = s.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "succeeded" || job.ProgressDone != 2 {
		t.Fatalf("terminal job was overwritten: %+v", job)
	}
	cancelID := "cancel-" + p.ID
	if err := s.CreateJob(ctx, JobRecord{ID: cancelID, Kind: "project_run", Input: json.RawMessage(`{"kind":"project_run"}`), ProgressTotal: 2, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.DeleteJob(context.Background(), cancelID) })
	if _, err := s.CancelJob(ctx, cancelID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunSnapshotForJob(ctx, p, cancelID, "worker-a", nil); err != context.Canceled {
		t.Fatalf("cancelled job replay: %v", err)
	}
	runs, err = s.Runs(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("cancelled job inserted a run: %d", len(runs))
	}
}

func TestJobLeaseFencesStaleWorker(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := "lease-" + time.Now().Format("20060102150405.000000000")
	if err := s.CreateJob(ctx, JobRecord{ID: id, Kind: "simulation", Input: json.RawMessage(`{}`), ProgressTotal: 1, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.DeleteJob(context.Background(), id) })
	if ok, err := s.ClaimJob(ctx, id, "worker-a"); err != nil || !ok {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	if ok, err := s.ClaimJob(ctx, id, "worker-b"); err != nil || ok {
		t.Fatalf("overlapping claim: %v %v", ok, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE background_jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.ClaimJob(ctx, id, "worker-b"); err != nil || !ok {
		t.Fatalf("reclaim: %v %v", ok, err)
	}
	if ok, err := s.UpdateLeasedJob(ctx, id, "worker-a", "succeeded", 1, 1, json.RawMessage(`{}`), ""); err != nil || ok {
		t.Fatalf("stale completion accepted: %v %v", ok, err)
	}
	if ok, err := s.UpdateLeasedJob(ctx, id, "worker-b", "succeeded", 1, 1, json.RawMessage(`{}`), ""); err != nil || !ok {
		t.Fatalf("new owner completion: %v %v", ok, err)
	}
}

func TestProjectLifecycle(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a := Architecture{Nodes: []PositionedNode{{Node: sim.Node{ID: "client", Kind: "client", Concurrency: 10, HourlyCost: 36}, PositionX: 20, PositionY: 30}, {Node: sim.Node{ID: "service", Kind: "gateway", ServiceTimeUs: 1000, Concurrency: 2, QueueCapacity: 10, RateLimitRps: 8, AutoscaleMaxConcurrency: 5, AutoscaleQueueThreshold: 2, AutoscaleDelayUs: 100_000, AutoscaleIdleUs: 200_000}, PositionX: 250, PositionY: 30}}, Edges: []sim.Edge{{ID: "link", Source: "client", Target: "service", LatencyUs: 500, MaxRetries: 2, RetryBackoffUs: 1000, CircuitFailureThreshold: 3, CircuitOpenUs: 500_000, CostPerMillionAttempts: 1000}}, Workload: sim.Workload{Source: "client", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 42}}
	a.Failures = []sim.Failure{{ID: "outage", Kind: "node_outage", TargetID: "service", StartUs: 200_000, DurationUs: 200_000}}
	created, err := s.Create(ctx, "Integration fixture", a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Delete(context.Background(), created.ID) })
	loaded, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != 1 || loaded.Architecture.Nodes[0].PositionX != 20 || loaded.Architecture.Nodes[0].HourlyCost != 36 || loaded.Architecture.Nodes[1].RateLimitRps != 8 || loaded.Architecture.Nodes[1].AutoscaleMaxConcurrency != 5 || loaded.Architecture.Nodes[1].AutoscaleDelayUs != 100_000 || len(loaded.Architecture.Failures) != 1 || loaded.Architecture.Edges[0].MaxRetries != 2 || loaded.Architecture.Edges[0].CostPerMillionAttempts != 1000 {
		t.Fatalf("load mismatch: %+v", loaded)
	}
	a.Nodes[1].Concurrency = 4
	saved, err := s.Save(ctx, created.ID, a)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Version != 2 {
		t.Fatalf("version = %d", saved.Version)
	}
	run, err := s.Run(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Result.Generated != 10 || run.Result.Generated != run.Result.Completed+run.Result.Failed+run.Result.TimedOut {
		t.Fatalf("bad run: %+v", run)
	}
	runs, err := s.Runs(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Version != 2 || runs[0].ID != run.ID || runs[0].Result.FailuresTriggered != 1 || runs[0].Result.EstimatedScenarioCost != run.Result.EstimatedScenarioCost || runs[0].Result.EdgeAttempts["link"] != run.Result.EdgeAttempts["link"] || len(runs[0].Result.Findings) != len(run.Result.Findings) {
		t.Fatalf("history mismatch: %+v", runs)
	}
	traces, err := s.Traces(ctx, created.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) == 0 || traces[0].ID != run.Result.Traces[0].ID || len(traces[0].Events) == 0 {
		t.Fatalf("trace roundtrip mismatch: %+v", traces)
	}
}

func TestAsyncQueueAndDatabasePersistence(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a := Architecture{Nodes: []PositionedNode{{Node: sim.Node{ID: "queue", Kind: "queue", Concurrency: 10, QueueCapacity: 10, ServiceTimeUs: 1000, BrokerCapacity: 20, MaxDeliveries: 2, RedeliveryDelayUs: 100_000}}, {Node: sim.Node{ID: "db", Kind: "database", Concurrency: 10, ConnectionLimit: 1, QueueCapacity: 1, ServiceTimeUs: 500_000, TransactionLockUs: 100_000}}}, Edges: []sim.Edge{{ID: "dispatch", Source: "queue", Target: "db"}}, Workload: sim.Workload{Source: "queue", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 1}}
	p, err := s.Create(ctx, "Async persistence fixture", a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Delete(context.Background(), p.ID) })
	loaded, err := s.Get(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Architecture.Nodes[0].ConnectionLimit != 1 || loaded.Architecture.Nodes[0].TransactionLockUs != 100_000 || loaded.Architecture.Nodes[1].BrokerCapacity != 20 || loaded.Architecture.Nodes[1].MaxDeliveries != 2 || loaded.Architecture.Nodes[1].RedeliveryDelayUs != 100_000 {
		t.Fatalf("connection limit lost: %+v", loaded)
	}
	run, err := s.Run(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	history, err := s.Runs(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Result.AsyncEnqueued != 10 || len(history) != 1 || history[0].Result.AsyncEnqueued != 10 || history[0].Result.BackgroundFailed != run.Result.BackgroundFailed || history[0].Result.Nodes["db"].ConnectionRejected != run.Result.Nodes["db"].ConnectionRejected {
		t.Fatalf("async history mismatch: run=%+v history=%+v", run, history)
	}
}

func TestAutoscalingRunPersistence(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a := Architecture{Nodes: []PositionedNode{{Node: sim.Node{ID: "service", Kind: "service", ServiceTimeUs: 500_000, Concurrency: 1, QueueCapacity: 10, AutoscaleMaxConcurrency: 5, AutoscaleQueueThreshold: 1, AutoscaleDelayUs: 100_000, AutoscaleIdleUs: 200_000, HourlyCost: 36}}}, Workload: sim.Workload{Source: "service", RequestsPerSecond: 10, DurationSeconds: 2, Seed: 1}}
	p, err := s.Create(ctx, "Autoscaling fixture", a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Delete(context.Background(), p.ID) })
	loaded, err := s.Get(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Architecture.Nodes[0].AutoscaleMaxConcurrency != 5 || loaded.Architecture.Nodes[0].AutoscaleIdleUs != 200_000 {
		t.Fatalf("autoscale policy lost: %+v", loaded)
	}
	run, err := s.Run(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	history, err := s.Runs(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Result.Nodes["service"].ScaleUps == 0 || len(history) != 1 || history[0].Result.Nodes["service"].ScaleUps != run.Result.Nodes["service"].ScaleUps || history[0].Result.EstimatedScenarioCost != run.Result.EstimatedScenarioCost {
		t.Fatalf("autoscale history mismatch: run=%+v history=%+v", run, history)
	}
}

func TestApplicationRetryPersistence(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a := Architecture{Nodes: []PositionedNode{{Node: sim.Node{ID: "client", Kind: "client", Concurrency: 1}}, {Node: sim.Node{ID: "service", Kind: "service", Concurrency: 1}}}, Edges: []sim.Edge{{ID: "link", Source: "client", Target: "service", LossProbability: 1}}, Workload: sim.Workload{Source: "client", RequestsPerSecond: 1, DurationSeconds: 1, MaxApplicationRetries: 2, ApplicationRetryBackoffUs: 100_000}}
	p, err := s.Create(ctx, "Application retry fixture", a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Delete(context.Background(), p.ID) })
	loaded, err := s.Get(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Architecture.Workload.MaxApplicationRetries != 2 || loaded.Architecture.Workload.ApplicationRetryBackoffUs != 100_000 {
		t.Fatalf("retry policy lost: %+v", loaded.Architecture.Workload)
	}
	run, err := s.Run(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	history, err := s.Runs(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Result.ApplicationRetries != 2 || run.Result.ApplicationRetryExhausted != 1 || len(history) != 1 || history[0].Result.ApplicationRetries != 2 || history[0].Result.ApplicationRetryExhausted != 1 {
		t.Fatalf("retry history mismatch: run=%+v history=%+v", run, history)
	}
}

func TestPartialDegradationPersistence(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a := Architecture{Nodes: []PositionedNode{{Node: sim.Node{ID: "client", Kind: "client", Concurrency: 1}}, {Node: sim.Node{ID: "service", Kind: "service", Concurrency: 1, ServiceTimeUs: 100_000}}}, Edges: []sim.Edge{{ID: "link", Source: "client", Target: "service"}}, Workload: sim.Workload{Source: "client", RequestsPerSecond: 2, DurationSeconds: 1}, Failures: []sim.Failure{{ID: "loss", Kind: "edge_loss_spike", TargetID: "link", StartUs: 0, DurationUs: 250_000, LossProbability: 1}, {ID: "slow", Kind: "node_degradation", TargetID: "service", StartUs: 0, DurationUs: 750_000, ServiceMultiplier: 2}}}
	p, err := s.Create(ctx, "Partial degradation fixture", a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Delete(context.Background(), p.ID) })
	loaded, err := s.Get(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Architecture.Failures) != 2 || loaded.Architecture.Failures[0].LossProbability != 1 || loaded.Architecture.Failures[1].ServiceMultiplier != 2 {
		t.Fatalf("failure settings lost: %+v", loaded.Architecture.Failures)
	}
	run, err := s.Run(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Result.FailuresTriggered != 2 || run.Result.EdgeDrops["link"] != 1 || run.Result.Nodes["service"].DegradedOperations != 1 {
		t.Fatalf("degradation run mismatch: %+v", run.Result)
	}
}
