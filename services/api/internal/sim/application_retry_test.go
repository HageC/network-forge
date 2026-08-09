package sim

import (
	"context"
	"testing"
)

func retryFixture() Input {
	return Input{Nodes: []Node{{ID: "client", Kind: "client", Concurrency: 1}, {ID: "service", Kind: "service", Concurrency: 1, ServiceTimeUs: 100_000}}, Edges: []Edge{{ID: "link", Source: "client", Target: "service"}}, Workload: Workload{Source: "client", RequestsPerSecond: 1, DurationSeconds: 1, Seed: 1, MaxApplicationRetries: 2, ApplicationRetryBackoffUs: 100_000}}
}

func TestApplicationRetryRestartsCallerAfterPartition(t *testing.T) {
	in := retryFixture()
	in.Failures = []Failure{{ID: "partition", Kind: "edge_partition", TargetID: "link", StartUs: 0, DurationUs: 200_000}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Generated != 1 || r.Completed != 1 || r.Failed != 0 || r.ApplicationRetries != 2 || r.ApplicationRetryExhausted != 0 || r.Retries != 0 || r.P95Us != 400_000 {
		t.Fatalf("unexpected retry recovery: %+v", r)
	}
	if r.Nodes["client"].Completed != 3 {
		t.Fatalf("source was not reprocessed: %+v", r.Nodes["client"])
	}
	if r.Traces[0].Events[len(r.Traces[0].Events)-1].Kind != "completed" {
		t.Fatalf("trace missing terminal event: %+v", r.Traces[0])
	}
}

func TestApplicationRetryExhaustionAndHorizon(t *testing.T) {
	in := retryFixture()
	in.Edges[0].LossProbability = 1
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Generated != 1 || r.Failed != 1 || r.ApplicationRetries != 2 || r.ApplicationRetryExhausted != 1 || r.EdgeAttempts["link"] != 3 {
		t.Fatalf("unexpected exhausted retries: %+v", r)
	}
	in.Workload.ApplicationRetryBackoffUs = 2_000_000
	r, err = Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Failed != 0 || r.TimedOut != 1 || r.ApplicationRetries != 1 || r.ApplicationRetryExhausted != 0 {
		t.Fatalf("retry beyond horizon should time out: %+v", r)
	}
}

func TestApplicationRetryDoesNotRestartAcknowledgedQueueJob(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "q", Kind: "queue", Concurrency: 1}, {ID: "worker", Kind: "worker", Concurrency: 1}}, Edges: []Edge{{ID: "dispatch", Source: "q", Target: "worker", LossProbability: 1}}, Workload: Workload{Source: "q", RequestsPerSecond: 1, DurationSeconds: 1, MaxApplicationRetries: 2, ApplicationRetryBackoffUs: 100_000}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 1 || r.BackgroundFailed != 1 || r.ApplicationRetries != 0 {
		t.Fatalf("background job retried as caller: %+v", r)
	}
}

func TestApplicationRetryPolicyValidation(t *testing.T) {
	in := retryFixture()
	in.Workload.ApplicationRetryBackoffUs = 0
	if Validate(in) == nil {
		t.Fatal("zero backoff accepted")
	}
	in.Workload.ApplicationRetryBackoffUs = 100_000
	in.Workload.MaxApplicationRetries = 4
	if Validate(in) == nil {
		t.Fatal("unbounded retry count accepted")
	}
}

func TestComparisonCanMeasureApplicationRetryPolicy(t *testing.T) {
	baseline := retryFixture()
	baseline.Workload.MaxApplicationRetries = 0
	baseline.Workload.ApplicationRetryBackoffUs = 0
	baseline.Failures = []Failure{{ID: "partition", Kind: "edge_partition", TargetID: "link", StartUs: 0, DurationUs: 200_000}}
	candidate := baseline
	candidate.Workload.MaxApplicationRetries = 2
	candidate.Workload.ApplicationRetryBackoffUs = 100_000
	r, err := Compare(context.Background(), ComparisonInput{Baseline: baseline, Candidate: candidate, Repetitions: 3, SLOP95Us: 1_000_000, Objective: "throughput"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Winner != "candidate" || r.Baseline.Summary.MeanThroughputRps != 0 || r.Candidate.Summary.MeanThroughputRps != 1 {
		t.Fatalf("retry comparison mismatch: %+v", r)
	}
}
