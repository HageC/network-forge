package sim

import (
	"context"
	"strings"
	"testing"
)

func TestComparisonUsesCommonWorkloadAndObjective(t *testing.T) {
	base := sample()
	base.Nodes[1].ServiceTimeUs = 100_000
	base.Nodes[1].QueueCapacity = 100
	base.Workload.RequestsPerSecond = 20
	base.Workload.DurationSeconds = 2
	candidate := base
	candidate.Nodes = append([]Node(nil), base.Nodes...)
	candidate.Nodes[1].ServiceTimeUs = 10_000
	in := ComparisonInput{Baseline: base, Candidate: candidate, Repetitions: 3, SLOP95Us: 500_000, Objective: "throughput"}
	r, err := Compare(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Winner != "candidate" || r.ObjectiveDelta <= 0 || r.Candidate.Summary.MeanThroughputRps <= r.Baseline.Summary.MeanThroughputRps {
		t.Fatalf("comparison incorrect: %+v", r)
	}
	if r.ObjectiveDeltaConfidenceLow <= 0 || r.ComparedPairs != 3 {
		t.Fatalf("paired interval incorrect: %+v", r)
	}
	in.Objective = "p95_latency"
	r, err = Compare(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Winner != "candidate" || r.ObjectiveDelta >= 0 {
		t.Fatalf("latency objective incorrect: %+v", r)
	}
}
func TestComparisonTieAndUndefinedLatency(t *testing.T) {
	in := ComparisonInput{Baseline: sample(), Candidate: sample(), Repetitions: 3, SLOP95Us: 1000, Objective: "error_rate"}
	r, err := Compare(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Winner != "tie" || r.ObjectiveDelta != 0 {
		t.Fatalf("identical models did not tie: %+v", r)
	}
	in.Objective = "p95_latency"
	in.Baseline.Nodes[1].ServiceTimeUs = 2_000_000
	in.Candidate.Nodes = append([]Node(nil), in.Baseline.Nodes...)
	r, err = Compare(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Winner != "indeterminate" {
		t.Fatalf("undefined latency should be indeterminate: %+v", r)
	}
}
func TestComparisonRejectsUnfairWorkload(t *testing.T) {
	in := ComparisonInput{Baseline: sample(), Candidate: sample(), Repetitions: 2, SLOP95Us: 1000, Objective: "error_rate"}
	in.Candidate.Workload.Seed++
	_, err := Compare(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "identical") {
		t.Fatalf("expected workload mismatch, got %v", err)
	}
}
