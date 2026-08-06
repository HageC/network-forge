package sim

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestExperimentIsReproducibleAndVariesBySeed(t *testing.T) {
	in := sample()
	in.Edges[0].LossProbability = .4
	in.Workload.RequestsPerSecond = 30
	in.Workload.DurationSeconds = 2
	request := ExperimentInput{Simulation: in, Repetitions: 20, SLOP95Us: 1}
	a, err := Experiment(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Experiment(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("experiment changed with same seeds")
	}
	if len(a.Runs) != 20 || a.Summary.MeanErrorRate <= 0 || a.Summary.MeanErrorRate >= 1 {
		t.Fatalf("unexpected experiment: %+v", a.Summary)
	}
	if a.Runs[0].Seed != in.Workload.Seed || a.Runs[19].Seed != in.Workload.Seed+19 {
		t.Fatal("seed sequence incorrect")
	}
	different := false
	for _, run := range a.Runs[1:] {
		if run.ErrorRate != a.Runs[0].ErrorRate {
			different = true
			break
		}
	}
	if !different {
		t.Fatal("seed changes had no observed variation")
	}
	if a.Summary.SLOViolationProbability != 1 {
		t.Fatalf("SLO probability incorrect: %v", a.Summary.SLOViolationProbability)
	}
	if a.Summary.MeanP95ConfidenceLowUs > a.Summary.MeanP95ConfidenceHighUs || a.Summary.MeanP95ConfidenceLowUs < 0 {
		t.Fatalf("invalid confidence interval: %+v", a.Summary)
	}
}
func TestExperimentBudgetAndCancellation(t *testing.T) {
	in := sample()
	in.Workload.RequestsPerSecond = 10000
	in.Workload.DurationSeconds = 1
	in.Nodes = append(in.Nodes, Node{ID: "unused", Kind: "service", Concurrency: 1})
	_, err := Experiment(context.Background(), ExperimentInput{Simulation: in, Repetitions: 100, SLOP95Us: 1000})
	if err == nil || !strings.Contains(err.Error(), "budget") {
		t.Fatalf("expected budget error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Experiment(ctx, ExperimentInput{Simulation: sample(), Repetitions: 2, SLOP95Us: 1000})
	if err != context.Canceled {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
func TestExperimentAggregatesModeledCost(t *testing.T) {
	in := sample()
	in.Nodes[0].HourlyCost = 36
	in.Edges[0].CostPerMillionAttempts = 1000
	r, err := Experiment(context.Background(), ExperimentInput{Simulation: in, Repetitions: 3, SLOP95Us: 1000000})
	if err != nil {
		t.Fatal(err)
	}
	if r.Summary.MeanEstimatedScenarioCost <= .01 || r.Summary.MeanEstimatedHourlyCost <= 36 || r.Summary.CostPerSuccessfulRequest == nil {
		t.Fatalf("cost aggregation missing: %+v", r.Summary)
	}
}
