package sim

import (
	"context"
	"testing"
)

func TestSimulationProgressEndsAtGeneratedCount(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "s", Kind: "service", Concurrency: 100}}, Workload: Workload{Source: "s", RequestsPerSecond: 100, DurationSeconds: 1}}
	last := 0
	calls := 0
	result, err := RunContextWithProgress(context.Background(), in, func(done, total int) {
		if total != 100 || done < last || done > total {
			t.Errorf("invalid progress %d/%d after %d", done, total, last)
		}
		last = done
		calls++
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls < 2 || last != result.Generated {
		t.Fatalf("progress stopped early: calls=%d last=%d generated=%d", calls, last, result.Generated)
	}
}

func TestExperimentAndComparisonProgress(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "s", Kind: "service", Concurrency: 1}}, Workload: Workload{Source: "s", RequestsPerSecond: 1, DurationSeconds: 1}}
	steps := []int{}
	_, err := ExperimentWithProgress(context.Background(), ExperimentInput{Simulation: in, Repetitions: 3, SLOP95Us: 1000}, func(done, total int) {
		if total != 3 {
			t.Errorf("total=%d", total)
		}
		steps = append(steps, done)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 3 || steps[0] != 1 || steps[2] != 3 {
		t.Fatalf("experiment steps: %v", steps)
	}
	steps = nil
	_, err = CompareWithProgress(context.Background(), ComparisonInput{Baseline: in, Candidate: in, Repetitions: 2, SLOP95Us: 1000, Objective: "throughput"}, func(done, total int) {
		if total != 4 {
			t.Errorf("total=%d", total)
		}
		steps = append(steps, done)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 4 || steps[0] != 1 || steps[3] != 4 {
		t.Fatalf("comparison steps: %v", steps)
	}
}
