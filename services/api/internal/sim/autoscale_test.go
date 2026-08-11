package sim

import "testing"

func TestAutoscalingImprovesBurstCapacityAndTracksCost(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "service", Kind: "service", ServiceTimeUs: 500_000, Concurrency: 1, QueueCapacity: 10, HourlyCost: 36}}, Workload: Workload{Source: "service", RequestsPerSecond: 10, DurationSeconds: 2, Seed: 1}}
	fixed, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	in.Nodes[0].AutoscaleMaxConcurrency = 5
	in.Nodes[0].AutoscaleQueueThreshold = 1
	in.Nodes[0].AutoscaleDelayUs = 100_000
	in.Nodes[0].AutoscaleIdleUs = 200_000
	scaled, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	m := scaled.Nodes["service"]
	if m.ScaleUps == 0 || m.PeakConcurrency <= 1 || scaled.Completed <= fixed.Completed {
		t.Fatalf("scaling did not improve capacity: fixed=%+v scaled=%+v", fixed, scaled)
	}
	if scaled.EstimatedScenarioCost <= fixed.EstimatedScenarioCost {
		t.Fatalf("scale cost not modeled: fixed=%f scaled=%f", fixed.EstimatedScenarioCost, scaled.EstimatedScenarioCost)
	}
	if scaled.Completed+scaled.Failed+scaled.TimedOut != scaled.Generated {
		t.Fatalf("bad accounting: %+v", scaled)
	}
}

func TestAutoscaleReturnsToBaseAfterPressure(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "s", Kind: "service", ServiceTimeUs: 300_000, Concurrency: 1, QueueCapacity: 5, AutoscaleMaxConcurrency: 3, AutoscaleQueueThreshold: 1, AutoscaleDelayUs: 50_000, AutoscaleIdleUs: 100_000}}, Workload: Workload{Source: "s", RequestsPerSecond: 4, DurationSeconds: 2, Seed: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Nodes["s"].ScaleUps == 0 || r.Nodes["s"].ScaleDowns == 0 {
		t.Fatalf("expected scale up and down: %+v", r.Nodes["s"])
	}
}

func TestAutoscaleValidation(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "s", Kind: "service", Concurrency: 2, QueueCapacity: 2, AutoscaleMaxConcurrency: 2, AutoscaleQueueThreshold: 1}}, Workload: Workload{Source: "s", RequestsPerSecond: 1, DurationSeconds: 1}}
	if Validate(in) == nil {
		t.Fatal("maximum equal to initial capacity accepted")
	}
	in.Nodes[0].AutoscaleMaxConcurrency = 4
	in.Nodes[0].AutoscaleQueueThreshold = 4
	if Validate(in) == nil {
		t.Fatal("unreachable threshold accepted")
	}
}

func TestAutoscaleDoesNotFireWithoutPressureOrBeforeDelay(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "s", Kind: "service", ServiceTimeUs: 100_000, Concurrency: 1, QueueCapacity: 3, AutoscaleMaxConcurrency: 3, AutoscaleQueueThreshold: 1, AutoscaleDelayUs: 100_000}}, Workload: Workload{Source: "s", RequestsPerSecond: 1, DurationSeconds: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Nodes["s"].ScaleUps != 0 {
		t.Fatalf("scaled without pressure: %+v", r.Nodes["s"])
	}
	in.Workload.RequestsPerSecond = 2
	in.Nodes[0].ServiceTimeUs = 1_000_000
	in.Nodes[0].AutoscaleQueueThreshold = 2
	r, err = Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Nodes["s"].ScaleUps != 0 {
		t.Fatalf("scaled below threshold: %+v", r.Nodes["s"])
	}
	in.Nodes[0].AutoscaleQueueThreshold = 1
	in.Workload.RequestsPerSecond = 10
	in.Nodes[0].ServiceTimeUs = 500_000
	in.Nodes[0].AutoscaleDelayUs = 2_000_000
	r, err = Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Nodes["s"].ScaleUps != 0 {
		t.Fatalf("scaled after time horizon: %+v", r.Nodes["s"])
	}
}
