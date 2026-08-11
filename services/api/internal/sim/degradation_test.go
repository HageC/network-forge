package sim

import "testing"

func TestNodeDegradationAffectsOnlyOperationsStartedInsideWindow(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "s", Kind: "service", ServiceTimeUs: 100_000, Concurrency: 1}}, Workload: Workload{Source: "s", RequestsPerSecond: 2, DurationSeconds: 1}, Failures: []Failure{{ID: "slow", Kind: "node_degradation", TargetID: "s", StartUs: 0, DurationUs: 500_000, ServiceMultiplier: 2}}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 2 || r.P95Us != 200_000 || r.Nodes["s"].DegradedOperations != 1 || r.FailuresTriggered != 1 {
		t.Fatalf("degradation mismatch: %+v", r)
	}
}

func TestEdgeLossSpikeRecoversAtEndBoundary(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "a", Kind: "client", Concurrency: 1}, {ID: "b", Kind: "service", Concurrency: 1}}, Edges: []Edge{{ID: "link", Source: "a", Target: "b"}}, Workload: Workload{Source: "a", RequestsPerSecond: 2, DurationSeconds: 1, Seed: 1}, Failures: []Failure{{ID: "loss", Kind: "edge_loss_spike", TargetID: "link", StartUs: 0, DurationUs: 500_000, LossProbability: 1}}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 1 || r.Failed != 1 || r.EdgeDrops["link"] != 1 || r.FailuresTriggered != 1 {
		t.Fatalf("loss spike mismatch: %+v", r)
	}
}

func TestDegradationSettingsAreValidated(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "s", Kind: "service", Concurrency: 1}}, Workload: Workload{Source: "s", RequestsPerSecond: 1, DurationSeconds: 1}, Failures: []Failure{{ID: "slow", Kind: "node_degradation", TargetID: "s", StartUs: 0, DurationUs: 100_000, ServiceMultiplier: 0.5}}}
	if Validate(in) == nil {
		t.Fatal("speed-up accepted as degradation")
	}
	in.Failures[0].ServiceMultiplier = 2
	if Validate(in) != nil {
		t.Fatal("valid degradation rejected")
	}
}
