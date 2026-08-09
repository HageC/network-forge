package sim

import "testing"

func findRule(items []Finding, id string) *Finding {
	for i := range items {
		if items[i].ID == id {
			return &items[i]
		}
	}
	return nil
}
func TestFindingsUseObservedMetrics(t *testing.T) {
	in := sample()
	in.Nodes[1].ServiceTimeUs = 500_000
	in.Nodes[1].QueueCapacity = 1
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	f := findRule(r.Findings, "capacity:service")
	if f == nil {
		t.Fatalf("capacity finding missing: %+v", r.Findings)
	}
	if f.ComponentID != "service" || len(f.Evidence) < 4 || f.Evidence[0].Value != r.Nodes["service"].Utilization*100 || f.Evidence[3].Value != float64(r.Nodes["service"].QueueRejected) {
		t.Fatalf("finding evidence differs from metrics: %+v", f)
	}
	for i := 1; i < len(r.Findings); i++ {
		if r.Findings[i].Score > r.Findings[i-1].Score {
			t.Fatal("findings not ranked")
		}
	}
}
func TestNoUnsupportedFindingForHealthyModel(t *testing.T) {
	in := sample()
	in.Nodes[1].ServiceTimeUs = 1000
	in.Nodes[1].Concurrency = 10
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Findings) != 0 {
		t.Fatalf("unexpected finding: %+v", r.Findings)
	}
}
func TestFailedLinkFindingUsesAttemptDenominator(t *testing.T) {
	in := sample()
	in.Edges[0].LossProbability = 1
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	f := findRule(r.Findings, "link:path")
	if f == nil {
		t.Fatalf("link finding missing: %+v", r.Findings)
	}
	if f.Evidence[0].Value != 100 || f.Evidence[2].Value != float64(r.EdgeAttempts["path"]) {
		t.Fatalf("link denominator incorrect: %+v", f)
	}
}
