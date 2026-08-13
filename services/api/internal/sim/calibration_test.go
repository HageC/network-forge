package sim

import (
	"context"
	"testing"
)

func TestCalibrationSelectsMeasuredServiceScale(t *testing.T) {
	model := Input{Nodes: []Node{{ID: "service", Kind: "service", ServiceTimeUs: 100_000, Concurrency: 1, QueueCapacity: 10}}, Workload: Workload{Source: "service", RequestsPerSecond: 1, DurationSeconds: 1}}
	out, err := Calibrate(context.Background(), CalibrationInput{Simulation: model, Observed: ObservedMetrics{ThroughputRps: 1, ErrorRate: 0, P95Us: 200_000}})
	if err != nil {
		t.Fatal(err)
	}
	if out.ServiceTimeMultiplier != 2 || out.Calibrated.P95Us != 200_000 || out.Calibrated.Score >= out.Baseline.Score {
		t.Fatalf("calibration: %+v", out)
	}
}
