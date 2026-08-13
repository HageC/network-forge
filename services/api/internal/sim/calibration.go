package sim

import (
	"context"
	"errors"
	"math"
)

// Calibration fits one shared service-time multiplier to measured aggregate outcomes.
// It is a diagnostic scenario, not an estimate of an individual node's true latency.
type ObservedMetrics struct {
	ThroughputRps float64 `json:"throughputRps"`
	ErrorRate     float64 `json:"errorRate"`
	P95Us         int64   `json:"p95Us"`
}
type CalibrationInput struct {
	Simulation Input           `json:"simulation"`
	Observed   ObservedMetrics `json:"observed"`
}
type CalibrationPoint struct {
	ThroughputRps float64 `json:"throughputRps"`
	ErrorRate     float64 `json:"errorRate"`
	P95Us         int64   `json:"p95Us"`
	Score         float64 `json:"score"`
}
type CalibrationResult struct {
	Observed              ObservedMetrics  `json:"observed"`
	Baseline              CalibrationPoint `json:"baseline"`
	Calibrated            CalibrationPoint `json:"calibrated"`
	ServiceTimeMultiplier float64          `json:"serviceTimeMultiplier"`
	CandidatesEvaluated   int              `json:"candidatesEvaluated"`
}

func Calibrate(ctx context.Context, in CalibrationInput) (CalibrationResult, error) {
	if err := Validate(in.Simulation); err != nil {
		return CalibrationResult{}, err
	}
	o := in.Observed
	if math.IsNaN(o.ThroughputRps) || math.IsInf(o.ThroughputRps, 0) || o.ThroughputRps < 0 || o.ThroughputRps > float64(in.Simulation.Workload.RequestsPerSecond) || math.IsNaN(o.ErrorRate) || math.IsInf(o.ErrorRate, 0) || o.ErrorRate < 0 || o.ErrorRate > 1 || o.P95Us < 0 || o.P95Us > 60_000_000 {
		return CalibrationResult{}, errors.New("observed metrics outside limits")
	}
	multipliers := []float64{0.25, 0.5, 0.75, 1, 1.25, 1.5, 2, 3, 4}
	work := int64(in.Simulation.Workload.RequestsPerSecond) * int64(in.Simulation.Workload.DurationSeconds) * int64(len(in.Simulation.Nodes)) * int64(len(multipliers)) * int64(in.Simulation.Workload.MaxApplicationRetries+1)
	if work > 2_000_000 {
		return CalibrationResult{}, errors.New("calibration work budget exceeded")
	}
	point := func(r Result) CalibrationPoint {
		p := CalibrationPoint{ThroughputRps: float64(r.Completed) / float64(in.Simulation.Workload.DurationSeconds), ErrorRate: float64(r.Failed+r.TimedOut) / float64(r.Generated), P95Us: r.P95Us}
		rel := func(actual, target, floor float64) float64 { return math.Abs(actual-target) / math.Max(target, floor) }
		p.Score = 0.3*rel(p.ThroughputRps, o.ThroughputRps, 1) + 0.3*rel(p.ErrorRate, o.ErrorRate, 0.01) + 0.4*rel(float64(p.P95Us), float64(o.P95Us), 1000)
		return p
	}
	out := CalibrationResult{Observed: o, ServiceTimeMultiplier: 1}
	for _, multiplier := range multipliers {
		model := in.Simulation
		model.Nodes = append([]Node(nil), in.Simulation.Nodes...)
		valid := true
		for i := range model.Nodes {
			n := &model.Nodes[i]
			if n.Kind == "client" {
				continue
			}
			n.ServiceTimeUs = int64(math.Round(float64(n.ServiceTimeUs) * multiplier))
			n.TransactionLockUs = int64(math.Round(float64(n.TransactionLockUs) * multiplier))
			if n.ServiceTimeUs > 60_000_000 {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		r, err := RunContext(ctx, model)
		if err != nil {
			return CalibrationResult{}, err
		}
		p := point(r)
		out.CandidatesEvaluated++
		if multiplier == 1 {
			out.Baseline = p
		}
		if out.CandidatesEvaluated == 1 || p.Score < out.Calibrated.Score {
			out.Calibrated = p
			out.ServiceTimeMultiplier = multiplier
		}
	}
	return out, nil
}
