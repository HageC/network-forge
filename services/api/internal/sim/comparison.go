package sim

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"slices"
)

type ComparisonInput struct {
	Baseline    Input  `json:"baseline"`
	Candidate   Input  `json:"candidate"`
	Repetitions int    `json:"repetitions"`
	SLOP95Us    int64  `json:"sloP95Us"`
	Objective   string `json:"objective"`
}
type ComparisonResult struct {
	Objective                    string           `json:"objective"`
	Winner                       string           `json:"winner"`
	ObjectiveDelta               float64          `json:"objectiveDelta"`
	ObjectiveDeltaConfidenceLow  float64          `json:"objectiveDeltaConfidenceLow"`
	ObjectiveDeltaConfidenceHigh float64          `json:"objectiveDeltaConfidenceHigh"`
	ComparedPairs                int              `json:"comparedPairs"`
	Baseline                     ExperimentResult `json:"baseline"`
	Candidate                    ExperimentResult `json:"candidate"`
}

func Compare(ctx context.Context, in ComparisonInput) (ComparisonResult, error) {
	return CompareWithProgress(ctx, in, nil)
}

func CompareWithProgress(ctx context.Context, in ComparisonInput, progress func(done, total int)) (ComparisonResult, error) {
	if in.Objective != "throughput" && in.Objective != "p95_latency" && in.Objective != "error_rate" {
		return ComparisonResult{}, errors.New("objective must be throughput, p95_latency, or error_rate")
	}
	a, b := in.Baseline.Workload, in.Candidate.Workload
	if a.RequestsPerSecond != b.RequestsPerSecond || a.DurationSeconds != b.DurationSeconds || a.Seed != b.Seed {
		return ComparisonResult{}, errors.New("comparison requires identical request rate, duration and base seed")
	}
	if in.Repetitions < 2 || in.Repetitions > 100 {
		return ComparisonResult{}, errors.New("repetitions must be 2 to 100")
	}
	work := int64(a.RequestsPerSecond) * int64(a.DurationSeconds) * int64(in.Repetitions) * (int64(len(in.Baseline.Nodes))*int64(a.MaxApplicationRetries+1) + int64(len(in.Candidate.Nodes))*int64(b.MaxApplicationRetries+1))
	if work > 2_000_000 {
		return ComparisonResult{}, errors.New("comparison work budget exceeded")
	}
	base, err := ExperimentWithProgress(ctx, ExperimentInput{Simulation: in.Baseline, Repetitions: in.Repetitions, SLOP95Us: in.SLOP95Us}, func(done, _ int) {
		if progress != nil {
			progress(done, 2*in.Repetitions)
		}
	})
	if err != nil {
		return ComparisonResult{}, err
	}
	candidate, err := ExperimentWithProgress(ctx, ExperimentInput{Simulation: in.Candidate, Repetitions: in.Repetitions, SLOP95Us: in.SLOP95Us}, func(done, _ int) {
		if progress != nil {
			progress(in.Repetitions+done, 2*in.Repetitions)
		}
	})
	if err != nil {
		return ComparisonResult{}, err
	}
	out := ComparisonResult{Objective: in.Objective, Winner: "tie", Baseline: base, Candidate: candidate}
	if in.Objective == "p95_latency" && (base.Summary.CompletedLatencyRuns != in.Repetitions || candidate.Summary.CompletedLatencyRuns != in.Repetitions) {
		out.Winner = "indeterminate"
		return out, nil
	}
	deltas := make([]float64, 0, in.Repetitions)
	for i := range base.Runs {
		x, y := base.Runs[i], candidate.Runs[i]
		switch in.Objective {
		case "throughput":
			deltas = append(deltas, y.ThroughputRps-x.ThroughputRps)
		case "error_rate":
			deltas = append(deltas, y.ErrorRate-x.ErrorRate)
		case "p95_latency":
			deltas = append(deltas, float64(y.P95Us-x.P95Us))
		}
	}
	out.ComparedPairs = len(deltas)
	var sum float64
	allZero := true
	for _, delta := range deltas {
		sum += delta
		if delta != 0 {
			allZero = false
		}
	}
	out.ObjectiveDelta = sum / float64(len(deltas))
	if allZero {
		return out, nil
	}
	bootstrap := make([]float64, 1000)
	rng := rand.New(rand.NewSource(in.Baseline.Workload.Seed ^ 0x41a7c0de))
	for i := range bootstrap {
		var total float64
		for j := 0; j < len(deltas); j++ {
			total += deltas[rng.Intn(len(deltas))]
		}
		bootstrap[i] = total / float64(len(deltas))
	}
	slices.Sort(bootstrap)
	out.ObjectiveDeltaConfidenceLow = bootstrap[24]
	out.ObjectiveDeltaConfidenceHigh = bootstrap[974]
	if out.ObjectiveDeltaConfidenceLow <= 0 && out.ObjectiveDeltaConfidenceHigh >= 0 {
		out.Winner = "inconclusive"
		return out, nil
	}
	if (in.Objective == "throughput" && out.ObjectiveDelta > 0) || (in.Objective != "throughput" && out.ObjectiveDelta < 0) {
		out.Winner = "candidate"
	} else {
		out.Winner = "baseline"
	}
	if math.IsNaN(out.ObjectiveDelta) {
		return ComparisonResult{}, errors.New("invalid comparison metric")
	}
	return out, nil
}
