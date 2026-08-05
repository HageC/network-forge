package sim

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"slices"
)

type ExperimentInput struct {
	Simulation  Input `json:"simulation"`
	Repetitions int   `json:"repetitions"`
	SLOP95Us    int64 `json:"sloP95Us"`
}
type ExperimentRun struct {
	Seed                  int64   `json:"seed"`
	Generated             int     `json:"generated"`
	Completed             int     `json:"completed"`
	Failed                int     `json:"failed"`
	TimedOut              int     `json:"timedOut"`
	P95Us                 int64   `json:"p95Us"`
	ThroughputRps         float64 `json:"throughputRps"`
	ErrorRate             float64 `json:"errorRate"`
	SLOViolated           bool    `json:"sloViolated"`
	EstimatedScenarioCost float64 `json:"estimatedScenarioCost"`
	EstimatedHourlyCost   float64 `json:"estimatedHourlyCost"`
}
type ExperimentSummary struct {
	Repetitions               int      `json:"repetitions"`
	CompletedLatencyRuns      int      `json:"completedLatencyRuns"`
	MeanP95Us                 float64  `json:"meanP95Us"`
	StddevP95Us               float64  `json:"stddevP95Us"`
	OutcomeP05P95Us           int64    `json:"outcomeP05P95Us"`
	OutcomeP95P95Us           int64    `json:"outcomeP95P95Us"`
	MeanP95ConfidenceLowUs    int64    `json:"meanP95ConfidenceLowUs"`
	MeanP95ConfidenceHighUs   int64    `json:"meanP95ConfidenceHighUs"`
	MeanThroughputRps         float64  `json:"meanThroughputRps"`
	MeanErrorRate             float64  `json:"meanErrorRate"`
	SLOViolationProbability   float64  `json:"sloViolationProbability"`
	MeanEstimatedScenarioCost float64  `json:"meanEstimatedScenarioCost"`
	MeanEstimatedHourlyCost   float64  `json:"meanEstimatedHourlyCost"`
	CostPerSuccessfulRequest  *float64 `json:"costPerSuccessfulRequest"`
}
type ExperimentResult struct {
	Summary ExperimentSummary `json:"summary"`
	Runs    []ExperimentRun   `json:"runs"`
}

func Experiment(ctx context.Context, in ExperimentInput) (ExperimentResult, error) {
	return ExperimentWithProgress(ctx, in, nil)
}

func ExperimentWithProgress(ctx context.Context, in ExperimentInput, progress func(done, total int)) (ExperimentResult, error) {
	if in.Repetitions < 2 || in.Repetitions > 100 || in.SLOP95Us < 1 || in.SLOP95Us > 60_000_000 {
		return ExperimentResult{}, errors.New("repetitions must be 2 to 100 and SLO P95 must be 1 to 60000000 microseconds")
	}
	if err := Validate(in.Simulation); err != nil {
		return ExperimentResult{}, err
	}
	work := int64(in.Simulation.Workload.RequestsPerSecond) * int64(in.Simulation.Workload.DurationSeconds) * int64(len(in.Simulation.Nodes)) * int64(in.Repetitions) * int64(in.Simulation.Workload.MaxApplicationRetries+1)
	if work > 2_000_000 {
		return ExperimentResult{}, errors.New("experiment work budget exceeded; reduce rate, duration, topology or repetitions")
	}
	out := ExperimentResult{Runs: make([]ExperimentRun, 0, in.Repetitions)}
	latencies := []int64{}
	violations := 0
	totalCompleted := 0
	totalCost := float64(0)
	for i := 0; i < in.Repetitions; i++ {
		if err := ctx.Err(); err != nil {
			return ExperimentResult{}, err
		}
		model := in.Simulation
		model.Workload.Seed = in.Simulation.Workload.Seed + int64(i)
		result, err := RunContext(ctx, model)
		if err != nil {
			return ExperimentResult{}, err
		}
		run := ExperimentRun{Seed: model.Workload.Seed, Generated: result.Generated, Completed: result.Completed, Failed: result.Failed, TimedOut: result.TimedOut, P95Us: result.P95Us, ThroughputRps: float64(result.Completed) / float64(model.Workload.DurationSeconds), ErrorRate: float64(result.Failed+result.TimedOut) / float64(result.Generated), EstimatedScenarioCost: result.EstimatedScenarioCost, EstimatedHourlyCost: result.EstimatedHourlyCost}
		totalCompleted += result.Completed
		totalCost += result.EstimatedScenarioCost
		if result.Completed > 0 {
			latencies = append(latencies, result.P95Us)
		}
		run.SLOViolated = result.Completed == 0 || result.P95Us > in.SLOP95Us
		if run.SLOViolated {
			violations++
		}
		out.Runs = append(out.Runs, run)
		if progress != nil {
			progress(i+1, in.Repetitions)
		}
		out.Summary.MeanThroughputRps += run.ThroughputRps
		out.Summary.MeanErrorRate += run.ErrorRate
		out.Summary.MeanEstimatedScenarioCost += run.EstimatedScenarioCost
		out.Summary.MeanEstimatedHourlyCost += run.EstimatedHourlyCost
	}
	out.Summary.Repetitions = in.Repetitions
	out.Summary.CompletedLatencyRuns = len(latencies)
	out.Summary.MeanThroughputRps /= float64(in.Repetitions)
	out.Summary.MeanErrorRate /= float64(in.Repetitions)
	out.Summary.MeanEstimatedScenarioCost /= float64(in.Repetitions)
	out.Summary.MeanEstimatedHourlyCost /= float64(in.Repetitions)
	if totalCompleted > 0 {
		value := totalCost / float64(totalCompleted)
		out.Summary.CostPerSuccessfulRequest = &value
	}
	out.Summary.SLOViolationProbability = float64(violations) / float64(in.Repetitions)
	if len(latencies) > 0 {
		slices.Sort(latencies)
		var sum float64
		for _, v := range latencies {
			sum += float64(v)
		}
		out.Summary.MeanP95Us = sum / float64(len(latencies))
		if len(latencies) > 1 {
			var squared float64
			for _, v := range latencies {
				diff := float64(v) - out.Summary.MeanP95Us
				squared += diff * diff
			}
			out.Summary.StddevP95Us = math.Sqrt(squared / float64(len(latencies)-1))
		}
		quantile := func(p float64) int64 { idx := int(math.Ceil(p*float64(len(latencies)))) - 1; return latencies[idx] }
		out.Summary.OutcomeP05P95Us = quantile(.05)
		out.Summary.OutcomeP95P95Us = quantile(.95)
		bootstrap := make([]float64, 1000)
		bootstrapRNG := rand.New(rand.NewSource(in.Simulation.Workload.Seed ^ 0x5eed5eed))
		for i := range bootstrap {
			var total float64
			for j := 0; j < len(latencies); j++ {
				total += float64(latencies[bootstrapRNG.Intn(len(latencies))])
			}
			bootstrap[i] = total / float64(len(latencies))
		}
		slices.Sort(bootstrap)
		out.Summary.MeanP95ConfidenceLowUs = int64(math.Round(bootstrap[24]))
		out.Summary.MeanP95ConfidenceHighUs = int64(math.Round(bootstrap[974]))
	}
	return out, nil
}
