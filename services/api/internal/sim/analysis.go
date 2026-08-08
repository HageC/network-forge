package sim

import "slices"

type Evidence struct {
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
	Unit   string  `json:"unit"`
}
type Finding struct {
	ID             string     `json:"id"`
	Title          string     `json:"title"`
	ComponentID    string     `json:"componentId,omitempty"`
	Severity       string     `json:"severity"`
	Confidence     string     `json:"confidence"`
	Score          float64    `json:"score"`
	Evidence       []Evidence `json:"evidence"`
	Recommendation string     `json:"recommendation"`
	ExpectedImpact string     `json:"expectedImpact"`
	Tradeoffs      string     `json:"tradeoffs"`
}

// Analyze applies documented heuristics to observed simulation metrics only.
func Analyze(in Input, result Result) []Finding {
	findings := []Finding{}
	nodes := slices.Clone(in.Nodes)
	slices.SortFunc(nodes, func(a, b Node) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	for _, node := range nodes {
		m := result.Nodes[node.ID]
		if node.Kind == "database" && m.ConnectionRejected > 0 {
			findings = append(findings, Finding{ID: "database_connections:" + node.ID, Title: "Database connection limit rejected work", ComponentID: node.ID, Severity: "high", Confidence: "high", Score: 90 + float64(m.ConnectionRejected), Evidence: []Evidence{{Metric: "connection_utilization", Value: m.ConnectionUtilization * 100, Unit: "percent"}, {Metric: "connection_waits", Value: float64(m.ConnectionWaits), Unit: "requests"}, {Metric: "connection_rejected", Value: float64(m.ConnectionRejected), Unit: "requests"}}, Recommendation: "Compare a larger connection limit or faster database operations under the same workload.", ExpectedImpact: "May reduce connection waits and rejections; rerun to quantify.", Tradeoffs: "More connections can increase database resource use."})
		}
		if m.Utilization >= .85 && (m.P95QueueWaitUs > 0 || m.QueueRejected > 0) {
			severity := "moderate"
			if m.Utilization >= .95 || m.QueueRejected > 0 {
				severity = "high"
			}
			findings = append(findings, Finding{ID: "capacity:" + node.ID, Title: "Capacity saturation with queue pressure", ComponentID: node.ID, Severity: severity, Confidence: "high", Score: 100*m.Utilization + float64(m.QueueRejected), Evidence: []Evidence{{Metric: "utilization", Value: m.Utilization * 100, Unit: "percent"}, {Metric: "peak_queue", Value: float64(m.PeakQueue), Unit: "requests"}, {Metric: "p95_queue_wait", Value: float64(m.P95QueueWaitUs) / 1000, Unit: "ms"}, {Metric: "queue_rejected", Value: float64(m.QueueRejected), Unit: "requests"}}, Recommendation: "Compare more concurrent capacity or lower service time under the same workload.", ExpectedImpact: "May reduce queue delay and rejections; rerun to quantify the change.", Tradeoffs: "Additional capacity may increase configured cost."})
		} else if m.QueueRejected > 0 {
			findings = append(findings, Finding{ID: "queue:" + node.ID, Title: "Queue capacity rejected requests", ComponentID: node.ID, Severity: "moderate", Confidence: "high", Score: 50 + float64(m.QueueRejected), Evidence: []Evidence{{Metric: "queue_rejected", Value: float64(m.QueueRejected), Unit: "requests"}, {Metric: "peak_queue", Value: float64(m.PeakQueue), Unit: "requests"}}, Recommendation: "Compare a larger queue or faster service and inspect wait time alongside completions.", ExpectedImpact: "May accept more bursts, subject to processing capacity.", Tradeoffs: "Larger queues can increase tail latency."})
		}
		if m.RateLimited > 0 {
			findings = append(findings, Finding{ID: "rate_limit:" + node.ID, Title: "Gateway rate limit rejected traffic", ComponentID: node.ID, Severity: "moderate", Confidence: "high", Score: 40 + float64(m.RateLimited), Evidence: []Evidence{{Metric: "rate_limited", Value: float64(m.RateLimited), Unit: "requests"}}, Recommendation: "Check the rate-limit policy against the intended workload before changing it.", ExpectedImpact: "Changing the limit changes admitted traffic; rerun to measure downstream load.", Tradeoffs: "Higher admission may move overload downstream."})
		}
		if m.FailureDrops > 0 {
			findings = append(findings, Finding{ID: "outage:" + node.ID, Title: "Scheduled outage failed work", ComponentID: node.ID, Severity: "high", Confidence: "high", Score: 80 + float64(m.FailureDrops), Evidence: []Evidence{{Metric: "failure_drops", Value: float64(m.FailureDrops), Unit: "requests"}, {Metric: "outage_duration", Value: float64(m.OutageUs) / 1_000_000, Unit: "seconds"}}, Recommendation: "Compare the same workload with and without this outage, then test a resilience variant.", ExpectedImpact: "A resilience change may preserve completions; only a rerun can quantify it.", Tradeoffs: "Redundancy and recovery controls add complexity and cost."})
		}
		if node.Kind == "cache" && m.CacheHits+m.CacheMisses >= 10 {
			ratio := float64(m.CacheMisses) / float64(m.CacheHits+m.CacheMisses)
			if ratio >= .7 {
				findings = append(findings, Finding{ID: "cache_miss:" + node.ID, Title: "High modeled cache miss ratio", ComponentID: node.ID, Severity: "moderate", Confidence: "high", Score: ratio * 70, Evidence: []Evidence{{Metric: "cache_miss_ratio", Value: ratio * 100, Unit: "percent"}, {Metric: "cache_misses", Value: float64(m.CacheMisses), Unit: "requests"}}, Recommendation: "Validate the configured hit probability and compare a higher-hit scenario against origin utilization.", ExpectedImpact: "Higher hit rates may bypass more origin operations; rerun to measure.", Tradeoffs: "A higher assumed hit rate must be justified by the workload."})
			}
		}
	}
	edges := slices.Clone(in.Edges)
	slices.SortFunc(edges, func(a, b Edge) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	for _, edge := range edges {
		attempts := result.EdgeAttempts[edge.ID]
		drops := result.EdgeDrops[edge.ID]
		if attempts >= 10 && drops > 0 {
			ratio := float64(drops) / float64(attempts)
			if ratio >= .05 {
				findings = append(findings, Finding{ID: "link:" + edge.ID, Title: "Failed network sends", ComponentID: edge.ID, Severity: "moderate", Confidence: "high", Score: ratio * 80, Evidence: []Evidence{{Metric: "failed_send_ratio", Value: ratio * 100, Unit: "percent"}, {Metric: "failed_sends", Value: float64(drops), Unit: "attempts"}, {Metric: "send_attempts", Value: float64(attempts), Unit: "attempts"}}, Recommendation: "Inspect packet-loss and partition settings; compare retry and circuit policies under the same seed.", ExpectedImpact: "A policy change may alter success rate and retry load; rerun to measure.", Tradeoffs: "Retries can amplify traffic and latency."})
			}
		}
	}
	if result.Generated > 0 && result.Retries > 0 {
		ratio := float64(result.Retries) / float64(result.Generated)
		if ratio >= .2 {
			findings = append(findings, Finding{ID: "retry_amplification", Title: "Retry traffic amplified the workload", Severity: "moderate", Confidence: "high", Score: ratio * 65, Evidence: []Evidence{{Metric: "retries_per_generated_request", Value: ratio, Unit: "ratio"}, {Metric: "retries", Value: float64(result.Retries), Unit: "attempts"}}, Recommendation: "Compare lower retry limits or improved link reliability while monitoring success rate and tail latency.", ExpectedImpact: "May reduce repeated sends; rerun to measure completions.", Tradeoffs: "Fewer retries can expose transient failures to callers."})
		}
	}
	if result.Generated > 0 && result.ApplicationRetries > 0 {
		ratio := float64(result.ApplicationRetries) / float64(result.Generated)
		if ratio >= .2 {
			findings = append(findings, Finding{ID: "application_retry_amplification", Title: "Application retries reprocessed caller work", Severity: "moderate", Confidence: "high", Score: ratio * 70, Evidence: []Evidence{{Metric: "application_retries_per_generated_request", Value: ratio, Unit: "ratio"}, {Metric: "application_retries", Value: float64(result.ApplicationRetries), Unit: "attempts"}, {Metric: "retry_exhausted", Value: float64(result.ApplicationRetryExhausted), Unit: "requests"}}, Recommendation: "Compare the retry policy against the same failure scenario and inspect downstream load, success rate and tail latency.", ExpectedImpact: "A retry budget may recover transient failures; rerun to quantify.", Tradeoffs: "Reprocessing amplifies node work and can worsen overload."})
		}
	}
	if result.BackgroundFailed > 0 || result.BackgroundTimedOut > 0 {
		findings = append(findings, Finding{ID: "background_jobs", Title: "Acknowledged jobs did not complete", Severity: "high", Confidence: "high", Score: 85 + float64(result.BackgroundFailed+result.BackgroundTimedOut), Evidence: []Evidence{{Metric: "background_failed", Value: float64(result.BackgroundFailed), Unit: "jobs"}, {Metric: "background_timed_out", Value: float64(result.BackgroundTimedOut), Unit: "jobs"}}, Recommendation: "Inspect worker capacity, queue dispatch reliability and the simulated time horizon.", ExpectedImpact: "A worker or transport change may improve completed jobs; rerun to measure.", Tradeoffs: "Caller success at enqueue does not guarantee background completion."})
	}
	slices.SortFunc(findings, func(a, b Finding) int {
		if a.Score > b.Score {
			return -1
		}
		if a.Score < b.Score {
			return 1
		}
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return findings
}
