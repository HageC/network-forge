package sim

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
)

type Node struct {
	ID                      string  `json:"id"`
	Kind                    string  `json:"kind"`
	ServiceTimeUs           int64   `json:"serviceTimeUs"`
	Concurrency             int     `json:"concurrency"`
	QueueCapacity           int     `json:"queueCapacity"`
	ConnectionLimit         int     `json:"connectionLimit"`
	TransactionLockUs       int64   `json:"transactionLockUs"`
	BrokerCapacity          int     `json:"brokerCapacity"`
	MaxDeliveries           int     `json:"maxDeliveries"`
	RedeliveryDelayUs       int64   `json:"redeliveryDelayUs"`
	AutoscaleMaxConcurrency int     `json:"autoscaleMaxConcurrency"`
	AutoscaleQueueThreshold int     `json:"autoscaleQueueThreshold"`
	AutoscaleDelayUs        int64   `json:"autoscaleDelayUs"`
	AutoscaleIdleUs         int64   `json:"autoscaleIdleUs"`
	RateLimitRps            int     `json:"rateLimitRps"`
	HitRate                 float64 `json:"hitRate"`
	HourlyCost              float64 `json:"hourlyCost"`
}
type Edge struct {
	ID                      string  `json:"id"`
	Source                  string  `json:"source"`
	Target                  string  `json:"target"`
	LatencyUs               int64   `json:"latencyUs"`
	LossProbability         float64 `json:"lossProbability"`
	MaxRetries              int     `json:"maxRetries"`
	RetryBackoffUs          int64   `json:"retryBackoffUs"`
	CircuitFailureThreshold int     `json:"circuitFailureThreshold"`
	CircuitOpenUs           int64   `json:"circuitOpenUs"`
	CostPerMillionAttempts  float64 `json:"costPerMillionAttempts"`
}
type Workload struct {
	Source                    string `json:"source"`
	RequestsPerSecond         int    `json:"requestsPerSecond"`
	DurationSeconds           int    `json:"durationSeconds"`
	Seed                      int64  `json:"seed"`
	MaxApplicationRetries     int    `json:"maxApplicationRetries"`
	ApplicationRetryBackoffUs int64  `json:"applicationRetryBackoffUs"`
}
type Failure struct {
	ID                string  `json:"id"`
	Kind              string  `json:"kind"`
	TargetID          string  `json:"targetId"`
	StartUs           int64   `json:"startUs"`
	DurationUs        int64   `json:"durationUs"`
	ServiceMultiplier float64 `json:"serviceMultiplier,omitempty"`
	LossProbability   float64 `json:"lossProbability,omitempty"`
}
type Input struct {
	Nodes    []Node    `json:"nodes"`
	Edges    []Edge    `json:"edges"`
	Workload Workload  `json:"workload"`
	Failures []Failure `json:"failures"`
}
type NodeMetric struct {
	Completed             int     `json:"completed"`
	PeakQueue             int     `json:"peakQueue"`
	Utilization           float64 `json:"utilization"`
	RateLimited           int     `json:"rateLimited"`
	CacheHits             int     `json:"cacheHits"`
	CacheMisses           int     `json:"cacheMisses"`
	FailureDrops          int     `json:"failureDrops"`
	OutageUs              int64   `json:"outageUs"`
	QueueRejected         int     `json:"queueRejected"`
	QueueWaitObservations int     `json:"queueWaitObservations"`
	MeanQueueWaitUs       float64 `json:"meanQueueWaitUs"`
	P95QueueWaitUs        int64   `json:"p95QueueWaitUs"`
	ConnectionUtilization float64 `json:"connectionUtilization"`
	ConnectionWaits       int     `json:"connectionWaits"`
	ConnectionRejected    int     `json:"connectionRejected"`
	LockWaits             int     `json:"lockWaits"`
	TotalLockWaitUs       int64   `json:"totalLockWaitUs"`
	BrokerRejected        int     `json:"brokerRejected"`
	PeakBrokerDepth       int     `json:"peakBrokerDepth"`
	ScaleUps              int     `json:"scaleUps"`
	ScaleDowns            int     `json:"scaleDowns"`
	PeakConcurrency       int     `json:"peakConcurrency"`
	DegradedOperations    int     `json:"degradedOperations"`
}
type TraceEvent struct {
	TimeUs      int64  `json:"timeUs"`
	Kind        string `json:"kind"`
	ComponentID string `json:"componentId,omitempty"`
}
type Trace struct {
	ID           string       `json:"id"`
	RequestIndex int          `json:"requestIndex"`
	StartUs      int64        `json:"startUs"`
	EndUs        int64        `json:"endUs"`
	LatencyUs    int64        `json:"latencyUs"`
	Status       string       `json:"status"`
	Events       []TraceEvent `json:"events"`
	Truncated    bool         `json:"truncated"`
}
type Result struct {
	Generated                 int                   `json:"generated"`
	Completed                 int                   `json:"completed"`
	Failed                    int                   `json:"failed"`
	TimedOut                  int                   `json:"timedOut"`
	MeanLatencyUs             float64               `json:"meanLatencyUs"`
	P50Us                     int64                 `json:"p50Us"`
	P95Us                     int64                 `json:"p95Us"`
	P99Us                     int64                 `json:"p99Us"`
	Nodes                     map[string]NodeMetric `json:"nodes"`
	EventsProcessed           int                   `json:"eventsProcessed"`
	FailuresTriggered         int                   `json:"failuresTriggered"`
	EdgeDrops                 map[string]int        `json:"edgeDrops"`
	Retries                   int                   `json:"retries"`
	ApplicationRetries        int                   `json:"applicationRetries"`
	ApplicationRetryExhausted int                   `json:"applicationRetryExhausted"`
	CircuitOpens              int                   `json:"circuitOpens"`
	CircuitRejected           int                   `json:"circuitRejected"`
	Traces                    []Trace               `json:"traces"`
	EdgeAttempts              map[string]int        `json:"edgeAttempts"`
	ConfiguredNodeCostPerHour float64               `json:"configuredNodeCostPerHour"`
	EstimatedScenarioCost     float64               `json:"estimatedScenarioCost"`
	EstimatedHourlyCost       float64               `json:"estimatedHourlyCost"`
	CostPerSuccessfulRequest  *float64              `json:"costPerSuccessfulRequest"`
	Findings                  []Finding             `json:"findings"`
	AsyncEnqueued             int                   `json:"asyncEnqueued"`
	BackgroundCompleted       int                   `json:"backgroundCompleted"`
	BackgroundFailed          int                   `json:"backgroundFailed"`
	BackgroundTimedOut        int                   `json:"backgroundTimedOut"`
	BackgroundRedeliveries    int                   `json:"backgroundRedeliveries"`
	BackgroundDeadLetters     int                   `json:"backgroundDeadLetters"`
}

func Validate(in Input) error {
	if len(in.Nodes) == 0 || len(in.Nodes) > 100 || len(in.Edges) > 300 || len(in.Failures) > 100 {
		return errors.New("node or edge count outside limits")
	}
	if in.Workload.DurationSeconds < 1 || in.Workload.DurationSeconds > 60 || in.Workload.RequestsPerSecond < 1 || int64(in.Workload.DurationSeconds)*int64(in.Workload.RequestsPerSecond) > 10000 {
		return errors.New("workload outside limits")
	}
	if in.Workload.MaxApplicationRetries < 0 || in.Workload.MaxApplicationRetries > 3 || in.Workload.ApplicationRetryBackoffUs < 0 || in.Workload.ApplicationRetryBackoffUs > 60_000_000 || (in.Workload.MaxApplicationRetries > 0 && in.Workload.ApplicationRetryBackoffUs == 0) || (in.Workload.MaxApplicationRetries == 0 && in.Workload.ApplicationRetryBackoffUs != 0) {
		return errors.New("application retry policy outside limits")
	}
	ids := map[string]bool{}
	out := map[string][]string{}
	for _, n := range in.Nodes {
		if n.ID == "" || ids[n.ID] || n.Concurrency < 1 || n.QueueCapacity < 0 || n.ConnectionLimit < 0 || n.ConnectionLimit > 10000 || (n.Kind != "database" && n.ConnectionLimit != 0) || n.TransactionLockUs < 0 || n.TransactionLockUs > n.ServiceTimeUs || (n.Kind != "database" && n.TransactionLockUs != 0) || n.BrokerCapacity < 0 || n.BrokerCapacity > 10000 || n.MaxDeliveries < 0 || n.MaxDeliveries > 5 || n.RedeliveryDelayUs < 0 || n.RedeliveryDelayUs > 60_000_000 || (n.Kind != "queue" && (n.BrokerCapacity != 0 || n.MaxDeliveries != 0 || n.RedeliveryDelayUs != 0)) || (n.MaxDeliveries > 1 && n.RedeliveryDelayUs == 0) || (n.MaxDeliveries <= 1 && n.RedeliveryDelayUs != 0) || n.ServiceTimeUs < 0 || n.ServiceTimeUs > 60_000_000 || n.RateLimitRps < 0 || math.IsNaN(n.HitRate) || n.HitRate < 0 || n.HitRate > 1 || math.IsNaN(n.HourlyCost) || math.IsInf(n.HourlyCost, 0) || n.HourlyCost < 0 || n.HourlyCost > 1_000_000 || n.AutoscaleMaxConcurrency < 0 || n.AutoscaleMaxConcurrency > 10000 || n.AutoscaleQueueThreshold < 0 || n.AutoscaleDelayUs < 0 || n.AutoscaleDelayUs > 60_000_000 || n.AutoscaleIdleUs < 0 || n.AutoscaleIdleUs > 60_000_000 || (n.AutoscaleMaxConcurrency > 0 && (n.AutoscaleMaxConcurrency <= n.Concurrency || n.AutoscaleQueueThreshold < 1 || n.AutoscaleQueueThreshold > n.QueueCapacity+1)) || (n.AutoscaleMaxConcurrency == 0 && (n.AutoscaleQueueThreshold != 0 || n.AutoscaleDelayUs != 0 || n.AutoscaleIdleUs != 0)) {
			return fmt.Errorf("invalid node %q", n.ID)
		}
		ids[n.ID] = true
	}
	if !ids[in.Workload.Source] {
		return errors.New("workload source is missing")
	}
	edges := map[string]bool{}
	for _, e := range in.Edges {
		if e.ID == "" || edges[e.ID] || !ids[e.Source] || !ids[e.Target] || e.LatencyUs < 0 || e.LatencyUs > 60_000_000 || math.IsNaN(e.LossProbability) || e.LossProbability < 0 || e.LossProbability > 1 || e.MaxRetries < 0 || e.MaxRetries > 5 || e.RetryBackoffUs < 0 || e.RetryBackoffUs > 60_000_000 || e.CircuitFailureThreshold < 0 || e.CircuitFailureThreshold > 1000 || e.CircuitOpenUs < 0 || e.CircuitOpenUs > 60_000_000 || (e.MaxRetries > 0 && e.RetryBackoffUs == 0) || (e.CircuitFailureThreshold > 0 && e.CircuitOpenUs == 0) || (e.CircuitFailureThreshold == 0 && e.CircuitOpenUs > 0) || math.IsNaN(e.CostPerMillionAttempts) || math.IsInf(e.CostPerMillionAttempts, 0) || e.CostPerMillionAttempts < 0 || e.CostPerMillionAttempts > 1_000_000 {
			return fmt.Errorf("invalid edge %q", e.ID)
		}
		edges[e.ID] = true
		out[e.Source] = append(out[e.Source], e.Target)
	}
	failureIDs := map[string]bool{}
	for _, f := range in.Failures {
		if f.ID == "" || failureIDs[f.ID] || f.StartUs < 0 || f.StartUs >= int64(in.Workload.DurationSeconds)*1_000_000 || f.DurationUs < 1 || f.DurationUs > 60_000_000 {
			return fmt.Errorf("invalid failure %q", f.ID)
		}
		failureIDs[f.ID] = true
		if (f.Kind == "node_outage" && !ids[f.TargetID]) || (f.Kind == "node_degradation" && !ids[f.TargetID]) || (f.Kind == "edge_partition" && !edges[f.TargetID]) || (f.Kind == "edge_loss_spike" && !edges[f.TargetID]) || (f.Kind != "node_outage" && f.Kind != "edge_partition" && f.Kind != "node_degradation" && f.Kind != "edge_loss_spike") {
			return fmt.Errorf("invalid failure target %q", f.TargetID)
		}
		if math.IsNaN(f.ServiceMultiplier) || math.IsInf(f.ServiceMultiplier, 0) || math.IsNaN(f.LossProbability) || math.IsInf(f.LossProbability, 0) || (f.Kind == "node_degradation" && (f.ServiceMultiplier <= 1 || f.ServiceMultiplier > 10 || f.LossProbability != 0)) || (f.Kind == "edge_loss_spike" && (f.LossProbability <= 0 || f.LossProbability > 1 || f.ServiceMultiplier != 0)) || ((f.Kind == "node_outage" || f.Kind == "edge_partition") && (f.ServiceMultiplier != 0 || f.LossProbability != 0)) {
			return fmt.Errorf("invalid degradation settings for %q", f.ID)
		}
	}
	for _, n := range in.Nodes {
		if n.Kind == "queue" && len(out[n.ID]) != 1 {
			return fmt.Errorf("queue %q requires exactly one worker edge", n.ID)
		}
		if len(out[n.ID]) > 1 && n.Kind != "load balancer" && n.Kind != "load_balancer" {
			return fmt.Errorf("node %q has multiple outgoing edges but is not a load balancer", n.ID)
		}
		if n.Kind == "cache" && len(out[n.ID]) != 1 {
			return fmt.Errorf("cache %q requires exactly one origin edge", n.ID)
		}
	}
	color := map[string]int{}
	var visit func(string) bool
	visit = func(id string) bool {
		if color[id] == 1 {
			return false
		}
		if color[id] == 2 {
			return true
		}
		color[id] = 1
		for _, to := range out[id] {
			if !visit(to) {
				return false
			}
		}
		color[id] = 2
		return true
	}
	for id := range ids {
		if !visit(id) {
			return errors.New("cycles are unsupported")
		}
	}
	return nil
}

type event struct {
	at      int64
	seq     uint64
	kind    uint8
	req     int
	node    string
	fault   int
	edge    string
	attempt int
}
type events []event

func (q events) Len() int { return len(q) }
func (q events) Less(i, j int) bool {
	if q[i].at == q[j].at {
		return q[i].seq < q[j].seq
	}
	return q[i].at < q[j].at
}
func (q events) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *events) Push(x any)   { *q = append(*q, x.(event)) }
func (q *events) Pop() any     { old := *q; n := len(old); x := old[n-1]; *q = old[:n-1]; return x }

const (
	arrive uint8 = iota
	finish
	faultStart
	faultEnd
	retrySend
	scaleUp
	scaleDown
	applicationRetry
	redeliver
)

type state struct {
	node                   Node
	busy                   int
	queue                  []int
	busyTime               int64
	capacityTime           int64
	connectionCapacityTime int64
	last                   int64
	currentCapacity        int
	scaleUpPending         bool
	scaleDownPending       bool
	scaleDownGeneration    int
	metric                 NodeMetric
	windowSecond           int64
	windowCount            int
	routeIndex             int
	active                 map[int]bool
	waiting                map[int]bool
	faultCount             int
	downAt                 int64
	queuedAt               map[int]int64
	waits                  []int64
	degradations           map[int]float64
	lockAvailableAt        int64
}

func (s *state) capacity() int {
	if s.node.Kind == "database" && s.node.ConnectionLimit > 0 {
		return min(s.currentCapacity, s.node.ConnectionLimit)
	}
	return s.currentCapacity
}
func (s *state) advance(clock int64) {
	elapsed := clock - s.last
	s.busyTime += int64(s.busy) * elapsed
	s.capacityTime += int64(s.currentCapacity) * elapsed
	s.connectionCapacityTime += int64(s.capacity()) * elapsed
	s.last = clock
}
func (s *state) serviceDelay() (int64, bool) {
	if len(s.degradations) == 0 {
		return s.node.ServiceTimeUs, false
	}
	keys := make([]int, 0, len(s.degradations))
	for key := range s.degradations {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	delay := float64(s.node.ServiceTimeUs)
	for _, key := range keys {
		delay *= s.degradations[key]
		if delay >= 60_000_000 {
			return 60_000_000, true
		}
	}
	return int64(math.Round(delay)), true
}
func (s *state) startDelay(clock int64) (int64, bool) {
	delay, degraded := s.serviceDelay()
	if s.node.Kind == "database" && s.node.TransactionLockUs > 0 {
		wait := max(int64(0), s.lockAvailableAt-clock)
		if wait > 0 {
			s.metric.LockWaits++
			s.metric.TotalLockWaitUs += wait
		}
		s.lockAvailableAt = max(clock, s.lockAvailableAt) + s.node.TransactionLockUs
		return delay + wait, degraded
	}
	return delay, degraded
}

type linkState struct {
	consecutiveFailures int
	openUntil           int64
}

func Run(in Input) (Result, error) { return RunContext(context.Background(), in) }

func RunContext(ctx context.Context, in Input) (Result, error) {
	return RunContextWithProgress(ctx, in, nil)
}

func RunContextWithProgress(ctx context.Context, in Input, progress func(done, total int)) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := Validate(in); err != nil {
		return Result{}, err
	}
	duration := int64(in.Workload.DurationSeconds) * 1_000_000
	states := map[string]*state{}
	outgoing := map[string][]Edge{}
	links := map[string]Edge{}
	linkStates := map[string]*linkState{}
	for _, n := range in.Nodes {
		states[n.ID] = &state{node: n, currentCapacity: n.Concurrency, metric: NodeMetric{PeakConcurrency: n.Concurrency}, active: map[int]bool{}, waiting: map[int]bool{}, queuedAt: map[int]int64{}, degradations: map[int]float64{}}
	}
	for _, e := range in.Edges {
		outgoing[e.Source] = append(outgoing[e.Source], e)
		links[e.ID] = e
		linkStates[e.ID] = &linkState{}
	}
	for id := range outgoing {
		slices.SortFunc(outgoing[id], func(a, b Edge) int {
			if a.ID < b.ID {
				return -1
			}
			if a.ID > b.ID {
				return 1
			}
			return 0
		})
	}
	result := Result{Nodes: map[string]NodeMetric{}, EdgeDrops: map[string]int{}, Traces: []Trace{}, EdgeAttempts: map[string]int{}}
	count := in.Workload.DurationSeconds * in.Workload.RequestsPerSecond
	sampleCount := min(count, 50)
	for i := 0; i < sampleCount; i++ {
		idx := 0
		if sampleCount > 1 {
			idx = i * (count - 1) / (sampleCount - 1)
		}
		start := int64(idx) * 1_000_000 / int64(in.Workload.RequestsPerSecond)
		result.Traces = append(result.Traces, Trace{ID: fmt.Sprintf("req-%06d", idx), RequestIndex: idx, StartUs: start, Status: "running", Events: []TraceEvent{{TimeUs: start, Kind: "generated", ComponentID: in.Workload.Source}}})
	}
	traceFor := map[int]*Trace{}
	for i := range result.Traces {
		traceFor[result.Traces[i].RequestIndex] = &result.Traces[i]
	}
	record := func(req int, at int64, kind, component string) {
		t := traceFor[req]
		if t == nil {
			return
		}
		if len(t.Events) < 128 {
			t.Events = append(t.Events, TraceEvent{TimeUs: at, Kind: kind, ComponentID: component})
		} else {
			t.Truncated = true
		}
	}
	closeTrace := func(req int, at int64, status string) {
		t := traceFor[req]
		if t == nil {
			return
		}
		t.Status = status
		t.EndUs = at
		t.LatencyUs = at - t.StartUs
		ev := TraceEvent{TimeUs: at, Kind: status}
		if len(t.Events) < 128 {
			t.Events = append(t.Events, ev)
		} else {
			t.Events[127] = ev
			t.Truncated = true
		}
	}
	starts := make([]int64, 0)
	terminal := make([]bool, 0)
	latencies := make([]int64, 0)
	requestCount := in.Workload.DurationSeconds * in.Workload.RequestsPerSecond
	applicationAttempts := make([]int, requestCount)
	applicationRetryPending := make([]bool, requestCount)
	backgroundOrigin := map[int]Edge{}
	backgroundQueue := map[int]string{}
	backgroundAttempts := map[int]int{}
	backgroundPending := map[int]bool{}
	brokerDepth := map[string]int{}
	rng := rand.New(rand.NewSource(in.Workload.Seed))
	q := &events{}
	heap.Init(q)
	var seq uint64
	schedule := func(at int64, kind uint8, req int, node string) {
		seq++
		heap.Push(q, event{at: at, seq: seq, kind: kind, req: req, node: node})
	}
	scheduleScaleDown := func(s *state, node string, at int64) {
		if s.node.AutoscaleIdleUs == 0 || s.currentCapacity <= s.node.Concurrency || s.busy >= s.currentCapacity || len(s.queue) > 0 || s.scaleDownPending {
			return
		}
		s.scaleDownGeneration++
		s.scaleDownPending = true
		seq++
		heap.Push(q, event{at: at + s.node.AutoscaleIdleUs, seq: seq, kind: scaleDown, node: node, attempt: s.scaleDownGeneration})
	}
	failures := slices.Clone(in.Failures)
	slices.SortFunc(failures, func(a, b Failure) int {
		if a.StartUs < b.StartUs {
			return -1
		}
		if a.StartUs > b.StartUs {
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
	for i, f := range failures {
		seq++
		heap.Push(q, event{at: f.StartUs, seq: seq, kind: faultStart, fault: i})
		seq++
		heap.Push(q, event{at: f.StartUs + f.DurationUs, seq: seq, kind: faultEnd, fault: i})
	}
	var clock int64
	complete := func(req int) {
		terminal[req] = true
		if req >= requestCount {
			result.BackgroundCompleted++
			if queueID := backgroundQueue[req]; queueID != "" {
				brokerDepth[queueID]--
			}
		} else {
			result.Completed++
			if progress != nil && (result.Completed+result.Failed)%64 == 0 {
				progress(result.Completed+result.Failed, requestCount)
			}
			latencies = append(latencies, clock-starts[req])
			closeTrace(req, clock, "completed")
		}
	}
	fail := func(req int) {
		if terminal[req] {
			return
		}
		if req < requestCount {
			if applicationRetryPending[req] {
				return
			}
			if applicationAttempts[req] < in.Workload.MaxApplicationRetries {
				delay := in.Workload.ApplicationRetryBackoffUs << applicationAttempts[req]
				applicationAttempts[req]++
				applicationRetryPending[req] = true
				result.ApplicationRetries++
				record(req, clock, "application_retry_scheduled", in.Workload.Source)
				schedule(clock+delay, applicationRetry, req, in.Workload.Source)
				return
			}
			if applicationAttempts[req] > 0 {
				result.ApplicationRetryExhausted++
			}
		}
		if req >= requestCount {
			if backgroundPending[req] {
				return
			}
			if queueID := backgroundQueue[req]; queueID != "" {
				maxDeliveries := states[queueID].node.MaxDeliveries
				if maxDeliveries == 0 {
					maxDeliveries = 1
				}
				if backgroundAttempts[req] < maxDeliveries {
					backgroundAttempts[req]++
					backgroundPending[req] = true
					result.BackgroundRedeliveries++
					schedule(clock+states[queueID].node.RedeliveryDelayUs, redeliver, req, queueID)
					return
				}
				result.BackgroundDeadLetters++
				brokerDepth[queueID]--
			}
		}
		terminal[req] = true
		if req >= requestCount {
			result.BackgroundFailed++
		} else {
			result.Failed++
			if progress != nil && (result.Completed+result.Failed)%64 == 0 {
				progress(result.Completed+result.Failed, requestCount)
			}
			closeTrace(req, clock, "failed")
		}
	}
	edgeFaults := map[string]int{}
	edgeLossSpikes := map[string]map[int]float64{}
	var send func(req int, edge Edge, attempt int)
	send = func(req int, edge Edge, attempt int) {
		if terminal[req] || (req < requestCount && applicationRetryPending[req]) {
			return
		}
		link := linkStates[edge.ID]
		record(req, clock, "send_attempt", edge.ID)
		if edge.CircuitFailureThreshold > 0 && clock < link.openUntil {
			result.CircuitRejected++
			record(req, clock, "circuit_rejected", edge.ID)
			fail(req)
			return
		}
		result.EdgeAttempts[edge.ID]++
		loss := edge.LossProbability
		if spikes := edgeLossSpikes[edge.ID]; len(spikes) > 0 {
			keys := make([]int, 0, len(spikes))
			for key := range spikes {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			survival := 1 - loss
			for _, key := range keys {
				survival *= 1 - spikes[key]
			}
			loss = 1 - survival
		}
		if edgeFaults[edge.ID] > 0 || rng.Float64() < loss {
			result.EdgeDrops[edge.ID]++
			record(req, clock, "edge_drop", edge.ID)
			link.consecutiveFailures++
			opened := false
			if edge.CircuitFailureThreshold > 0 && link.consecutiveFailures >= edge.CircuitFailureThreshold {
				link.openUntil = clock + edge.CircuitOpenUs
				link.consecutiveFailures = 0
				result.CircuitOpens++
				record(req, clock, "circuit_opened", edge.ID)
				opened = true
			}
			if attempt < edge.MaxRetries && !opened {
				delay := edge.RetryBackoffUs << attempt
				states[edge.Source].waiting[req] = true
				seq++
				heap.Push(q, event{at: clock + delay, seq: seq, kind: retrySend, req: req, edge: edge.ID, attempt: attempt + 1})
				result.Retries++
				record(req, clock, "retry_scheduled", edge.ID)
			} else {
				fail(req)
			}
			return
		}
		link.consecutiveFailures = 0
		record(req, clock, "edge_transit", edge.ID)
		schedule(clock+edge.LatencyUs, arrive, req, edge.Target)
	}
	for i := 0; i < count; i++ {
		at := int64(i) * 1_000_000 / int64(in.Workload.RequestsPerSecond)
		starts = append(starts, at)
		terminal = append(terminal, false)
		schedule(at, arrive, i, in.Workload.Source)
	}
	result.Generated = count
	for q.Len() > 0 {
		if result.EventsProcessed&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
		}
		if result.EventsProcessed >= 250000 {
			return Result{}, errors.New("event budget exceeded")
		}
		ev := heap.Pop(q).(event)
		if ev.at > duration {
			break
		}
		clock = ev.at
		result.EventsProcessed++
		if ev.kind == applicationRetry {
			if terminal[ev.req] || !applicationRetryPending[ev.req] {
				continue
			}
			applicationRetryPending[ev.req] = false
			record(ev.req, clock, "application_retry_started", in.Workload.Source)
			schedule(clock, arrive, ev.req, in.Workload.Source)
			continue
		}
		if ev.kind == redeliver {
			if terminal[ev.req] || !backgroundPending[ev.req] {
				continue
			}
			backgroundPending[ev.req] = false
			record(ev.req, clock, "background_redelivered", ev.node)
			send(ev.req, backgroundOrigin[ev.req], 0)
			continue
		}
		if ev.kind == retrySend {
			delete(states[links[ev.edge].Source].waiting, ev.req)
			record(ev.req, clock, "retry_attempt", ev.edge)
			send(ev.req, links[ev.edge], ev.attempt)
			continue
		}
		if ev.kind == faultStart || ev.kind == faultEnd {
			f := failures[ev.fault]
			if f.Kind == "edge_loss_spike" {
				if ev.kind == faultStart {
					if edgeLossSpikes[f.TargetID] == nil {
						edgeLossSpikes[f.TargetID] = map[int]float64{}
					}
					edgeLossSpikes[f.TargetID][ev.fault] = f.LossProbability
					result.FailuresTriggered++
				} else {
					delete(edgeLossSpikes[f.TargetID], ev.fault)
				}
				continue
			}
			if f.Kind == "edge_partition" {
				if ev.kind == faultStart {
					edgeFaults[f.TargetID]++
					result.FailuresTriggered++
				} else {
					edgeFaults[f.TargetID]--
				}
				continue
			}
			s := states[f.TargetID]
			s.advance(clock)
			if f.Kind == "node_degradation" {
				if ev.kind == faultStart {
					s.degradations[ev.fault] = f.ServiceMultiplier
					result.FailuresTriggered++
				} else {
					delete(s.degradations, ev.fault)
				}
				continue
			}
			if ev.kind == faultStart {
				result.FailuresTriggered++
				if s.faultCount == 0 {
					s.downAt = clock
					for req := range s.active {
						if !terminal[req] {
							s.metric.FailureDrops++
							record(req, clock, "node_outage", f.TargetID)
							fail(req)
						}
					}
					for req := range s.waiting {
						if !terminal[req] {
							s.metric.FailureDrops++
							record(req, clock, "node_outage", f.TargetID)
							fail(req)
						}
					}
					for _, req := range s.queue {
						if !terminal[req] {
							s.metric.FailureDrops++
							record(req, clock, "node_outage", f.TargetID)
							fail(req)
						}
					}
					s.active = map[int]bool{}
					s.waiting = map[int]bool{}
					s.queue = nil
					s.queuedAt = map[int]int64{}
					s.busy = 0
				}
				s.faultCount++
			} else {
				s.faultCount--
				if s.faultCount == 0 {
					s.metric.OutageUs += clock - s.downAt
				}
			}
			continue
		}
		s := states[ev.node]
		s.advance(clock)
		if ev.kind == scaleUp {
			s.scaleUpPending = false
			if s.currentCapacity < s.node.AutoscaleMaxConcurrency && s.faultCount == 0 {
				s.currentCapacity++
				s.metric.ScaleUps++
				s.metric.PeakConcurrency = max(s.metric.PeakConcurrency, s.currentCapacity)
				if s.busy < s.capacity() && len(s.queue) > 0 {
					next := s.queue[0]
					s.queue = s.queue[1:]
					s.busy++
					s.active[next] = true
					s.waits = append(s.waits, clock-s.queuedAt[next])
					delete(s.queuedAt, next)
					record(next, clock, "dequeued", ev.node)
					record(next, clock, "processing_started", ev.node)
					delay, degraded := s.startDelay(clock)
					if degraded {
						s.metric.DegradedOperations++
					}
					schedule(clock+delay, finish, next, ev.node)
				}
				if len(s.queue) >= s.node.AutoscaleQueueThreshold && s.currentCapacity < s.node.AutoscaleMaxConcurrency {
					s.scaleUpPending = true
					schedule(clock+s.node.AutoscaleDelayUs, scaleUp, 0, ev.node)
				}
				scheduleScaleDown(s, ev.node, clock)
			}
			continue
		}
		if ev.kind == scaleDown {
			if !s.scaleDownPending || ev.attempt != s.scaleDownGeneration {
				continue
			}
			s.scaleDownPending = false
			if s.currentCapacity > s.node.Concurrency && s.busy < s.currentCapacity && len(s.queue) == 0 {
				s.currentCapacity--
				s.metric.ScaleDowns++
				scheduleScaleDown(s, ev.node, clock)
			}
			continue
		}
		if ev.kind == arrive {
			if terminal[ev.req] || (ev.req < requestCount && applicationRetryPending[ev.req]) || backgroundPending[ev.req] {
				continue
			}
			record(ev.req, clock, "arrived", ev.node)
			if s.faultCount > 0 {
				s.metric.FailureDrops++
				record(ev.req, clock, "node_unavailable", ev.node)
				fail(ev.req)
				continue
			}
			if s.node.Kind == "gateway" && s.node.RateLimitRps > 0 {
				second := clock / 1_000_000
				if second != s.windowSecond {
					s.windowSecond = second
					s.windowCount = 0
				}
				if s.windowCount >= s.node.RateLimitRps {
					s.metric.RateLimited++
					record(ev.req, clock, "rate_limited", ev.node)
					fail(ev.req)
					continue
				}
				s.windowCount++
			}
			pressureLevel := 0
			if s.busy < s.capacity() {
				s.busy++
				s.active[ev.req] = true
				s.waits = append(s.waits, 0)
				record(ev.req, clock, "processing_started", ev.node)
				delay, degraded := s.startDelay(clock)
				if degraded {
					s.metric.DegradedOperations++
				}
				schedule(clock+delay, finish, ev.req, ev.node)
			} else if len(s.queue) < s.node.QueueCapacity {
				s.queue = append(s.queue, ev.req)
				pressureLevel = len(s.queue)
				s.queuedAt[ev.req] = clock
				if s.node.Kind == "database" && s.capacity() < s.node.Concurrency {
					s.metric.ConnectionWaits++
				}
				record(ev.req, clock, "queued", ev.node)
				if len(s.queue) > s.metric.PeakQueue {
					s.metric.PeakQueue = len(s.queue)
				}
			} else {
				pressureLevel = len(s.queue) + 1
				s.metric.QueueRejected++
				if s.node.Kind == "database" && s.capacity() < s.node.Concurrency {
					s.metric.ConnectionRejected++
				}
				record(ev.req, clock, "queue_rejected", ev.node)
				fail(ev.req)
			}
			if pressureLevel >= s.node.AutoscaleQueueThreshold && s.node.AutoscaleMaxConcurrency > s.currentCapacity && !s.scaleUpPending {
				s.scaleUpPending = true
				schedule(clock+s.node.AutoscaleDelayUs, scaleUp, 0, ev.node)
			}
			if s.scaleDownPending && (s.busy >= s.currentCapacity || len(s.queue) > 0) {
				s.scaleDownGeneration++
				s.scaleDownPending = false
			}
		} else {
			if !s.active[ev.req] || backgroundPending[ev.req] {
				continue
			}
			delete(s.active, ev.req)
			record(ev.req, clock, "processing_completed", ev.node)
			s.busy--
			s.metric.Completed++
			if len(s.queue) > 0 {
				next := s.queue[0]
				s.queue = s.queue[1:]
				s.busy++
				s.active[next] = true
				s.waits = append(s.waits, clock-s.queuedAt[next])
				delete(s.queuedAt, next)
				record(next, clock, "dequeued", ev.node)
				record(next, clock, "processing_started", ev.node)
				delay, degraded := s.startDelay(clock)
				if degraded {
					s.metric.DegradedOperations++
				}
				schedule(clock+delay, finish, next, ev.node)
			}
			scheduleScaleDown(s, ev.node, clock)
			edges := outgoing[ev.node]
			if s.node.Kind == "cache" {
				if rng.Float64() < s.node.HitRate {
					s.metric.CacheHits++
					record(ev.req, clock, "cache_hit", ev.node)
					complete(ev.req)
					continue
				}
				s.metric.CacheMisses++
				record(ev.req, clock, "cache_miss", ev.node)
			}
			if len(edges) == 0 {
				complete(ev.req)
			} else {
				edge := edges[0]
				if s.node.Kind == "load balancer" || s.node.Kind == "load_balancer" {
					edge = edges[s.routeIndex%len(edges)]
					s.routeIndex++
				}
				if s.node.Kind == "queue" && ev.req < requestCount {
					if s.node.BrokerCapacity > 0 && brokerDepth[ev.node] >= s.node.BrokerCapacity {
						s.metric.BrokerRejected++
						record(ev.req, clock, "broker_full", ev.node)
						fail(ev.req)
						continue
					}
					record(ev.req, clock, "async_enqueued", ev.node)
					complete(ev.req)
					job := len(terminal)
					terminal = append(terminal, false)
					starts = append(starts, clock)
					result.AsyncEnqueued++
					backgroundOrigin[job] = edge
					backgroundQueue[job] = ev.node
					backgroundAttempts[job] = 1
					brokerDepth[ev.node]++
					s.metric.PeakBrokerDepth = max(s.metric.PeakBrokerDepth, brokerDepth[ev.node])
					send(job, edge, 0)
				} else {
					send(ev.req, edge, 0)
				}
			}
		}
	}
	for req, done := range terminal {
		if !done {
			if req >= requestCount {
				result.BackgroundTimedOut++
				if queueID := backgroundQueue[req]; queueID != "" {
					brokerDepth[queueID]--
				}
			} else {
				result.TimedOut++
				closeTrace(req, duration, "timed_out")
			}
		}
	}
	if progress != nil {
		progress(result.Completed+result.Failed+result.TimedOut, requestCount)
	}
	for id, s := range states {
		s.advance(duration)
		if s.faultCount > 0 {
			s.metric.OutageUs += duration - s.downAt
		}
		s.metric.Utilization = float64(s.busyTime) / float64(s.capacityTime)
		if s.node.Kind == "database" {
			s.metric.ConnectionUtilization = float64(s.busyTime) / float64(s.connectionCapacityTime)
		}
		s.metric.QueueWaitObservations = len(s.waits)
		if len(s.waits) > 0 {
			var total int64
			for _, wait := range s.waits {
				total += wait
			}
			s.metric.MeanQueueWaitUs = float64(total) / float64(len(s.waits))
			slices.Sort(s.waits)
			s.metric.P95QueueWaitUs = s.waits[int(math.Ceil(.95*float64(len(s.waits))))-1]
		}
		result.Nodes[id] = s.metric
		result.ConfiguredNodeCostPerHour += s.node.HourlyCost
		result.EstimatedScenarioCost += s.node.HourlyCost * float64(s.capacityTime) / float64(s.node.Concurrency) / 3_600_000_000
	}
	for _, edge := range in.Edges {
		result.EstimatedScenarioCost += float64(result.EdgeAttempts[edge.ID]) * edge.CostPerMillionAttempts / 1_000_000
	}
	result.EstimatedHourlyCost = result.EstimatedScenarioCost * 3600 / float64(in.Workload.DurationSeconds)
	if result.Completed > 0 {
		cost := result.EstimatedScenarioCost / float64(result.Completed)
		result.CostPerSuccessfulRequest = &cost
	}
	slices.Sort(latencies)
	if len(latencies) > 0 {
		var sum int64
		for _, v := range latencies {
			sum += v
		}
		result.MeanLatencyUs = float64(sum) / float64(len(latencies))
		rank := func(p float64) int64 { i := int(math.Ceil(p*float64(len(latencies)))) - 1; return latencies[i] }
		result.P50Us = rank(.5)
		result.P95Us = rank(.95)
		result.P99Us = rank(.99)
	}
	result.Findings = Analyze(in, result)
	return result, nil
}
