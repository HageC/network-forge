package sim

import (
	"container/heap"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func sample() Input {
	return Input{Nodes: []Node{{ID: "client", Kind: "client", Concurrency: 100, QueueCapacity: 0}, {ID: "service", Kind: "service", ServiceTimeUs: 100_000, Concurrency: 1, QueueCapacity: 2}}, Edges: []Edge{{ID: "path", Source: "client", Target: "service", LatencyUs: 10_000}}, Workload: Workload{Source: "client", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 42}}
}

func TestEventOrdering(t *testing.T) {
	q := &events{{at: 10, seq: 2}, {at: 5, seq: 9}, {at: 10, seq: 1}}
	heap.Init(q)
	for _, want := range []uint64{9, 1, 2} {
		got := heap.Pop(q).(event).seq
		if got != want {
			t.Fatalf("event sequence %d, want %d", got, want)
		}
	}
}
func TestDeterministicAndConserved(t *testing.T) {
	a, e := Run(sample())
	if e != nil {
		t.Fatal(e)
	}
	b, e := Run(sample())
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("runs differ: %+v %+v", a, b)
	}
	if a.Generated != a.Completed+a.Failed+a.TimedOut {
		t.Fatalf("requests not conserved: %+v", a)
	}
	if a.P95Us < a.P50Us || a.P99Us < a.P95Us {
		t.Fatalf("invalid quantiles: %+v", a)
	}
}
func TestQueueCapacity(t *testing.T) {
	in := sample()
	in.Nodes[1].ServiceTimeUs = 500_000
	in.Nodes[1].QueueCapacity = 1
	r, e := Run(in)
	if e != nil {
		t.Fatal(e)
	}
	if r.Failed == 0 || r.Nodes["service"].PeakQueue != 1 {
		t.Fatalf("queue limit not enforced: %+v", r)
	}
}
func TestLoss(t *testing.T) {
	in := sample()
	in.Edges[0].LossProbability = 1
	r, e := Run(in)
	if e != nil {
		t.Fatal(e)
	}
	if r.Completed != 0 || r.Failed != r.Generated {
		t.Fatalf("loss not applied: %+v", r)
	}
}
func TestCycleRejected(t *testing.T) {
	in := sample()
	in.Edges = append(in.Edges, Edge{ID: "loop", Source: "service", Target: "client"})
	if _, e := Run(in); e == nil {
		t.Fatal("expected cycle rejection")
	}
}

func TestGatewayRateLimit(t *testing.T) {
	in := sample()
	in.Nodes[1].Kind = "gateway"
	in.Nodes[1].ServiceTimeUs = 0
	in.Nodes[1].Concurrency = 10
	in.Nodes[1].RateLimitRps = 5
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 5 || r.Failed != 5 || r.Nodes["service"].RateLimited != 5 {
		t.Fatalf("rate limit did not affect outcomes: %+v", r)
	}
}

func TestLoadBalancerRoundRobin(t *testing.T) {
	in := Input{Nodes: []Node{
		{ID: "lb", Kind: "load balancer", Concurrency: 10},
		{ID: "a", Kind: "service", Concurrency: 10, ServiceTimeUs: 1000},
		{ID: "b", Kind: "service", Concurrency: 10, ServiceTimeUs: 1000},
	}, Edges: []Edge{{ID: "a", Source: "lb", Target: "a"}, {ID: "b", Source: "lb", Target: "b"}}, Workload: Workload{Source: "lb", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 10 || r.Nodes["a"].Completed != 5 || r.Nodes["b"].Completed != 5 {
		t.Fatalf("round robin imbalance: %+v", r)
	}
}

func TestCacheHitAvoidsOrigin(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "cache", Kind: "cache", Concurrency: 10, HitRate: 1, ServiceTimeUs: 1000}, {ID: "origin", Kind: "database", Concurrency: 10, ServiceTimeUs: 1000}}, Edges: []Edge{{ID: "miss", Source: "cache", Target: "origin"}}, Workload: Workload{Source: "cache", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 3}}
	hits, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if hits.Completed != 10 || hits.Nodes["cache"].CacheHits != 10 || hits.Nodes["origin"].Completed != 0 {
		t.Fatalf("cache hit did not bypass origin: %+v", hits)
	}
	in.Nodes[0].HitRate = 0
	misses, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if misses.Nodes["cache"].CacheMisses != 10 || misses.Nodes["origin"].Completed != 10 || misses.P95Us <= hits.P95Us {
		t.Fatalf("cache miss did not reach origin: %+v", misses)
	}
}

func TestUnsupportedBranchRejected(t *testing.T) {
	in := sample()
	in.Edges = append(in.Edges, Edge{ID: "second", Source: "client", Target: "service"})
	if _, err := Run(in); err == nil {
		t.Fatal("expected branch validation error")
	}
}

func TestOutOfRangeDelayRejected(t *testing.T) {
	in := sample()
	in.Edges[0].LatencyUs = 1 << 62
	if _, err := Run(in); err == nil {
		t.Fatal("expected excessive latency rejection")
	}
}

func TestNodeOutageInterruptsAndRecovers(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "service", Kind: "service", ServiceTimeUs: 200_000, Concurrency: 1, QueueCapacity: 10}}, Workload: Workload{Source: "service", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 1}, Failures: []Failure{{ID: "outage", Kind: "node_outage", TargetID: "service", StartUs: 250_000, DurationUs: 300_000}}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Generated != r.Completed+r.Failed+r.TimedOut || r.Nodes["service"].FailureDrops < 5 || r.Nodes["service"].OutageUs != 300_000 || r.Nodes["service"].Utilization > 1 {
		t.Fatalf("outage accounting incorrect: %+v", r)
	}
	if r.Completed == 0 {
		t.Fatalf("recovery did not resume processing: %+v", r)
	}
}

func TestEdgePartitionBlocksAndRecovers(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "source", Kind: "client", Concurrency: 10}, {ID: "sink", Kind: "service", Concurrency: 10}}, Edges: []Edge{{ID: "link", Source: "source", Target: "sink"}}, Workload: Workload{Source: "source", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 1}, Failures: []Failure{{ID: "partition", Kind: "edge_partition", TargetID: "link", StartUs: 0, DurationUs: 500_000}}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.EdgeDrops["link"] != 5 || r.Failed != 5 || r.Completed != 5 || r.FailuresTriggered != 1 {
		t.Fatalf("partition did not affect traffic: %+v", r)
	}
}

func TestOverlappingOutagesDoNotRecoverEarly(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "service", Kind: "service", Concurrency: 10}}, Workload: Workload{Source: "service", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 1}, Failures: []Failure{{ID: "a", Kind: "node_outage", TargetID: "service", StartUs: 0, DurationUs: 400_000}, {ID: "b", Kind: "node_outage", TargetID: "service", StartUs: 200_000, DurationUs: 400_000}}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Nodes["service"].OutageUs != 600_000 || r.Failed != 6 || r.Completed != 4 {
		t.Fatalf("overlap incorrect: %+v", r)
	}
}

func TestTransportRetryBudget(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "source", Kind: "client", Concurrency: 1}, {ID: "sink", Kind: "service", Concurrency: 1}}, Edges: []Edge{{ID: "link", Source: "source", Target: "sink", LossProbability: 1, MaxRetries: 2, RetryBackoffUs: 1000}}, Workload: Workload{Source: "source", RequestsPerSecond: 1, DurationSeconds: 1, Seed: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Retries != 2 || r.EdgeDrops["link"] != 3 || r.Failed != 1 || r.Completed != 0 {
		t.Fatalf("retry budget incorrect: %+v", r)
	}
}

func TestCircuitBreakerFailsFast(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "source", Kind: "client", Concurrency: 10}, {ID: "sink", Kind: "service", Concurrency: 10}}, Edges: []Edge{{ID: "link", Source: "source", Target: "sink", LossProbability: 1, MaxRetries: 2, RetryBackoffUs: 1000, CircuitFailureThreshold: 1, CircuitOpenUs: 1_000_000}}, Workload: Workload{Source: "source", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.CircuitOpens != 1 || r.CircuitRejected != 9 || r.Retries != 0 || r.EdgeDrops["link"] != 1 || r.Failed != 10 {
		t.Fatalf("breaker incorrect: %+v", r)
	}
}

func TestRetryAfterPartitionRecovery(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "source", Kind: "client", Concurrency: 1}, {ID: "sink", Kind: "service", Concurrency: 1}}, Edges: []Edge{{ID: "link", Source: "source", Target: "sink", MaxRetries: 3, RetryBackoffUs: 100_000}}, Workload: Workload{Source: "source", RequestsPerSecond: 1, DurationSeconds: 1, Seed: 1}, Failures: []Failure{{ID: "fault", Kind: "edge_partition", TargetID: "link", StartUs: 0, DurationUs: 250_000}}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Retries != 2 || r.Completed != 1 || r.Failed != 0 || r.P95Us != 300_000 {
		t.Fatalf("retry did not recover: %+v", r)
	}
}

func TestSourceOutageCancelsPendingRetry(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "source", Kind: "service", Concurrency: 1}, {ID: "sink", Kind: "service", Concurrency: 1}}, Edges: []Edge{{ID: "link", Source: "source", Target: "sink", LossProbability: 1, MaxRetries: 3, RetryBackoffUs: 500_000}}, Workload: Workload{Source: "source", RequestsPerSecond: 1, DurationSeconds: 1, Seed: 1}, Failures: []Failure{{ID: "outage", Kind: "node_outage", TargetID: "source", StartUs: 200_000, DurationUs: 300_000}}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Retries != 1 || r.EdgeDrops["link"] != 1 || r.Nodes["source"].FailureDrops != 1 || r.Failed != 1 || r.Completed != 0 {
		t.Fatalf("pending retry survived source outage: %+v", r)
	}
}

func hasEvent(trace Trace, kind string) bool {
	for _, e := range trace.Events {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

func TestSampledTraceShowsQueueAndTerminalOutcome(t *testing.T) {
	in := sample()
	in.Nodes[1].ServiceTimeUs = 200_000
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Traces) != 10 {
		t.Fatalf("sample count: %d", len(r.Traces))
	}
	trace := r.Traces[1]
	if !hasEvent(trace, "queued") || !hasEvent(trace, "dequeued") || trace.Status != "completed" || trace.LatencyUs <= 0 {
		t.Fatalf("queue trace incomplete: %+v", trace)
	}
	timeoutInput := Input{Nodes: []Node{{ID: "slow", Kind: "service", ServiceTimeUs: 2_000_000, Concurrency: 1}}, Workload: Workload{Source: "slow", RequestsPerSecond: 1, DurationSeconds: 1, Seed: 1}}
	timeoutResult, err := Run(timeoutInput)
	if err != nil {
		t.Fatal(err)
	}
	last := timeoutResult.Traces[0]
	if last.Status != "timed_out" || !hasEvent(last, "timed_out") {
		t.Fatalf("timeout trace incomplete: %+v", last)
	}
}

func TestTraceRetentionIsBounded(t *testing.T) {
	nodes := make([]Node, 100)
	edges := make([]Edge, 99)
	for i := range nodes {
		nodes[i] = Node{ID: fmt.Sprintf("n%03d", i), Kind: "service", Concurrency: 1}
		if i > 0 {
			edges[i-1] = Edge{ID: fmt.Sprintf("e%03d", i), Source: nodes[i-1].ID, Target: nodes[i].ID}
		}
	}
	in := Input{Nodes: nodes, Edges: edges, Workload: Workload{Source: nodes[0].ID, RequestsPerSecond: 100, DurationSeconds: 10, Seed: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Traces) != 50 {
		t.Fatalf("trace count: %d", len(r.Traces))
	}
	for _, trace := range r.Traces {
		if len(trace.Events) > 128 {
			t.Fatalf("trace too long: %d", len(trace.Events))
		}
	}
	if !r.Traces[0].Truncated || r.Traces[0].Status != "completed" || r.Traces[0].Events[len(r.Traces[0].Events)-1].Kind != "completed" {
		t.Fatalf("trace termination lost: %+v", r.Traces[0])
	}
}
func TestSyntheticCostUsesActualEdgeAttempts(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "source", Kind: "client", Concurrency: 10, HourlyCost: 36}, {ID: "sink", Kind: "service", Concurrency: 10}}, Edges: []Edge{{ID: "link", Source: "source", Target: "sink", CostPerMillionAttempts: 1000}}, Workload: Workload{Source: "source", RequestsPerSecond: 1, DurationSeconds: 10, Seed: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.EdgeAttempts["link"] != 10 || math.Abs(r.EstimatedScenarioCost-.11) > 1e-9 || math.Abs(r.EstimatedHourlyCost-39.6) > 1e-9 || r.CostPerSuccessfulRequest == nil || math.Abs(*r.CostPerSuccessfulRequest-.011) > 1e-9 {
		t.Fatalf("cost model incorrect: %+v", r)
	}
	in.Edges[0].LossProbability = 1
	in.Edges[0].MaxRetries = 2
	in.Edges[0].RetryBackoffUs = 1000
	r, err = Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.EdgeAttempts["link"] != 30 || r.CostPerSuccessfulRequest != nil {
		t.Fatalf("retry or no-success cost incorrect: %+v", r)
	}
}
func TestQueueWaitAndRejectionMetrics(t *testing.T) {
	in := sample()
	in.Nodes[1].ServiceTimeUs = 200_000
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	m := r.Nodes["service"]
	if m.QueueWaitObservations == 0 || m.P95QueueWaitUs <= 0 || m.MeanQueueWaitUs <= 0 {
		t.Fatalf("queue wait not measured: %+v", m)
	}
	in.Nodes[1].QueueCapacity = 0
	in.Nodes[1].ServiceTimeUs = 500_000
	r, err = Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Nodes["service"].QueueRejected == 0 || r.Generated != r.Completed+r.Failed+r.TimedOut {
		t.Fatalf("queue rejection incorrect: %+v", r)
	}
}
