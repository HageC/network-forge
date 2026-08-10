package sim

import "testing"

func TestDatabaseConnectionLimit(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "db", Kind: "database", ServiceTimeUs: 500_000, Concurrency: 10, ConnectionLimit: 1, QueueCapacity: 1}}, Workload: Workload{Source: "db", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 1}}
	limited, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	m := limited.Nodes["db"]
	if m.ConnectionWaits == 0 || m.ConnectionRejected == 0 || m.ConnectionUtilization <= m.Utilization {
		t.Fatalf("connection pressure not measured: %+v", m)
	}
	in.Nodes[0].ConnectionLimit = 10
	unlimited, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if unlimited.Completed <= limited.Completed || unlimited.Nodes["db"].ConnectionRejected != 0 {
		t.Fatalf("limit did not affect outcomes: limited=%+v unlimited=%+v", limited, unlimited)
	}
}

func TestAsyncQueueAcknowledgesBeforeWorkerAndTracksFailure(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "queue", Kind: "queue", Concurrency: 10, QueueCapacity: 10, ServiceTimeUs: 1000}, {ID: "worker", Kind: "worker", Concurrency: 1, QueueCapacity: 0, ServiceTimeUs: 200_000}}, Edges: []Edge{{ID: "dispatch", Source: "queue", Target: "worker", LatencyUs: 1000}}, Workload: Workload{Source: "queue", RequestsPerSecond: 10, DurationSeconds: 1, Seed: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 10 || r.AsyncEnqueued != 10 || r.BackgroundCompleted == 0 || r.BackgroundFailed == 0 || r.Completed+r.Failed+r.TimedOut != r.Generated {
		t.Fatalf("bad async accounting: %+v", r)
	}
	if r.P95Us != 1000 {
		t.Fatalf("caller latency includes worker: %d", r.P95Us)
	}
	in.Edges[0].LossProbability = 1
	r, err = Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 10 || r.BackgroundCompleted != 0 || r.BackgroundFailed != 10 {
		t.Fatalf("transport failure changed caller ack: %+v", r)
	}
}

func TestQueueRequiresWorkerEdge(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "q", Kind: "queue", Concurrency: 1}}, Workload: Workload{Source: "q", RequestsPerSecond: 1, DurationSeconds: 1}}
	if err := Validate(in); err == nil {
		t.Fatal("queue without worker edge accepted")
	}
}
