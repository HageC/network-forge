package sim

import "testing"

func TestDatabaseTransactionLockAddsContention(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "db", Kind: "database", ServiceTimeUs: 500_000, TransactionLockUs: 500_000, Concurrency: 2, QueueCapacity: 4}}, Workload: Workload{Source: "db", RequestsPerSecond: 4, DurationSeconds: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Nodes["db"].LockWaits == 0 || r.Nodes["db"].TotalLockWaitUs == 0 || r.Completed >= 4 {
		t.Fatalf("lock contention not reflected: %+v", r)
	}
}

func TestQueueRedeliversAfterTemporaryLoss(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "q", Kind: "queue", Concurrency: 1, MaxDeliveries: 2, RedeliveryDelayUs: 200_000}, {ID: "w", Kind: "worker", Concurrency: 1}}, Edges: []Edge{{ID: "send", Source: "q", Target: "w"}}, Workload: Workload{Source: "q", RequestsPerSecond: 1, DurationSeconds: 1, Seed: 1}, Failures: []Failure{{ID: "spike", Kind: "edge_loss_spike", TargetID: "send", StartUs: 0, DurationUs: 100_000, LossProbability: 1}}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 1 || r.BackgroundCompleted != 1 || r.BackgroundRedeliveries != 1 || r.BackgroundDeadLetters != 0 {
		t.Fatalf("redelivery: %+v", r)
	}
}

func TestQueueBrokerCapacityRejectsExcess(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "q", Kind: "queue", Concurrency: 1, BrokerCapacity: 1}, {ID: "w", Kind: "worker", Concurrency: 1, ServiceTimeUs: 900_000}}, Edges: []Edge{{ID: "send", Source: "q", Target: "w"}}, Workload: Workload{Source: "q", RequestsPerSecond: 2, DurationSeconds: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 1 || r.Failed != 1 || r.Nodes["q"].BrokerRejected != 1 || r.Nodes["q"].PeakBrokerDepth != 1 {
		t.Fatalf("broker pressure: %+v", r)
	}
}

func TestQueueDeliveryExhaustionCreatesDeadLetter(t *testing.T) {
	in := Input{Nodes: []Node{{ID: "q", Kind: "queue", Concurrency: 1, MaxDeliveries: 2, RedeliveryDelayUs: 100_000}, {ID: "w", Kind: "worker", Concurrency: 1}}, Edges: []Edge{{ID: "send", Source: "q", Target: "w", LossProbability: 1}}, Workload: Workload{Source: "q", RequestsPerSecond: 1, DurationSeconds: 1}}
	r, err := Run(in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 1 || r.BackgroundRedeliveries != 1 || r.BackgroundFailed != 1 || r.BackgroundDeadLetters != 1 || r.EdgeAttempts["send"] != 2 {
		t.Fatalf("dead letter: %+v", r)
	}
}
