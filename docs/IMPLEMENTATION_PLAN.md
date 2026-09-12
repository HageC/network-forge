# Implementation plan

## Completed

- Audited repository: only README and project-local skills existed.
- Defined initial model, API boundary and engine semantics.

## Verified

- Deterministic Go event engine with bounded queues, edge latency/loss, accounting and observed quantiles; `go test ./...` passes.
- Gateway per-second rate limits, round-robin load balancing, and seeded cache hits/misses affect actual request outcomes; unit tests pass.
- Scheduled node outages interrupt active and queued work; edge partitions block transmissions. Recovery and overlapping failures are covered by engine tests.
- Bounded exponential transport retries and edge circuit breakers affect outcomes; retry budgets, recovery, breaker opening and pending-retry cancellation have unit tests.
- Deterministically sampled request traces record lifecycle events and terminal outcomes with a 50-trace / 128-event retention cap; tests cover queue, timeout and truncation semantics.
- Sampled traces are stored separately from run summaries and can be opened from saved-run history in the web workspace.
- Repeated-seed experiments return per-run metrics, empirical outcome spread, an SLO violation rate, and a deterministic bootstrap interval for mean run P95; the workspace plots per-seed error rates.
- Architecture comparison pairs repeated seeds across a captured baseline and candidate, reports side-by-side metrics and a paired bootstrap interval, and only names a winner for the selected objective when the observed difference is clear.
- Configurable synthetic node and edge pricing produces scenario, hourly-at-load and per-success estimates from actual modeled edge attempts; saved runs and experiment summaries retain the cost metrics.
- Completed runs include ranked architecture review findings backed by observed queue, utilization, cache, outage, gateway and link metrics; run history preserves the findings.
- Database nodes can cap active operations by connection limit; asynchronous queue nodes acknowledge callers and dispatch separately measured background jobs. Both settings and run outcomes persist through PostgreSQL.
- Queue-pressure autoscaling adds slots after a configured delay, removes idle spare slots, and integrates changing capacity into node utilization and synthetic cost. Scaling policy is saved with architecture versions.
- Bounded application retries restart failed caller attempts from the workload source with exponential delay; policy and retry counters persist in PostgreSQL, and review findings flag amplification.
- The workspace has bounded undo/redo history for graph, failure schedule and workload edits; adjacent changes are grouped into a single history step.
- The editor exposes gateway/cache configuration and edge latency/loss controls.
- The editor schedules node outages and edge partitions; project versions and saved runs preserve their configuration and measured effects.
- Next.js editor and API proxy; TypeScript check and production build pass.
- Project create/open/version/run/history/traces/delete passed through the live web proxy with PostgreSQL. Saved versions, findings and runs survived an API and web container restart. The Go lifecycle integration test passed against PostgreSQL.
- Docker Compose built and started the API, web and PostgreSQL containers. The web build excludes host `node_modules` and `.next`; the project proxy returns HTTP 204 correctly for deletion.
- Node degradation and temporary edge loss spikes run on the virtual event clock, recover at window boundaries, and persist with saved project versions. Engine and PostgreSQL integration tests pass.
- An in-process job manager executes simulations, experiments, comparisons and captured saved-project versions independently of browser requests. Polling exposes progress and results; cancellation uses contexts; the workspace reconnects to its active job after reload.
- The editor provides three templates, live server-side validation, a learning guide, and architecture JSON import/export and clipboard actions.
- Playwright browser tests cover template validation and a run, export/import, and experiment reconnection after reload. Go tests pass with PostgreSQL, and the Compose production build passes.
- PostgreSQL now persists background job snapshots and results; startup requeues unfinished jobs. A database-backed recovery test simulates a running job interrupted by an API restart.
- Saved-project runs, sampled traces and successful job results now commit atomically; replaying the same job ID returns the original run. Database tests cover replay, terminal-state protection and cancellation.
- PostgreSQL jobs use renewable leases and fenced writes. Tests cover overlapping claims, expired-lease takeover, stale-worker rejection and two workers contending for one saved run.
- Database nodes can model one exclusive transaction lock. Queue nodes can cap outstanding broker messages, redeliver failed background jobs and count dead letters; configuration and outcomes persist in project versions and run history.
- The calibration endpoint and editor compare measured aggregate throughput, error rate and P95 with a bounded search over shared service-time scales. The report displays both baseline and fitted error scores.

## Remaining

1. Validate calibration against independent production measurements; add per-node fitting only where data can identify parameters.
2. Extend the virtual database and broker beyond one hot lock and bounded redelivery: isolation, deadlocks, visibility timeouts and broker restart semantics.
3. Add per-error retry policies, nested tracing where supported and correlated failure models.
4. Extend analytics, comparison persistence, resource-aware pricing and architecture reviewer rules.
5. Broaden live database coverage for project errors and concurrent writes; add observability, lease-health monitoring and deployment hardening.

Only verified behavior is marked complete.
