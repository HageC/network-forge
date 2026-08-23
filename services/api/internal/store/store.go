package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"networkforge/services/api/internal/sim"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct{ db *sql.DB }
type ProjectSummary struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	UpdatedAt time.Time `json:"updatedAt"`
	Version   int       `json:"version"`
}
type PositionedNode struct {
	sim.Node
	PositionX float64 `json:"positionX"`
	PositionY float64 `json:"positionY"`
}
type Architecture struct {
	Nodes    []PositionedNode `json:"nodes"`
	Edges    []sim.Edge       `json:"edges"`
	Workload sim.Workload     `json:"workload"`
	Failures []sim.Failure    `json:"failures"`
}
type Project struct {
	ProjectSummary
	Architecture Architecture `json:"architecture"`
}
type RunSummary struct {
	ID        int64      `json:"id"`
	Version   int        `json:"version"`
	CreatedAt time.Time  `json:"createdAt"`
	Result    sim.Result `json:"result"`
}

func Open(ctx context.Context, url string) (*Store, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Migrate(ctx context.Context) error {
	files, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(723541826174)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}
		var applied bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name=$1)`, file.Name()).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		sqlText, readErr := migrations.ReadFile("migrations/" + file.Name())
		if readErr != nil {
			return readErr
		}
		for _, statement := range strings.Split(string(sqlText), ";") {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue
			}
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("migration %s failed: %w", file.Name(), err)
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(name) VALUES($1)`, file.Name()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) List(ctx context.Context) ([]ProjectSummary, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.id::text,p.name,p.updated_at,COALESCE(MAX(v.version_number),0) FROM projects p LEFT JOIN architecture_versions v ON v.project_id=p.id WHERE p.owner_id='local' GROUP BY p.id ORDER BY p.updated_at DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ProjectSummary{}
	for rows.Next() {
		var p ProjectSummary
		if err = rows.Scan(&p.ID, &p.Name, &p.UpdatedAt, &p.Version); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) Create(ctx context.Context, name string, a Architecture) (Project, error) {
	if err := validate(a); err != nil {
		return Project{}, err
	}
	if len(name) == 0 || len(name) > 120 {
		return Project{}, errors.New("project name must be 1 to 120 characters")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Project{}, err
	}
	defer tx.Rollback()
	var id string
	var updated time.Time
	err = tx.QueryRowContext(ctx, `INSERT INTO projects(owner_id,name) VALUES('local',$1) RETURNING id::text,updated_at`, name).Scan(&id, &updated)
	if err != nil {
		return Project{}, err
	}
	if _, err = writeVersion(ctx, tx, id, 1, a); err != nil {
		return Project{}, err
	}
	if err = tx.Commit(); err != nil {
		return Project{}, err
	}
	return Project{ProjectSummary: ProjectSummary{ID: id, Name: name, UpdatedAt: updated, Version: 1}, Architecture: a}, nil
}
func (s *Store) Get(ctx context.Context, id string) (Project, error) {
	var p Project
	err := s.db.QueryRowContext(ctx, `SELECT id::text,name,updated_at FROM projects WHERE id::text=$1 AND owner_id='local'`, id).Scan(&p.ID, &p.Name, &p.UpdatedAt)
	if err != nil {
		return Project{}, err
	}
	var versionID int64
	err = s.db.QueryRowContext(ctx, `SELECT id,version_number FROM architecture_versions WHERE project_id=$1::uuid ORDER BY version_number DESC LIMIT 1`, id).Scan(&versionID, &p.Version)
	if err != nil {
		return Project{}, err
	}
	p.Architecture, err = s.loadArchitecture(ctx, versionID)
	return p, err
}
func (s *Store) Save(ctx context.Context, id string, a Architecture) (Project, error) {
	if err := validate(a); err != nil {
		return Project{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Project{}, err
	}
	defer tx.Rollback()
	var p Project
	p.ID = id
	err = tx.QueryRowContext(ctx, `SELECT name FROM projects WHERE id::text=$1 AND owner_id='local' FOR UPDATE`, id).Scan(&p.Name)
	if err != nil {
		return Project{}, err
	}
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version_number),0)+1 FROM architecture_versions WHERE project_id=$1::uuid`, id).Scan(&p.Version)
	if err != nil {
		return Project{}, err
	}
	if _, err = writeVersion(ctx, tx, id, p.Version, a); err != nil {
		return Project{}, err
	}
	err = tx.QueryRowContext(ctx, `UPDATE projects SET updated_at=now() WHERE id=$1::uuid RETURNING updated_at`, id).Scan(&p.UpdatedAt)
	if err != nil {
		return Project{}, err
	}
	if err = tx.Commit(); err != nil {
		return Project{}, err
	}
	p.Architecture = a
	return p, nil
}
func (s *Store) Rename(ctx context.Context, id, name string) error {
	if len(name) == 0 || len(name) > 120 {
		return errors.New("invalid project name")
	}
	r, err := s.db.ExecContext(ctx, `UPDATE projects SET name=$2,updated_at=now() WHERE id::text=$1 AND owner_id='local'`, id, name)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
func (s *Store) Delete(ctx context.Context, id string) error {
	r, err := s.db.ExecContext(ctx, `DELETE FROM projects WHERE id::text=$1 AND owner_id='local'`, id)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func validate(a Architecture) error {
	nodes := make([]sim.Node, len(a.Nodes))
	for i, n := range a.Nodes {
		nodes[i] = n.Node
	}
	return sim.Validate(sim.Input{Nodes: nodes, Edges: a.Edges, Workload: a.Workload, Failures: a.Failures})
}
func writeVersion(ctx context.Context, tx *sql.Tx, projectID string, version int, a Architecture) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `INSERT INTO architecture_versions(project_id,version_number) VALUES($1::uuid,$2) RETURNING id`, projectID, version).Scan(&id)
	if err != nil {
		return 0, err
	}
	for _, n := range a.Nodes {
		_, err = tx.ExecContext(ctx, `INSERT INTO architecture_nodes(version_id,node_id,kind,service_time_us,concurrency,queue_capacity,position_x,position_y,rate_limit_rps,hit_rate,hourly_cost,connection_limit,autoscale_max_concurrency,autoscale_queue_threshold,autoscale_delay_us,autoscale_idle_us,transaction_lock_us,broker_capacity,max_deliveries,redelivery_delay_us) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`, id, n.ID, n.Kind, n.ServiceTimeUs, n.Concurrency, n.QueueCapacity, n.PositionX, n.PositionY, n.RateLimitRps, n.HitRate, n.HourlyCost, n.ConnectionLimit, n.AutoscaleMaxConcurrency, n.AutoscaleQueueThreshold, n.AutoscaleDelayUs, n.AutoscaleIdleUs, n.TransactionLockUs, n.BrokerCapacity, n.MaxDeliveries, n.RedeliveryDelayUs)
		if err != nil {
			return 0, err
		}
	}
	for _, e := range a.Edges {
		_, err = tx.ExecContext(ctx, `INSERT INTO architecture_edges(version_id,edge_id,source_node_id,target_node_id,latency_us,loss_probability,max_retries,retry_backoff_us,circuit_failure_threshold,circuit_open_us,cost_per_million_attempts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, id, e.ID, e.Source, e.Target, e.LatencyUs, e.LossProbability, e.MaxRetries, e.RetryBackoffUs, e.CircuitFailureThreshold, e.CircuitOpenUs, e.CostPerMillionAttempts)
		if err != nil {
			return 0, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO workload_profiles(version_id,source_node_id,requests_per_second,duration_seconds,seed,max_application_retries,application_retry_backoff_us) VALUES($1,$2,$3,$4,$5,$6,$7)`, id, a.Workload.Source, a.Workload.RequestsPerSecond, a.Workload.DurationSeconds, a.Workload.Seed, a.Workload.MaxApplicationRetries, a.Workload.ApplicationRetryBackoffUs)
	if err != nil {
		return 0, err
	}
	for _, f := range a.Failures {
		_, err = tx.ExecContext(ctx, `INSERT INTO failure_scenarios(version_id,failure_id,kind,target_id,start_us,duration_us,service_multiplier,loss_probability) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, id, f.ID, f.Kind, f.TargetID, f.StartUs, f.DurationUs, f.ServiceMultiplier, f.LossProbability)
		if err != nil {
			return 0, err
		}
	}
	return id, err
}
func (s *Store) loadArchitecture(ctx context.Context, versionID int64) (Architecture, error) {
	a := Architecture{Nodes: []PositionedNode{}, Edges: []sim.Edge{}}
	rows, err := s.db.QueryContext(ctx, `SELECT node_id,kind,service_time_us,concurrency,queue_capacity,position_x,position_y,rate_limit_rps,hit_rate,hourly_cost,connection_limit,autoscale_max_concurrency,autoscale_queue_threshold,autoscale_delay_us,autoscale_idle_us,transaction_lock_us,broker_capacity,max_deliveries,redelivery_delay_us FROM architecture_nodes WHERE version_id=$1 ORDER BY node_id`, versionID)
	if err != nil {
		return a, err
	}
	for rows.Next() {
		var n PositionedNode
		if err = rows.Scan(&n.ID, &n.Kind, &n.ServiceTimeUs, &n.Concurrency, &n.QueueCapacity, &n.PositionX, &n.PositionY, &n.RateLimitRps, &n.HitRate, &n.HourlyCost, &n.ConnectionLimit, &n.AutoscaleMaxConcurrency, &n.AutoscaleQueueThreshold, &n.AutoscaleDelayUs, &n.AutoscaleIdleUs, &n.TransactionLockUs, &n.BrokerCapacity, &n.MaxDeliveries, &n.RedeliveryDelayUs); err != nil {
			rows.Close()
			return a, err
		}
		a.Nodes = append(a.Nodes, n)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return a, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT edge_id,source_node_id,target_node_id,latency_us,loss_probability,max_retries,retry_backoff_us,circuit_failure_threshold,circuit_open_us,cost_per_million_attempts FROM architecture_edges WHERE version_id=$1 ORDER BY edge_id`, versionID)
	if err != nil {
		return a, err
	}
	for rows.Next() {
		var e sim.Edge
		if err = rows.Scan(&e.ID, &e.Source, &e.Target, &e.LatencyUs, &e.LossProbability, &e.MaxRetries, &e.RetryBackoffUs, &e.CircuitFailureThreshold, &e.CircuitOpenUs, &e.CostPerMillionAttempts); err != nil {
			rows.Close()
			return a, err
		}
		a.Edges = append(a.Edges, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return a, err
	}
	err = s.db.QueryRowContext(ctx, `SELECT source_node_id,requests_per_second,duration_seconds,seed,max_application_retries,application_retry_backoff_us FROM workload_profiles WHERE version_id=$1`, versionID).Scan(&a.Workload.Source, &a.Workload.RequestsPerSecond, &a.Workload.DurationSeconds, &a.Workload.Seed, &a.Workload.MaxApplicationRetries, &a.Workload.ApplicationRetryBackoffUs)
	if err != nil {
		return a, err
	}
	a.Failures = []sim.Failure{}
	rows, err = s.db.QueryContext(ctx, `SELECT failure_id,kind,target_id,start_us,duration_us,service_multiplier,loss_probability FROM failure_scenarios WHERE version_id=$1 ORDER BY failure_id`, versionID)
	if err != nil {
		return a, err
	}
	for rows.Next() {
		var f sim.Failure
		if err = rows.Scan(&f.ID, &f.Kind, &f.TargetID, &f.StartUs, &f.DurationUs, &f.ServiceMultiplier, &f.LossProbability); err != nil {
			rows.Close()
			return a, err
		}
		a.Failures = append(a.Failures, f)
	}
	err = rows.Err()
	rows.Close()
	return a, err
}
func (s *Store) Run(ctx context.Context, projectID string) (RunSummary, error) {
	return s.RunWithProgress(ctx, projectID, nil)
}
func (s *Store) RunWithProgress(ctx context.Context, projectID string, progress func(done, total int)) (RunSummary, error) {
	p, err := s.Get(ctx, projectID)
	if err != nil {
		return RunSummary{}, err
	}
	return s.RunSnapshotWithProgress(ctx, p, progress)
}
func (s *Store) RunSnapshotWithProgress(ctx context.Context, p Project, progress func(done, total int)) (RunSummary, error) {
	return s.runSnapshot(ctx, p, progress, "")
}

// RunSnapshotForJob commits the saved run and terminal job result in one transaction.
// Replaying a job after that commit returns the original run instead of inserting another.
func (s *Store) RunSnapshotForJob(ctx context.Context, p Project, jobID, owner string, progress func(done, total int)) (RunSummary, error) {
	if jobID == "" || owner == "" {
		return RunSummary{}, errors.New("job ID and lease owner are required")
	}
	return s.runSnapshot(ctx, p, progress, jobID, owner)
}
func (s *Store) runSnapshot(ctx context.Context, p Project, progress func(done, total int), jobID string, owner ...string) (RunSummary, error) {
	if jobID != "" {
		j, err := s.GetJob(ctx, jobID)
		if err != nil {
			return RunSummary{}, err
		}
		if j.Status == "succeeded" {
			var existing RunSummary
			if err := json.Unmarshal(j.Result, &existing); err != nil {
				return RunSummary{}, err
			}
			return existing, nil
		}
		if j.Status == "cancelled" {
			return RunSummary{}, context.Canceled
		}
	}
	projectID := p.ID
	nodes := make([]sim.Node, len(p.Architecture.Nodes))
	for i, n := range p.Architecture.Nodes {
		nodes[i] = n.Node
	}
	result, err := sim.RunContextWithProgress(ctx, sim.Input{Nodes: nodes, Edges: p.Architecture.Edges, Workload: p.Architecture.Workload, Failures: p.Architecture.Failures}, progress)
	if err != nil {
		return RunSummary{}, err
	}
	metrics, err := json.Marshal(result.Nodes)
	if err != nil {
		return RunSummary{}, err
	}
	edgeDrops, err := json.Marshal(result.EdgeDrops)
	if err != nil {
		return RunSummary{}, err
	}
	edgeAttempts, err := json.Marshal(result.EdgeAttempts)
	if err != nil {
		return RunSummary{}, err
	}
	findings, err := json.Marshal(result.Findings)
	if err != nil {
		return RunSummary{}, err
	}
	var run RunSummary
	run.Version = p.Version
	run.Result = result
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RunSummary{}, err
	}
	defer tx.Rollback()
	if jobID != "" {
		var status string
		var data []byte
		var leaseOwner sql.NullString
		var leaseValid bool
		if err := tx.QueryRowContext(ctx, `SELECT status,result,lease_owner,COALESCE(lease_expires_at>now(),false) FROM background_jobs WHERE id=$1 FOR UPDATE`, jobID).Scan(&status, &data, &leaseOwner, &leaseValid); err != nil {
			return RunSummary{}, err
		}
		if status == "succeeded" {
			var existing RunSummary
			if err := json.Unmarshal(data, &existing); err != nil {
				return RunSummary{}, err
			}
			return existing, nil
		}
		if status == "cancelled" {
			return RunSummary{}, context.Canceled
		}
		if status != "running" || !leaseValid || !leaseOwner.Valid || leaseOwner.String != owner[0] {
			return RunSummary{}, errors.New("job lease is no longer owned")
		}
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO simulation_runs(project_id,version_id,generated,completed,failed,timed_out,mean_latency_us,p50_us,p95_us,p99_us,events_processed,node_metrics,failures_triggered,edge_drops,retries,circuit_opens,circuit_rejected,edge_attempts,configured_node_cost_per_hour,estimated_scenario_cost,estimated_hourly_cost,cost_per_successful_request,findings,async_enqueued,background_completed,background_failed,background_timed_out,application_retries,application_retry_exhausted) SELECT $1::uuid,id,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13::jsonb,$14,$15,$16,$17::jsonb,$18,$19,$20,$21,$22::jsonb,$23,$24,$25,$26,$27,$28 FROM architecture_versions WHERE project_id=$1::uuid AND version_number=$29 RETURNING simulation_runs.id,simulation_runs.created_at`, projectID, result.Generated, result.Completed, result.Failed, result.TimedOut, result.MeanLatencyUs, result.P50Us, result.P95Us, result.P99Us, result.EventsProcessed, string(metrics), result.FailuresTriggered, string(edgeDrops), result.Retries, result.CircuitOpens, result.CircuitRejected, string(edgeAttempts), result.ConfiguredNodeCostPerHour, result.EstimatedScenarioCost, result.EstimatedHourlyCost, result.CostPerSuccessfulRequest, string(findings), result.AsyncEnqueued, result.BackgroundCompleted, result.BackgroundFailed, result.BackgroundTimedOut, result.ApplicationRetries, result.ApplicationRetryExhausted, p.Version).Scan(&run.ID, &run.CreatedAt)
	if err != nil {
		return RunSummary{}, fmt.Errorf("save run: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE simulation_runs SET background_redeliveries=$2,background_dead_letters=$3 WHERE id=$1`, run.ID, result.BackgroundRedeliveries, result.BackgroundDeadLetters); err != nil {
		return RunSummary{}, err
	}
	for _, trace := range result.Traces {
		data, marshalErr := json.Marshal(trace)
		if marshalErr != nil {
			return RunSummary{}, marshalErr
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO sampled_traces(run_id,request_index,trace_json) VALUES($1,$2,$3::jsonb)`, run.ID, trace.RequestIndex, string(data)); err != nil {
			return RunSummary{}, err
		}
	}
	if jobID != "" {
		data, err := json.Marshal(run)
		if err != nil {
			return RunSummary{}, err
		}
		_, err = tx.ExecContext(ctx, `UPDATE background_jobs SET status='succeeded',progress_done=progress_total,result=$2::jsonb,error='',updated_at=now() WHERE id=$1`, jobID, string(data))
		if err != nil {
			return RunSummary{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return RunSummary{}, err
	}
	return run, nil
}

func (s *Store) Traces(ctx context.Context, projectID string, runID int64) ([]sim.Trace, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM simulation_runs r JOIN projects p ON p.id=r.project_id WHERE p.id::text=$1 AND p.owner_id='local' AND r.id=$2`, projectID, runID).Scan(&exists); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT t.trace_json FROM sampled_traces t JOIN simulation_runs r ON r.id=t.run_id JOIN projects p ON p.id=r.project_id WHERE p.id::text=$1 AND p.owner_id='local' AND r.id=$2 ORDER BY t.request_index`, projectID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []sim.Trace{}
	for rows.Next() {
		var data []byte
		var trace sim.Trace
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &trace); err != nil {
			return nil, err
		}
		out = append(out, trace)
	}
	return out, rows.Err()
}
func (s *Store) Runs(ctx context.Context, projectID string) ([]RunSummary, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,v.version_number,r.created_at,r.generated,r.completed,r.failed,r.timed_out,r.mean_latency_us,r.p50_us,r.p95_us,r.p99_us,r.events_processed,r.node_metrics,r.failures_triggered,r.edge_drops,r.retries,r.circuit_opens,r.circuit_rejected,r.edge_attempts,r.configured_node_cost_per_hour,r.estimated_scenario_cost,r.estimated_hourly_cost,r.cost_per_successful_request,r.findings,r.async_enqueued,r.background_completed,r.background_failed,r.background_timed_out,r.application_retries,r.application_retry_exhausted,r.background_redeliveries,r.background_dead_letters FROM simulation_runs r JOIN architecture_versions v ON v.id=r.version_id JOIN projects p ON p.id=r.project_id WHERE p.id::text=$1 AND p.owner_id='local' ORDER BY r.created_at DESC,r.id DESC LIMIT 100`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunSummary{}
	for rows.Next() {
		var r RunSummary
		var metrics, edgeDrops, edgeAttempts, findings []byte
		var costPerSuccess sql.NullFloat64
		err = rows.Scan(&r.ID, &r.Version, &r.CreatedAt, &r.Result.Generated, &r.Result.Completed, &r.Result.Failed, &r.Result.TimedOut, &r.Result.MeanLatencyUs, &r.Result.P50Us, &r.Result.P95Us, &r.Result.P99Us, &r.Result.EventsProcessed, &metrics, &r.Result.FailuresTriggered, &edgeDrops, &r.Result.Retries, &r.Result.CircuitOpens, &r.Result.CircuitRejected, &edgeAttempts, &r.Result.ConfiguredNodeCostPerHour, &r.Result.EstimatedScenarioCost, &r.Result.EstimatedHourlyCost, &costPerSuccess, &findings, &r.Result.AsyncEnqueued, &r.Result.BackgroundCompleted, &r.Result.BackgroundFailed, &r.Result.BackgroundTimedOut, &r.Result.ApplicationRetries, &r.Result.ApplicationRetryExhausted, &r.Result.BackgroundRedeliveries, &r.Result.BackgroundDeadLetters)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(metrics, &r.Result.Nodes); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(edgeDrops, &r.Result.EdgeDrops); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(edgeAttempts, &r.Result.EdgeAttempts); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(findings, &r.Result.Findings); err != nil {
			return nil, err
		}
		if costPerSuccess.Valid {
			value := costPerSuccess.Float64
			r.Result.CostPerSuccessfulRequest = &value
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
