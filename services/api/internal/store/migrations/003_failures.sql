CREATE TABLE IF NOT EXISTS failure_scenarios (
  version_id bigint NOT NULL REFERENCES architecture_versions(id) ON DELETE CASCADE,
  failure_id text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('node_outage', 'edge_partition')),
  target_id text NOT NULL,
  start_us bigint NOT NULL CHECK (start_us >= 0),
  duration_us bigint NOT NULL CHECK (duration_us > 0),
  PRIMARY KEY(version_id, failure_id)
);
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS failures_triggered integer NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS edge_drops jsonb NOT NULL DEFAULT '{}'::jsonb;
