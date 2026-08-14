CREATE TABLE IF NOT EXISTS profiles (
  id text PRIMARY KEY,
  display_name text NOT NULL
);
INSERT INTO profiles (id, display_name) VALUES ('local', 'Local workspace') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS projects (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id text NOT NULL REFERENCES profiles(id),
  name text NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS projects_owner_updated_idx ON projects(owner_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS architecture_versions (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  version_number integer NOT NULL CHECK (version_number > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(project_id, version_number)
);
CREATE INDEX IF NOT EXISTS architecture_versions_project_idx ON architecture_versions(project_id, version_number DESC);

CREATE TABLE IF NOT EXISTS architecture_nodes (
  version_id bigint NOT NULL REFERENCES architecture_versions(id) ON DELETE CASCADE,
  node_id text NOT NULL,
  kind text NOT NULL,
  service_time_us bigint NOT NULL CHECK (service_time_us >= 0),
  concurrency integer NOT NULL CHECK (concurrency > 0),
  queue_capacity integer NOT NULL CHECK (queue_capacity >= 0),
  position_x double precision NOT NULL DEFAULT 0,
  position_y double precision NOT NULL DEFAULT 0,
  PRIMARY KEY(version_id, node_id)
);
CREATE TABLE IF NOT EXISTS architecture_edges (
  version_id bigint NOT NULL REFERENCES architecture_versions(id) ON DELETE CASCADE,
  edge_id text NOT NULL,
  source_node_id text NOT NULL,
  target_node_id text NOT NULL,
  latency_us bigint NOT NULL CHECK (latency_us >= 0),
  loss_probability double precision NOT NULL CHECK (loss_probability BETWEEN 0 AND 1),
  PRIMARY KEY(version_id, edge_id),
  FOREIGN KEY(version_id, source_node_id) REFERENCES architecture_nodes(version_id, node_id),
  FOREIGN KEY(version_id, target_node_id) REFERENCES architecture_nodes(version_id, node_id)
);
CREATE TABLE IF NOT EXISTS workload_profiles (
  version_id bigint PRIMARY KEY REFERENCES architecture_versions(id) ON DELETE CASCADE,
  source_node_id text NOT NULL,
  requests_per_second integer NOT NULL CHECK (requests_per_second > 0),
  duration_seconds integer NOT NULL CHECK (duration_seconds > 0),
  seed bigint NOT NULL,
  FOREIGN KEY(version_id, source_node_id) REFERENCES architecture_nodes(version_id, node_id)
);
CREATE TABLE IF NOT EXISTS simulation_runs (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  version_id bigint NOT NULL REFERENCES architecture_versions(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  generated integer NOT NULL,
  completed integer NOT NULL,
  failed integer NOT NULL,
  timed_out integer NOT NULL,
  mean_latency_us double precision NOT NULL,
  p50_us bigint NOT NULL,
  p95_us bigint NOT NULL,
  p99_us bigint NOT NULL,
  events_processed integer NOT NULL,
  node_metrics jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS simulation_runs_project_idx ON simulation_runs(project_id, created_at DESC);
