ALTER TABLE architecture_nodes ADD COLUMN IF NOT EXISTS hourly_cost double precision NOT NULL DEFAULT 0 CHECK (hourly_cost >= 0);
ALTER TABLE architecture_edges ADD COLUMN IF NOT EXISTS cost_per_million_attempts double precision NOT NULL DEFAULT 0 CHECK (cost_per_million_attempts >= 0);
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS configured_node_cost_per_hour double precision NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS estimated_scenario_cost double precision NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS estimated_hourly_cost double precision NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS cost_per_successful_request double precision;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS edge_attempts jsonb NOT NULL DEFAULT '{}'::jsonb;
