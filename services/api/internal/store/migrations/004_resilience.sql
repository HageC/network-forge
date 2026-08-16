ALTER TABLE architecture_edges ADD COLUMN IF NOT EXISTS max_retries integer NOT NULL DEFAULT 0 CHECK (max_retries BETWEEN 0 AND 5);
ALTER TABLE architecture_edges ADD COLUMN IF NOT EXISTS retry_backoff_us bigint NOT NULL DEFAULT 0 CHECK (retry_backoff_us >= 0);
ALTER TABLE architecture_edges ADD COLUMN IF NOT EXISTS circuit_failure_threshold integer NOT NULL DEFAULT 0 CHECK (circuit_failure_threshold >= 0);
ALTER TABLE architecture_edges ADD COLUMN IF NOT EXISTS circuit_open_us bigint NOT NULL DEFAULT 0 CHECK (circuit_open_us >= 0);
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS retries integer NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS circuit_opens integer NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS circuit_rejected integer NOT NULL DEFAULT 0;
