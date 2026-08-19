ALTER TABLE architecture_nodes ADD COLUMN IF NOT EXISTS connection_limit integer NOT NULL DEFAULT 0 CHECK (connection_limit >= 0);
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS async_enqueued integer NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS background_completed integer NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS background_failed integer NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS background_timed_out integer NOT NULL DEFAULT 0;
