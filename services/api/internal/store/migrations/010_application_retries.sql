ALTER TABLE workload_profiles ADD COLUMN IF NOT EXISTS max_application_retries integer NOT NULL DEFAULT 0 CHECK (max_application_retries BETWEEN 0 AND 3);
ALTER TABLE workload_profiles ADD COLUMN IF NOT EXISTS application_retry_backoff_us bigint NOT NULL DEFAULT 0 CHECK (application_retry_backoff_us >= 0);
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS application_retries integer NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS application_retry_exhausted integer NOT NULL DEFAULT 0;
