CREATE TABLE IF NOT EXISTS background_jobs (
  id text PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('simulation','experiment','comparison','project_run')),
  status text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
  input jsonb NOT NULL,
  progress_done integer NOT NULL DEFAULT 0,
  progress_total integer NOT NULL CHECK (progress_total >= 0),
  result jsonb,
  error text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS background_jobs_status_idx ON background_jobs(status, created_at);
