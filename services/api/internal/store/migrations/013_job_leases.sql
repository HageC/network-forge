ALTER TABLE background_jobs ADD COLUMN lease_owner text;
ALTER TABLE background_jobs ADD COLUMN lease_expires_at timestamptz;
CREATE INDEX background_jobs_claim_idx ON background_jobs(status,lease_expires_at,created_at);
