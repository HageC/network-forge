ALTER TABLE architecture_nodes ADD COLUMN IF NOT EXISTS rate_limit_rps integer NOT NULL DEFAULT 0 CHECK (rate_limit_rps >= 0);
ALTER TABLE architecture_nodes ADD COLUMN IF NOT EXISTS hit_rate double precision NOT NULL DEFAULT 0 CHECK (hit_rate BETWEEN 0 AND 1);
