ALTER TABLE failure_scenarios DROP CONSTRAINT IF EXISTS failure_scenarios_kind_check;
ALTER TABLE failure_scenarios ADD CONSTRAINT failure_scenarios_kind_check CHECK (kind IN ('node_outage', 'edge_partition', 'node_degradation', 'edge_loss_spike'));
ALTER TABLE failure_scenarios ADD COLUMN IF NOT EXISTS service_multiplier double precision NOT NULL DEFAULT 0 CHECK (service_multiplier >= 0);
ALTER TABLE failure_scenarios ADD COLUMN IF NOT EXISTS loss_probability double precision NOT NULL DEFAULT 0 CHECK (loss_probability BETWEEN 0 AND 1);
