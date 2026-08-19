ALTER TABLE architecture_nodes ADD COLUMN IF NOT EXISTS autoscale_max_concurrency integer NOT NULL DEFAULT 0 CHECK (autoscale_max_concurrency >= 0);
ALTER TABLE architecture_nodes ADD COLUMN IF NOT EXISTS autoscale_queue_threshold integer NOT NULL DEFAULT 0 CHECK (autoscale_queue_threshold >= 0);
ALTER TABLE architecture_nodes ADD COLUMN IF NOT EXISTS autoscale_delay_us bigint NOT NULL DEFAULT 0 CHECK (autoscale_delay_us >= 0);
ALTER TABLE architecture_nodes ADD COLUMN IF NOT EXISTS autoscale_idle_us bigint NOT NULL DEFAULT 0 CHECK (autoscale_idle_us >= 0);
