ALTER TABLE architecture_nodes ADD COLUMN transaction_lock_us bigint NOT NULL DEFAULT 0 CHECK (transaction_lock_us>=0);
ALTER TABLE architecture_nodes ADD COLUMN broker_capacity integer NOT NULL DEFAULT 0 CHECK (broker_capacity>=0);
ALTER TABLE architecture_nodes ADD COLUMN max_deliveries integer NOT NULL DEFAULT 0 CHECK (max_deliveries>=0);
ALTER TABLE architecture_nodes ADD COLUMN redelivery_delay_us bigint NOT NULL DEFAULT 0 CHECK (redelivery_delay_us>=0);
ALTER TABLE simulation_runs ADD COLUMN background_redeliveries integer NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN background_dead_letters integer NOT NULL DEFAULT 0;
