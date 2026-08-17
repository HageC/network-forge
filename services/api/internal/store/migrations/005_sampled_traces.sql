CREATE TABLE IF NOT EXISTS sampled_traces (
  run_id bigint NOT NULL REFERENCES simulation_runs(id) ON DELETE CASCADE,
  request_index integer NOT NULL CHECK (request_index >= 0),
  trace_json jsonb NOT NULL,
  PRIMARY KEY(run_id, request_index)
);
