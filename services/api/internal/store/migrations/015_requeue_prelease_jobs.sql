UPDATE background_jobs SET status='queued',progress_done=0,updated_at=now() WHERE status='running' AND lease_owner IS NULL;
