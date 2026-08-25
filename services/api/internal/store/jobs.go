package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

type JobRecord struct {
	ID            string
	Kind          string
	Status        string
	Input         json.RawMessage
	ProgressDone  int
	ProgressTotal int
	Result        json.RawMessage
	Error         string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (s *Store) CreateJob(ctx context.Context, j JobRecord) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO background_jobs(id,kind,status,input,progress_total,created_at,updated_at) VALUES($1,$2,'queued',$3,$4,$5,$5)`, j.ID, j.Kind, j.Input, j.ProgressTotal, j.CreatedAt)
	return err
}

func scanJob(row interface{ Scan(...any) error }) (JobRecord, error) {
	var j JobRecord
	var result []byte
	err := row.Scan(&j.ID, &j.Kind, &j.Status, &j.Input, &j.ProgressDone, &j.ProgressTotal, &result, &j.Error, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return JobRecord{}, err
	}
	j.Result = result
	return j, nil
}

const jobColumns = `id,kind,status,input,progress_done,progress_total,result,error,created_at,updated_at`

func (s *Store) GetJob(ctx context.Context, id string) (JobRecord, error) {
	return scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM background_jobs WHERE id=$1`, id))
}

func (s *Store) RecoverJobs(ctx context.Context, idFilter ...string) ([]JobRecord, error) {
	query := `SELECT ` + jobColumns + ` FROM background_jobs WHERE status='queued' OR (status='running' AND lease_expires_at<now()) ORDER BY created_at LIMIT 32`
	args := []any{}
	if len(idFilter) > 0 {
		query = `SELECT ` + jobColumns + ` FROM background_jobs WHERE (status='queued' OR (status='running' AND lease_expires_at<now())) AND id=$1 ORDER BY created_at`
		args = append(args, idFilter[0])
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []JobRecord{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) ClaimJob(ctx context.Context, id, owner string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE background_jobs SET status='running',lease_owner=$2,lease_expires_at=now()+interval '15 seconds',progress_done=0,updated_at=now() WHERE id=$1 AND (status='queued' OR (status='running' AND lease_expires_at<now()))`, id, owner)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Store) RenewJob(ctx context.Context, id, owner string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE background_jobs SET lease_expires_at=now()+interval '15 seconds' WHERE id=$1 AND status='running' AND lease_owner=$2 AND lease_expires_at>now()`, id, owner)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Store) ReleaseJob(ctx context.Context, id, owner string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE background_jobs SET status='queued',lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status='running' AND lease_owner=$2`, id, owner)
	return err
}

func (s *Store) UpdateLeasedJob(ctx context.Context, id, owner, status string, done, total int, result json.RawMessage, message string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE background_jobs SET status=$3,progress_done=$4,progress_total=$5,result=$6,error=$7,updated_at=now(),lease_owner=CASE WHEN $3='running' THEN lease_owner ELSE NULL END,lease_expires_at=CASE WHEN $3='running' THEN lease_expires_at ELSE NULL END WHERE id=$1 AND lease_owner=$2 AND status='running' AND lease_expires_at>now()`, id, owner, status, done, total, result, message)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Store) UpdateJob(ctx context.Context, id, status string, done, total int, result json.RawMessage, message string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE background_jobs SET status=$2,progress_done=$3,progress_total=$4,result=$5,error=$6,updated_at=now() WHERE id=$1 AND status IN ('queued','running')`, id, status, done, total, result, message)
	return err
}

func (s *Store) CancelJob(ctx context.Context, id string) (JobRecord, error) {
	_, err := s.db.ExecContext(ctx, `UPDATE background_jobs SET status='cancelled',lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status IN ('queued','running')`, id)
	if err != nil {
		return JobRecord{}, err
	}
	return s.GetJob(ctx, id)
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM background_jobs WHERE id=$1`, id)
	return err
}

func IsJobNotFound(err error) bool { return err == sql.ErrNoRows }
