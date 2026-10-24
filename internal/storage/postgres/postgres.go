// Package postgres implements tasker.Storage using PostgreSQL.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tasker/internal/tasker"
)

//go:embed schema.sql
var schema string

// Storage uses a caller-owned pool. The caller must close the pool after use.
type Storage struct{ pool *pgxpool.Pool }

var _ tasker.Storage = (*Storage)(nil)

func New(pool *pgxpool.Pool) *Storage { return &Storage{pool: pool} }

// Migrate installs the initial schema and indexes atomically. Call it before
// serving requests; future schema changes require versioned migrations.
func (s *Storage) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Serialize schema setup across service instances.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(749218630017)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("postgres: migrate: %w", err)
	}
	return tx.Commit(ctx)
}

const columns = `id, kind, url, input, status, schedule_time, dispatch_deadline,
 attempt, max_attempts, result, last_error, created_at, updated_at, started_at,
 finished_at, lease_token, lease_expires_at`

func scanTask(row pgx.Row) (tasker.Task, error) {
	var t tasker.Task
	err := row.Scan(&t.ID, &t.Kind, &t.URL, &t.Input, &t.Status, &t.ScheduleTime,
		&t.DispatchDeadline, &t.Attempt, &t.MaxAttempts, &t.Result, &t.LastError,
		&t.CreatedAt, &t.UpdatedAt, &t.StartedAt, &t.FinishedAt, &t.LeaseToken, &t.LeaseExpiresAt)
	return t, err
}

func (s *Storage) Create(ctx context.Context, t tasker.Task) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO tasker_tasks (`+columns+`) VALUES
 ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		t.ID, t.Kind, t.URL, t.Input, t.Status, t.ScheduleTime, t.DispatchDeadline,
		t.Attempt, t.MaxAttempts, t.Result, t.LastError, t.CreatedAt, t.UpdatedAt,
		t.StartedAt, t.FinishedAt, t.LeaseToken, t.LeaseExpiresAt)
	return err
}

func (s *Storage) Get(ctx context.Context, id string) (tasker.Task, error) {
	t, err := scanTask(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM tasker_tasks WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return tasker.Task{}, &tasker.TaskNotFoundError{ID: id}
	}
	return t, err
}

func (s *Storage) ClaimDue(ctx context.Context, duration time.Duration) (tasker.Task, error) {
	if duration < time.Microsecond {
		return tasker.Task{}, fmt.Errorf("postgres: lease duration must be at least one microsecond")
	}
	// Recover a bounded batch even when no task can be dispatched. Each statement
	// commits independently, including when the subsequent claim finds no work.
	_, err := s.pool.Exec(ctx, `WITH exhausted AS (
 SELECT id FROM tasker_tasks
 WHERE status='running' AND lease_expires_at <= statement_timestamp() AND attempt >= max_attempts
 ORDER BY lease_expires_at, id LIMIT 100 FOR UPDATE SKIP LOCKED
 ) UPDATE tasker_tasks SET status='failed', finished_at=clock_timestamp(),
 updated_at=clock_timestamp(), lease_token='', lease_expires_at=NULL,
 last_error=CASE WHEN last_error='' THEN 'lease expired after final attempt' ELSE last_error END
 WHERE id IN (SELECT id FROM exhausted)`)
	if err != nil {
		return tasker.Task{}, err
	}
	t, err := scanTask(s.pool.QueryRow(ctx, `WITH candidate AS (
 SELECT id FROM tasker_tasks WHERE attempt < max_attempts AND
 ((status='queued' AND schedule_time <= statement_timestamp()) OR
 (status='running' AND lease_expires_at <= statement_timestamp()))
 ORDER BY schedule_time, id LIMIT 1 FOR UPDATE SKIP LOCKED
 ) UPDATE tasker_tasks SET status='running', attempt=attempt+1,
 lease_token=gen_random_uuid()::text,
 lease_expires_at=clock_timestamp()+$1::bigint * interval '1 microsecond',
 started_at=COALESCE(started_at, clock_timestamp()), updated_at=clock_timestamp()
 WHERE id IN (SELECT id FROM candidate) RETURNING `+columns, duration.Microseconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return tasker.Task{}, tasker.ErrNoTaskDue
	}
	return t, err
}

const owned = ` WHERE id=$1 AND lease_token=$2 AND status='running' AND lease_expires_at > clock_timestamp()`

func (s *Storage) updateOwned(ctx context.Context, query string, lease tasker.Lease, args ...any) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Lock in a separate statement before checking ownership. An UPDATE can
	// evaluate its expiry predicate before waiting for a row lock, and a lock
	// holder that makes no changes does not force that predicate to be rechecked.
	var id string
	err = tx.QueryRow(ctx, `SELECT id FROM tasker_tasks WHERE id=$1 FOR UPDATE`, lease.TaskID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return tasker.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	params := append([]any{lease.TaskID, lease.Token}, args...)
	tag, err := tx.Exec(ctx, query+owned, params...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tasker.ErrLeaseLost
	}
	return tx.Commit(ctx)
}

func (s *Storage) RenewLease(ctx context.Context, lease tasker.Lease, duration time.Duration) error {
	if duration < time.Microsecond {
		return fmt.Errorf("postgres: lease duration must be at least one microsecond")
	}
	return s.updateOwned(ctx, `UPDATE tasker_tasks SET
 lease_expires_at=clock_timestamp()+$3::bigint * interval '1 microsecond',
 updated_at=clock_timestamp()`, lease, duration.Microseconds())
}

func (s *Storage) Complete(ctx context.Context, lease tasker.Lease, result json.RawMessage) error {
	return s.updateOwned(ctx, `UPDATE tasker_tasks SET status='succeeded', result=$3,
 last_error='', finished_at=clock_timestamp(), updated_at=clock_timestamp(),
 lease_token='', lease_expires_at=NULL`, lease, result)
}

// Retry finalizes the task if its last permitted attempt has been consumed.
func (s *Storage) Retry(ctx context.Context, lease tasker.Lease, scheduleTime time.Time, lastError string) error {
	return s.updateOwned(ctx, `UPDATE tasker_tasks SET
 status=CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'queued' END,
 finished_at=CASE WHEN attempt >= max_attempts THEN clock_timestamp() ELSE NULL END,
 schedule_time=$3, last_error=$4, updated_at=clock_timestamp(),
 lease_token='', lease_expires_at=NULL`, lease, scheduleTime, lastError)
}

func (s *Storage) Fail(ctx context.Context, lease tasker.Lease, lastError string) error {
	return s.updateOwned(ctx, `UPDATE tasker_tasks SET status='failed', last_error=$3,
 finished_at=clock_timestamp(), updated_at=clock_timestamp(),
 lease_token='', lease_expires_at=NULL`, lease, lastError)
}

func (s *Storage) DeleteFinishedBefore(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("postgres: cleanup limit must be positive")
	}
	tag, err := s.pool.Exec(ctx, `WITH finished AS (
 SELECT id FROM tasker_tasks WHERE status IN ('succeeded','failed') AND finished_at < $1
 ORDER BY finished_at, id LIMIT $2 FOR UPDATE SKIP LOCKED
 ) DELETE FROM tasker_tasks WHERE id IN (SELECT id FROM finished)`, cutoff, limit)
	return int(tag.RowsAffected()), err
}
