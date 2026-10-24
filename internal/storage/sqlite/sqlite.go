// Package sqlite stores tasks in a Tasker-owned SQLite database file.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
	"tasker/internal/tasker"
)

//go:embed 001_initial.sql
var initialSchema string

const applicationID = 0x5441534b // TASK

// Storage owns its database connection. Call Close after stopping the service.
type Storage struct{ db *sql.DB }

var _ tasker.Storage = (*Storage)(nil)

// Open creates or opens a dedicated database file and applies migrations.
// The parent directory must exist. Paths are filenames, not SQLite DSNs.
func Open(ctx context.Context, path string) (*Storage, error) {
	if path == "" {
		return nil, errors.New("sqlite: database path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dsn := url.URL{Scheme: "file", Path: filepath.ToSlash(absolute)}
	params := url.Values{"_txlock": {"immediate"}, "_pragma": {"busy_timeout(5000)", "synchronous(FULL)"}}
	dsn.RawQuery = params.Encode()
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, err
	}
	// Serialize local operations; independent handles are coordinated by SQLite.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Storage{db: db}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&mode); err != nil {
		db.Close()
		return nil, err
	}
	if mode != "wal" {
		db.Close()
		return nil, fmt.Errorf("sqlite: could not enable WAL (mode %q)", mode)
	}
	return s, nil
}

func (s *Storage) Close() error { return s.db.Close() }

// write takes the writer lock before reading the clock or task state. In
// particular, time spent waiting for another writer cannot extend ownership.
func (s *Storage) write(ctx context.Context, fn func(*sql.Tx, time.Time) error) error {
	tx, err := s.db.BeginTx(ctx, nil) // _txlock=immediate
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Storage) migrate(ctx context.Context) error {
	return s.write(ctx, func(tx *sql.Tx, _ time.Time) error {
		var appID, version int
		if err := tx.QueryRowContext(ctx, `PRAGMA application_id`).Scan(&appID); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
			return err
		}
		if appID == applicationID && version == 1 {
			return nil
		}
		if appID != 0 || version != 0 {
			return fmt.Errorf("sqlite: unsupported database application ID %d or schema version %d", appID, version)
		}
		var tables int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'`).Scan(&tables); err != nil {
			return err
		}
		if tables != 0 {
			return errors.New("sqlite: database is not empty; use a dedicated Tasker file")
		}
		if _, err := tx.ExecContext(ctx, initialSchema); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `PRAGMA application_id=1413567307; PRAGMA user_version=1`)
		return err
	})
}

const columns = `id, kind, url, input, status, schedule_time, dispatch_deadline,
 attempt, max_attempts, result, last_error, created_at, updated_at, started_at,
 finished_at, lease_token, lease_expires_at`

func scanTask(row *sql.Row) (tasker.Task, error) {
	var t tasker.Task
	var input string
	var result sql.NullString
	var scheduled, created, updated int64
	var started, finished, expires sql.NullInt64
	err := row.Scan(&t.ID, &t.Kind, &t.URL, &input, &t.Status, &scheduled,
		&t.DispatchDeadline, &t.Attempt, &t.MaxAttempts, &result, &t.LastError,
		&created, &updated, &started, &finished, &t.LeaseToken, &expires)
	if err != nil {
		return tasker.Task{}, err
	}
	t.Input = json.RawMessage(input)
	if result.Valid {
		t.Result = json.RawMessage(result.String)
	}
	t.ScheduleTime, t.CreatedAt, t.UpdatedAt = fromMicros(scheduled), fromMicros(created), fromMicros(updated)
	t.StartedAt, t.FinishedAt, t.LeaseExpiresAt = nullableTime(started), nullableTime(finished), nullableTime(expires)
	return t, nil
}
func fromMicros(n int64) time.Time { return time.UnixMicro(n).UTC() }
func nullableTime(n sql.NullInt64) *time.Time {
	if !n.Valid {
		return nil
	}
	t := fromMicros(n.Int64)
	return &t
}
func timeValue(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UnixMicro()
}
func jsonValue(j json.RawMessage) any {
	if j == nil {
		return nil
	}
	return string(j)
}

func (s *Storage) Create(ctx context.Context, t tasker.Task) error {
	return s.write(ctx, func(tx *sql.Tx, _ time.Time) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO tasker_tasks (`+columns+`) VALUES
 (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)`,
			t.ID, t.Kind, t.URL, jsonValue(t.Input), t.Status, t.ScheduleTime.UnixMicro(), t.DispatchDeadline,
			t.Attempt, t.MaxAttempts, jsonValue(t.Result), t.LastError, t.CreatedAt.UnixMicro(), t.UpdatedAt.UnixMicro(),
			timeValue(t.StartedAt), timeValue(t.FinishedAt), t.LeaseToken, timeValue(t.LeaseExpiresAt))
		return err
	})
}

func (s *Storage) Get(ctx context.Context, id string) (tasker.Task, error) {
	t, err := scanTask(s.db.QueryRowContext(ctx, `SELECT `+columns+` FROM tasker_tasks WHERE id=?1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return tasker.Task{}, &tasker.TaskNotFoundError{ID: id}
	}
	return t, err
}

func (s *Storage) ClaimDue(ctx context.Context, duration time.Duration) (tasker.Task, error) {
	if duration < time.Microsecond {
		return tasker.Task{}, errors.New("sqlite: lease duration must be at least one microsecond")
	}
	var task tasker.Task
	var found bool
	err := s.write(ctx, func(tx *sql.Tx, now time.Time) error {
		// Finalize a bounded recovery batch, including when there is no due task.
		_, err := tx.ExecContext(ctx, `UPDATE tasker_tasks SET status='failed', finished_at=?1,
 updated_at=?1, lease_token='', lease_expires_at=NULL,
 last_error=CASE WHEN last_error='' THEN 'lease expired after final attempt' ELSE last_error END
 WHERE id IN (SELECT id FROM tasker_tasks WHERE status='running'
 AND lease_expires_at <= ?1 AND attempt >= max_attempts ORDER BY lease_expires_at, id LIMIT 100)`, now.UnixMicro())
		if err != nil {
			return err
		}
		task, err = scanTask(tx.QueryRowContext(ctx, `UPDATE tasker_tasks SET
 status='running', attempt=attempt+1, lease_token=?2, lease_expires_at=?3,
 started_at=COALESCE(started_at, ?1), updated_at=?1
 WHERE id=(SELECT id FROM tasker_tasks WHERE attempt < max_attempts AND
 ((status='queued' AND schedule_time <= ?1) OR (status='running' AND lease_expires_at <= ?1))
 ORDER BY schedule_time, id LIMIT 1) RETURNING `+columns,
			now.UnixMicro(), uuid.NewString(), now.Add(duration).UnixMicro()))
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} // Commit exhausted-task recovery.
		found = err == nil
		return err
	})
	if err != nil {
		return tasker.Task{}, err
	}
	if !found {
		return tasker.Task{}, tasker.ErrNoTaskDue
	}
	return task, nil
}

const owned = ` WHERE id=:id AND lease_token=:token AND status='running' AND lease_expires_at > :now`

func (s *Storage) updateOwned(ctx context.Context, query string, lease tasker.Lease, args ...any) error {
	return s.write(ctx, func(tx *sql.Tx, now time.Time) error {
		params := append([]any{sql.Named("id", lease.TaskID), sql.Named("token", lease.Token), sql.Named("now", now.UnixMicro())}, args...)
		result, err := tx.ExecContext(ctx, query+owned, params...)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			return tasker.ErrLeaseLost
		}
		return nil
	})
}
func (s *Storage) RenewLease(ctx context.Context, lease tasker.Lease, duration time.Duration) error {
	if duration < time.Microsecond {
		return errors.New("sqlite: lease duration must be at least one microsecond")
	}
	return s.updateOwned(ctx, `UPDATE tasker_tasks SET lease_expires_at=:now+:duration, updated_at=:now`, lease, sql.Named("duration", duration.Microseconds()))
}
func (s *Storage) Complete(ctx context.Context, lease tasker.Lease, result json.RawMessage) error {
	return s.updateOwned(ctx, `UPDATE tasker_tasks SET status='succeeded', result=:result,
 last_error='', finished_at=:now, updated_at=:now, lease_token='', lease_expires_at=NULL`, lease, sql.Named("result", jsonValue(result)))
}

// Retry finalizes the task if its last permitted attempt has been consumed.
func (s *Storage) Retry(ctx context.Context, lease tasker.Lease, scheduleTime time.Time, lastError string) error {
	return s.updateOwned(ctx, `UPDATE tasker_tasks SET
 status=CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'queued' END,
 finished_at=CASE WHEN attempt >= max_attempts THEN :now ELSE NULL END,
 schedule_time=:schedule, last_error=:error, updated_at=:now, lease_token='', lease_expires_at=NULL`,
		lease, sql.Named("schedule", scheduleTime.UnixMicro()), sql.Named("error", lastError))
}
func (s *Storage) Fail(ctx context.Context, lease tasker.Lease, lastError string) error {
	return s.updateOwned(ctx, `UPDATE tasker_tasks SET status='failed', last_error=:error,
 finished_at=:now, updated_at=:now, lease_token='', lease_expires_at=NULL`, lease, sql.Named("error", lastError))
}
func (s *Storage) DeleteFinishedBefore(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, errors.New("sqlite: cleanup limit must be positive")
	}
	var count int64
	err := s.write(ctx, func(tx *sql.Tx, _ time.Time) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM tasker_tasks WHERE id IN (
 SELECT id FROM tasker_tasks WHERE status IN ('succeeded','failed') AND finished_at < ?1
 ORDER BY finished_at, id LIMIT ?2)`, cutoff.UnixMicro(), limit)
		if err != nil {
			return err
		}
		count, err = result.RowsAffected()
		return err
	})
	if err != nil {
		return 0, err
	}
	return int(count), nil
}
