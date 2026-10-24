package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"database/sql"
	"tasker/internal/tasker"
)

func testStorage(t *testing.T) (*Storage, context.Context) {
	t.Helper()
	s := openStorage(t, filepath.Join(t.TempDir(), "tasks.db"))
	return s, t.Context()
}
func openStorage(t *testing.T, path string) *Storage {
	t.Helper()
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func createTask(t *testing.T, s *Storage, ctx context.Context, id string, max int) tasker.Task {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	task := tasker.Task{ID: id, Kind: "test", URL: "https://example.com/task",
		Input: json.RawMessage(`{ "value": 1 }`), Status: tasker.StatusQueued,
		ScheduleTime: now.Add(-time.Minute), DispatchDeadline: "1m", MaxAttempts: max,
		CreatedAt: now, UpdatedAt: now}
	if err := s.Create(ctx, task); err != nil {
		t.Fatal(err)
	}
	return task
}

func claim(t *testing.T, s *Storage, ctx context.Context) tasker.Task {
	t.Helper()
	task, err := s.ClaimDue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return task
}
func lease(task tasker.Task) tasker.Lease {
	return tasker.Lease{TaskID: task.ID, Token: task.LeaseToken}
}
func get(t *testing.T, s *Storage, ctx context.Context, id string) tasker.Task {
	t.Helper()
	task, err := s.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return task
}
func expire(t *testing.T, s *Storage, ctx context.Context, id string) {
	t.Helper()
	if _, err := s.db.ExecContext(ctx, `UPDATE tasker_tasks SET lease_expires_at=?1 WHERE id=?2`, time.Now().Add(-time.Second).UnixMicro(), id); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycle(t *testing.T) {
	s, ctx := testStorage(t)
	original := createTask(t, s, ctx, "task", 3)
	fetched := get(t, s, ctx, original.ID)
	if !reflect.DeepEqual(original, fetched) {
		t.Fatalf("round trip: %+v != %+v", original, fetched)
	}
	if err := s.Create(ctx, original); err == nil {
		t.Fatal("duplicate accepted")
	}
	_, err := s.Get(ctx, "missing")
	var notFound *tasker.TaskNotFoundError
	if !errors.Is(err, tasker.ErrTaskNotFound) || !errors.As(err, &notFound) || notFound.ID != "missing" {
		t.Fatalf("not found: %v", err)
	}
	first := claim(t, s, ctx)
	if first.Attempt != 1 || first.Status != tasker.StatusRunning || first.LeaseToken == "" || first.StartedAt == nil || first.LeaseExpiresAt == nil {
		t.Fatalf("invalid claim: %+v", first)
	}
	if err := s.RenewLease(ctx, lease(first), 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	renewed := get(t, s, ctx, first.ID)
	if !renewed.LeaseExpiresAt.After(*first.LeaseExpiresAt) {
		t.Fatal("lease not extended")
	}
	future := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	if err := s.Retry(ctx, lease(first), future, "try again"); err != nil {
		t.Fatal(err)
	}
	queued := get(t, s, ctx, first.ID)
	if queued.Status != tasker.StatusQueued || queued.LeaseToken != "" || queued.LeaseExpiresAt != nil || queued.LastError != "try again" || !queued.ScheduleTime.Equal(future) {
		t.Fatalf("retry: %+v", queued)
	}
	if _, err := s.ClaimDue(ctx, time.Minute); !errors.Is(err, tasker.ErrNoTaskDue) {
		t.Fatalf("future claim: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE tasker_tasks SET schedule_time=?1 WHERE id=?2`, time.Now().Add(-time.Second).UnixMicro(), first.ID); err != nil {
		t.Fatal(err)
	}
	second := claim(t, s, ctx)
	if second.Attempt != 2 || second.LeaseToken == first.LeaseToken || !second.StartedAt.Equal(*first.StartedAt) {
		t.Fatalf("second claim: %+v", second)
	}
	if err := s.Complete(ctx, lease(first), nil); !errors.Is(err, tasker.ErrLeaseLost) {
		t.Fatalf("stale completion: %v", err)
	}
	result := json.RawMessage(`{"ok":true}`)
	if err := s.Complete(ctx, lease(second), result); err != nil {
		t.Fatal(err)
	}
	done := get(t, s, ctx, first.ID)
	if done.Status != tasker.StatusSucceeded || string(done.Result) != string(result) || done.FinishedAt == nil || done.LeaseToken != "" || done.LeaseExpiresAt != nil || done.LastError != "" {
		t.Fatalf("completion: %+v", done)
	}
	if err := s.Fail(ctx, lease(second), "late"); !errors.Is(err, tasker.ErrLeaseLost) {
		t.Fatalf("terminal write: %v", err)
	}
}

func TestExpiredLeaseAndExhaustion(t *testing.T) {
	s, ctx := testStorage(t)
	createTask(t, s, ctx, "task", 2)
	first := claim(t, s, ctx)
	expire(t, s, ctx, first.ID)
	for name, update := range map[string]func() error{
		"renew":    func() error { return s.RenewLease(ctx, lease(first), time.Minute) },
		"complete": func() error { return s.Complete(ctx, lease(first), nil) },
		"retry":    func() error { return s.Retry(ctx, lease(first), time.Now(), "retry") },
		"fail":     func() error { return s.Fail(ctx, lease(first), "failed") },
	} {
		if err := update(); !errors.Is(err, tasker.ErrLeaseLost) {
			t.Errorf("expired %s: %v", name, err)
		}
	}
	second := claim(t, s, ctx)
	if second.Attempt != 2 || second.LeaseToken == first.LeaseToken {
		t.Fatalf("reclaim: %+v", second)
	}
	expire(t, s, ctx, second.ID)
	if _, err := s.ClaimDue(ctx, time.Minute); !errors.Is(err, tasker.ErrNoTaskDue) {
		t.Fatalf("exhausted claim: %v", err)
	}
	failed := get(t, s, ctx, first.ID)
	if failed.Status != tasker.StatusFailed || failed.Attempt != 2 || failed.FinishedAt == nil || failed.LeaseToken != "" || failed.LeaseExpiresAt != nil || failed.LastError == "" {
		t.Fatalf("exhausted: %+v", failed)
	}
}

func TestOutcomesAndCleanup(t *testing.T) {
	s, ctx := testStorage(t)
	for _, outcome := range []string{"complete", "fail", "retry"} {
		createTask(t, s, ctx, outcome, 1)
		task := claim(t, s, ctx)
		var err error
		switch outcome {
		case "complete":
			err = s.Complete(ctx, lease(task), nil)
		case "fail":
			err = s.Fail(ctx, lease(task), "failure")
		case "retry":
			err = s.Retry(ctx, lease(task), time.Now(), "exhausted")
		}
		if err != nil {
			t.Fatal(err)
		}
		done := get(t, s, ctx, task.ID)
		if done.FinishedAt == nil || (outcome == "complete" && done.Result != nil) || (outcome != "complete" && done.Status != tasker.StatusFailed) {
			t.Fatalf("%s: %+v", outcome, done)
		}
	}
	createTask(t, s, ctx, "running", 2)
	claim(t, s, ctx)
	createTask(t, s, ctx, "queued", 2)
	if n, err := s.DeleteFinishedBefore(ctx, time.Now().Add(-time.Hour), 10); err != nil || n != 0 {
		t.Fatalf("early cleanup: %d %v", n, err)
	}
	for _, want := range []int{2, 1, 0} {
		if n, err := s.DeleteFinishedBefore(ctx, time.Now().Add(time.Hour), 2); err != nil || n != want {
			t.Fatalf("cleanup: %d %v; want %d", n, err, want)
		}
	}
	if get(t, s, ctx, "running").Status != tasker.StatusRunning || get(t, s, ctx, "queued").Status != tasker.StatusQueued {
		t.Fatal("cleanup changed active tasks")
	}
	if _, err := s.Get(ctx, "complete"); !errors.Is(err, tasker.ErrTaskNotFound) {
		t.Fatalf("deleted lookup: %v", err)
	}
}

func TestConcurrentClaims(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	stores := []*Storage{openStorage(t, path), openStorage(t, path)}
	ctx := t.Context()
	const count = 24
	for i := 0; i < count; i++ {
		createTask(t, stores[0], ctx, fmt.Sprintf("task-%02d", i), 2)
	}
	var wg sync.WaitGroup
	ids := make(chan string, count)
	for i := 0; i < count; i++ {
		s := stores[i%len(stores)]
		wg.Go(func() {
			task, err := s.ClaimDue(ctx, time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			if task.Attempt != 1 {
				t.Errorf("attempt: %d", task.Attempt)
			}
			ids <- task.ID
		})
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate claim: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("claimed %d tasks, want %d", len(seen), count)
	}
}

func TestInvalidArgumentsAndCancellation(t *testing.T) {
	s, ctx := testStorage(t)
	if _, err := s.ClaimDue(ctx, 0); err == nil {
		t.Fatal("accepted zero duration")
	}
	if err := s.RenewLease(ctx, tasker.Lease{}, -time.Second); err == nil {
		t.Fatal("accepted negative duration")
	}
	if _, err := s.DeleteFinishedBefore(ctx, time.Now(), 0); err == nil {
		t.Fatal("accepted zero limit")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Get(cancelled, "missing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled get: %v", err)
	}
}

func TestJSONValuesAndFailedWrites(t *testing.T) {
	s, ctx := testStorage(t)
	template := createTask(t, s, ctx, "template", 2)
	for i, input := range []string{`null`, `true`, `42`, `"hello"`, `[1, null]`} {
		task := template
		task.ID = fmt.Sprintf("json-%d", i)
		task.Input = json.RawMessage(input)
		if err := s.Create(ctx, task); err != nil {
			t.Fatal(err)
		}
		if got := get(t, s, ctx, task.ID); string(got.Input) != input {
			t.Fatalf("JSON round trip: %s != %s", got.Input, input)
		}
	}
	template.ID = "invalid"
	template.Input = json.RawMessage(`{`)
	if err := s.Create(ctx, template); err == nil {
		t.Fatal("accepted malformed JSON")
	}
	if _, err := s.Get(ctx, template.ID); !errors.Is(err, tasker.ErrTaskNotFound) {
		t.Fatalf("failed insert persisted: %v", err)
	}
	task := claim(t, s, ctx)
	if err := s.Complete(ctx, lease(task), json.RawMessage(`{`)); err == nil {
		t.Fatal("accepted malformed result")
	}
	if got := get(t, s, ctx, task.ID); got.Status != tasker.StatusRunning || got.LeaseToken != task.LeaseToken {
		t.Fatalf("failed completion changed state: %+v", got)
	}
	if err := s.Complete(ctx, lease(task), json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	if got := get(t, s, ctx, task.ID); string(got.Result) != "null" {
		t.Fatalf("explicit null result: %s", got.Result)
	}
}

func TestOwnershipExpiryWhileWaitingForWriter(t *testing.T) {
	for _, operation := range []string{"renew", "complete", "retry", "fail"} {
		t.Run(operation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tasks.db")
			s := openStorage(t, path)
			blocker := openStorage(t, path)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			createTask(t, s, ctx, "task", 2)
			task, err := s.ClaimDue(ctx, 250*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := blocker.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			result := make(chan error, 1)
			go func() {
				switch operation {
				case "renew":
					result <- s.RenewLease(ctx, lease(task), time.Minute)
				case "complete":
					result <- s.Complete(ctx, lease(task), json.RawMessage(`true`))
				case "retry":
					result <- s.Retry(ctx, lease(task), time.Now(), "retry")
				case "fail":
					result <- s.Fail(ctx, lease(task), "failed")
				}
			}()
			// Confirm the operation has taken the sole connection while the independent
			// handle holds the writer lock, before waiting out the lease.
			for s.db.Stats().InUse == 0 {
				if !time.Now().Before(*task.LeaseExpiresAt) {
					t.Fatal("operation did not start before lease expiry")
				}
				select {
				case err := <-result:
					t.Fatalf("operation did not wait: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			select {
			case err := <-result:
				t.Fatalf("operation bypassed writer lock: %v", err)
			case <-time.After(time.Until(*task.LeaseExpiresAt) + 10*time.Millisecond):
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if !errors.Is(err, tasker.ErrLeaseLost) {
					t.Fatalf("expired %s after lock wait: %v", operation, err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if got := get(t, s, ctx, task.ID); !reflect.DeepEqual(got, task) {
				t.Fatalf("rejected operation changed task: %+v", got)
			}
		})
	}
}

func TestReopenAndMigrations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks ?# owned.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	original := createTask(t, s, t.Context(), "persisted", 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStorage(t, path)
	if got := get(t, reopened, t.Context(), original.ID); !reflect.DeepEqual(got, original) {
		t.Fatalf("reopen changed task: %+v", got)
	}
	var mode string
	var version, appID, synchronous, timeout int
	for query, dest := range map[string]any{
		"PRAGMA journal_mode": &mode, "PRAGMA user_version": &version,
		"PRAGMA application_id": &appID, "PRAGMA synchronous": &synchronous,
		"PRAGMA busy_timeout": &timeout,
	} {
		if err := reopened.db.QueryRow(query).Scan(dest); err != nil {
			t.Fatal(err)
		}
	}
	if mode != "wal" || version != 1 || appID != applicationID || synchronous != 2 || timeout != 5000 {
		t.Fatalf("settings: %s %d %d %d %d", mode, version, appID, synchronous, timeout)
	}
}

func TestRejectUnownedAndNewerDatabases(t *testing.T) {
	for name, setup := range map[string]string{
		"unowned": `CREATE TABLE users (id INTEGER PRIMARY KEY)`,
		"newer":   `PRAGMA application_id=1413567307; PRAGMA user_version=2`,
		"other":   `PRAGMA application_id=1234`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tasks.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(setup); err != nil {
				t.Fatal(err)
			}
			if s, err := Open(t.Context(), path); err == nil {
				s.Close()
				t.Fatal("accepted incompatible database")
			}
			var count int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='tasker_tasks'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("failed migration created tables")
			}
		})
	}
}
