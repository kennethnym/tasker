package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tasker/internal/tasker"
)

func testStorage(t *testing.T) (*Storage, context.Context) {
	t.Helper()
	url := os.Getenv("TASKER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TASKER_TEST_DATABASE_URL to run Postgres integration tests")
	}
	ctx := t.Context()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := "test_" + tasker.NewTaskID()
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	return s, ctx
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
	if _, err := s.pool.Exec(ctx, `UPDATE tasker_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycle(t *testing.T) {
	s, ctx := testStorage(t)
	original := createTask(t, s, ctx, "task", 3)
	fetched := get(t, s, ctx, original.ID)
	// Compare timestamps as instants; pgx may decode in the local timezone.
	if !original.ScheduleTime.Equal(fetched.ScheduleTime) || !original.CreatedAt.Equal(fetched.CreatedAt) || !original.UpdatedAt.Equal(fetched.UpdatedAt) {
		t.Fatal("timestamps changed during round trip")
	}
	original.ScheduleTime = fetched.ScheduleTime
	original.CreatedAt = fetched.CreatedAt
	original.UpdatedAt = fetched.UpdatedAt
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
	if _, err := s.pool.Exec(ctx, `UPDATE tasker_tasks SET schedule_time=clock_timestamp()-interval '1 second' WHERE id=$1`, first.ID); err != nil {
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

func TestConcurrentClaimsAndSkipLocked(t *testing.T) {
	s, ctx := testStorage(t)
	createTask(t, s, ctx, "locked", 2)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT id FROM tasker_tasks WHERE id='locked' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	const count = 24
	for i := 0; i < count; i++ {
		createTask(t, s, ctx, fmt.Sprintf("task-%02d", i), 2)
	}
	var wg sync.WaitGroup
	ids := make(chan string, count)
	for i := 0; i < count; i++ {
		wg.Go(func() {
			claimCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			task, err := s.ClaimDue(claimCtx, time.Minute)
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
		if seen[id] || id == "locked" {
			t.Errorf("duplicate or locked claim: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("claimed %d tasks, want %d", len(seen), count)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if task := claim(t, s, ctx); task.ID != "locked" {
		t.Fatalf("unlocked claim: %s", task.ID)
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

func TestOwnershipExpiryWhileWaitingForLock(t *testing.T) {
	for _, operation := range []string{"renew", "complete", "retry", "fail"} {
		t.Run(operation, func(t *testing.T) {
			s, baseCtx := testStorage(t)
			ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
			defer cancel()
			createTask(t, s, ctx, "task", 2)
			task, err := s.ClaimDue(ctx, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			blocker, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			if _, err := blocker.Exec(ctx, `SELECT id FROM tasker_tasks WHERE id=$1 FOR UPDATE`, task.ID); err != nil {
				t.Fatal(err)
			}
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
			// Observe the actual lock wait before expiry, so the test cannot pass just
			// because the ownership operation started after the lease had expired.
			for {
				var waiting, expired bool
				err := s.pool.QueryRow(ctx, `SELECT EXISTS (
 SELECT 1 FROM pg_stat_activity WHERE $1::integer = ANY(pg_blocking_pids(pid))
 ), clock_timestamp() >= $2::timestamptz`, int32(blocker.Conn().PgConn().PID()), task.LeaseExpiresAt).Scan(&waiting, &expired)
				if err != nil {
					t.Fatal(err)
				}
				if expired {
					t.Fatal("lease expired before observing the lock wait")
				}
				if waiting {
					break
				}
				select {
				case err := <-result:
					t.Fatalf("operation did not wait for lock: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(5 * time.Millisecond):
				}
			}
			// Wait on database time without modifying the locked row. Updating that row
			// would trigger PostgreSQL's predicate recheck and hide the original bug.
			for {
				var expired bool
				if err := s.pool.QueryRow(ctx, `SELECT clock_timestamp() >= $1::timestamptz`, task.LeaseExpiresAt).Scan(&expired); err != nil {
					t.Fatal(err)
				}
				if expired {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(5 * time.Millisecond):
				}
			}
			if err := blocker.Commit(ctx); err != nil {
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
