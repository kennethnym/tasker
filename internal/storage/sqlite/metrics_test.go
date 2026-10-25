package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"tasker/internal/tasker"
)

func TestQueueStatsSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tasks.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { s.Close() })

	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, item := range []struct {
		id                 string
		status             tasker.Status
		scheduled, expires time.Time
	}{
		{"oldest", tasker.StatusQueued, now.Add(-time.Minute), time.Time{}},
		{"due", tasker.StatusQueued, now, time.Time{}},
		{"future", tasker.StatusQueued, now.Add(time.Hour), time.Time{}},
		{"running", tasker.StatusRunning, now.Add(-time.Hour), now.Add(time.Minute)},
		{"expired", tasker.StatusRunning, now.Add(-time.Hour), now},
		{"succeeded", tasker.StatusSucceeded, now.Add(-time.Hour), time.Time{}},
		{"failed", tasker.StatusFailed, now.Add(-time.Hour), time.Time{}},
	} {
		task := tasker.Task{ID: item.id, Status: item.status, Input: []byte(`{}`), MaxAttempts: 3,
			ScheduleTime: item.scheduled, CreatedAt: now, UpdatedAt: now}
		if item.status == tasker.StatusRunning {
			task.LeaseToken, task.LeaseExpiresAt = "lease", &item.expires
		}

		if item.status == tasker.StatusSucceeded || item.status == tasker.StatusFailed {
			task.FinishedAt = &now
		}

		if err := s.Create(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := s.QueueStats(ctx, now)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Due != 2 || stats.Scheduled != 1 || stats.Running != 1 || stats.Expired != 1 {
		t.Fatalf("incorrect persisted snapshot: %+v", stats)
	}

	if stats.OldestDueAt == nil || !stats.OldestDueAt.Equal(now.Add(-time.Minute)) {
		t.Fatalf("oldest due time = %v", stats.OldestDueAt)
	}

	// Moving the observation time must reclassify scheduled tasks without writes.
	stats, err = s.QueueStats(ctx, now.Add(2*time.Hour))
	if err != nil || stats.Due != 3 || stats.Scheduled != 0 || stats.Running != 0 || stats.Expired != 2 {
		t.Fatalf("time-based snapshot: %+v, %v", stats, err)
	}
}

func TestClaimReportsCommittedRecovery(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}

	defer s.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	expired := now.Add(-time.Second)
	for _, id := range []string{"exhausted", "recoverable"} {
		attempt := 1
		if id == "exhausted" {
			attempt = 2
		}

		err := s.Create(ctx, tasker.Task{ID: id, Input: []byte(`{}`), Status: tasker.StatusRunning,
			ScheduleTime: now.Add(-time.Hour), Attempt: attempt, MaxAttempts: 2, LeaseToken: "old",
			LeaseExpiresAt: &expired, CreatedAt: now, UpdatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
	}

	claim, err := s.ClaimDue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if claim.Task.ID != "recoverable" || claim.Task.Attempt != 2 || !claim.Recovered || claim.Exhausted != 1 || !claim.ReadyAt.Equal(expired) {
		t.Fatalf("incorrect recovery metadata: %+v", claim)
	}

	if err := s.Complete(ctx, tasker.Lease{TaskID: "recoverable", Token: "old"}, nil); !errors.Is(err, tasker.ErrLeaseLost) {
		t.Fatalf("old lease was not fenced: %v", err)
	}

	claim, err = s.ClaimDue(ctx, time.Minute)
	if !errors.Is(err, tasker.ErrNoTaskDue) || claim.Exhausted != 0 {
		t.Fatalf("recovery counted twice: %+v, %v", claim, err)
	}

	// Final attempts can be recovered without a new task being claimed.
	if _, err := s.db.ExecContext(ctx, `UPDATE tasker_tasks SET lease_expires_at=?1 WHERE id='recoverable'`, expired.UnixMicro()); err != nil {
		t.Fatal(err)
	}

	claim, err = s.ClaimDue(ctx, time.Minute)
	if !errors.Is(err, tasker.ErrNoTaskDue) || claim.Exhausted != 1 {
		t.Fatalf("missing final recovery: %+v, %v", claim, err)
	}

	task, err := s.Get(ctx, "recoverable")
	if err != nil || task.Status != tasker.StatusFailed {
		t.Fatalf("final outcome was not persisted: %+v, %v", task, err)
	}
}
