package tasker

import (
	"context"
	"encoding/json"
	"time"
)

type Storage interface {
	Create(context.Context, Task) error
	Get(ctx context.Context, id string) (Task, error)

	// ClaimDue also reports committed recovery work when it returns ErrNoTaskDue.
	ClaimDue(ctx context.Context, leaseDuration time.Duration) (ClaimResult, error)
	QueueStats(ctx context.Context, now time.Time) (QueueStats, error)

	RenewLease(ctx context.Context, lease Lease, duration time.Duration) error
	Complete(ctx context.Context, lease Lease, result json.RawMessage) error
	Retry(ctx context.Context, lease Lease, scheduleTime time.Time, lastError string) error
	Fail(ctx context.Context, lease Lease, lastError string) error

	// DeleteFinishedBefore deletes at most limit succeeded or failed tasks whose
	// finishedAt is strictly before cutoff, including their input, result, and
	// last error. It returns the number deleted. Queued and running tasks must
	// never be deleted. Implementations must honor context cancellation.
	DeleteFinishedBefore(ctx context.Context, cutoff time.Time, limit int) (int, error)
}

type ClaimResult struct {
	Task      Task
	ReadyAt   time.Time // Schedule time, or the previous lease expiry for recovery.
	Recovered bool
	Exhausted int // Tasks failed because their final lease expired.
}

// QueueStats is a single database snapshot. Expired leases are separate from
// running tasks, including final attempts still awaiting recovery.
type QueueStats struct {
	Due, Scheduled, Running, Expired int64
	OldestDueAt                      *time.Time
}

type Lease struct {
	TaskID string
	Token  string
}
