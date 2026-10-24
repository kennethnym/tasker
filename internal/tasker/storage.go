package tasker

import (
	"context"
	"encoding/json"
	"time"
)

type Storage interface {
	Create(context.Context, Task) error
	Get(ctx context.Context, id string) (Task, error)

	ClaimDue(ctx context.Context, leaseDuration time.Duration) (Task, error)

	RenewLease(ctx context.Context, lease Lease, duration time.Duration) error
	Complete(ctx context.Context, lease Lease, result json.RawMessage) error
	Retry(ctx context.Context, lease Lease, scheduleTime time.Time, lastError string) error
	Fail(ctx context.Context, lease Lease, lastError string) error

	DeleteFinishedBefore(ctx context.Context, cutoff time.Time, limit int) (int, error)
}

type Lease struct {
	TaskID string
	Token  string
}
