package tasker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type dispatcher struct {
	storage       Storage
	limiter       *rate.Limiter
	slots         chan struct{}
	wake          chan struct{}
	leaseDuration time.Duration
	pollInterval  time.Duration
	// execute must handle lease renewal, task outcomes, and cancellation.
	// Leave nil until task execution is implemented to avoid claiming work.
	execute func(context.Context, Task)
}

func newDispatcher(config Config) *dispatcher {
	return &dispatcher{
		storage:       config.Storage,
		limiter:       rate.NewLimiter(rate.Limit(config.MaxDispatchesPerSecond), 1),
		slots:         make(chan struct{}, config.MaxConcurrentDispatches),
		wake:          make(chan struct{}, 1),
		leaseDuration: config.DispatchDeadline,
		pollInterval:  time.Second,
	}
}

// notify coalesces wake-ups; storage remains the source of truth.
func (d *dispatcher) notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *dispatcher) run(ctx context.Context) error {
	if d.execute == nil {
		return ErrNotImplemented
	}

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()
	var active sync.WaitGroup
	defer active.Wait() // Wait for in-progress executions before returning.

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case d.slots <- struct{}{}:
			// Slot acquired. Continue after the select; cases do not fall through.
		case <-ctx.Done():
			return ctx.Err()
		}
		task, err := d.storage.ClaimDue(ctx, d.leaseDuration)
		if err != nil {
			<-d.slots
			if ctx.Err() != nil {
				return ctx.Err()
			}
			wake := d.wake
			if !errors.Is(err, ErrNoTaskDue) {
				slog.ErrorContext(ctx, "failed to claim task", "error", err)
				// Notifications must not cause a tight retry loop on storage errors.
				wake = nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wake:
			case <-ticker.C:
			}
			continue
		}

		// Limit execution starts, so slow claims cannot replenish a token
		// already spent on a task that has not started yet.
		// The claimed task's lease also covers this wait.
		if err := d.limiter.Wait(ctx); err != nil {
			<-d.slots
			return err
		}

		active.Add(1)
		go func(task Task) {
			defer active.Done()
			defer func() { <-d.slots }()
			d.execute(ctx, task)
		}(task)
	}
}
