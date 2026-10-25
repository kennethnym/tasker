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
	logger        *slog.Logger
	storage       Storage
	limiter       *rate.Limiter
	slots         chan struct{}
	wake          chan struct{}
	leaseDuration time.Duration
	pollInterval  time.Duration

	// execute handles lease renewal, outcomes, and cancellation.
	execute func(context.Context, ClaimResult)
}

func newDispatcher(config Config, instrumentation *metrics) *dispatcher {
	d := &dispatcher{
		logger:        slog.With("component", "dispatcher"),
		storage:       config.Storage,
		limiter:       rate.NewLimiter(rate.Limit(config.MaxDispatchesPerSecond), 1),
		slots:         make(chan struct{}, config.MaxConcurrentDispatches),
		wake:          make(chan struct{}, 1),
		leaseDuration: 30 * time.Second,
		pollInterval:  time.Second,
	}

	executor := newExecutor(config, d.leaseDuration, instrumentation)
	d.execute = executor.execute

	return d
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

		// Obtain rate permission before creating a lease or consuming an attempt.
		if err := d.limiter.Wait(ctx); err != nil {
			<-d.slots
			return err
		}

		claim, err := d.storage.ClaimDue(ctx, d.leaseDuration)
		if err != nil {
			<-d.slots

			if ctx.Err() != nil {
				return ctx.Err()
			}

			wake := d.wake
			if !errors.Is(err, ErrNoTaskDue) {
				d.logger.ErrorContext(ctx, "failed to claim task", "error", err)
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

		d.logger.DebugContext(ctx, "task claimed", "task_id", claim.Task.ID, "attempt", claim.Task.Attempt)
		active.Add(1)
		go func() {
			defer active.Done()
			defer func() { <-d.slots }()

			d.execute(ctx, claim)
		}()
	}
}
