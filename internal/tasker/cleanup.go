package tasker

import (
	"context"
	"log/slog"
	"time"
)

const (
	cleanupInterval  = time.Minute
	cleanupBatchSize = 1000
)

type cleanupReconciler struct {
	storage   Storage
	retention time.Duration
}

func (c *cleanupReconciler) run(ctx context.Context) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		if err := c.reconcile(ctx, time.Now().UTC()); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "failed to clean up finished tasks", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *cleanupReconciler) reconcile(ctx context.Context, now time.Time) error {
	cutoff := now.Add(-c.retention)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		deleted, err := c.storage.DeleteFinishedBefore(ctx, cutoff, cleanupBatchSize)
		if err != nil {
			return err
		}
		if deleted < cleanupBatchSize {
			return nil
		}
	}
}
