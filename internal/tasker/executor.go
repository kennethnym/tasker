package tasker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

const maxResultBytes = 1 << 20

type executor struct {
	storage       Storage
	client        *http.Client
	leaseDuration time.Duration
	minBackoff    time.Duration
	maxBackoff    time.Duration
}

func newExecutor(config Config, leaseDuration time.Duration) *executor {
	return &executor{
		storage: config.Storage,
		client: &http.Client{
			// Redirects are delivery failures; never change the method or target.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		leaseDuration: leaseDuration,
		minBackoff:    config.MinBackoff,
		maxBackoff:    config.MaxBackoff,
	}
}

func (e *executor) execute(ctx context.Context, task Task) {
	lease := Lease{TaskID: task.ID, Token: task.LeaseToken}

	// Confirm ownership before starting work, even if returning the claim was slow.
	claimCtx, stop := context.WithTimeout(ctx, e.leaseDuration/3)
	err := e.storage.RenewLease(claimCtx, lease, e.leaseDuration)
	stop()

	if err != nil {
		slog.WarnContext(ctx, "cannot renew claimed task; abandoning attempt", "task_id", task.ID, "error", err)
		return
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stopRenewal := make(chan struct{})
	renewed := make(chan error, 1)
	go e.sendHeartbeat(workCtx, lease, cancel, stopRenewal, renewed)

	result, err := e.deliver(workCtx, task)
	close(stopRenewal)

	if renewalErr := <-renewed; renewalErr != nil {
		if ctx.Err() == nil {
			slog.WarnContext(ctx, "task lease renewal failed; abandoning attempt", "task_id", task.ID, "error", renewalErr)
		}

		return
	}

	if ctx.Err() != nil {
		// Shutdown leaves the claim recoverable once its lease expires.
		return
	}

	// Bound the final write. The adapter must fence it against lease expiry.
	writeCtx, stop := context.WithTimeout(ctx, e.leaseDuration/3)
	defer stop()

	var writeErr error
	switch {
	case err == nil:
		writeErr = e.storage.Complete(writeCtx, lease, result)

	case task.Attempt >= task.MaxAttempts:
		writeErr = e.storage.Fail(writeCtx, lease, err.Error())

	default:
		writeErr = e.storage.Retry(writeCtx, lease, time.Now().UTC().Add(e.backoff(task.Attempt)), err.Error())
	}

	if writeErr != nil && !errors.Is(writeErr, ErrLeaseLost) {
		slog.ErrorContext(ctx, "failed to persist task outcome", "task_id", task.ID, "error", writeErr)
	}
}

// sendHeartbeat renews ownership until execution stops or renewal fails.
func (e *executor) sendHeartbeat(ctx context.Context, lease Lease, cancel context.CancelFunc, stopRenewal <-chan struct{}, renewed chan<- error) {
	ticker := time.NewTicker(e.leaseDuration / 3)
	defer ticker.Stop()

	for {
		select {
		case <-stopRenewal:
			renewed <- nil
			return

		case <-ctx.Done():
			renewed <- ctx.Err()
			return

		case <-ticker.C:
			renewCtx, stop := context.WithTimeout(ctx, e.leaseDuration/3)
			err := e.storage.RenewLease(renewCtx, lease, e.leaseDuration)
			stop()

			if err != nil {
				cancel()
				renewed <- err
				return
			}
		}
	}
}

func (e *executor) deliver(ctx context.Context, task Task) (json.RawMessage, error) {
	deadline, err := time.ParseDuration(task.DispatchDeadline)
	if err != nil || deadline <= 0 {
		return nil, ErrInvalidDispatchDeadline
	}

	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, task.URL, bytes.NewReader(task.Input))
	if err != nil {
		return nil, fmt.Errorf("create executor request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Task-Id", task.ID)
	req.Header.Set("X-Task-Attempt", strconv.Itoa(task.Attempt))

	response, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executor request: %w", err)
	}

	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("executor returned HTTP %d", response.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxResultBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read executor response: %w", err)
	}

	if len(body) > maxResultBytes {
		slog.WarnContext(ctx, "successful executor result exceeds size limit; discarding result", "task_id", task.ID)
		return nil, nil
	}

	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, nil
	}

	if !json.Valid(body) {
		slog.WarnContext(ctx, "successful executor result is invalid JSON; discarding result", "task_id", task.ID)
		return nil, nil
	}

	return json.RawMessage(body), nil
}

// Jitter between the minimum and an exponentially growing, capped upper bound.
func (e *executor) backoff(attempt int) time.Duration {
	upper := e.minBackoff

	for i := 1; i < attempt && upper < e.maxBackoff; i++ {
		if upper > e.maxBackoff/2 {
			upper = e.maxBackoff
		} else {
			upper *= 2
		}
	}

	if upper <= e.minBackoff {
		return e.minBackoff
	}

	return e.minBackoff + time.Duration(rand.Int64N(int64(upper-e.minBackoff)))
}
