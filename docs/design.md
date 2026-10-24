# Tasker HTTP task service design

Tasker is a standalone Go service for durable, scheduled HTTP task delivery. Producers submit a target URL and arbitrary JSON input. Tasker claims due tasks, POSTs their input to executors, and records success or schedules retries.

The initial deployment is one process with pluggable durable storage. Postgres is the first planned adapter. This document describes the intended service; the current code is a skeleton.

## Scope and guarantees

Delivery is at least once: duplicate requests are possible. Executors own idempotency and protection against duplicate side effects. Tasker provides a stable task ID across attempts; it does not guarantee exactly-once execution or consistent business results across attempts.

Retries are bounded. Acceptance does not guarantee eventual successful execution. A task can exhaust its attempts and become failed. A crash after claiming but before sending can consume an attempt, because attempts are counted on claim.

`scheduleTime` is the earliest eligible dispatch time, not an exact execution guarantee. Capacity, rate limits, polling, and outages can delay delivery. There is no strict ordering guarantee.

Tasker has no application-specific debounce, conversation scheduling, or job-handler logic. Authentication, live progress, cancellation, enqueue deduplication, and distributed rate limiting are deferred.

## Package and lifecycle

Implementation lives under `internal/`; there are no root Go source files.

- `cmd/tasker`: executable entrypoint and error reporting.
- `internal/app`: configuration loading, storage construction, HTTP serving, and process lifecycle.
- `internal/tasker`: service API, task types, configuration, errors, HTTP handlers, and storage contract.

The constructor shape is:

```go
service, err := tasker.New(tasker.Config{
    Storage: adapter,
    MaxConcurrentDispatches: 20,
})
```

`New` constructs the service without starting background work. `Run(ctx)` will manage dispatchers and cleanup. The caller serves `Handler()` separately and owns the storage connection lifecycle.

`CreateTask(ctx, CreateTaskConfig)` and `FindTask(ctx, id)` provide the service operations. HTTP handlers delegate to them. Context cancellation will stop background work; the precise shutdown grace period and handling of active requests remain implementation decisions.

## Public HTTP API

The initial API has two endpoints:

- `POST /tasks`: durably enqueue a task.
- `GET /tasks/{id}`: retrieve its current state and optional result.

### Enqueue

```http
POST /tasks
Content-Type: application/json
```

```json
{
  "kind": "generate-report",
  "url": "https://executor.example.com/reports",
  "input": {
    "accountId": "acct_123",
    "month": "2026-09"
  },
  "scheduleTime": "2026-10-24T15:00:00Z",
  "dispatchDeadline": "120s"
}
```

`input` can be any JSON value, not only an object. `kind` is optional metadata and does not select an in-process handler. Omitting `scheduleTime` makes the task immediately eligible. Omitting `dispatchDeadline` uses the service default. Retry policy is service-level; the initial create payload has no per-task `maxAttempts` override.

The service persists the task before returning `201 Created`, then notifies local dispatchers. A failed persistence operation must not be acknowledged as an accepted task.

The following is an illustrative subset of the response:

```json
{
  "id": "task_123",
  "status": "queued",
  "scheduleTime": "2026-10-24T15:00:00Z",
  "attempt": 0
}
```

Task IDs above are illustrative; the generation format is not yet specified. Retrying an enqueue after an uncertain response may create another task because enqueue deduplication is deferred.

### Retrieve

`GET /tasks/{id}` returns the task, including its status, attempt count, timestamps, last error, and result. Unknown or cleaned-up IDs return `404`. Lease fields are never exposed in JSON.

A completed task includes fields such as:

```json
{
  "id": "task_123",
  "status": "succeeded",
  "attempt": 1,
  "result": {
    "reportUrl": "https://files.example.com/report_456.pdf",
    "rowCount": 842
  },
  "finishedAt": "2026-10-24T15:00:08Z"
}
```

The exact HTTP error envelope and request-validation responses remain to be specified.

## Task data

Persisted tasks contain:

- Identity and payload: `id`, `kind`, `url`, `input`.
- Scheduling and execution: `status`, `scheduleTime`, `dispatchDeadline`, `attempt`, `maxAttempts`.
- Ownership: `leaseToken`, `leaseExpiresAt`.
- Outcome: optional JSON `result` and `lastError`.
- Timestamps: `createdAt`, `updatedAt`, `startedAt`, `finishedAt`.

The statuses are `queued`, `running`, `succeeded`, and `failed`. New tasks have attempt zero. Each claim increments the attempt count. `maxAttempts` and the effective deadline are stored on the task so its execution bounds can be inspected.

Success and final failure are terminal. They set `finishedAt`, update `updatedAt`, and clear the active lease. A retry returns the task to `queued`, updates `scheduleTime`, records the error, and clears the lease.

The proposed meaning of `startedAt` is the first claim time, preserved across retries. This should be settled before implementing storage updates.

## Dispatch and wakeups

Each dispatcher is a worker goroutine. It claims one task, sends one HTTP request, waits for its result while renewing the lease, and then looks for another task. It does not claim a batch and leave tasks waiting behind its own request.

Storage is the source of truth. A wakeup signals that storage may contain due work; it carries no task payload or task ID.

A buffered channel of capacity one coalesces notifications. Enqueue signals it non-blockingly after persistence commits. A shared channel wakes one receiving worker per signal, not all workers. A worker that claims a task can pass on a wakeup before executing, allowing other idle workers to check for more work.

Workers check storage again after completing a request. When no task is available, they wait for a notification or polling timer. Polling discovers delayed tasks, expired leases, and work whose notification was missed. Immediate local notifications reduce enqueue latency without being required for correctness.

Polling and lease timing are internal defaults. Initial examples were a one-second poll, a 30-second lease, and renewal every 10 seconds; these are implementation starting points, not public configuration commitments.

## Rate and concurrency limits

`maxDispatchesPerSecond` limits the rate at which HTTP attempts start, including retries. Use a shared token bucket, with `golang.org/x/time/rate` as the planned implementation. Each attempt consumes a token; tokens replenish with time, not when requests finish. Bucket capacity determines burst allowance and remains to be selected.

`maxConcurrentDispatches` limits in-flight HTTP requests. A bounded worker pool can enforce this directly; a semaphore is another implementation option. A slot remains occupied until the local request completes or times out.

Both limits apply to the single service process. Do not claim a large backlog while waiting for rate or concurrency permission. The precise coordination of rate permission and claiming must preserve the request-start limit without leaving claimed tasks idle for long periods.

These limits do not bound executor work that continues after a timeout. Multiple service replicas would multiply in-memory limits unless shared coordination is added. Redis is not required for the initial deployment.

## Atomic claims and leases

A task is eligible if it is queued and due, or running with an expired lease, and has attempts remaining. Expired tasks that have exhausted their attempts must be finalized as failed rather than left running indefinitely.

The adapter's `ClaimDue(ctx, leaseDuration)` claims one task atomically. The planned Postgres implementation selects an eligible row ordered by `scheduleTime, id` using `FOR UPDATE SKIP LOCKED`, updates it in the same transaction, and returns the committed claim.

Each claim assigns a fresh UUID `leaseToken`, sets `leaseExpiresAt`, marks the task running, and increments `attempt`. Dispatch begins only after the claim commits.

Renewal and completion, retry, or failure updates must atomically verify running status, matching token, and valid ownership. A reclaimed task receives a different token, preventing an old worker from overwriting its state. Failed ownership checks return `ErrLeaseLost`.

Lease renewal runs while the HTTP request is active. Losing ownership prevents further task-state writes by that worker. Best-effort local request cancellation does not guarantee that the remote executor stops.

Lease fencing protects Tasker's storage updates. It cannot prevent side effects from concurrent or stale executor requests. Executors remain responsible for that.

## HTTP execution and results

The dispatcher POSTs the original input as JSON, with delivery metadata in headers:

```http
POST /reports
Content-Type: application/json
X-Task-Id: task_123
X-Task-Attempt: 1
```

```json
{
  "accountId": "acct_123",
  "month": "2026-09"
}
```

The task ID stays the same on retries. The attempt number changes. The lease token stays internal.

The executor returns a `2xx` only after finishing the work. A successful JSON response becomes `result`. An empty successful response, including `204`, produces a null result. Completion and result persistence happen in one conditional storage update matching the lease.

Non-2xx responses, transport failures, and deadline expiry cause retry or final failure. The deadline limits how long Tasker waits; it does not guarantee remote execution stops.

A successful executor may lose its response before Tasker records completion. Delivery can then repeat. Tasker stores the successful response it accepts; it does not enforce identical results across attempts.

Result size limits and handling of malformed or oversized successful bodies remain open. The proposed policy for malformed JSON is to preserve delivery success, store no result, and record a diagnostic rather than repeat completed work solely to parse its output.

## Retries and recovery

Use exponential backoff with jitter, bounded by `minBackoff` and `maxBackoff`. The exact jitter formula remains to be chosen. Retry scheduling updates `scheduleTime` to the next eligible attempt time.

`maxAttempts` includes the initial attempt. Once exhausted, mark the task failed and retain its last delivery error until cleanup. The initial policy retries all non-2xx responses; status-specific policies are deferred.

A process restart does not lose durable tasks. Queued tasks are rediscovered through polling; abandoned running tasks become recoverable when their leases expire. The adapter must handle exhausted expired leases even when no further dispatch is allowed.

`maxRetryDuration` is deferred. If introduced, retries stop when either the attempt limit or duration limit is reached.

## Storage contract

Adapters must provide atomic operations, not only CRUD:

- `Create` and `Get` for persistence and lookup.
- `ClaimDue` for selecting and owning one eligible task.
- `RenewLease` for extending current ownership.
- `Complete`, `Retry`, and `Fail` for conditional state transitions.
- `DeleteFinishedBefore` for bounded terminal-task cleanup.

The caller supplies the adapter through `Config.Storage`. Postgres connection setup belongs in the application layer. No adapter is implemented yet.

Postgres will need indexes supporting due queued tasks, expired running leases, and terminal tasks ordered by `finishedAt`. Exact schema, migrations, timestamp authority, and transaction queries remain implementation work.

Errors are declared in `error.go`. `TaskNotFoundError` includes the requested ID and unwraps to `ErrTaskNotFound`, allowing both `errors.Is` and `errors.AsType`. Other sentinels cover no due task, lost lease, and unimplemented operations.

## Configuration and deployment

Configuration is flat and service-wide. Current `DefaultConfig` values are:

```go
MaxDispatchesPerSecond:  10
MaxConcurrentDispatches: 20
DispatchDeadline:        5 * time.Minute
MaxAttempts:             5
MinBackoff:              time.Second
MaxBackoff:              time.Minute
TaskRetention:          7 * 24 * time.Hour
```

`New(config)` applies `DefaultConfig.Merge(config)`. Merge returns a copy, using non-zero fields and a non-nil storage adapter from the supplied config. Neither input is mutated. Explicit zero overrides cannot be represented by this API; zero currently means use the inherited value.

The planned binary accepts `tasker --config /path/config.yaml`, with environment overrides prefixed by `TASKER_`. Precedence is environment, then file, then defaults. Secrets such as the Postgres connection string are injected through the environment, using `DATABASE_URL` for the initial adapter.

Configuration loading and duration parsing belong outside the service. The Go config struct has no YAML tags. Loading must explicitly map the flat external names to its fields.

Startup validation and `tasker config validate --config ...` are planned. Invalid configuration should fail clearly. Configuration changes take effect through restart or redeployment; no hot reload or mutable configuration API is planned. IaC can mount a generated file and inject secrets without a custom provider.

There are no named queues in the initial service. Polling, lease renewal, and cleanup timing stay internal to keep the public config minimal.

## Retention and cleanup

`taskRetention` is measured from success or final failure, using `finishedAt`. It applies to completed task history, not the age of queued or running tasks.

A periodic cleanup goroutine deletes terminal tasks where `finishedAt` precedes `now - taskRetention`. It uses bounded batches through `DeleteFinishedBefore(ctx, cutoff, limit)` and an index supporting that query. A one-minute cleanup interval is a proposed internal default.

The task, input, result, and last error are deleted together. Cleanup failures are retried on a later sweep. Retention makes a task eligible for deletion; it is not an exact deletion deadline. Retrieval returns `404` after deletion.

Cleanup is owned by the service, with no database TTL extension required.

## Deferred features and remaining decisions

Deferred features are authentication in both directions, live stage/progress reporting, executor cancellation, task rescheduling endpoints, enqueue deduplication, named queues, distributed rate limits, and per-task retention overrides.

Before implementing dispatch, settle request and response size limits, malformed success-body diagnostics, redirect handling, URL validation, exact backoff jitter and burst capacity, and rate-gate coordination with claims. Before implementing lifecycle and storage, settle shutdown behaviour, clock authority, and first-start timestamp semantics.

Application-specific debounce and atomic coordination between producer writes and enqueueing remain outside Tasker. Producers needing transactional enqueue intent can implement an outbox and a relay; enqueue deduplication would need a separate design.

## Current implementation status

The skeleton includes the constructor, flat config and merge function, task types, storage interface, centralized errors, and two HTTP routes. HTTP handlers return `501`; `Run`, `CreateTask`, and `FindTask` return `ErrNotImplemented`. The executable constructs the service and exits with that error.

Dispatch, storage adapters, config loading, HTTP serving, validation, retries, and cleanup are not implemented. No behaviour described above should be inferred to be operational merely from the presence of its types.
