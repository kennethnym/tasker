# End-to-end tests

The suite builds `cmd/tasker` once and starts the executable as a child process
for each test. It uses real HTTP connections, a temporary SQLite database, and
a local executor HTTP server. Tasks are created and inspected exclusively
through the public HTTP API; the tests do not call service methods or modify
database rows.

Run the fast suite:

```sh
nix develop -c go test ./tests/e2e -short -count=1
```

Run all tests, including lease recovery, renewal, and periodic retention:

```sh
nix develop -c go test ./... -count=1 -timeout=5m
```

The slow tests use the production 30-second lease and one-minute cleanup
interval. Tests run in parallel with independent databases and ephemeral
localhost ports, except when a scenario deliberately shares a database across
processes. The full suite needs at least one minute. No containers, external
services, or production configuration changes are required.

## CI

The [E2E workflow](../../.github/workflows/e2e.yml) runs the full suite on pull
requests, pushes to `main`, and manual dispatches. It uses Ubuntu and
`actions/setup-go` with the version from `go.mod`, caches Go dependencies and
build outputs, and prints verbose test results. Each test run has a five-minute timeout; the job
has a fifteen-minute timeout including setup. New runs cancel superseded runs
for the same pull request or branch.

## Coverage

- Creation, request method/path/body/headers, observable running state, result
  persistence, lifecycle timestamps, and hidden lease fields.
- JSON objects, arrays, strings, numbers, booleans, and null; empty, malformed,
  and oversized successful results, including the 1 MiB result boundary.
- Future and past scheduling; polling discovers due work without another enqueue.
- Retries, backoff bounds, stable task identity and first-start timestamp,
  attempt limits, and final failure.
- HTTP errors, transport disconnects, truncated responses, per-task deadlines,
  and redirects that must not reach their destination.
- Invalid creation requests, the request size limit, unknown task IDs, and
  absence of executor deliveries for rejected requests.
- Concurrent delivery limits, tasks remaining unclaimed while slots are full,
  and cumulative dispatch rate including retries.
- Queued and completed tasks surviving a forced exit; SIGTERM and forced-exit
  recovery; abandoned final attempts becoming failed after lease expiry.
- Lease renewal beyond 30 seconds with another process actively polling the
  same database.
- Startup and periodic retention removing succeeded/failed tasks while
  preserving queued/running tasks.
- The periodic retention test follows enqueue, gated execution, persisted success
  and failure outcomes, and eventual deletion in one uninterrupted process. It
  checks that outcomes are not deleted before their retention period expires.

## Harness behavior

The process binds `127.0.0.1:0`; the harness reads its actual address from the
startup log and waits for an HTTP response. Settings are written to a temporary
YAML config file whose path is passed to the executable with `--config`.

Executor handlers record requests in a channel and can block, fail, or return
specific responses. Assertions compare those requests with the submitted input
and the task ID returned by the API. Gates make transient states observable.
Polling has bounded deadlines; negative assertions use bounded observation
windows. Failure diagnostics include process logs, recorded deliveries, and
the last API response. Cleanup stops child processes and executor servers.

Rate assertions allow timing jitter and test cumulative throughput under local
storage conditions. They do not establish strict spacing of HTTP request
starts: the implementation currently rate-limits claims.

Recovery intentionally exercises repeated delivery of the same task. It does
not assert exactly-once executor side effects; executor idempotency remains
the application's responsibility.
