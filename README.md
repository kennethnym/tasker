# Tasker

Tasker is a durable task queue service that allows you to asynchronously perform work. It handles the dispatch and delivery of tasks, as well as outcome persistence. Tasks are dispatched to specified HTTP endpoints on which work is performed.

It is written in Go, and it is backed by SQLite.

## Running Tasker

> [!NOTE]
> Tasker is available as a [Docker image](https://hub.docker.com/r/kennethnym/tasker).

By default, Tasker listens on port 8080. In order to persist data, provide a volume mapping to `/data`.
Tasker will automatically create and migrate an SQLite database if one does not exist.

The image uses Docker's default user (root). To run as another user, set
`--user UID:GID` (or `user` in Compose) and ensure that user can write to the
mounted data directory.

To run it via Docker:

```bash
docker run -d \
  --name tasker \
  -p 8080:8080 \
  -v ./tasker-data:/data \
  kennethnym/tasker
```

To customize settings, create `tasker.yaml` and mount it into the container:

```bash
docker run -d \
  --name tasker \
  -p 8080:8080 \
  -v ./tasker-data:/data \
  -v ./tasker.yaml:/data/tasker.yaml:ro \
  kennethnym/tasker
```

Or use Docker Compose:

```yaml
services:
  tasker:
    image: kennethnym/tasker
    ports:
      - "8080:8080"
    volumes:
      - ./tasker-data:/data
      - ./tasker.yaml:/data/tasker.yaml:ro
    restart: unless-stopped
```

## Configuration

Tasker reads a YAML file: `tasker.yaml` in the working directory, or the path supplied with `--config` (`tasker --config /etc/tasker.yaml`). The Docker image, by default, looks for `/data/tasker.yaml`, which you can mount to customize settings. If the selected file does not exist, Tasker falls back to defaults.

Omitted fields use the defaults shown below:

```yaml
logging:
  format: json
  level: info
  access: false
metrics:
  enabled: false
http:
  listen_address: ":8080"
  shutdown_timeout: 10s
database:
  path: tasker.db
queue:
  max_dispatches_per_second: 10
  max_concurrent_dispatches: 20
  dispatch_deadline: 5m
  max_attempts: 5
  min_backoff: 1s
  max_backoff: 1m
  task_retention: 168h
```

Please refer to the [reference](#configuration-fields) for more details.

## Metrics

Set `metrics.enabled: true` in `tasker.yaml` to expose `GET /metrics` for
Prometheus and Grafana. It uses the HTTP API's listen address and authentication.
Metrics cover queue depth and delay, deliveries, retries, persisted outcomes,
lease recovery, storage errors, HTTP requests, and Go/process health.

See the [metrics reference and setup guide](docs/metrics.md), including a sample
Prometheus configuration, alert rules, and an importable Grafana dashboard.

## Authentication

Takser supports optional HTTP Basic Auth on inbound and outbound.

Add either or both sections to `tasker.yaml`:

```yaml
auth:
  username: tasker-client
  password_file: /run/secrets/tasker_api_password

outbound_auth:
  - origin: https://reports.example.com
    username: tasker
    password_env: REPORTS_PASSWORD

  - origin: http://worker:3001
    username: tasker
    password_file: /run/secrets/worker_password
```

When dispatching to a task endpoint, Tasker finds a matching basic auth credentials based on the domain, and adds it to the request header if one is found. The receiver of the task can then validate the provided credentials.

### Incoming API requests

When `auth` is configured, callers must send its username and password using HTTP Basic Auth. Missing or invalid credentials receive `401 Unauthorized` with a `WWW-Authenticate` challenge. The configured account can create and read all tasks.

### Outgoing worker requests

Tasker selects outbound credentials by the task URL's exact origin: scheme, hostname, and effective port. For example, `https://reports.example.com` matches every path on that origin, including an explicit port `443`, but not HTTP or another port.

**Unmatched origins receive no Authorization header.** Tasks need no auth field, and inbound credentials are not forwarded to workers. Redirects are not followed.

### Password sources

Each credential entry requires a username and exactly one of `password_env` or `password_file`. Supply the referenced variable or mount the secret file inside the Tasker container. Missing, unreadable, or empty secrets fail startup.

Relative secret paths resolve against the configuration file's directory. Credentials load at startup; restart Tasker after rotation. Passwords are not stored in task records.

Use HTTPS outside a trusted private network. Tasker's inbound HTTPS can terminate at a reverse proxy. See the [authentication details](docs/auth.md) for hostname normalization and validation rules.

## Using Tasker

A task can be dispatched via `POST /tasks`. Here is an example task payload:

```json
{
  "kind": "send-email",
  "url": "https://example.com/workers/send-email",
  "input": {
    "to": "alice@example.com",
    "subject": "Hello"
  },
  "scheduleTime": "2027-01-01T09:00:00Z",
  "dispatchDeadline": "30s"
}
```

A successful enqueue gives `201` and the following response:

```json
{
  "id": "cm12345670000abcd1234efghi",
  "kind": "send-email",
  "url": "https://example.com/workers/send-email",
  "input": {
    "to": "alice@example.com",
    "subject": "Hello"
  },
  "status": "queued",
  "scheduleTime": "2027-01-01T09:00:00Z",
  "dispatchDeadline": "30s",
  "attempt": 0,
  "maxAttempts": 5,
  "result": null,
  "createdAt": "2026-10-25T12:00:00Z",
  "updatedAt": "2026-10-25T12:00:00Z"
}
```

## Worker delivery

Tasker sends an HTTP `POST` to the task's `url`, with `input` as the request body and these headers:

| Header | Value |
| --- | --- |
| `Content-Type` | `application/json` |
| `X-Task-Id` | Task ID. |
| `X-Task-Attempt` | Current attempt number, starting at `1`. |

Tasker guarantees at-least-once delivery, but deliveries of the same task can happen more than once. Task executors should take this into consideration.

### Result handling

- Any `2xx` response marks the task as succeeded.
- Non-`2xx` responses, network errors, and timeouts are all considered failures. Tasker will attempt to retry the task with backoff, until `maxAttempts` is reached.
- A valid JSON response body of at most 1 MiB is stored in `result`.
- Empty, invalid JSON, or oversized response bodies will be discarded and leave `result` as `null` without failing the task.
- Redirects are not followed.

## Reference

Successful responses use `Content-Type: application/json`. Error responses contain plain text, not JSON.

### Configuration fields

| Field | Type | Default | Description and constraints |
| --- | --- | --- | --- |
| `logging.format` | string | `json` | Log format: `json` or `text`. Logs are written to stderr. |
| `logging.level` | string | `info` | Minimum severity for runtime and access logs: `debug`, `info`, `warn`, or `error` (case-insensitive). Go slog level offsets such as `info+2` are also accepted. |
| `logging.access` | boolean | `false` | Enable HTTP access logs, including authentication failures. Records include method, route pattern, status, response size, duration, and a generated request ID returned in `X-Request-Id`. Subject to `logging.level`; server errors use error level, other requests use info level. |
| `metrics.enabled` | boolean | `false` | Expose `GET /metrics` on the HTTP listener, protected by the same inbound auth as the task API. See [metrics](docs/metrics.md). |
| `http.listen_address` | string | `:8080` | HTTP server listen address in `host:port` form. An empty host listens on all interfaces. Must not be empty or whitespace-only. |
| `http.shutdown_timeout` | duration string | `10s` | Grace period for in-flight HTTP requests during shutdown. Must be positive. |
| `database.path` | string | `tasker.db` | SQLite database filename. Relative paths resolve against the working directory (`/data` in Docker), not the config file's directory. The parent directory must exist; the database is created and migrated automatically. Must not be empty or whitespace-only. |
| `queue.max_dispatches_per_second` | number | `10` | Maximum rate of task dispatches. Fractional values are accepted. Must be positive and finite. |
| `queue.max_concurrent_dispatches` | integer | `20` | Maximum number of simultaneous task deliveries. Must be positive. |
| `queue.dispatch_deadline` | duration string | `5m` | Default timeout for each delivery attempt. A task can override this with the `dispatchDeadline` request field. Must be positive. |
| `queue.max_attempts` | integer | `5` | Maximum delivery attempts per task, including the initial attempt. Must be positive. |
| `queue.min_backoff` | duration string | `1s` | Minimum retry backoff after a failed delivery. Must be positive. |
| `queue.max_backoff` | duration string | `1m` | Upper bound for retry backoff. Must be positive and at least `queue.min_backoff`. |
| `queue.task_retention` | duration string | `168h` | How long succeeded and failed tasks are retained after finishing before becoming eligible for cleanup. Must be positive. |

Notes:
- All fields are optional.
- Omitted or `null` values retain their defaults.
- Durations are strings using Go duration syntax, such as `30s`, `5m`, or `1h30m`; use `168h` for seven days.

### Task

`Task` is returned after a successful task creation and after a `GET` lookup.

| Field | Type | Description |
| --- | --- | --- |
| `id` | string | Unique task ID. |
| `kind` | string | Task label. Omitted when empty. |
| `url` | string | Worker endpoint. |
| `input` | Any JSON value | Payload sent to the worker. |
| `status` | string | `queued`, `running`, `succeeded`, or `failed`. |
| `scheduleTime` | RFC 3339 timestamp | Scheduled delivery time. Updated when a retry is scheduled. |
| `dispatchDeadline` | string | Timeout per delivery attempt, e.g. `30s`. |
| `attempt` | integer | Delivery attempt count. Starts at `0`. |
| `maxAttempts` | integer | Maximum delivery attempts, including the initial attempt. |
| `result` | Any JSON value | Worker’s JSON response, or `null` when no result is stored. |
| `lastError` | string | Last delivery error. Omitted when empty. |
| `createdAt` | RFC 3339 timestamp | When the task was created. |
| `updatedAt` | RFC 3339 timestamp | When the task was last updated. |
| `startedAt` | RFC 3339 timestamp | When execution started. Omitted until set. |
| `finishedAt` | RFC 3339 timestamp | When the task succeeded or permanently failed. Omitted until set. |

### `POST /tasks`

Enqueues a task for delivery. A successful response means the task has been stored, not that the worker has completed it.

Request:

Send a JSON object with `Content-Type: application/json`. The request body is limited to 1 MiB.

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `kind` | string | No | Empty | Optional label for the task. Does not affect delivery. |
| `url` | string | Yes | — | HTTP or HTTPS endpoint that receives the task. |
| `input` | Any JSON value | Yes | — | Sent as the worker’s POST body. Can be an object, array, scalar, or `null`. |
| `scheduleTime` | RFC 3339 timestamp | No | Current time | Earliest time the task can run. Omitting this makes the task immediately eligible for delivery. |
| `dispatchDeadline` | Duration string | No | Server setting (`5m` by default) | Timeout for each delivery attempt. Must be positive, e.g. `30s` or `2m`. `null` is rejected. |

**Returns**:

| Code | Body | Description |
| ---- | ---- | ----------- |
| `201 Created` | [Task](#task) | Task stored. The `Location` header contains `/tasks/{id}`. |
| `401 Unauthorized` | `unauthorized` | Inbound auth is enabled and credentials are missing or invalid. |
| `400 Bad Request` | Error message | Invalid JSON, unknown fields, missing required fields, or invalid field values. |
| `413 Request Entity Too Large` | `request body exceeds 1 MiB` | Request body exceeds the size limit. |
| `500 Internal Server Error` | `failed to create task` | Task could not be stored. |

### `GET /tasks/{id}`

Retrieves the current [Task](#task), including its status, delivery attempt count, and result. Uses the same response shape as task creation.

| Path parameter | Type | Description |
| --- | --- | --- |
| `id` | string | Task ID returned by `POST /tasks`. |

**Returns**:

| Code | Body | Description |
| ---- | ---- | ----------- |
| `200 OK` | [Task](#task) | Current task state. |
| `401 Unauthorized` | `unauthorized` | Inbound auth is enabled and credentials are missing or invalid. |
| `404 Not Found` | `task not found` | No task exists with this ID, or it has been removed by retention cleanup. |
| `500 Internal Server Error` | `failed to find task` | Task could not be read. |

Poll this endpoint until `status` is `succeeded` or `failed`. A failed delivery can return to `queued` while waiting for another attempt.
