# Tasker

Tasker is a durable task queue service that allows you to asynchronously perform work. It handles the dispatch and delivery of tasks, as well as outcome persistence. Tasks are dispatched to specified HTTP endpoints on which work is performed.

It is written in Go, and it is backed by SQLite.

## Running Tasker

> [!NOTE]
> Tasker is available as a [Docker image](https://hub.docker.com/r/kennethnym/tasker).

By default, Tasker listens on port 8080. In order to persist data, provide a volumn mapping to `/data`.
Tasker will automatically create and migrate an SQLite database if one does not exist.

To run it via Docker:

```bash
docker run -d \
  --name tasker \
  -p 8080:8080 \
  -v ./tasker-data:/data \
  kennethnym/tasker
```

Optional configuration can be done via program arguments:

```bash
docker run -d \
  --name tasker \
  -p 8080:8080 \
  -v ./tasker-data:/data \
  kennethnym/tasker --max-attempts 3 --max-concurrent-dispatches 10
```

Below is an example Docker Compose definition for Tasker:

```yaml
services:
  tasker:
    image: kennethnym/tasker
    command:
      - --max-attempts=3
      - --max-concurrent-dispatches=10
    ports:
      - "8080:8080"
    volumes:
      - ./tasker-data:/data
    restart: unless-stopped
```

## Configuration

Tasker accepts command-line arguments and environment variables. Environment variables override flags.

| Flag | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `--listen-address` | `TASKER_LISTEN_ADDRESS` | `:8080` | HTTP listen address. |
| `--database-path` | `TASKER_DATABASE_PATH` | `tasker.db` | SQLite database path, relative to the working directory. In Docker, this defaults to `/data/tasker.db`. |
| `--shutdown-timeout` | `TASKER_SHUTDOWN_TIMEOUT` | `10s` | Grace period for HTTP requests during shutdown. |
| `--max-dispatches-per-second` | `TASKER_MAX_DISPATCHES_PER_SECOND` | `10` | At most how many task dispatches can be made per second. Fractional values are accepted. |
| `--max-concurrent-dispatches` | `TASKER_MAX_CONCURRENT_DISPATCHES` | `20` | Maximum number of concurrent task deliveries. |
| `--dispatch-deadline` | `TASKER_DISPATCH_DEADLINE` | `5m` | Timeout for each delivery attempt. Tasks can override this with `dispatchDeadline` request field. |
| `--max-attempts` | `TASKER_MAX_ATTEMPTS` | `5` | Maximum delivery attempts per task, including the initial attempt. |
| `--min-backoff` | `TASKER_MIN_BACKOFF` | `1s` | Minimum delay before retrying a failed delivery. |
| `--max-backoff` | `TASKER_MAX_BACKOFF` | `1m` | Upper limit for retry backoff. |
| `--task-retention` | `TASKER_TASK_RETENTION` | `168h` | How long completed or failed tasks are retained before cleanup. |

Note:
- Durations use Go duration syntax, such as `30s`, `5m`, or `1h30m`. Use `168h` for seven days; `7d` is not supported.
- Numeric values and durations must be positive.
- String values must not be empty.

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

## Reference

Successful responses use `Content-Type: application/json`. Error responses contain plain text, not JSON.

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
| `404 Not Found` | `task not found` | No task exists with this ID, or it has been removed by retention cleanup. |
| `500 Internal Server Error` | `failed to find task` | Task could not be read. |

Poll this endpoint until `status` is `succeeded` or `failed`. A failed delivery can return to `queued` while waiting for another attempt.

## Worker delivery

Tasker sends an HTTP `POST` to the task's `url`, with `input` as the request body and these headers:

| Header | Value |
| --- | --- |
| `Content-Type` | `application/json` |
| `X-Task-Id` | Task ID. |
| `X-Task-Attempt` | Current attempt number, starting at `1`. |

Any `2xx` response marks the task as succeeded. A valid JSON response body of at most 1 MiB is stored in `result`. Empty, invalid JSON, or oversized response bodies leave `result` as `null` without failing the task.

Non-`2xx` responses, network errors, and timeouts cause retries with backoff until `maxAttempts` is reached. Redirects are not followed. Tasks can be delivered more than once; workers should use `X-Task-Id` to avoid repeating side effects.
