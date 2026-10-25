# Metrics

Enable Prometheus/OpenMetrics exposition in `tasker.yaml`:

```yaml
metrics:
  enabled: true
```

`GET /metrics` uses the existing HTTP listen address and the same inbound Basic
Auth as the task API. It is disabled by default (404). When `auth` is absent,
the enabled endpoint is unauthenticated, just like the API. Use a private network
or the existing HTTPS reverse proxy and authentication for remote scrapers.
API credentials used for scraping also grant access to the task API.

Metrics are independent of log level and `logging.access`. Scrapes are excluded
from API request metrics. Labels never contain task IDs, worker URLs, payloads,
credentials, error messages, or user-supplied `kind` values.

## Metrics reference

All durations are seconds. Histograms expose `_bucket`, `_sum`, and `_count`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `tasker_queue_tasks` | Gauge | `state` | SQLite task counts: `due` (queued and eligible), `scheduled` (queued for the future), `running` (valid lease), `expired` (lease expired and awaiting recovery). Terminal tasks are excluded. |
| `tasker_queue_oldest_due_age_seconds` | Gauge | — | Time past schedule time of the oldest due queued task; zero for an empty due queue. Excludes running tasks and expired leases. |
| `tasker_queue_stats_up` | Gauge | — | `1` when the database snapshot succeeds, `0` when it fails. |
| `tasker_queue_delay_seconds` | Histogram | — | Time from eligibility to actual delivery start. Includes waiting for dispatch rate and concurrency capacity. |
| `tasker_tasks_enqueued_total` | Counter | — | Successfully persisted new tasks. |
| `tasker_tasks_finished_total` | Counter | `outcome` | Persisted `succeeded` or `failed` outcomes. Includes tasks failed during recovery of expired final leases. |
| `tasker_retries_total` | Counter | — | Successfully persisted retry schedules. |
| `tasker_delivery_attempts_total` | Counter | `outcome` | Delivery attempts that returned, classified as `success`, `http_error`, `timeout`, `network_error`, `response_error`, `invalid_task`, or `canceled`. |
| `tasker_delivery_duration_seconds` | Histogram | — | Delivery duration through reading the response; excludes saving the outcome. Includes failed and canceled deliveries. |
| `tasker_deliveries_in_flight` | Gauge | — | Deliveries currently executing in this process. Excludes ownership confirmation and saving outcomes. |
| `tasker_lease_recoveries_total` | Counter | — | Expired leases reclaimed for another attempt. Final exhausted leases count as failed task outcomes instead. |
| `tasker_lease_lost_total` | Counter | `operation` | Ownership checks rejected with a lost lease: `renew`, `complete`, `retry`, or `fail`. |
| `tasker_storage_errors_total` | Counter | `operation` | Errors from `create`, `get`, `claim`, `renew`, `complete`, `retry`, `fail`, `cleanup`, or `queue_stats`. Excludes no-task-due, task-not-found, lost-lease, and context-cancellation results. |
| `tasker_http_requests_total` | Counter | `method`, `route`, `status` | API requests, including auth failures, invalid requests, and unmatched routes. |
| `tasker_http_request_duration_seconds` | Histogram | `method`, `route` | API request latency. |
| `go_*`, `process_*` | Standard collectors | Collector-defined | Go memory, garbage collection, goroutines, process CPU, resident memory, start time, and file descriptors where supported by the host OS. |

HTTP routes are `/tasks`, `/tasks/{id}`, or `unmatched`. Methods are the standard
HTTP methods, with all other values grouped as `other`. `status` is the numeric
HTTP response code. Unauthorized requests still use the matched route label.
Series with fixed outcome/operation labels start at zero; HTTP series appear
when the corresponding requests occur.

## Queue delay and persistence

A task scheduled for 10:00 and dispatched at 10:02 has 120 seconds of queue delay.
Intentional waiting before 10:00 is excluded. Retries measure from their new
schedule time, excluding backoff. Recovered deliveries measure from the previous
lease expiry, excluding the earlier execution and lease lifetime. Histograms
only observe deliveries that start; the oldest-due gauge detects a stalled queue
even when no deliveries start. Use the expired-lease gauge to detect stalled
recovery separately.

Queue gauges come from one database read per scrape, using the existing queue
indexes. They survive restarts and include work owned by other processes using
the same database. A snapshot has a two-second timeout. On failure,
`tasker_queue_stats_up` is zero and queue count/age series are omitted rather than
reported as zero or stale values. Other metrics remain available. Only one
scrape is processed at a time; concurrent scrapes receive 503. Start with a
15-second scrape interval and a five-second scrape timeout.

Counters and histograms describe observations by the current process and reset
on restart. Use Prometheus `rate()`/`increase()` for rates and intervals. They
are operational telemetry, not a durable accounting ledger: a crash between a
database commit and updating its counter, or between scrapes, can lose an
observation. Delivery attempts interrupted by a forced exit have no completed
delivery observation. Graceful cancellation is recorded as `canceled` when the
delivery returns; it does not count as a persisted terminal task outcome.

A worker can succeed while saving the outcome fails or its lease is lost. This
increments successful delivery attempts, but not succeeded task outcomes. The
storage-error or lost-lease counter explains the difference. A successful HTTP
response with an unusable JSON result still counts as delivery success, matching
Tasker's existing result handling.

When multiple processes share one SQLite database, each exports the same queue
snapshot. Inspect those gauges per instance; do not sum them across instances.
Process counters can be summed. With separate databases, queue counts can also
be summed.

## Prometheus and Grafana

1. Enable metrics and choose inbound authentication in Tasker's config.
2. Copy [prometheus.yml](../examples/monitoring/prometheus.yml) and
   [alerts.yml](../examples/monitoring/alerts.yml) into your Prometheus config
   directory. Set the target to Tasker's address as reachable from Prometheus.
   The example `tasker:8080` assumes a shared container network; for processes
   on the same host, use `127.0.0.1:8080`. If API auth is enabled, uncomment
   `basic_auth` and mount the API password file into Prometheus.
3. Start Prometheus with `--config.file=/path/to/prometheus.yml`. Check its
   targets page for a successful `tasker` scrape.
4. Add that Prometheus server as a Grafana Prometheus data source. Import
   [grafana-dashboard.json](../examples/monitoring/grafana-dashboard.json), then
   select the data source, job, and instances at the top of the dashboard.

The starter dashboard covers queue state and age, queue/delivery latency,
enqueue/completion/attempt/retry rates, active deliveries, recovery, storage and
lease errors, API traffic and latency, and process health. Queue panels retain
the instance label so shared-database snapshots are not added together.

The alert rules cover unreachable targets, failed queue snapshots, queue delay
over 60 seconds for two minutes, expired leases awaiting recovery for two
minutes, and permanent task failures. Adjust thresholds to your workload and
configure your Prometheus Alertmanager separately for notifications.

Useful queries:

```promql
# Queue delay p95 by instance over five minutes.
histogram_quantile(0.95, sum by (instance, le) (rate(tasker_queue_delay_seconds_bucket[5m])))

# Persisted permanent failures observed in the last five minutes.
sum by (instance) (increase(tasker_tasks_finished_total{outcome="failed"}[5m]))

# Delivery timeout rate.
sum by (instance) (rate(tasker_delivery_attempts_total{outcome="timeout"}[5m]))
```

See the [Prometheus scrape configuration reference](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)
and [Grafana Prometheus data source documentation](https://grafana.com/docs/grafana/latest/datasources/prometheus/).
