CREATE TABLE tasker_tasks (
    id text PRIMARY KEY,
    kind text NOT NULL,
    url text NOT NULL,
    input TEXT NOT NULL CHECK (json_valid(input)),
    status text NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    schedule_time INTEGER NOT NULL,
    dispatch_deadline text NOT NULL,
    attempt integer NOT NULL CHECK (attempt >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    result TEXT CHECK (result IS NULL OR json_valid(result)),
    last_error text NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    lease_token text NOT NULL DEFAULT '',
    lease_expires_at INTEGER,
    CHECK ((status = 'running' AND lease_token <> '' AND lease_expires_at IS NOT NULL)
        OR (status <> 'running' AND lease_token = '' AND lease_expires_at IS NULL)),
    CHECK ((status IN ('succeeded', 'failed')) = (finished_at IS NOT NULL))
);

CREATE INDEX tasker_tasks_due ON tasker_tasks (schedule_time, id)
    WHERE status = 'queued';
CREATE INDEX tasker_tasks_expired ON tasker_tasks (lease_expires_at, id)
    WHERE status = 'running';
CREATE INDEX tasker_tasks_finished ON tasker_tasks (finished_at, id)
    WHERE status IN ('succeeded', 'failed');
