# Authentication

Inbound and outbound HTTP Basic Auth are independently optional. Without auth
configuration, Tasker's API accepts unauthenticated requests and outbound
requests have no Authorization header. There are no task-level auth fields.

Add authentication sections to the same YAML file used for `http`, `database`,
and `queue`. Tasker loads `tasker.yaml` by default, or the path passed through
`--config /etc/tasker/tasker.yaml`. Unknown YAML fields and multiple documents
are rejected. Environment variables supply referenced passwords only; they do
not override configuration settings.

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

Omit `auth` to disable inbound authentication. When configured, every API request
must provide its username and password using HTTP Basic Auth. Invalid or missing
credentials receive `401 Unauthorized` and a Basic authentication challenge.
The single account has access to all tasks; there are no per-task permissions.

Omit `outbound_auth` to disable outbound authentication. Each entry matches the
exact origin: scheme, hostname, and effective port. DNS names use Go’s HTTP IDNA
lookup rules and are case-insensitive; IPv6 zone identifiers remain case-sensitive.
HTTPS without a port equals port 443, and HTTP without a port equals port 80.
An origin may have a trailing slash but cannot contain a path, query, fragment,
or URL credentials. Duplicate normalized origins are rejected. All routes on a
matched origin use the same credentials. Unmatched origins receive no Authorization
header. Redirects are not followed. Task URLs containing userinfo are rejected.

Each credential entry requires a username and exactly one of `password_env` or
`password_file`. Missing, unreadable, or empty secrets fail startup. Usernames cannot
contain colons or control characters; passwords cannot contain control characters.
Files may end with one LF or CRLF, which is removed; other whitespace is preserved.
Relative secret paths resolve against the configuration file's directory.

Secrets are loaded once at startup. Restart Tasker after rotating credentials;
queued tasks use the new credentials on subsequent dispatches. Passwords are not
stored in task records or returned by the API.

Basic Auth does not encrypt credentials. Use HTTPS outside a trusted private
network, terminating TLS at a proxy for Tasker's inbound API. Any authorized
Tasker caller can enqueue work for configured destinations; origin matching does
not provide permissions between callers.
