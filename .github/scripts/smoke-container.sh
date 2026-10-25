#!/usr/bin/env bash
set -euo pipefail

image="${1:?Supply the locally built image tag}"
data_dir="$(mktemp -d)"
container_id=""
cleanup() {
  status=$?
  if [[ -n "$container_id" ]]; then
    if (( status != 0 )); then
      docker logs "$container_id" >&2 || true
    fi
    docker rm -f "$container_id" >/dev/null || true
  fi
  rm -rf "$data_dir"
  exit "$status"
}
trap cleanup EXIT

# Exercise the documented default user with a host-owned bind mount.
cat > "$data_dir/tasker.yaml" <<'YAML'
database:
  path: /data/smoke.db
metrics:
  enabled: true
YAML

container_id="$(docker run --detach \
  --publish 127.0.0.1::8080 \
  --mount "type=bind,src=$data_dir,dst=/data" \
  "$image" --config /data/tasker.yaml)"
address="$(docker port "$container_id" 8080/tcp)"
curl --fail --silent --show-error --retry 30 --retry-delay 1 \
  --retry-connrefused --max-time 2 "http://$address/metrics" \
  > "$data_dir/metrics.txt"
grep -q '^go_goroutines ' "$data_dir/metrics.txt"
test -s "$data_dir/smoke.db"

docker stop --time 15 "$container_id" >/dev/null
test "$(docker inspect --format '{{.State.ExitCode}}' "$container_id")" = 0
