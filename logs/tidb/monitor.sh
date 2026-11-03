#!/bin/bash
set -u
run_dir="$(cd "$(dirname "$0")" && pwd)"
pid=$(cat "$run_dir/launcher.pid")
while kill -0 "$pid" 2>/dev/null; do sleep 15; done
printf 'finished_at_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$run_dir/exit.status"
printf 'launcher_pid=%s\n' "$pid" >> "$run_dir/exit.status"
