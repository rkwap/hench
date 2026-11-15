#!/usr/bin/env bash
# A worker's memory (crates/hench/examples/memory.rs): what hench itself keeps in use at rest,
# at the peak of a burst and afterwards, and what the process's resident memory does under
# different allocator settings. Each run is one worker process against a fresh local
# redis-server without persistence. Prints one line per run.
#
# Usage: ci/memory.sh [--jobs N] [--concurrency N] [--port N]
#   --jobs N         jobs per run (default 20000)
#   --concurrency N  worker slots (default 25)
#   --port N         the redis-server's port (default 16398)
#
# The runs: no-op jobs with small (100 B) and large (100 kB) arguments, then jobs that each
# allocate and free 4 MB (what an application's jobs do), under the system allocator, with
# MALLOC_ARENA_MAX=2, and with jemalloc preloaded when JEMALLOC names its shared library (or
# one is found in the usual places). Needs redis-server in PATH. Honours CARGO_TARGET_DIR.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
jobs=20000
concurrency=25
port=16398
while [ $# -gt 0 ]; do
  case "$1" in
    --jobs) jobs="$2"; shift 2 ;;
    --concurrency) concurrency="$2"; shift 2 ;;
    --port) port="$2"; shift 2 ;;
    *) echo "memory: unknown argument $1" >&2; exit 2 ;;
  esac
done
command -v redis-server >/dev/null || { echo "memory: redis-server is not in PATH" >&2; exit 1; }
target="${CARGO_TARGET_DIR:-$root/target}"
(cd "$root" && cargo build --quiet --release -p hench --example memory)
bin="$target/release/examples/memory"
work=$(mktemp -d)
redis_pid=""
cleanup() {
  if [ -n "$redis_pid" ]; then kill "$redis_pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

jemalloc="${JEMALLOC:-}"
if [ -z "$jemalloc" ]; then
  for candidate in /usr/lib/*/libjemalloc.so.2 /usr/lib/libjemalloc.so.2 /usr/local/lib/libjemalloc.so.2; do
    if [ -e "$candidate" ]; then jemalloc="$candidate"; break; fi
  done
fi

# run <label> <jobs> <arg bytes> <job kB> [VAR=value...]
run() {
  local label="$1" count="$2" bytes="$3" job_kb="$4"
  shift 4
  redis-server --port "$port" --bind 127.0.0.1 --save '' --appendonly no --dir "$work" \
    --logfile "$work/redis.log" &
  redis_pid=$!
  for _ in $(seq 100); do
    if redis-cli -p "$port" ping >/dev/null 2>&1; then break; fi
    sleep 0.05
  done
  local url="redis://127.0.0.1:$port/"
  "$bin" push "$url" "$count" "$bytes"
  printf '%-34s' "$label"
  env "$@" "$bin" work "$url" "$count" "$concurrency" "$job_kb" | tr '\n' ' '
  echo
  kill "$redis_pid" 2>/dev/null || true
  wait "$redis_pid" 2>/dev/null || true
  redis_pid=""
}

large=$((jobs / 10))
run "no-op, 100 B arguments" "$jobs" 100 0 MEMORY_RUN=1
run "no-op, 100 kB arguments" "$large" 100000 0 MEMORY_RUN=1
run "4 MB jobs, system allocator" "$jobs" 100 4096 MEMORY_RUN=1
run "4 MB jobs, MALLOC_ARENA_MAX=2" "$jobs" 100 4096 MALLOC_ARENA_MAX=2
if [ -n "$jemalloc" ]; then
  run "4 MB jobs, jemalloc" "$jobs" 100 4096 LD_PRELOAD="$jemalloc" \
    MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0
else
  echo "memory: no jemalloc library found (set JEMALLOC to its path to include it)" >&2
fi
