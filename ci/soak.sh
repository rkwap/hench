#!/usr/bin/env bash
# Runs the soak and chaos scenarios of crates/hench/examples/soak one after the other: several
# worker processes against a private redis-server, with load and faults (see the example's
# documentation). Each scenario writes its logs and report.json under target/soak/<scenario>/
# and prints a summary; the script fails if any scenario violates an invariant.
#
# Usage: ci/soak.sh [--short] [scenario...]
#   --short   CI durations (about 12 minutes of load and chaos in all, plus the drains)
#   scenario  crash redis-aof proxy redis-none mixed-basic long-pause soak-long stale-sched
#             sched-control stale-procs pro pro-crash pro-redis-none (default: all but
#             stale-procs; with --short: crash proxy redis-none stale-sched pro pro-crash
#             pro-redis-none)
#
# Needs redis-server in PATH. Environment: SOAK (the harness binary, default
# target/release/examples/soak, built here when missing), HENCH_BIN (a `hench` binary from
# hench-server: the dashboard's JSON API is then sampled and checked), SOAK_PORT (the first of
# the three local ports used, default 26379).
set -uo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
target=${CARGO_TARGET_DIR:-$root/target}
soak=${SOAK:-$target/release/examples/soak}
if [ ! -x "$soak" ]; then
  cargo build --release -p hench --example soak || exit 1
fi
command -v redis-server >/dev/null || { echo "soak: redis-server is not installed" >&2; exit 1; }

short=0
if [ "${1:-}" = "--short" ]; then
  short=1
  shift
fi
port=${SOAK_PORT:-26379}
common=(--port "$port" --proxy-port $((port + 1)) --web-port $((port + 2)) --rate 30)
if [ -n "${HENCH_BIN:-}" ]; then
  # Checked here: a missing binary would fail every scenario with a bare "No such file".
  if [ ! -x "$HENCH_BIN" ]; then
    echo "soak: HENCH_BIN ($HENCH_BIN) is not an executable file" \
      "(build it with: cargo build --release -p hench-server --bin hench)" >&2
    exit 1
  fi
  common+=(--hench-bin "$HENCH_BIN")
fi

# Durations: the full suite's, or about a third of them with --short.
seconds() {
  if [ "$short" = 1 ]; then echo "$2"; else echo "$1"; fi
}

failed=()
run() {
  local name=$1
  shift
  echo "=== $name ($(date -u +%H:%M:%S))"
  if ! "$soak" run --scenario "$name" --dir "target/soak/$name" "${common[@]}" "$@"; then
    failed+=("$name")
  fi
}

scenarios=("$@")
if [ ${#scenarios[@]} -eq 0 ]; then
  if [ "$short" = 1 ]; then
    scenarios=(crash proxy redis-none stale-sched pro pro-crash pro-redis-none)
  else
    scenarios=(crash redis-aof proxy redis-none mixed-basic long-pause soak-long stale-sched sched-control pro
      pro-crash pro-redis-none)
  fi
fi
mkdir -p target/soak
for scenario in "${scenarios[@]}"; do
  case $scenario in
    crash)       run crash --duration "$(seconds 240 90)" --workers 4 --concurrency 8 --nonce-base 1000000000 \
                   --actions kill9,term,dterm,tstp,remote_stop,remote_quiet,queue_pause --chaos-min 6 --chaos-max 15 ;;
    redis-aof)   run redis-aof --duration "$(seconds 180 60)" --workers 4 --concurrency 8 --nonce-base 2000000000 --persist aof \
                   --actions redis_restart,redis_kill9,pause_write,pause_all,kill9 --chaos-min 8 --chaos-max 20 ;;
    proxy)       run proxy --duration "$(seconds 180 90)" --workers 4 --concurrency 8 --nonce-base 3000000000 --proxy \
                   --actions proxy_drop,proxy_delay,proxy_reset,pause_write,kill9,term --chaos-min 6 --chaos-max 15 ;;
    redis-none)  run redis-none --duration "$(seconds 120 60)" --workers 3 --concurrency 8 --nonce-base 4000000000 --persist none \
                   --actions redis_restart,kill9,term --chaos-min 10 --chaos-max 25 ;;
    mixed-basic) run mixed-basic --duration "$(seconds 120 60)" --workers 4 --concurrency 8 --nonce-base 5000000000 --fetch mixed --proxy \
                   --actions kill9,term,proxy_drop,redis_restart --chaos-min 8 --chaos-max 18 ;;
    long-pause)  run long-pause --duration "$(seconds 150 120)" --workers 3 --concurrency 8 --nonce-base 6000000000 \
                   --actions long_pause --chaos-min 20 --chaos-max 30 ;;
    soak-long)   run soak-long --duration "$(seconds 300 120)" --workers 4 --concurrency 10 --nonce-base 7000000000 --proxy --persist aof \
                   --actions kill9,term,tstp,remote_stop,redis_restart,pause_write,proxy_drop,proxy_delay,queue_pause --chaos-min 15 --chaos-max 35 ;;
    # A crash loop of kill -9 restarts, then calm: dead processes must be pruned, and the
    # scheduler must not slow down for them (compare with sched-control).
    stale-sched) run stale-sched --duration "$(seconds 260 120)" --workers 2 --concurrency 8 --rate 4 --nonce-base 9000000000 \
                   --actions kill9 --chaos-min 5 --chaos-max 8 --chaos-until "$(seconds 70 50)" ;;
    stale-procs) run stale-procs --duration "$(seconds 300 120)" --workers 2 --concurrency 8 --rate 8 --nonce-base 8000000000 \
                   --actions kill9 --chaos-min 5 --chaos-max 8 --chaos-until "$(seconds 70 50)" ;;
    sched-control) run sched-control --duration "$(seconds 150 60)" --workers 2 --concurrency 8 --rate 4 --nonce-base 9500000000 ;;
    # The pro features together (batches of throttled members with a limiter inside, cancels,
    # a unique periodic schedule) under kills, Redis restarts and the reply-dropping proxy.
    # The pro scenarios end with a minute without faults: the schedule must tick again by then
    # (a fault may cost it a lease, 15 s in the soak's workers).
    pro)         run pro --pro --duration "$(seconds 240 120)" --chaos-until "$(seconds 180 60)" --workers 4 --concurrency 8 --rate 10 --nonce-base 9700000000 --proxy --persist aof \
                   --actions kill9,term,redis_restart,proxy_drop,proxy_delay --chaos-min 5 --chaos-max 12 ;;
    # The pro features under the other faults: stopped and remotely stopped or quieted
    # workers, paused queues, CLIENT PAUSE and kill -9 of Redis with AOF; then Redis restarts
    # that lose everything (batches and locks committed before them are excused, as jobs are).
    pro-crash)   run pro-crash --pro --duration "$(seconds 180 100)" --chaos-until "$(seconds 120 40)" --workers 4 --concurrency 8 --rate 10 --nonce-base 9800000000 --persist aof \
                   --actions kill9,term,dterm,tstp,remote_stop,remote_quiet,queue_pause,pause_write,pause_all,redis_kill9 \
                   --chaos-min 6 --chaos-max 15 ;;
    pro-redis-none) run pro-redis-none --pro --duration "$(seconds 150 100)" --chaos-until "$(seconds 90 40)" --workers 3 --concurrency 8 --rate 10 --nonce-base 9900000000 --persist none \
                   --actions redis_restart,kill9,term --chaos-min 10 --chaos-max 25 ;;
    *) echo "soak: unknown scenario $scenario" >&2; failed+=("$scenario") ;;
  esac
done

if [ ${#failed[@]} -gt 0 ]; then
  echo "soak: invariants violated (or the harness failed) in: ${failed[*]}" >&2
  exit 1
fi
echo "soak: every scenario held its invariants"
