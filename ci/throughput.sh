#!/usr/bin/env bash
# The plain-job throughput gate (X22 of docs/design/pro-features.md): builds
# crates/hench/examples/throughput.rs in release mode at this checkout and, with --base, at an
# older revision (in a temporary git worktree, with this checkout's copy of the example), then
# runs the builds in alternation, each run against a fresh local redis-server without
# persistence, and prints every run and a summary (median, mean and standard deviation of each
# build, and the change of the medians). With --kind, the jobs use one feature each
# (crates/hench/examples/throughput_features.rs): what a feature costs per job, and whether a
# change made it costlier.
#
# Usage: ci/throughput.sh [--base REV] [--kind KIND] [--rounds N] [--jobs N] [--concurrency N]
#                         [--port N]
#   --base REV       also build REV and compare (for example f3da43d, the tree before the pro
#                    features; with --kind, a revision that has them)
#   --kind KIND      plain (default: throughput.rs), or unique, unique-start, throttled,
#                    per-key or member (throughput_features.rs)
#   --rounds N       runs of each build (default 15), in alternation, the first build to run
#                    swapping every round
#   --jobs N         no-op jobs per run (default 100000)
#   --concurrency N  worker slots (default 25)
#   --port N         the redis-server's port (default 16399)
#
# Needs redis-server in PATH. Honours CARGO_TARGET_DIR (the builds of both revisions share it:
# each binary is copied out before the next build). Run it on an otherwise idle machine.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
base=""
kind=plain
rounds=15
jobs=100000
concurrency=25
port=16399
while [ $# -gt 0 ]; do
  case "$1" in
    --base) base="$2"; shift 2 ;;
    --kind) kind="$2"; shift 2 ;;
    --rounds) rounds="$2"; shift 2 ;;
    --jobs) jobs="$2"; shift 2 ;;
    --concurrency) concurrency="$2"; shift 2 ;;
    --port) port="$2"; shift 2 ;;
    *) echo "throughput: unknown argument $1" >&2; exit 2 ;;
  esac
done
command -v redis-server >/dev/null || { echo "throughput: redis-server is not in PATH" >&2; exit 1; }
target="${CARGO_TARGET_DIR:-$root/target}"
work=$(mktemp -d)
tree=""
redis_pid=""
cleanup() {
  if [ -n "$redis_pid" ]; then kill "$redis_pid" 2>/dev/null || true; fi
  if [ -n "$tree" ]; then git -C "$root" worktree remove --force "$tree" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

# The example timed, and the arguments before the job count.
example=throughput
kind_args=()
if [ "$kind" != plain ]; then
  example=throughput_features
  kind_args=("$kind")
fi

# build <source tree> <name>: the example in release mode, copied to $work/<name>.
build() {
  (cd "$1" && CARGO_TARGET_DIR="$target" cargo build --quiet --release -p hench --example "$example")
  cp "$target/release/examples/$example" "$work/$2"
}

builds=(head)
build "$root" head
if [ -n "$base" ]; then
  tree="$work/base-tree"
  git -C "$root" worktree add --quiet --detach "$tree" "$base"
  cp "$root/crates/hench/examples/$example.rs" "$tree/crates/hench/examples/$example.rs"
  build "$tree" base
  git -C "$root" worktree remove --force "$tree"
  tree=""
  builds=(base head)
fi

# run <name>: one run against a fresh redis-server; its line goes to $work/line.
run() {
  redis-server --port "$port" --bind 127.0.0.1 --save '' --appendonly no --dir "$work" \
    --logfile "$work/redis.log" &
  redis_pid=$!
  for _ in $(seq 100); do
    if redis-cli -p "$port" ping >/dev/null 2>&1; then break; fi
    sleep 0.05
  done
  local status=0
  "$work/$1" "redis://127.0.0.1:$port/" "${kind_args[@]}" "$jobs" "$concurrency" >"$work/line" \
    || status=$?
  kill "$redis_pid"
  wait "$redis_pid" 2>/dev/null || true
  redis_pid=""
  return "$status"
}

: >"$work/results"
for round in $(seq "$rounds"); do
  order=("${builds[@]}")
  if [ $((round % 2)) -eq 0 ] && [ ${#builds[@]} -eq 2 ]; then order=(head base); fi
  for name in "${order[@]}"; do
    run "$name"
    echo "$name run $round: $(cat "$work/line")"
    rate=$(sed -n 's/.*= \([0-9]*\) jobs\/s.*/\1/p' "$work/line")
    echo "$name $rate" >>"$work/results"
  done
done

# The summary: median, mean and standard deviation of each build, and the change of medians.
sort -k1,1 -k2,2n "$work/results" | awk '
  { rates[$1] = rates[$1] " " $2; count[$1]++; sum[$1] += $2; squares[$1] += $2 * $2 }
  END {
    names[1] = "base"; names[2] = "head"
    for (i = 1; i <= 2; i++) {
      name = names[i]
      if (!(name in count)) continue
      n = split(substr(rates[name], 2), value, " ")
      median[name] = (n % 2) ? value[(n + 1) / 2] : (value[n / 2] + value[n / 2 + 1]) / 2
      mean = sum[name] / n
      sd = (n > 1) ? sqrt((squares[name] - n * mean * mean) / (n - 1)) : 0
      printf "%s: %d runs, median %.0f jobs/s, mean %.0f, standard deviation %.0f\n", name, n, median[name], mean, sd
    }
    if ("base" in median && "head" in median) {
      printf "change of the medians: %+.1f %%\n", (median["head"] - median["base"]) / median["base"] * 100
    }
  }'
