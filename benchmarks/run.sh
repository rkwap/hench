#!/usr/bin/env bash
# Runs Hench's ports of Sidekiq's benchmark scripts (benchmarks/src/bin) and Sidekiq's own
# scripts on this machine, against private servers, and records every run. benchmarks/README.md
# describes each benchmark and has the results.
#
# Usage: benchmarks/run.sh [--suite all|load|web|multi] [--system both|hench|sidekiq]
#                          [--rounds N] [--multi-rounds N] [--quick] [--out DIR]
#                          [--max-load L] [--max-wait S] [--port N]
#   --suite           the benchmarks to run (default all): load (bin/sidekiqload), web
#                     (bin/webload) and multi (bin/multi_queue_bench, 8,000,000 jobs a run)
#   --system          whose (default both)
#   --rounds N        rounds of each load and web configuration (default 5)
#   --multi-rounds N  rounds of the multi-queue benchmark (default 3)
#   --quick           a smoke test: few jobs and requests, one round, recorded in a temporary
#                     directory unless --out names one
#   --out DIR         where to record (default benchmarks/results/<date>-<cpu>)
#   --max-load L      before each run, wait for the one-minute load average to fall below L
#                     (default 3): runs should not share the machine
#   --max-wait S      how long to wait for that, in seconds, before running anyway (default 600)
#   --port N          the private redis-server's port (default 16379); the toxiproxy proxy
#                     listens on N + 1
#
# Every run gets a fresh redis-server without persistence, and the rounds alternate which
# system runs first. The output directory gets environment.txt (the machine and every version),
# raw/ (each run's output, colour codes removed), runs.csv (a line per run) and summary.md (the
# medians, from benchmarks/summarize.sh); a directory that exists gets more runs. On Linux,
# environment.txt lists the CPU idle states, and run.sh warns when states that take more than
# 20 us to leave are enabled (benchmarks/README.md, Method).
#
# Needs redis-server and redis-cli; for the runs with latency, toxiproxy-server (started on
# 127.0.0.1:8474 unless it runs) and curl; for Sidekiq, Ruby 3.2 or newer built with YJIT (RUBY
# names the interpreter, default ruby) and its Bundler, which installs benchmarks/sidekiq's
# Gemfile.lock into benchmarks/sidekiq/vendor. Honours CARGO_TARGET_DIR.
set -euo pipefail

bench=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$bench/.." && pwd)
suite=all
system=both
rounds=5
multi_rounds=3
quick=false
out=""
max_load=3
max_wait=600
port=16379
while [ $# -gt 0 ]; do
  case "$1" in
    --suite) suite="$2"; shift 2 ;;
    --system) system="$2"; shift 2 ;;
    --rounds) rounds="$2"; shift 2 ;;
    --multi-rounds) multi_rounds="$2"; shift 2 ;;
    --quick) quick=true; shift ;;
    --out) out="$2"; shift 2 ;;
    --max-load) max_load="$2"; shift 2 ;;
    --max-wait) max_wait="$2"; shift 2 ;;
    --port) port="$2"; shift 2 ;;
    *) echo "run.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done
case "$suite" in all|load|web|multi) ;; *) echo "run.sh: --suite $suite?" >&2; exit 2 ;; esac
case "$system" in both|hench|sidekiq) ;; *) echo "run.sh: --system $system?" >&2; exit 2 ;; esac
proxy_port=$((port + 1))
ruby=${RUBY:-ruby}

# suite_has <suite>, system_has <hench|sidekiq>: what this invocation runs.
suite_has() { [ "$suite" = all ] || [ "$suite" = "$1" ]; }
system_has() { [ "$system" = both ] || [ "$system" = "$1" ]; }
need() {
  command -v "$1" >/dev/null || { echo "run.sh: $1 is not in PATH" >&2; exit 1; }
}

need redis-server
need redis-cli
if suite_has load; then
  need toxiproxy-server
  need curl
fi

if $quick; then
  rounds=1
  multi_rounds=1
  # Sidekiq's bin/sidekiqload has no job count: GC=1 runs 10,000 jobs (with GC off).
  load_hench="ITERATIONS=20"
  load_sidekiq="GC=1"
  multi_size="ITERATIONS=5"
  web_count=500
else
  load_hench=""
  load_sidekiq=""
  multi_size=""
  web_count=10000
fi

work=$(mktemp -d)
redis_pid=""
seed_pid=""
toxiproxy_pid=""
cleanup() {
  if [ -n "$seed_pid" ]; then kill "$seed_pid" 2>/dev/null || true; fi
  if [ -n "$redis_pid" ]; then kill "$redis_pid" 2>/dev/null || true; fi
  if [ -n "$toxiproxy_pid" ]; then kill "$toxiproxy_pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

# The CPU's name as a directory name ("Apple M3" -> apple-m3).
cpu_name() {
  if [ "$(uname)" = Darwin ]; then
    sysctl -n machdep.cpu.brand_string
  else
    sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -n 1
  fi
}
# Linux: the idle states of CPU 0 and the time each takes to leave ("POLL 0 us, C1 1 us,
# C6 190 us disabled"); nothing elsewhere.
idle_states() {
  local state list=""
  for state in /sys/devices/system/cpu/cpu0/cpuidle/state*; do
    [ -r "$state/latency" ] || continue
    list="$list, $(cat "$state/name") $(cat "$state/latency") us"
    if [ "$(cat "$state/disable" 2>/dev/null)" = 1 ]; then list="$list disabled"; fi
  done
  echo "${list#, }"
}
# The idle states, on any CPU, that are enabled and take more than 20 us to leave: a thread
# that wakes another one waits that long when the other's CPU sleeps in one of them.
deep_idle_states() {
  local state
  for state in /sys/devices/system/cpu/cpu[0-9]*/cpuidle/state*; do
    [ -r "$state/latency" ] || continue
    if [ "$(cat "$state/latency")" -gt 20 ] && [ "$(cat "$state/disable" 2>/dev/null)" != 1 ]; then
      cat "$state/name"
    fi
  done | sort -u | tr '\n' ' '
}
if [ -z "$out" ]; then
  if $quick; then
    out="$work/results"
  else
    slug=$(cpu_name | tr '[:upper:]' '[:lower:]' | sed -e 's/([^)]*)//g' -e 's/[^a-z0-9]\{1,\}/-/g' \
      -e 's/^-//' -e 's/-$//')
    out="$bench/results/$(date -u +%Y-%m-%d)-$slug"
  fi
fi
mkdir -p "$out/raw"

# Builds and installs what the runs need.
target="${CARGO_TARGET_DIR:-$root/target}"
if system_has hench || suite_has web; then
  echo "building the Hench benchmarks"
  (cd "$root" && cargo build --quiet --release -p hench-benchmarks)
fi
bin="$target/release"
if system_has sidekiq; then
  need "$ruby"
  ruby_path=$(command -v "$ruby")
  bundle="$(dirname "$ruby_path")/bundle"
  [ -x "$bundle" ] || bundle=bundle
  # Frozen: the gems Gemfile.lock pins, and the file left as it is, whatever Bundler runs.
  export BUNDLE_GEMFILE="$bench/sidekiq/Gemfile" BUNDLE_PATH="$bench/sidekiq/vendor/bundle" \
    BUNDLE_FROZEN=true
  # The Bundler RUBY ships: when Gemfile.lock names another one, `bundler/setup` would
  # otherwise switch to it by exec'ing the script anew through its `#!/usr/bin/env ruby`,
  # without the -r of redirect.rb. The scripts' shebangs find RUBY first all the same.
  export BUNDLER_VERSION="$("$ruby" -rbundler -e 'print Bundler::VERSION')"
  export PATH="$(dirname "$ruby_path"):$PATH"
  # Sidekiq's web UI reads its locale files as UTF-8.
  export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
  echo "installing Sidekiq (benchmarks/sidekiq/Gemfile.lock)"
  "$bundle" install --quiet
  sidekiq_bin="$("$ruby" -rbundler/setup -e 'print Gem::Specification.find_by_name("sidekiq").gem_dir')/bin"
  if ! RUBY_YJIT_ENABLE=1 "$ruby" -e 'exit(defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled?)' 2>/dev/null; then
    echo "run.sh: $ruby has no YJIT: Sidekiq's published numbers use it" >&2
  fi
fi

# The machine and every version, once per output directory.
if [ ! -f "$out/environment.txt" ]; then
  {
    echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [ "$(uname)" = Darwin ]; then
      echo "machine: $(sysctl -n hw.model), $(cpu_name), $(sysctl -n hw.physicalcpu) cores" \
        "($(sysctl -n hw.perflevel0.physicalcpu 2>/dev/null || echo '?') performance," \
        "$(sysctl -n hw.perflevel1.physicalcpu 2>/dev/null || echo '?') efficiency)," \
        "$(($(sysctl -n hw.memsize) / 1073741824)) GB"
      echo "os: macOS $(sw_vers -productVersion) ($(sw_vers -buildVersion))"
      echo "power: $(pmset -g batt 2>/dev/null | sed -n "s/^Now drawing from '\(.*\)'/\1/p")"
    else
      echo "machine: $(cpu_name), $(nproc) cores, $(awk '/MemTotal/ { printf "%d GB", $2 / 1048576 }' /proc/meminfo)"
      echo "os: $(. /etc/os-release && echo "$PRETTY_NAME"), kernel $(uname -r)"
      states=$(idle_states)
      if [ -n "$states" ]; then echo "cpu idle states: $states"; fi
      # On EC2, the instance type (IMDSv2; nothing elsewhere).
      token=$(curl -s -m 1 -X PUT http://169.254.169.254/latest/api/token \
        -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)
      if [ -n "$token" ]; then
        echo "ec2: $(curl -s -m 1 -H "X-aws-ec2-metadata-token: $token" \
          http://169.254.169.254/latest/meta-data/instance-type)," \
          "$(curl -s -m 1 -H "X-aws-ec2-metadata-token: $token" \
          http://169.254.169.254/latest/meta-data/placement/availability-zone)"
      fi
    fi
    echo "hench: $(git -C "$root" rev-parse --short HEAD)$(git -C "$root" diff --quiet HEAD -- . 2>/dev/null || echo ' (with uncommitted changes)')"
    echo "rustc: $(rustc --version)"
    echo "redis-server: $(redis-server --version)"
    if suite_has load; then echo "toxiproxy: $(toxiproxy-server --version 2>&1 | head -n 1)"; fi
    if system_has sidekiq; then
      echo "ruby: $(RUBY_YJIT_ENABLE=1 "$ruby" -e 'print RUBY_DESCRIPTION')"
      echo "sidekiq: $("$ruby" -rbundler/setup -e 'require "sidekiq"; print Sidekiq::VERSION')," \
        "redis-client $("$ruby" -rbundler/setup -e 'require "redis-client"; print RedisClient::VERSION')"
    fi
  } >"$out/environment.txt"
fi
cat "$out/environment.txt"
deep=$(deep_idle_states)
if [ -n "$deep" ]; then
  echo "run.sh: CPU idle states that take more than 20 us to leave are enabled (${deep% }):" \
    "every thread woken on a CPU that sleeps in one waits that long (benchmarks/README.md," \
    "Method; benchmarks/setup-ubuntu.sh disables them)" >&2
fi
if [ ! -f "$out/runs.csv" ]; then
  echo "benchmark,config,system,round,jobs,seconds,jobs_per_sec,last_job_seconds,load_average,log" \
    >"$out/runs.csv"
fi
sequence=$(find "$out/raw" -name '*.log' | wc -l | tr -d ' ')

# start_redis / stop_redis: a fresh redis-server without persistence.
start_redis() {
  redis-server --port "$port" --bind 127.0.0.1 --save '' --appendonly no --dir "$work" \
    --logfile "$work/redis.log" &
  redis_pid=$!
  for _ in $(seq 100); do
    if redis-cli -p "$port" ping >/dev/null 2>&1; then return 0; fi
    sleep 0.05
  done
  echo "run.sh: redis-server did not start (see $work/redis.log)" >&2
  return 1
}
stop_redis() {
  kill "$redis_pid"
  wait "$redis_pid" 2>/dev/null || true
  redis_pid=""
}

# The toxiproxy server, started unless one runs, and emptied of toxics before each run.
reset_toxiproxy() {
  if ! curl -s -o /dev/null http://127.0.0.1:8474/version; then
    toxiproxy-server -host 127.0.0.1 -port 8474 >"$work/toxiproxy.log" 2>&1 &
    toxiproxy_pid=$!
    for _ in $(seq 100); do
      if curl -s -o /dev/null http://127.0.0.1:8474/version; then break; fi
      sleep 0.05
    done
  fi
  curl -s -X POST http://127.0.0.1:8474/reset >/dev/null
}

load_average() {
  if [ -r /proc/loadavg ]; then
    awk '{ print $1 }' /proc/loadavg
  else
    sysctl -n vm.loadavg | awk '{ print $2 }'
  fi
}
wait_for_idle() {
  local waited=0 load
  load=$(load_average)
  while awk -v average="$load" -v max="$max_load" 'BEGIN { exit !(average >= max) }'; do
    if [ "$waited" -ge "$max_wait" ]; then
      echo "   load average still $load after ${max_wait}s: running anyway" >&2
      return 0
    fi
    if [ "$waited" -eq 0 ]; then echo "   waiting for the load average ($load) to fall below $max_load"; fi
    sleep 15
    waited=$((waited + 15))
    load=$(load_average)
  done
}

# run_one <benchmark> <config> <system> <round> <command...>: one run, recorded.
run_one() {
  local benchmark=$1 config=$2 name=$3 round=$4
  shift 4
  wait_for_idle
  local load log status=0
  load=$(load_average)
  sequence=$((sequence + 1))
  log="raw/$(printf '%03d' "$sequence")-$benchmark-$(echo "$config" | tr -c 'a-zA-Z0-9=\n' '-' \
    | tr -s '-' | sed 's/^-//; s/-$//')-$name.log"
  echo "== $benchmark, $config, $name, round $round (load average $load)"
  {
    echo "# $benchmark, $config, $name, round $round, $(date -u +%Y-%m-%dT%H:%M:%SZ), load average $load"
    echo "# $*"
  } >"$out/$log"
  "$@" 2>&1 | perl -pe 's/\e\[[0-9;]*m//g' >>"$out/$log" || status=$?
  if [ "$status" -ne 0 ]; then
    echo "run.sh: the run failed ($status): $out/$log" >&2
    tail -n 5 "$out/$log" >&2
    return 1
  fi
  # "Done, <jobs> jobs in <seconds> sec, <rate> jobs/sec" (the last one: WARM runs two), or
  # "<page> in <seconds> sec" for the web pages; Hench's load runs also report their last job.
  awk -v benchmark="$benchmark" -v config="$config" -v name="$name" -v round="$round" \
    -v average="$load" -v logfile="$log" -v requests="$web_count" '
    / Done, [0-9]+ jobs in / {
      for (i = 1; i <= NF; i++) if ($i == "Done,") { jobs = $(i + 1); seconds = $(i + 4); rate = $(i + 6) }
    }
    /^\/[^ ]* in [0-9.e-]+ sec/ { jobs = requests; seconds = $3; rate = sprintf("%d", requests / $3) }
    / Last job ended after / {
      for (i = 1; i <= NF; i++) if ($i == "after") last = $(i + 1)
    }
    END {
      if (seconds == "") { print "no result in " logfile > "/dev/stderr"; exit 1 }
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", benchmark, config, name, round, jobs, seconds, rate, last, average, logfile
    }' "$out/$log" | tee -a "$out/runs.csv" | awk -F, '{ printf "   %s s, %s per second\n", $6, $7 }'
}

# in_order <round> <systems...>: the systems, reversed on even rounds.
in_order() {
  local round=$1 reversed=""
  shift
  if [ $((round % 2)) -eq 1 ]; then
    echo "$@"
    return
  fi
  for name in "$@"; do reversed="$name $reversed"; done
  echo "$reversed"
}
# runs <system>: whether this invocation runs <system> (hench-* are Hench's).
runs() {
  case "$1" in
    hench*) system_has hench ;;
    *) system_has sidekiq ;;
  esac
}

# bin/sidekiqload: 500,000 jobs, as its header runs it (LATENCY=0 THREADS=10) and as Sidekiq's
# README table does (LATENCY=1 THREADS=30); then with 100 threads, to see what more concurrency
# buys each system when every reply takes a millisecond.
if suite_has load; then
  for round in $(seq "$rounds"); do
    for setting in 0:10 1:30 1:100; do
      latency=${setting%:*}
      threads=${setting#*:}
      for name in $(in_order "$round" hench-reliable hench-basic sidekiq); do
        runs "$name" || continue
        start_redis
        reset_toxiproxy
        if [ "$name" = sidekiq ]; then
          run_one load "LATENCY=$latency THREADS=$threads" "$name" "$round" \
            env RUBY_YJIT_ENABLE=1 LATENCY="$latency" THREADS="$threads" $load_sidekiq \
            REDIS_URL="redis://127.0.0.1:$port" BENCH_REDIS_PORT="$port" \
            BENCH_PROXY_PORT="$proxy_port" "$ruby" -r "$bench/sidekiq/redirect.rb" \
            "$sidekiq_bin/sidekiqload"
        else
          run_one load "LATENCY=$latency THREADS=$threads" "$name" "$round" \
            env LATENCY="$latency" THREADS="$threads" FETCH="${name#hench-}" $load_hench \
            PORT="$port" PROXY_PORT="$proxy_port" "$bin/henchload"
        fi
        stop_redis
      done
    done
  done
fi

# bin/webload: the pages of one seeded Redis, /busy (the script's page) and /queues.
if suite_has web; then
  start_redis
  REDIS_URL="redis://127.0.0.1:$port/0" "$bin/webload" seed >"$work/seed.log" 2>&1 &
  seed_pid=$!
  for _ in $(seq 300); do
    if grep -q Seeded "$work/seed.log"; then break; fi
    sleep 0.1
  done
  grep -q Seeded "$work/seed.log" || { echo "run.sh: webload seed failed" >&2; cat "$work/seed.log" >&2; exit 1; }
  for round in $(seq "$rounds"); do
    for page in /busy /queues; do
      for name in $(in_order "$round" hench sidekiq); do
        runs "$name" || continue
        if [ "$name" = sidekiq ]; then
          run_one web "$page" "$name" "$round" env RUBY_YJIT_ENABLE=1 PAGE="$page" \
            COUNT="$web_count" REDIS_URL="redis://127.0.0.1:$port/0" "$ruby" \
            "$bench/sidekiq/webload.rb"
        else
          run_one web "$page" "$name" "$round" env PAGE="$page" COUNT="$web_count" \
            REDIS_URL="redis://127.0.0.1:$port/0" "$bin/webload"
        fi
      done
    done
  done
  kill "$seed_pid"
  wait "$seed_pid" 2>/dev/null || true
  seed_pid=""
  stop_redis
fi

# bin/multi_queue_bench: 8 queues, 8 processes of 10 threads, as its header and the published
# runs use it.
if suite_has multi; then
  for round in $(seq "$multi_rounds"); do
    for name in $(in_order "$round" hench-reliable hench-basic sidekiq); do
      runs "$name" || continue
      start_redis
      if [ "$name" = sidekiq ]; then
        run_one multi "PROCESSES=8 QUEUES=8 THREADS=10" "$name" "$round" \
          env RUBY_YJIT_ENABLE=1 THREADS=10 PROCESSES=8 QUEUES=8 $multi_size \
          PORT="$port" IP=127.0.0.1 "$ruby" "$sidekiq_bin/multi_queue_bench"
      else
        run_one multi "PROCESSES=8 QUEUES=8 THREADS=10" "$name" "$round" \
          env THREADS=10 PROCESSES=8 QUEUES=8 FETCH="${name#hench-}" $multi_size \
          PORT="$port" IP=127.0.0.1 "$bin/multi_queue_bench"
      fi
      stop_redis
    done
  done
fi

"$bench/summarize.sh" "$out/runs.csv" >"$out/summary.md"
echo
cat "$out/summary.md"
echo "recorded in $out"
