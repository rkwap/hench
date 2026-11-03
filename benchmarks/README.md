# Benchmarks

Sidekiq ships three benchmark scripts in its gem: `bin/sidekiqload`, `bin/multi_queue_bench` and
`bin/webload`. This directory ports each of them to Hench (the same parameters, the same jobs
and arguments, the same clocks, the same output lines) and runs both systems on one machine,
against the same Redis, so that their numbers compare. Next to the measurements are the figures
Sidekiq has published for each script, with their sources.


## Results

Measured on November 3, 2026, on an AWS EC2 **c7i.4xlarge**: an Intel Xeon Platinum 8488C with
16 vCPUs and 30 GB, Ubuntu 26.04.1 (Linux 7.0), **Redis 8.10.3**, **Sidekiq 8.1.7** on **Ruby
4.0.9 with YJIT**, and Hench built by rustc 1.99.1, with the CPU idle state C6 disabled
([why](#cpu-idle-states)). Hench's `sidekiqload` and `multi_queue_bench` figures are from Hench
as it is now, with `TCP_NODELAY` on for its Redis connections
([why](#why-hench-falls-behind-with-latency)); its `webload` figures, and all of Sidekiq's, are
from the session before, on the same instance, with Hench before that change. The figures are
medians of five runs (three for `multi_queue_bench`), and each ratio is to Sidekiq's median on
this machine.

Free Sidekiq takes each job off the queue, so a job running in a process that crashes is lost.
"Hench, basic fetch" works the same way and is the like-for-like column. "Hench, default" is Hench
as it comes, with reliable fetch, which keeps each job in Redis until it is done, so a crash
loses nothing: the guarantee of Sidekiq Pro's paid `super_fetch`, which is not measured here. It
costs an extra Redis round trip and command per job
([`bin/sidekiqload`](#binsidekiqload-one-process-500000-jobs) explains both fetches).

| Benchmark | Sidekiq 8.1.7 (free) | Hench, basic fetch | **Hench, default** (reliable fetch) |
|---|---:|---:|---:|
| **Running jobs survive a crash** | ✗ | ✗ | **✓** as in Sidekiq Pro |
| `sidekiqload`, 10 threads | 23,449 jobs/s | 113,051 jobs/s (4.82×) | 65,450 jobs/s (2.79×) |
| `sidekiqload`, 30 threads, 1 ms latency | 21,155 jobs/s | 20,765 jobs/s (0.98×) | 10,933 jobs/s (0.52×) |
| `sidekiqload`, 100 threads, 1 ms latency | 20,106 jobs/s | 47,381 jobs/s (2.36×) | 27,739 jobs/s (1.38×) |
| `multi_queue_bench`, 8 processes of 10 threads | 126,482 jobs/s | 163,510 jobs/s (1.29×) | 97,552 jobs/s (0.77×) |
| `webload`, `/busy` | 894 requests/s | | 1,508 requests/s (1.69×) |
| `webload`, `/queues` | 1,409 requests/s | | 2,777 requests/s (1.97×) |

**What reliable fetch costs.** Reliable fetch, which keeps every job in Redis until it is done, costs a
Redis round trip and a command more per job than Sidekiq's fetch. With a millisecond between the
process and Redis, at 30 threads, it reaches 0.52 of Sidekiq's throughput, while basic fetch, one
round trip per job like Sidekiq, keeps pace (0.98); at 100 threads, which give Sidekiq nothing
more, both lead (1.38 and 2.36) ([why](#why-hench-falls-behind-with-latency)). On
`multi_queue_bench`, where Redis is the bottleneck, reliable fetch trails (0.77) and basic fetch
leads (1.29)
([`bin/multi_queue_bench`](#binmulti_queue_bench-8-processes-8-queues-8000000-jobs)). Elsewhere
Hench leads: without latency a Hench process runs the jobs 2.8 times as fast as a Sidekiq
process (4.8 times with basic fetch), and the dashboard serves its pages 1.7 to 2.0 times as
fast.

Every run is recorded, in three sessions on the same instance:
[`results/2026-11-03-intel-xeon-platinum-8488c`](results/2026-11-03-intel-xeon-platinum-8488c)
(both systems, Hench before `TCP_NODELAY`),
[`results/2026-11-03-intel-xeon-platinum-8488c-nodelay`](results/2026-11-03-intel-xeon-platinum-8488c-nodelay)
(Hench with `TCP_NODELAY`: `sidekiqload` and `multi_queue_bench`) and, first of all,
[`results/2026-11-03-intel-xeon-platinum-8488c-c6-enabled`](results/2026-11-03-intel-xeon-platinum-8488c-c6-enabled),
with C6 enabled, which slowed both systems down and Sidekiq the most
([compared below](#cpu-idle-states)).


## `bin/sidekiqload`: one process, 500,000 jobs

The script pushes 500,000 no-op jobs into the queue `default` (500 bulk pushes of 1,000; the
arguments of each job are `["string", index, {"mike": "bob"}]`, and `retry: 1`), then starts one
process with `THREADS` worker slots and stops the clock when a monitor, which reads the queue's
length every 200 ms, finds it empty. With `LATENCY=1`, every reply of Redis is delayed by 1 ms
through [toxiproxy](https://github.com/Shopify/toxiproxy), standing for a network between the
process and Redis; Sidekiq's README calls this an IO-bound benchmark. Sidekiq's open-source
fetch pops jobs (`BRPOP`); Hench's default, reliable fetch, moves each job to the process's
in-progress list and removes it from there once the job is done, so that a crash loses
nothing; Hench's basic fetch is the like-for-like counterpart of Sidekiq's.

On the c7i.4xlarge ([results](#results); Hench with `TCP_NODELAY`):

| Configuration | Sidekiq 8.1.7 (free) | Hench, basic fetch | **Hench, default** (reliable fetch) |
|---|---:|---:|---:|
| `LATENCY=0`, 10 threads | 23,449 jobs/s | 113,051 jobs/s (4.82×) | 65,450 jobs/s (2.79×) |
| `LATENCY=1`, 30 threads | 21,155 jobs/s | 20,765 jobs/s (0.98×) | 10,933 jobs/s (0.52×) |
| `LATENCY=1`, 100 threads | 20,106 jobs/s | 47,381 jobs/s (2.36×) | 27,739 jobs/s (1.38×) |

Published by Sidekiq:

| Version | Configuration | Time | Throughput | Ruby | Source |
|---|---|---:|---:|---|---|
| Sidekiq 7.0.3 | `LATENCY=1`, 30 threads | 21.3 s | 23,500 jobs/s | 3.2.0 + YJIT | [README, Performance][sidekiq-readme] |
| Sidekiq 7.0.3 | `LATENCY=1`, 30 threads | 23.5 s | 21,300 jobs/s | 3.2.0 | [README, Performance][sidekiq-readme] |
| Sidekiq 8 | `LATENCY=0`, 10 threads | 20.3 s | 24,673 jobs/s | YJIT | [the script's header][sidekiqload] |

On the c7i.4xlarge, Sidekiq 8.1.7 ran close to these: 23,449 jobs/s without latency (its
header: 24,673) and 21,155 with it at 30 threads (its README, for 7.0.3 with YJIT: 23,500).

### Why Hench falls behind with latency

With a millisecond between the process and Redis, a worker slot's throughput is set by the Redis
round trips it waits for between two jobs. Counted by Redis itself (`INFO commandstats` over a
run of each), a job costs Sidekiq one command, `BRPOP`; Hench with basic fetch one too, its
fetch script (an `EVALSHA` running `SISMEMBER` and `RPOP`); and Hench with reliable fetch two,
its fetch script (`SISMEMBER`, `LMOVE` to the in-progress list, `SADD`) and then `LREM`, which
takes the finished job out of the in-progress list. The code says the same. A processor
([`processor.rs`](../crates/hench/src/processor.rs), `Processor::work`) fetches with
`FetchWorker::retrieve_work_until`, whose first step, `FetchWorker::attempt`
([`fetch.rs`](../crates/hench/src/fetch.rs)), is that one script call (the blocking `BLMOVE` or
`BRPOP` comes only once every queue is empty); it runs the job; `Processor::succeeded` then
acknowledges it through `Fetcher::acknowledge`, an `LREM` with reliable fetch and nothing with
basic fetch; and only then does the processor fetch again. The work state, the counters and the
metrics stay in memory until the heartbeat ([`launcher.rs`](../crates/hench/src/launcher.rs))
writes them, so they cost no round trip per job. Reliable fetch's second round trip is the
acknowledgement, which the processor waits for before it fetches the next job.

Before Hench turned `TCP_NODELAY` on, the throughput said more than one and two. With 30 threads, a round trip
through the proxy takes 1.42 ms (`redis-benchmark`, 30 connections), and Sidekiq spends 1.42 ms
per job and slot, one round trip; Hench spent 2.41 ms with basic fetch and 4.74 ms
with reliable fetch. The rest was Nagle's algorithm. Every non-blocking command of a Hench
process goes through one shared, multiplexed connection
([`redis.rs`](../crates/hench/src/redis.rs), `RedisPool::conn`;
`RedisConfig::shared_connections` is 1 by default), on which Hench did not set `TCP_NODELAY`,
and redis-rs 1.8 leaves it off. With Nagle's algorithm, the kernel holds a small write back while
earlier data on the socket is unacknowledged, and on a socket that 30 processors share there
nearly always is some: the acknowledgement comes back with the earlier command's reply, up to a
round trip later. Each of Sidekiq's threads has a connection of its own with one command in
flight, which Nagle's algorithm never holds back.

Two changes to Hench showed it, in single runs of 100,000 jobs with the latency, on the same
machine with C6 disabled (jobs/s):

| Hench with `LATENCY=1` | Basic fetch, 30 threads | Reliable fetch, 30 threads | Basic fetch, 100 threads | Reliable fetch, 100 threads |
|---|---:|---:|---:|---:|
| Before `TCP_NODELAY` | 12,318 | 6,311 | 35,064 | 18,224 |
| With `TCP_NODELAY` | 20,564 | 10,739 | 44,707 | 27,360 |
| With `TCP_NODELAY` and 30 shared connections (64 at 100 threads) | 18,945 | 9,673 | 61,439 | 29,019 |

With `TCP_NODELAY`, a slot of 30 spent 1.46 ms per job with basic fetch and 2.79 ms with
reliable fetch: one round trip and two. At 100 threads one connection also held Hench back: with
64, a slot again spent one round trip per job with basic fetch (1.63 ms, against 1.69 ms through
the proxy with 100 connections) and two with reliable fetch (3.45 ms).

Hench now turns `TCP_NODELAY` on (`TcpSettings::set_nodelay(true)` on the connection info that
`RedisConfig::connection_info` builds, and on the Sentinel connections). Measured again like the
rest, five runs of each (three of `multi_queue_bench`), Hench's medians went from before the
change to after it as follows:

| Configuration | Hench | Hench, basic fetch |
|---|---:|---:|
| `sidekiqload`, 10 threads | 65,456 → 65,450 jobs/s | 113,046 → 113,051 jobs/s |
| `sidekiqload`, 30 threads, 1 ms latency | 6,332 → 10,933 jobs/s (1.73×) | 12,444 → 20,765 jobs/s (1.67×) |
| `sidekiqload`, 100 threads, 1 ms latency | 18,677 → 27,739 jobs/s (1.49×) | 35,635 → 47,381 jobs/s (1.33×) |
| `multi_queue_bench` | 90,777 → 97,552 jobs/s (1.07×) | 151,851 → 163,510 jobs/s (1.08×) |

At 30 threads a slot now spends 1.44 ms per job with basic fetch, as Sidekiq's threads do
(1.42 ms), and 2.74 ms with reliable fetch: one round trip and two. Without latency nothing
moved. At 100 threads the one shared connection still holds Hench back (basic fetch: 47,381
jobs/s with one, 61,439 with 64 in the single runs above); the number of shared connections is
a setting, `RedisConfig::shared_connections`, which `henchload` leaves at its default of one.


## `bin/multi_queue_bench`: 8 processes, 8 queues, 8,000,000 jobs

Eight producer processes fill eight queues, `q0` to `q7`, with 1,000,000 jobs each (1,000 bulk
pushes of 1,000 jobs whose arguments are `[index]`); then eight worker processes, 10 threads
each (the script's documented example), drain them, process `i` working on `q<i>`. Process 0
also watches every queue and stops its clock, started when it started, once all of them are
empty. Eight processes on one Redis make Redis the bottleneck: this measures how much work each
job costs Redis.

On the c7i.4xlarge ([results](#results); Hench with `TCP_NODELAY`):

| Configuration | Sidekiq 8.1.7 (free) | Hench, basic fetch | **Hench, default** (reliable fetch) |
|---|---:|---:|---:|
| 8 processes of 10 threads | 126,482 jobs/s | 163,510 jobs/s (1.29×) | 97,552 jobs/s (0.77×) |

Redis runs at 100% of a core through every drain (sampled every half second during runs of
2,000,000 jobs of each system, Hench before `TCP_NODELAY`), so the throughput measures what a job costs
Redis: 7.9 µs for Sidekiq's `BRPOP`, 6.6 µs for basic fetch's script, and 11.0 µs for reliable
fetch's script and the `LREM` of its acknowledgement. With `TCP_NODELAY`, Hench
drained the queues 7% faster with reliable fetch and 8% faster with basic fetch; Redis's load
was not sampled in those runs.

Published by Sidekiq: Mike Perham ran the script with the defaults on Redis when it was
contributed: 8,000,000 jobs in 124.4 s, **64,299 jobs/s** (an M1 Pro, Ruby 3.3.0 with YJIT;
[sidekiq/sidekiq#6167][pr-6167]). On Dragonfly 1.13 in a container, with `ITERATIONS=100`,
800,000 jobs took 40 s, about 20,000 jobs/s.


## `bin/webload`: 10,000 requests to one dashboard page

The script renders one page of the dashboard 10,000 times in process (no HTTP server: Sidekiq's
goes through `Rack::Test`, Hench's through the axum router), after 100 warm-up requests, and
prints how long that took. Its page is `/busy`; `/queues` is the page Sidekiq's published figure
is for. Both dashboards read one Redis, seeded by `webload seed` with the same data (Hench keeps
Sidekiq's data model, so Sidekiq's dashboard reads it as its own): three queues of 100 jobs, 100
scheduled jobs, 100 retries and 100 dead jobs from jobs that failed, and a worker process with
five busy jobs.

On the c7i.4xlarge ([results](#results)):

| Page | Sidekiq 8.1.7 (free) | Hench |
|---|---:|---:|
| `/busy` | 894 requests/s | 1,508 requests/s (1.69×) |
| `/queues` | 1,409 requests/s | 2,777 requests/s (1.97×) |

Published by Sidekiq: 10,000 requests to `/queues` took **6 s** (about 1,670 requests/s) once
Sidekiq 7.3.8 cached its locale files, down from 107 s ([sidekiq/sidekiq#6555][pr-6555]).


## CPU idle states

The c7i.4xlarge lets its guest put idle CPUs into C6, which the kernel's `intel_idle` driver
lists as taking 190 µs to leave. Both systems wake threads all the time: Sidekiq's threads hand
Ruby's global lock to one another around every Redis call, and Hench's tokio workers and
connection tasks wake one another for every reply. Each wake-up that lands on a CPU sleeping in
C6 waits for it. The first session on this machine ran with C6 enabled, as the instance came;
the second with C6 disabled (`setup-ubuntu.sh` now disables every idle state that takes more
than 20 µs to leave), as did the third, which measured Hench again with `TCP_NODELAY`. Medians of the
first two sessions, both with Hench before `TCP_NODELAY`, with C6 enabled and then disabled:

| Benchmark | Sidekiq 8.1.7 (free) | Hench, basic fetch | **Hench, default** (reliable fetch) |
|---|---:|---:|---:|
| `sidekiqload`, 10 threads | 10,526 → 23,449 | 56,497 → 113,046 | 36,563 → 65,456 |
| `sidekiqload`, 30 threads, 1 ms latency | 4,507 → 21,155 | 9,609 → 12,444 | 4,863 → 6,332 |
| `sidekiqload`, 100 threads, 1 ms latency | 4,428 → 20,106 | 29,154 → 35,635 | 14,934 → 18,677 |
| `multi_queue_bench` | 133,588 → 126,482 | 153,688 → 151,851 | 91,682 → 90,777 |
| `webload`, `/busy` | 587 → 894 | | 260 → 1,508 |
| `webload`, `/queues` | 966 → 1,409 | | 458 → 2,777 |

With C6 enabled, Sidekiq lost more than half of its throughput without latency, nearly four
fifths with it (at 30 threads as at 100) and a third on its dashboard; Hench lost a fifth to a
half on `sidekiqload` and five sixths on its dashboard; `multi_queue_bench`, during which Redis
never idles, barely moved. The comparison moved with them: with C6 enabled, Hench came out ahead
of Sidekiq with 1 ms of latency at 30 threads (1.08 and 2.13 times its throughput) and behind it
on the dashboard (0.44 and 0.47), the reverse of the second session. The results above therefore
come from the sessions with C6 disabled, and `run.sh` now warns when such an idle state is
enabled.


## Method

- **Same machine, same Redis.** Every run gets a fresh `redis-server` of its own, without
  persistence, on a private port; nothing else uses it. The latency runs go through a toxiproxy
  proxy on the next port.
- **Sidekiq's own scripts.** `run.sh` installs Sidekiq 8.1.7 from the gem (pinned, with every
  dependency, by [`sidekiq/Gemfile.lock`](sidekiq/Gemfile.lock)) and runs the scripts the gem
  ships, unchanged, with YJIT. Two shims make them run here: `bin/sidekiqload` hard-codes Redis
  on ports 6379 and 6380, which [`sidekiq/redirect.rb`](sidekiq/redirect.rb), loaded before it,
  points at the private servers; and `bin/webload` stops at its first request, since Sidekiq
  8.1's dashboard needs a Rack session that the script does not set up, so
  [`sidekiq/webload.rb`](sidekiq/webload.rb) makes the same measurement, for any page, with an
  empty session hash (the cheapest session there is).
- **Hench's ports.** [`src/bin`](src/bin) holds `henchload`, `multi_queue_bench` and `webload`,
  which take the scripts' environment variables (`THREADS`, `LATENCY`, `QUEUES`, `PROCESSES`,
  `ITERATIONS`, `ELEMENTS`...), plus `FETCH=basic|reliable`. They log through `tracing` at
  `warn`, the level the scripts give Sidekiq's logger, so both systems still run their job
  loggers and filter the lines out.
- **Rounds.** Each configuration runs several times; the rounds alternate which system goes
  first, so that a machine warming up or cooling down favors neither. The tables give medians
  and the range of throughputs.
- **Clocks.** The load scripts stop their clock when the queue is empty, which happens when its
  last job is fetched, polled every 200 ms: the medians use that clock for both systems. Hench's
  runs also report when the last job ended ("Last job ended" in `summary.md`), which shows the
  error is small.
- **Idle CPUs.** On Linux, `environment.txt` lists the CPU idle states, and `run.sh` warns when
  one that takes more than 20 µs to leave is enabled: a thread woken on a CPU sleeping in it
  waits that long ([CPU idle states](#cpu-idle-states)).
- **Everything recorded.** [`results/`](results) keeps, for each session, the machine and every
  version (`environment.txt`), each run's output (`raw/`, colour codes removed), one line per run
  (`runs.csv`, with the load average before the run) and the medians (`summary.md`).

## Reproduce

On a fresh Ubuntu machine, [`setup-ubuntu.sh`](setup-ubuntu.sh) installs everything below (Rust,
Redis, toxiproxy, and Ruby with YJIT), disables the CPU idle states that take more than 20 µs to
leave (until the next boot), and prints the command to run.

```sh
# Everything: 41 minutes on the c7i.4xlarge with C6 disabled, 70 with it enabled. Needs
# redis-server, toxiproxy-server and curl, and for Sidekiq Ruby 3.2+ built with YJIT and its
# Bundler (RUBY names the interpreter).
RUBY=/opt/homebrew/opt/ruby/bin/ruby benchmarks/run.sh

# A few minutes: one round of each, with few jobs.
benchmarks/run.sh --quick

# One benchmark, one system.
benchmarks/run.sh --suite load --system hench --rounds 3
```

Or run one port by hand, as Sidekiq's scripts are run:

```sh
cargo build --release -p hench-benchmarks
LATENCY=1 THREADS=30 PORT=6379 target/release/henchload
THREADS=10 PROCESSES=8 QUEUES=8 target/release/multi_queue_bench
target/release/webload seed & PAGE=/queues target/release/webload
```

`henchload` empties database 13 and `multi_queue_bench` fills database 0 of the Redis it is
given, as Sidekiq's scripts do: point them at a Redis of their own.

[sidekiq-readme]: https://github.com/sidekiq/sidekiq/blob/v8.1.7/README.md#performance
[sidekiqload]: https://github.com/sidekiq/sidekiq/blob/v8.1.7/bin/sidekiqload
[pr-6167]: https://github.com/sidekiq/sidekiq/pull/6167
[pr-6555]: https://github.com/sidekiq/sidekiq/pull/6555
