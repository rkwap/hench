# Configuration

## The builder

`Hench::builder()` sets everything; the defaults are Sidekiq's.

| Method | Default | Meaning |
|---|---|---|
| `redis_url(url)`, `redis_config(cfg)`, `redis(pool)` | from the environment | where Redis is (see below) |
| `concurrency(n)` | 5 | jobs processed at once by the default capsule |
| `queues([..])` | `["default"]` | queues in priority order (`"name,weight"` for weights) |
| `weighted_queues([(name, weight)])` | | random order biased by weights |
| `capsule(name, \|c\| ..)` | | another group of workers with its own queues and concurrency |
| `timeout(d)` | 25 s | how long a shutdown waits for running jobs |
| `max_retries(n)` | 25 | retries of jobs with `retry: true` |
| `dead_max_jobs(n)`, `dead_timeout(d)` | 10,000, 180 days | dead-set limits |
| `fetch(FetchStrategy::Basic)` | reliable | see [delivery guarantees](reliability.md#delivery-guarantees-reliable-and-basic-fetch) |
| `data(value)`, `data_arc(arc)` | | shared state for `ctx.data::<T>()` |
| `register(job)`, `register_default::<J>()`, `register_dyn(..)`, `auto_register(bool)` | macro jobs registered | jobs this process runs |
| `server_middleware(m)`, `client_middleware(m)` | | middleware |
| `error_handler(f)`, `clear_error_handlers()`, `death_handler(f)` | a logging handler | failure callbacks |
| `on_startup`, `on_quiet`, `on_shutdown`, `on_exit`, `on_heartbeat`, `on_beat` | | lifecycle callbacks (a startup callback that panics stops the start) |
| `on_startup_try(\|\| async { .. })` | | a check that stops the start when it returns an error: `start`/`run` return `Error::Startup` before any job is taken |
| `logged_job_attributes([..])` | none | payload keys (say a `request_id` a client middleware adds) shown on every log line of a job, as `attrs={"request_id":"..."}` |
| `backtrace_cleaner(\|lines\| ..)` | none | filters the backtraces stored with failed jobs (they already end where hench runs the job) |
| `tag(t)`, `label(l)`, `labels([..])` | | shown in the dashboard and `hench processes` |
| `default_job_options(opts)` | queue `default`, retries on | options of every job, merged over the defaults |
| `max_iteration_runtime(d)` | none | how long an iterable job runs before taking turns |
| `metrics(bool)`, `job_logging(bool)`, `systemd(bool)` | on, on, on (with `run()`, when `NOTIFY_SOCKET` is set) | features |
| `poll_interval_average(d)` | processes × 5 s | how often the scheduler looks for due jobs |
| `heartbeat_interval(d)`, `fetch_timeout(d)` | 10 s (at most 20 s), 2 s | internals, mostly for tests |
| `orphan_check_interval(d)` | 60 s | how often the cluster looks for the in-progress jobs of processes that died |
| `orphan_grace(d)` | 30 s | how long a process without a heartbeat must stay without one before its in-progress jobs are recovered |
| `max_recoveries(n)` | 3 | recoveries of a job from processes that died while running it before it is taken for a poison pill and moved to the morgue; a job recovered before comes back alone, one per orphan check (see [delivery guarantees](reliability.md#delivery-guarantees-reliable-and-basic-fetch)) |
| `unique_jobs(bool)` | on | whether pushes honour uniqueness ([unique jobs](unique-jobs.md)) |
| `periodic(p)`, `periodic_group(g)`, `periodic_poller(bool)` | none, `default`, on | schedules this process registers, their group, whether it fires due ones ([periodic jobs](periodic-jobs.md)) |
| `periodic_poll_interval(d)`, `periodic_lease(d)`, `periodic_check_interval(d)`, `periodic_history(n)` | 5 s, 60 s, 60 s, 10 | the poller's timing and each schedule's history |
| `batch_ttl(d)`, `batch_linger(d)`, `batch_max_commit_jobs(n)`, `batch_commit_timeout(d)` | 30 days, 24 h, 10 000, 30 s | [batches](batches.md): expiry, the jobs of one commit, its timeout |
| `batch_maintenance(bool)`, `batch_maintenance_interval(d)`, `batch_dispatch_grace(d)` | on, 30 s, 10 s | batch housekeeping |
| `throttle(t)` | none | a throttle jobs name ([rate limiting](rate-limiting.md)) |
| `queue_limits(queue, limits)` | none | limits on every fetch of a queue, written to Redis at start ([queue limits](rate-limiting.md#queue-limits)) |
| `over_limit_reschedules(n)`, `over_limit_backoff(f)`, `over_limit_max_delay(d)`, `over_limit_when(f)` | 20, the refusal's slot, 24 h, none | jobs a limiter refuses while they run |

`build()` validates everything before connecting (empty queue names, duplicate jobs, zero
concurrency...) and returns `Error::Config` with an explanation.

## Redis

Without `redis_url`, workers read the URL from the variable named by `REDIS_PROVIDER`, else
`REDIS_URL`, else use `redis://127.0.0.1:6379`. URLs may be `redis://`, `rediss://` (feature
`tls`), `redis+unix://` or `unix://` (`?db=N&pass=...`). `RedisConfig` sets the timeouts
(3 s) and reconnection attempts, and `TlsConfig` a private CA or a client certificate for
mutual TLS. Passwords never appear in logs, nor in `RedisConfig`'s `Debug` output. Workers log
the URL they connect to and where it came from (`REDIS_URL`, the default...); an unreachable
server is tried again (six times, over about 7 s), while a wrong password or database fails at
once.

A server whose certificate cannot be verified, such as Heroku's Redis (self-signed, naming
another host), needs an explicit opt-out: `TlsConfig::new().danger_accept_invalid_certs(true)`
(Sidekiq's `verify_mode: VERIFY_NONE`) or a URL ending in `#insecure`
(`rediss://host:port/#insecure`, which works for the `hench` command too).
`danger_accept_invalid_hostnames(true)` still checks who signed the certificate. Either logs a
warning: anyone between the process and Redis can then pose as the server.

TLS goes through rustls with its `ring` crypto provider: when hench creates a `rediss://`
client and the process has no default rustls provider yet, it installs `ring` as the default,
so that rustls never has to pick between `ring` and another provider a dependency enabled
(such as aws-lc-rs, rustls's own default). To use another provider, install it first, e.g.
`rustls::crypto::aws_lc_rs::default_provider().install_default()` at the top of `main`.

With Redis Sentinel, give the sentinels and the name they monitor the primary under:
`redis+sentinel://:password@sentinel-1:26379,sentinel-2:26379/mymaster` (the password and an
optional `/db` after the name are the primary's; `?sentinel_password=...` is the sentinels').
Clients and workers ask the sentinels for the primary, and ask again after a failover (a
`READONLY` reply or a lost connection), so they follow it. TLS through Sentinel is not
supported yet (see the [roadmap](../ROADMAP.md)).

## Command line and configuration file

With the `cli` feature (`hench = { version = "0.1", features = ["cli"] }`, plus `clap = "4"`
for its `Parser` trait), `hench::cli::WorkerArgs` gives worker binaries Sidekiq's command line
(`-c`, `-q NAME[,WEIGHT]`, `-t`, `-C`, `-e`, `-g`, `-v`) and a TOML configuration file
(`-C path`, or `config/hench.toml` in the current directory when it exists). The command line
wins over the file, which wins over the builder:

```rust,no_run
use clap::Parser;
use hench::cli::WorkerArgs;
use hench::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = WorkerArgs::parse().settings()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(settings.log_directives()))
        .init();
    let builder = Hench::builder().queues(["critical", "default"]).concurrency(10);
    let hench = settings.apply(builder).build().await?;
    hench.run().await?;
    Ok(())
}
```

```toml
# config/hench.toml
concurrency = 10
queues = [["critical", 3], ["default", 2], "low,1"]
timeout = 25                   # seconds
tag = "billing"
labels = ["eu-west"]
max_retries = 25
fetch = "reliable"             # or "basic"
metrics = true
average_scheduled_poll_interval = 5 # seconds between scheduler polls, cluster-wide

[capsules.serial]              # a second capsule: one job at a time
concurrency = 1
queues = ["serial"]
# A [capsules.default] table would configure the capsule of the top-level keys (and win over
# them); -c and -q win over both.

[production]                   # used with `-e production` (or APP_ENV=production)
concurrency = 25
```

```sh
worker -c 20 -q critical,3 -q default,1 -e production -v
```

The whole file is checked when it is read, the sections of other environments included, and
`apply` logs which file and environment it uses. It warns when an environment chosen with `-e`
(or `APP_ENV`) selects nothing: no file was read (started from another directory, such as a
systemd unit without `WorkingDirectory=`, which starts in `/`) or the file has no such section.

## Queues and weights

- **Strict order** (`queues(["critical", "default", "low"])`): `critical` is always emptied
  first, then `default`, then `low`. A busy high-priority queue can starve the others.
- **Weighted** (`queues(["critical,3", "default,2", "low,1"])` or `weighted_queues`): each
  fetch draws a random order in which a queue comes first with probability
  `weight / total`; every queue gets its share.
- **Random**: all weights equal (`"a,1"`, `"b,1"`).
- **Capsules** separate concerns: `.capsule("serial", |c| c.concurrency(1).queues(["serial"]))`
  runs one `serial` job at a time next to the default capsule.
- **Pausing**: `api.queue("mailers").pause()` (or the dashboard) stops every process from
  fetching from the queue until it is resumed; jobs keep accumulating.

---

[← Defining and enqueuing jobs](jobs.md) · [Guide](README.md) · [Retries and delivery guarantees →](reliability.md)
