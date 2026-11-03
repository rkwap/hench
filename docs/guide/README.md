# The Hench guide

How to define, run and operate background jobs with Hench, from a first job to production.
The [API documentation](https://docs.rs/hench) describes every type and option, and
[`DESIGN.md`](../DESIGN.md) is the contract behind them: the Redis data model, the semantics,
and every difference from Sidekiq.

Every Rust example of the guide is a complete program, compiled (and run where it needs no
Redis) by the test suite, with the dependencies [Getting started](getting-started.md) lists.

## Basics

| Page | What it covers |
|---|---|
| [Getting started](getting-started.md) | Dependencies, a first job, pushes, a worker process, workers next to an axum application |
| [Defining and enqueuing jobs](jobs.md) | `#[hench::job]` and its options, implementing `Job` by hand, every way to push |
| [Configuration](configuration.md) | The builder and its defaults, Redis (TLS, Sentinel), the worker command line and TOML file, queue order and capsules |
| [Retries and delivery guarantees](reliability.md) | Backoff, the dead set, scheduled jobs, reliable and basic fetch, poison pills, shutdowns |
| [Middleware](middleware.md) | Client and server middleware chains |
| [Testing](testing.md) | Fake and inline clients, `perform_inline`, a private Redis per test |

## Features

| Page | What it covers |
|---|---|
| [Unique jobs](unique-jobs.md) | One waiting or running copy per job and arguments, debouncing, seeing and releasing locks |
| [Periodic jobs](periodic-jobs.md) | Cron expressions and intervals in time zones, one push per tick across processes, missed ticks |
| [Batches and workflows](batches.md) | Callbacks when a group of jobs completes or succeeds, nesting, cancellation, staged workflows |
| [Rate limiting and throttling](rate-limiting.md) | Limiters inside jobs and throttles on job starts, shared by every process |
| [Iterable jobs](iterable-jobs.md) | Long jobs that save their cursor and resume after a stop, a failure or a deploy |
| [Metrics](metrics.md) | Execution counts, times and histograms per job class, deploy marks |
| [Using the features together](combining-features.md) | The order a worker applies them in, recipes, rollout, Redis CPU and memory per feature |

## Operations

| Page | What it covers |
|---|---|
| [The dashboard](dashboard.md) | Mounting `hench-web` in axum, its pages, extensions and protections |
| [The `hench` command](cli.md) | The standalone dashboard, the JSON API and the maintenance commands |
| [Deployment](deployment.md) | Redis settings, signals, panics, systemd, logging, health and capacity |
| [Differences from Sidekiq](sidekiq.md) | What carries over from Sidekiq, and what Hench does differently |

See also: [benchmarks](../../benchmarks/README.md) against Sidekiq, the [roadmap](../ROADMAP.md)
and the [changelog](../../CHANGELOG.md).
