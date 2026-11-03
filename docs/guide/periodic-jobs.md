# Periodic jobs

A **periodic job** (a *schedule*) enqueues a job at each **tick** of a timetable: a cron
expression in a time zone, or a fixed interval. Every worker process runs a small poller and no
process is the leader: a schedule's tick is enqueued **at most once**, whatever crashes, lost
replies or concurrent pollers do (the push and the schedule's advance are one compare-and-set
script), and **at least once** while some worker runs (a poller that dies mid-tick leaves a lease
that ends after a minute). Whether a tick is due is decided with Redis's clock, so clock skew
between hosts cannot fire a tick early or twice.

```rust
use std::time::{Duration, UNIX_EPOCH};

use hench::prelude::*;

#[hench::job(queue = "reports", retry = 3)]
async fn nightly_report(ctx: &JobContext) {
    // The tick the job is for (not when it runs): retries keep it.
    if let Some(tick) = ctx.periodic() {
        println!("report of the day before {:?}", tick.tick);
    }
}

#[tokio::main]
async fn main() -> hench::Result<()> {
    let nightly = Periodic::cron("nightly-report", "0 2 * * *")
        .tz("Europe/Paris")
        .job::<nightly_report>(())
        .description("The PDF report of the previous day");
    nightly.validate()?;
    assert_eq!(nightly.describe(), "At 02:00 (Europe/Paris)");
    // The next ticks after 2026-11-01 00:00 UTC: 02:00 in Paris is 01:00 UTC in winter.
    let after = UNIX_EPOCH + Duration::from_secs(1_793_491_200);
    let ticks = nightly.upcoming(after, 2)?;
    let at = |secs| UNIX_EPOCH + Duration::from_secs(secs);
    assert_eq!(ticks, [at(1_793_494_800), at(1_793_581_200)]);

    // Testing clients have no poller: a test pushes a tick's job the way the poller would.
    let client = Client::fake();
    nightly.enqueue(&client, ticks[0]).await?;
    let job = &client.jobs::<nightly_report>()[0];
    assert_eq!(job.periodic.as_ref().map(|meta| meta.name.as_str()), Some("nightly-report"));
    client.drain::<nightly_report>().await?;
    Ok(())
}
```

Schedules defined in code are registered by the workers when they start; schedules created at
runtime (one per tenant, say) go through the API (`Api::periodic().upsert`) or `hench periodic
add` (the dashboard lists every schedule and pauses, resumes, enqueues and deletes them, but
does not create or edit them yet: see the [roadmap](../ROADMAP.md)). Both kinds live in Redis and
fire the same way:

```rust,no_run
use std::time::Duration;

use hench::api::Api;
use hench::prelude::*;

#[hench::job(queue = "rollups")]
async fn rollup(hours: u32) {
    println!("rolling up the last {hours} hours");
}

#[hench::job(queue = "tenants")]
async fn sync_tenant(tenant: String) {
    println!("syncing {tenant}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        .redis_url("redis://127.0.0.1:6379")
        .periodic(
            // Five minutes past every hour; after downtime, each missed hour, at most 24.
            Periodic::cron("hourly-rollup", "5 * * * *")
                .job::<rollup>((1,))
                .catch_up(CatchUp::All { max: 24 }),
        )
        .periodic(
            // A job of another service, by class name, every 30 seconds; never two waiting or
            // running at once (a tick whose previous job holds the lock is skipped).
            Periodic::every("refresh-rates", Duration::from_secs(30))
                .class("rates::Refresh", vec![])
                .queue("rates")
                .retry(false)
                .unique_for(Duration::from_secs(60))
                .catch_up(CatchUp::Skip),
        )
        .build()
        .await?;

    // Dynamic schedules.
    let schedules = Api::from(&hench).periodic();
    let tenant = Periodic::cron("tenant-acme-sync", "*/15 * * * *")
        .job::<sync_tenant>(("acme".to_owned(),));
    schedules.upsert(&tenant).await?; // created, updated, rescheduled or unchanged
    schedules.pause("tenant-acme-sync").await?; // its ticks are not enqueued nor missed
    schedules.unpause("tenant-acme-sync").await?; // from its next tick on: no backfill
    let jid = schedules.enqueue("tenant-acme-sync").await?; // once, now; same timetable
    println!("enqueued {jid:?}");
    // The code schedules appear once a worker registered them (when it starts).
    for entry in schedules.page(1, 100).await?.items {
        println!("{}: next {:?}, last {:?}", entry.name, entry.next, entry.last);
    }
    hench.run().await?;
    Ok(())
}
```

- **Timetables**: cron expressions of 5 fields, or 6 with seconds first; `@hourly`, `@daily`
  (`@midnight`), `@weekly`, `@monthly`, `@yearly` (`@annually`); lists, ranges, steps, names,
  `L` (last), `W` (nearest weekday), `#` (nth weekday). When both the day of the month and the
  day of the week are restricted, either matches (as in Vixie cron). `Periodic::every(name,
  interval)` ticks at whole multiples of the interval since the Unix epoch, shifted by
  `.offset(..)`: `every(1 h).offset(5 min)` ticks at five past every hour. `hench periodic next
  "0 2 * * *" --tz Europe/Paris` previews an expression in English with its next ticks.
- **Time zones and daylight saving time**: IANA zone names (`.tz("America/New_York")`; UTC by
  default) from the system's database, with the copy bundled with hench for a zone the system's
  lacks (older tzdata). Like Vixie cron, a tick at a fixed time — seconds, minutes and hours
  fields that do not start with `*`: `30 2 * * *`, but also lists and ranges such as
  `30 1,13 * * *` or `15 1-3 * * *` — that falls in the hour skipped in spring runs right after
  the change, and one in the hour repeated in autumn runs once; fields starting with `*`
  (`*/15 * * * *`, `0 * * * *`) follow real time. Use UTC for jobs that must run every 24 hours
  exactly. Give every host the same tzdata (and hench version): a process that computes another
  instant for a tick than the one stored treats the stored one as standing for its nearest tick
  (within three hours), so each period still runs once, and warns. A worker whose time-zone
  database lacks a schedule's zone leaves it to the workers that have it.
- **Missed ticks** (no worker running, Redis unreachable, everything paused for a deploy):
  `CatchUp::Latest` (the default) enqueues one job for the most recent missed tick;
  `CatchUp::Skip` enqueues a tick only within its deadline (5 minutes unless `.deadline(..)`
  says otherwise), so a missed tick never runs late; `CatchUp::All { max }` enqueues every missed
  tick, oldest first, at most `max`. `.deadline(..)` also bounds the other two. The number of
  ticks missed is counted on the schedule.
- **Slow jobs**: ticks keep coming while a job runs; to skip a tick while the previous job waits
  or runs, give the schedule `unique_for` about one period ([unique jobs](unique-jobs.md)): the tick is then
  consumed as a `duplicate` and shows so in the history.
- **What a job knows**: `ctx.periodic()` gives the schedule's name, the tick and whether it was
  enqueued by hand; the job's payload carries them (`periodic: {"name", "tick", "manual"}`),
  through retries and recoveries. Client middleware runs for every tick's push, and may change,
  delay or stop it (a stopped push consumes the tick).
- **Deploys and groups**: when a worker starts, it registers its schedules in one script:
  new ones are created, changed ones updated (a changed timetable reschedules the next tick;
  other changes keep it), and those its code no longer defines are deleted; pauses, history and
  counters survive. Processes of a deploy in progress may disagree for a moment: the last one
  started wins, and the dashboard says so (it compares the crontabs the processes declare, names
  included). Applications sharing a Redis database keep their schedules apart with
  `.periodic_group("billing")` (a process registers and prunes only its group). A name a
  dynamic schedule already uses is a conflict, logged and left alone. Workers check their
  group's registration every minute and register again after Redis lost its data, or went back
  to older data (a restore from a backup, a failover to a replica that missed the
  registration).
- **Pollers**: every worker polls unless `.periodic_poller(false)`; idle, a poller costs one
  small script every 5 seconds at most (`.periodic_poll_interval(..)`), and each tick three round
  trips. `.periodic_lease(..)` (60 s) is how long a dying poller holds a tick back;
  `.periodic_history(..)` (10) how many handled ticks each schedule remembers.
- **Seeing and changing schedules**: `Api::periodic()` (`page`, `get`, `history`, `upsert`,
  `remove`, `pause`, `unpause`, `enqueue`, `summary`, `repair`), the dashboard's Periodic tab
  (each schedule's timetable, next run in its zone, last enqueue and last run, with banners when
  no worker polls, a schedule is overdue or a group's workers disagree; history; enqueue now,
  pause, resume, delete), `hench periodic [list|show|enqueue|pause|unpause|remove|add|repair|
  next]`, `hench stats periodic` and the JSON API (`GET <prefix>/api/v1/periodic`, `GET
  <prefix>/api/v1/periodic/{name}`, `POST <prefix>/api/v1/periodic/{name}/{enqueue|pause|
  unpause}`; `hench web --api`). Each schedule shows the outcome of its
  latest run (succeeded, failed with the error, interrupted, rescheduled) and its counters.
- **Configuration file**: `[[periodic]]` tables in the worker's TOML file (name, `cron` and
  `tz` or `every` and `offset`, `class`, `args`, `queue`, `retry`, `tags`, `catch_up` and
  `catch_up_max`, `deadline`, `paused`, `description`, `unique_for`, `unique_until`, `throttle`,
  `throttle_key`), next to the `periodic_*` settings. When the worker registers the job type the
  table names by `class`, the schedule takes that job type's options (queue, retries,
  uniqueness, throttle) and its `unique_key`/`throttle_key` of the table's `args`, under the
  table's own keys, as `Periodic::job` does (its `on_conflict` aside: a schedule's ticks drop
  duplicates); `build` refuses arguments that do not fit the job type. A class the worker does
  not register (another service's job) takes the table's keys alone: set its `queue`.
- **Testing**: `Periodic::validate`, `upcoming`, `describe` and `payload(tick)` check a
  definition; `enqueue(&client, tick)` pushes a tick's job through a fake or inline client (no
  poller runs in testing modes). Integration tests that cross hours, days or DST drive the
  workers' timetables with a `hench::periodic::ManualClock` (feature `test-util`) given to
  `HenchBuilder::data` and `PeriodicSet::with_clock`.
- **Upgrades**: schedules are stored with a format version; a worker that finds a newer one
  leaves it to the workers that read it, and the dashboard shows it as unreadable. A crontab's
  digest includes its schedules' names since this version: while the upgrade rolls out, the
  dashboard may show a group's old and new workers as disagreeing.

---

[← Unique jobs](unique-jobs.md) · [Guide](README.md) · [Batches and workflows →](batches.md)
