//! Redis key names and key-related constants.
//!
//! Every key hench reads or writes is produced by this module; no other code formats keys.
//! Keys are not namespaced (the same layout as Sidekiq 8): give each application its own
//! Redis database.
//!
//! | Key | Type | Contents |
//! |---|---|---|
//! | [`QUEUES`] | SET | names of known queues |
//! | [`queue`]`(name)` | LIST | job JSON, `LPUSH` to enqueue, popped from the right |
//! | [`SCHEDULE`], [`RETRY`], [`DEAD`] | ZSET | job JSON scored by epoch seconds |
//! | [`PROCESSES`] | SET | identities of live processes |
//! | [`process`]`(identity)` | HASH | heartbeat data of one process |
//! | [`work`]`(identity)` | HASH | jobs currently running in that process |
//! | [`signals`]`(identity)` | LIST | remote signals (`TSTP`, `TERM`) for that process |
//! | [`inprogress`]`(identity)` | LIST | jobs held by that process (reliable fetch) |
//! | [`INPROGRESS`] | SET | identities owning an in-progress list |
//! | [`RECOVERY_SUSPECTS`], [`RELEASED_SUSPECT`] | ZSET, HASH | recovered jobs of a crash loop waiting to go back alone, and the last one released |
//! | [`STAT_PROCESSED`], [`STAT_FAILED`] and daily variants | STRING | counters |
//! | [`flush_guard`]`(id)` | STRING | number of the last batch of counts a process applied |
//! | [`metrics_minute`]`(at)`, [`metrics_ten_minutes`]`(at)` | HASH | per-class execution metrics |
//! | [`histogram`]`(class, at)` | STRING (BITFIELD) | per-class execution-time histogram |
//! | [`deploy_marks`]`(date)`, [`deploy_lock`]`(label)` | HASH, STRING | deploy marks |
//! | [`iteration_state`]`(jid)` | HASH | cursor and progress of an iterable job |
//! | [`recoveries`]`(jid)` | STRING | times a job was recovered from processes that died while they held it |
//! | [`recovery_claim`]`(jid)` | HASH | the process settling a poison pill (orphan checks that overlap) |
//! | [`unique_lock`]`(digest)`, [`UNIQUE_LOCKS`] | HASH, ZSET | unique-job locks and their index |
//! | [`PERIODIC`], [`PERIODIC_NAMES`], [`periodic`]`(name)`, [`periodic_history`]`(name)`, [`periodic_group`]`(group)`, [`PERIODIC_CRONTABS`] | ZSET, ZSET, HASH, LIST, SET, HASH | periodic schedules |
//! | [`batch`]`(bid)` and its sets, [`BATCHES`], [`BATCH_EVENTS`], [`BATCH_MAINTENANCE`] | HASH, SET, ZSET, ZSET, STRING | batches |
//! | [`limit_state`]`(kind, id)`, [`limit_stats`]`(kind, id)`, [`LIMITS`], [`QUEUE_LIMITS`] (reserved) | ZSET/LIST/HASH/STRING, HASH, ZSET, HASH | limiters |
//! | [`unique_minute`]`(at)`, [`batch_minute`]`(at)`, [`limit_minute`]`(at)` | HASH | per-minute feature counters |
//!
//! The keys of the features follow one policy ([pro-features.md] §2.1): every
//! per-entity key expires after a time derived from what it means, set by the script that may
//! create it and never shortened; the global indexes (`unique-locks`, `batches`, `limits`) have
//! no expiry but are scored by the expiry of what they index and trimmed by score;
//! configuration (periodic schedules, `queue-limits`) never expires; the per-minute counters
//! expire after [`FEATURE_MINUTE_TTL`].
//!
//! [pro-features.md]: https://github.com/rkwap/hench/blob/v0.1.0/docs/design/pro-features.md

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use std::time::Duration;

/// SET of all known queue names.
pub const QUEUES: &str = "queues";

/// Prefix of queue list keys.
pub const QUEUE_PREFIX: &str = "queue:";

/// Sorted set of scheduled jobs (score: run-at epoch seconds).
pub const SCHEDULE: &str = "schedule";

/// Sorted set of jobs waiting to be retried (score: retry-at epoch seconds).
pub const RETRY: &str = "retry";

/// Sorted set of dead jobs, the morgue (score: death time in epoch seconds).
pub const DEAD: &str = "dead";

/// SET of live process identities.
pub const PROCESSES: &str = "processes";

/// SET of process identities that own an in-progress list (reliable fetch).
pub const INPROGRESS: &str = "inprogress";

/// Lifetime processed-jobs counter.
pub const STAT_PROCESSED: &str = "stat:processed";

/// Lifetime failed-jobs counter.
pub const STAT_FAILED: &str = "stat:failed";

/// Lock (SET NX with expiry) held while pruning dead identities from [`PROCESSES`].
pub const PROCESS_CLEANUP: &str = "process_cleanup";

/// Lock (SET NX with expiry) held while recovering orphaned in-progress jobs.
pub const INPROGRESS_RECOVERY: &str = "inprogress_recovery";

/// Sorted set of the jobs recovered from dead processes that were in flight at an earlier crash
/// too (suspects of a crash loop), waiting to go back to their queues one per orphan check; the
/// lowest score goes first (the most recovered, then the first parked).
pub const RECOVERY_SUSPECTS: &str = "recovery-suspects";

/// Hash of the suspect last released from [`RECOVERY_SUSPECTS`] (`job` its JSON, `queue`, `at`
/// the release in epoch milliseconds): the next one waits until it is no longer waiting in its
/// queue or running, so that two suspects never run at the same time.
pub const RELEASED_SUSPECT: &str = "recovery-suspects:released";

/// SET of paused queue names (queues a fetcher should skip).
pub const PAUSED: &str = "paused";

/// Remote signal that quiets a process (stop fetching new jobs).
pub const SIGNAL_QUIET: &str = "TSTP";

/// Remote signal that stops a process.
pub const SIGNAL_STOP: &str = "TERM";

/// Remote signal that makes a process log the jobs it is running (Sidekiq's `TTIN`, which
/// dumps its threads' backtraces).
pub const SIGNAL_DUMP: &str = "TTIN";

/// Expiry of the per-process heartbeat hash and work hash: a process whose keys are gone is
/// considered dead.
pub const PROCESS_TTL: Duration = Duration::from_secs(60);

/// Expiry of the daily `stat:*:YYYY-MM-DD` counters (five years).
pub const STATS_TTL: Duration = Duration::from_secs(5 * 365 * 24 * 60 * 60);

/// Expiry of the per-minute metrics hashes ([`metrics_minute`]).
pub const METRICS_MINUTE_TTL: Duration = Duration::from_secs(8 * 60 * 60);

/// Expiry of the ten-minute metrics hashes ([`metrics_ten_minutes`]).
pub const METRICS_TEN_MINUTES_TTL: Duration = Duration::from_secs(3 * 24 * 60 * 60);

/// Expiry of the execution-time histogram keys ([`histogram`]).
pub const HISTOGRAM_TTL: Duration = Duration::from_secs(8 * 60 * 60);

/// Expiry of the deploy marks hashes ([`deploy_marks`]).
pub const DEPLOY_MARKS_TTL: Duration = Duration::from_secs(90 * 24 * 60 * 60);

/// Expiry of a deploy lock ([`deploy_lock`]): one mark per label per minute.
pub const DEPLOY_LOCK_TTL: Duration = Duration::from_secs(60);

/// Expiry of an iterable job's state hash ([`iteration_state`]), refreshed on every write:
/// long enough to outlive the retries of a job that keeps failing (thirty days).
pub const ITERATION_STATE_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Expiry of a [`flush_guard`] key, refreshed by every batch it guards (one day).
pub const FLUSH_GUARD_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Metric field names in the [`metrics_minute`] and [`metrics_ten_minutes`] hashes: executions,
/// failures and total milliseconds of successful executions.
pub const METRIC_PROCESSED: &str = "p";

/// See [`METRIC_PROCESSED`].
pub const METRIC_FAILED: &str = "f";

/// See [`METRIC_PROCESSED`].
pub const METRIC_MS: &str = "ms";

/// Key of the job list for queue `name`: `queue:<name>`.
pub fn queue(name: &str) -> String {
    format!("{QUEUE_PREFIX}{name}")
}

/// Queue name of a queue list key (`queue:critical` → `critical`). Keys without the
/// prefix are returned unchanged.
pub fn queue_name(key: &str) -> &str {
    key.strip_prefix(QUEUE_PREFIX).unwrap_or(key)
}

/// Builds a process identity, `"{hostname}:{pid}:{nonce}"`.
pub fn identity(hostname: &str, pid: u32, nonce: &str) -> String {
    format!("{hostname}:{pid}:{nonce}")
}

/// Key of the heartbeat hash of a process. This is the identity itself.
pub fn process(identity: &str) -> String {
    identity.to_owned()
}

/// Key of the hash describing the jobs a process is currently running: `<identity>:work`.
pub fn work(identity: &str) -> String {
    format!("{identity}:work")
}

/// Key of the list of remote signals for a process: `<identity>-signals`.
pub fn signals(identity: &str) -> String {
    format!("{identity}-signals")
}

/// Key of the list of jobs held by a process (reliable fetch): `<identity>:inprogress`.
pub fn inprogress(identity: &str) -> String {
    format!("{identity}:inprogress")
}

/// Guard of the counts one process writes on its heartbeats (its `stat:*` counters, or its job
/// metrics): the number of the last batch of counts applied, `flush:<id>` with a random `id`.
/// A batch whose reply was lost is sent again with the same number, and applied only if that
/// number is above this one, so that it is counted once. Expires [`FLUSH_GUARD_TTL`] after the
/// last batch.
pub fn flush_guard(id: &str) -> String {
    format!("flush:{id}")
}

/// Daily counter key for `stat` (`processed` or `failed`): `stat:<stat>:YYYY-MM-DD`.
pub fn stat_on(stat: &str, date: NaiveDate) -> String {
    format!("stat:{stat}:{}", date.format("%Y-%m-%d"))
}

/// Daily processed counter: `stat:processed:YYYY-MM-DD` (UTC date).
pub fn stat_processed_on(date: NaiveDate) -> String {
    stat_on("processed", date)
}

/// Daily failed counter: `stat:failed:YYYY-MM-DD` (UTC date).
pub fn stat_failed_on(date: NaiveDate) -> String {
    stat_on("failed", date)
}

/// Per-minute metrics hash, `j|YYYYMMDD|H:MM` (hour not padded, minute zero-padded).
/// Fields are `<class>|p`, `<class>|f` and `<class>|ms`.
pub fn metrics_minute(at: DateTime<Utc>) -> String {
    format!(
        "j|{:04}{:02}{:02}|{}:{:02}",
        at.year(),
        at.month(),
        at.day(),
        at.hour(),
        at.minute()
    )
}

/// Ten-minute metrics hash, `j|YYYYMMDD|H:M` where `M` is the tens digit of the minute.
pub fn metrics_ten_minutes(at: DateTime<Utc>) -> String {
    format!(
        "j|{:04}{:02}{:02}|{}:{}",
        at.year(),
        at.month(),
        at.day(),
        at.hour(),
        at.minute() / 10
    )
}

/// Field of a metrics hash: `<class>|<metric>` (`metric` is one of [`METRIC_PROCESSED`],
/// [`METRIC_FAILED`], [`METRIC_MS`]).
pub fn metric_field(class: &str, metric: &str) -> String {
    format!("{class}|{metric}")
}

/// Splits a metrics hash field into `(class, metric)`, at the last `|` (class names may
/// contain `|`). `None` for a field without one.
pub fn parse_metric_field(field: &str) -> Option<(&str, &str)> {
    field.rsplit_once('|')
}

/// Execution-time histogram (BITFIELD of u16 counters) for one job class and one minute:
/// `h|<class>-<day>-<hour>:<minute>` (none of the numbers padded).
pub fn histogram(class: &str, at: DateTime<Utc>) -> String {
    format!("h|{class}-{}-{}:{}", at.day(), at.hour(), at.minute())
}

/// Hash of deploy marks for one UTC day: `YYYYMMDD-marks`.
pub fn deploy_marks(date: NaiveDate) -> String {
    format!("{}-marks", date.format("%Y%m%d"))
}

/// Lock preventing duplicate deploy marks for the same label: `deploylock-<label>`.
pub fn deploy_lock(label: &str) -> String {
    format!("deploylock-{label}")
}

/// Hash holding the state of an iterable job: `it-<jid>`. Its fields are
/// [`ITERATION_EXECUTIONS`], [`ITERATION_CURSOR`], [`ITERATION_RUNTIME`],
/// [`ITERATION_CANCELLED`] and [`ITERATION_DONE`].
pub fn iteration_state(jid: &str) -> String {
    format!("it-{jid}")
}

/// Counter of the recoveries of the job `jid` from processes that died while they held it
/// (reliable fetch's orphan check; `recovered-<jid>`), expiring
/// [`RECOVERY_WINDOW`](crate::fetch::RECOVERY_WINDOW) after the last one. Past
/// [`max_recoveries`](crate::Config::max_recoveries), the job is a poison pill. An iterable job
/// that saves progress deletes it (it is not what kills its processes).
pub fn recoveries(jid: &str) -> String {
    format!("recovered-{jid}")
}

/// Claim on the settlement of the poison pill `jid` (`recovered-<jid>:claim`, HASH: `of` the dead
/// process whose list holds it, `by` the process settling it), so that two orphan checks that
/// overlap never both run its handlers; it expires
/// [`RECOVERY_CLAIM_TTL`](crate::fetch::RECOVERY_CLAIM_TTL) after it was taken.
pub fn recovery_claim(jid: &str) -> String {
    format!("recovered-{jid}:claim")
}

/// Field of [`iteration_state`]: the number of executions so far.
pub const ITERATION_EXECUTIONS: &str = "ex";

/// Field of [`iteration_state`]: the cursor of the last item processed, as JSON.
pub const ITERATION_CURSOR: &str = "c";

/// Field of [`iteration_state`]: the total run time, in seconds (`f64`).
pub const ITERATION_RUNTIME: &str = "rt";

/// Field of [`iteration_state`]: when the job was cancelled, in epoch seconds.
pub const ITERATION_CANCELLED: &str = "cancelled";

/// Field of [`iteration_state`], written once the iteration is over, before its finishing hooks
/// run: how it ended (`completed`, `aborted` or `cancelled`). An execution that finds it only
/// runs those hooks (an earlier one failed, or its process stopped before the job was
/// acknowledged).
pub const ITERATION_DONE: &str = "done";

/// Expiry of the per-minute counters of the features ([`unique_minute`], [`batch_minute`],
/// [`limit_minute`]): the metrics' minute expiry.
pub const FEATURE_MINUTE_TTL: Duration = METRICS_MINUTE_TTL;

/// Metric field: executions that went over a limiter's limit and were rescheduled (counted as
/// processed, not failed, not timed).
pub const METRIC_OVER_LIMIT: &str = "o";

/// Metric field: jobs a throttle parked before they started (not processed).
pub const METRIC_THROTTLED: &str = "t";

/// Metric field: batch members skipped because their batch was cancelled (counted as processed,
/// not timed).
pub const METRIC_SKIPPED: &str = "s";

/// A per-minute counter hash of a feature: `<letter>|YYYYMMDD|H:MM` (the metrics' format).
fn feature_minute(letter: char, at: DateTime<Utc>) -> String {
    format!(
        "{letter}|{:04}{:02}{:02}|{}:{:02}",
        at.year(),
        at.month(),
        at.day(),
        at.hour(),
        at.minute()
    )
}

/// Prefix of the unique-job locks.
pub const UNIQUE_LOCK_PREFIX: &str = "unique:";

/// Hash holding the lock of unique jobs with this digest: `unique:<digest>` (fields `jid`,
/// `class`, `queue`, `until`, `for`, `at`), with an expiry that is never shortened.
pub fn unique_lock(digest: &str) -> String {
    format!("{UNIQUE_LOCK_PREFIX}{digest}")
}

/// Sorted set of the digests of every lock, scored by the lock's expiry (epoch seconds).
pub const UNIQUE_LOCKS: &str = "unique-locks";

/// Per-minute counters of unique jobs: `u|YYYYMMDD|H:MM`, fields `<class>|d` (duplicates),
/// `|r` (replaced), `|z` (lapses).
pub fn unique_minute(at: DateTime<Utc>) -> String {
    feature_minute('u', at)
}

/// Sorted set of the active (not paused) periodic schedules, scored by their next tick or the
/// end of their lease (epoch seconds).
pub const PERIODIC: &str = "periodic";

/// Sorted set of every periodic schedule name (score 0), for listing and counting.
pub const PERIODIC_NAMES: &str = "periodic-names";

/// Prefix of the periodic schedule hashes. Its length is part of the periodic scripts, which
/// cut it from key names: see the test pinning it.
pub const PERIODIC_PREFIX: &str = "periodic:";

/// Hash of one periodic schedule (definition and state): `periodic:<name>`.
pub fn periodic(name: &str) -> String {
    format!("{PERIODIC_PREFIX}{name}")
}

/// List of the latest enqueues of a periodic schedule: `periodic-history:<name>`.
pub fn periodic_history(name: &str) -> String {
    format!("periodic-history:{name}")
}

/// Set of the names of the code schedules a group registered: `periodic-group:<group>`.
pub fn periodic_group(group: &str) -> String {
    format!("periodic-group:{group}")
}

/// Hash of the registered crontabs, by group.
pub const PERIODIC_CRONTABS: &str = "periodic-crontabs";

/// Hash of a batch: `batch:<bid>`. Its sets ([`batch_pending`], [`batch_failed`],
/// [`batch_dead`], [`batch_kids`], [`batch_commits`]) share its expiry.
pub fn batch(bid: &str) -> String {
    format!("batch:{bid}")
}

/// Set of the jids of a batch's members not yet succeeded or skipped: `batch:<bid>:pending`.
pub fn batch_pending(bid: &str) -> String {
    format!("batch:{bid}:pending")
}

/// Hash of the failure summaries of a batch's failed members: `batch:<bid>:failed`.
pub fn batch_failed(bid: &str) -> String {
    format!("batch:{bid}:failed")
}

/// Set of the jids of a batch's dead members: `batch:<bid>:dead`.
pub fn batch_dead(bid: &str) -> String {
    format!("batch:{bid}:dead")
}

/// Set of the bids of a batch's children: `batch:<bid>:kids`.
pub fn batch_kids(bid: &str) -> String {
    format!("batch:{bid}:kids")
}

/// Hash of the commit ids applied with a batch as their root, each with the duplicates that
/// commit left out (so that a resend reports them again): `batch:<bid>:commits`.
pub fn batch_commits(bid: &str) -> String {
    format!("batch:{bid}:commits")
}

/// Sorted set of every batch, scored by its expiry (epoch seconds).
pub const BATCHES: &str = "batches";

/// Sorted set of the batch events waiting to be dispatched (the outbox).
pub const BATCH_EVENTS: &str = "batch-events";

/// Lock (SET NX with expiry) held while a process runs batch maintenance.
pub const BATCH_MAINTENANCE: &str = "batch-maintenance";

/// Per-minute counters of batches: `b|YYYYMMDD|H:MM`.
pub fn batch_minute(at: DateTime<Utc>) -> String {
    feature_minute('b', at)
}

/// State of a limit identity: `limit:<kind>:<id>`, `kind` being the state letter (`c`
/// concurrency, `w` window, `b` bucket, `g` leaky and points).
pub fn limit_state(kind: char, id: &str) -> String {
    format!("limit:{kind}:{id}")
}

/// Counters and signature of a limit identity: `limit-stats:<kind>:<id>`.
pub fn limit_stats(kind: char, id: &str) -> String {
    format!("limit-stats:{kind}:{id}")
}

/// Sorted set of the limit identities with counters, scored by their expiry (epoch seconds).
pub const LIMITS: &str = "limits";

/// Per-minute counters of limiters: `l|YYYYMMDD|H:MM`, fields `<name>|g` (granted) and `|r`
/// (refused).
pub fn limit_minute(at: DateTime<Utc>) -> String {
    feature_minute('l', at)
}

/// Reserved for queue limits (a hash: queue name → specs, configuration), which come in a later
/// version: this one neither reads nor writes it (the worker file's `[queue_limits]` table is
/// refused).
pub const QUEUE_LIMITS: &str = "queue-limits";

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn feature_keys() {
        let at = Utc.with_ymd_and_hms(2026, 11, 1, 9, 5, 59).unwrap();
        assert_eq!(unique_lock("rP3m"), "unique:rP3m");
        assert_eq!(unique_minute(at), "u|20261101|9:05");
        assert_eq!(batch_minute(at), "b|20261101|9:05");
        assert_eq!(limit_minute(at), "l|20261101|9:05");
        assert_eq!(periodic("nightly"), "periodic:nightly");
        assert_eq!(periodic_history("nightly"), "periodic-history:nightly");
        assert_eq!(periodic_group("billing"), "periodic-group:billing");
        let bid = "0123456789abcdef01234567";
        assert_eq!(batch(bid), format!("batch:{bid}"));
        assert_eq!(batch_pending(bid), format!("batch:{bid}:pending"));
        assert_eq!(batch_failed(bid), format!("batch:{bid}:failed"));
        assert_eq!(batch_dead(bid), format!("batch:{bid}:dead"));
        assert_eq!(batch_kids(bid), format!("batch:{bid}:kids"));
        assert_eq!(batch_commits(bid), format!("batch:{bid}:commits"));
        assert_eq!(limit_state('w', "stripe:42"), "limit:w:stripe:42");
        assert_eq!(limit_stats('c', "erp"), "limit-stats:c:erp");
        assert_eq!(
            [
                UNIQUE_LOCKS,
                PERIODIC,
                PERIODIC_NAMES,
                PERIODIC_CRONTABS,
                BATCHES,
                BATCH_EVENTS,
                BATCH_MAINTENANCE,
                LIMITS,
                QUEUE_LIMITS
            ],
            [
                "unique-locks",
                "periodic",
                "periodic-names",
                "periodic-crontabs",
                "batches",
                "batch-events",
                "batch-maintenance",
                "limits",
                "queue-limits"
            ]
        );
        assert_eq!(FEATURE_MINUTE_TTL, METRICS_MINUTE_TTL);
        assert_eq!(FEATURE_MINUTE_TTL.as_secs(), 8 * 3_600);
        assert_eq!(
            [METRIC_OVER_LIMIT, METRIC_THROTTLED, METRIC_SKIPPED],
            ["o", "t", "s"]
        );
    }

    #[test]
    fn the_periodic_prefix_has_nine_bytes() {
        // The periodic scripts cut the prefix from key names with `string.sub(key, 10)`.
        assert_eq!(PERIODIC_PREFIX.len(), 9);
        assert_eq!(&periodic("a:b")[PERIODIC_PREFIX.len()..], "a:b");
    }

    #[test]
    fn feature_prefixes_are_disjoint() {
        // No free-text name can produce another kind's key: every kind has its own prefix,
        // and none is a prefix of an existing hench key.
        let at = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let prefixes = [
            "unique:",
            "unique-locks",
            "periodic:",
            "periodic-history:",
            "periodic-group:",
            "batch:",
            "batch-",
            "limit:",
            "limit-stats:",
            "u|",
            "b|",
            "l|",
        ];
        for existing in [
            queue("x"),
            SCHEDULE.to_owned(),
            RETRY.to_owned(),
            DEAD.to_owned(),
            PROCESSES.to_owned(),
            INPROGRESS.to_owned(),
            STAT_PROCESSED.to_owned(),
            flush_guard("x"),
            PAUSED.to_owned(),
            iteration_state("x"),
            metrics_minute(at),
            histogram("x", at),
            deploy_lock("x"),
        ] {
            for prefix in prefixes {
                assert!(!existing.starts_with(prefix), "{existing} / {prefix}");
            }
        }
    }

    #[test]
    fn queue_keys() {
        assert_eq!(queue("default"), "queue:default");
        assert_eq!(queue_name("queue:critical"), "critical");
        assert_eq!(queue_name("queue:a:b"), "a:b");
        assert_eq!(queue_name("other"), "other");
    }

    #[test]
    fn process_keys() {
        let id = identity("host", 42, "0123456789ab");
        assert_eq!(id, "host:42:0123456789ab");
        assert_eq!(process(&id), id);
        assert_eq!(work(&id), "host:42:0123456789ab:work");
        assert_eq!(signals(&id), "host:42:0123456789ab-signals");
        assert_eq!(inprogress(&id), "host:42:0123456789ab:inprogress");
    }

    #[test]
    fn stat_keys() {
        let date = NaiveDate::from_ymd_opt(2025, 2, 3).unwrap();
        assert_eq!(stat_processed_on(date), "stat:processed:2025-02-03");
        assert_eq!(stat_failed_on(date), "stat:failed:2025-02-03");
        assert_eq!(stat_on("processed", date), "stat:processed:2025-02-03");
    }

    #[test]
    fn metrics_keys() {
        let at = Utc.with_ymd_and_hms(2025, 2, 14, 8, 43, 12).unwrap();
        assert_eq!(metrics_minute(at), "j|20250214|8:43");
        assert_eq!(metrics_ten_minutes(at), "j|20250214|8:4");
        assert_eq!(histogram("App::Job", at), "h|App::Job-14-8:43");
        let early = Utc.with_ymd_and_hms(2025, 1, 5, 0, 7, 0).unwrap();
        assert_eq!(metrics_minute(early), "j|20250105|0:07");
        assert_eq!(metrics_ten_minutes(early), "j|20250105|0:0");
        assert_eq!(histogram("J", early), "h|J-5-0:7");
        assert_eq!(metric_field("App::Job", METRIC_PROCESSED), "App::Job|p");
        assert_eq!(parse_metric_field("App::Job|ms"), Some(("App::Job", "ms")));
        assert_eq!(parse_metric_field("a|b|f"), Some(("a|b", "f")));
        assert_eq!(parse_metric_field("nofield"), None);
        assert_eq!(DEPLOY_LOCK_TTL.as_secs(), 60);
        assert_eq!(ITERATION_STATE_TTL.as_secs(), 30 * 86_400);
    }

    #[test]
    fn misc_keys() {
        let date = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        assert_eq!(deploy_marks(date), "20241231-marks");
        assert_eq!(deploy_lock("v1.2"), "deploylock-v1.2");
        assert_eq!(iteration_state("abc"), "it-abc");
        assert_eq!(recoveries("abc"), "recovered-abc");
        assert_eq!(recovery_claim("abc"), "recovered-abc:claim");
        assert_eq!(
            [
                ITERATION_EXECUTIONS,
                ITERATION_CURSOR,
                ITERATION_RUNTIME,
                ITERATION_CANCELLED,
                ITERATION_DONE
            ],
            ["ex", "c", "rt", "cancelled", "done"]
        );
        assert_eq!(
            [QUEUES, SCHEDULE, RETRY, DEAD, PROCESSES, INPROGRESS],
            [
                "queues",
                "schedule",
                "retry",
                "dead",
                "processes",
                "inprogress"
            ]
        );
        assert_eq!(RECOVERY_SUSPECTS, "recovery-suspects");
        assert_eq!(RELEASED_SUSPECT, "recovery-suspects:released");
        assert_eq!(PROCESS_TTL.as_secs(), 60);
    }
}
