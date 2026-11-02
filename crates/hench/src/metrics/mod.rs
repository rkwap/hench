//! Job execution metrics: how many times each job class ran, failed and how long it took,
//! per minute, plus deploy marks to correlate changes with them.
//!
//! Workers track every execution in memory ([`Tracker`]) and add the numbers to Redis on
//! every heartbeat (turn this off with [`HenchBuilder::metrics`](crate::HenchBuilder::metrics)).
//! [`Query`] reads them back for a period ([`Period`]): every class with its totals and
//! per-bucket series ([`Query::top_jobs`]), or one class with its execution-time histograms
//! ([`Query::for_job`]). [`Deploy`] records deploy marks shown with the metrics.
//!
//! The data model (all times UTC):
//!
//! | Key | Type | Contents | Expiry |
//! |---|---|---|---|
//! | `j\|YYYYMMDD\|H:MM` ([`keys::metrics_minute`](crate::keys::metrics_minute)) | HASH | `<class>\|p` executions, `<class>\|f` failures, `<class>\|ms` milliseconds of the executions that did not fail, for one minute | 8 hours |
//! | `j\|YYYYMMDD\|H:M` ([`keys::metrics_ten_minutes`](crate::keys::metrics_ten_minutes)) | HASH | the same for ten minutes (`M` is the tens digit of the minute) | 3 days |
//! | `h\|<class>-<day>-<hour>:<minute>` ([`keys::histogram`](crate::keys::histogram)) | BITFIELD | one saturating `u16` per [`Histogram`] bucket: executions of the class in that minute by duration | 8 hours |
//! | `YYYYMMDD-marks` ([`keys::deploy_marks`](crate::keys::deploy_marks)) | HASH | deploy marks of the day: RFC 3339 minute → label | 90 days |
//! | `deploylock-<label>` ([`keys::deploy_lock`](crate::keys::deploy_lock)) | STRING | one mark per label per minute | 60 s |
//!
//! ```no_run
//! use hench::metrics::{Deploy, Period, Query, SortBy};
//!
//! # async fn demo(redis: hench::RedisPool) -> hench::Result<()> {
//! Deploy::new(&redis).mark("v1.2.3", std::time::SystemTime::now()).await?;
//!
//! let top = Query::new(&redis).top_jobs(Period::Hours(24), None).await?;
//! for (class, metrics) in top.sorted(SortBy::TotalTime) {
//!     println!("{class}: {:.1}s in total", metrics.totals.seconds());
//! }
//! let one = Query::new(&redis).for_job("ImportJob", Period::Minutes(60)).await?;
//! println!("{:?}", one.jobs["ImportJob"].histogram());
//! # Ok(()) }
//! ```

mod deploy;
mod histogram;
mod query;
mod tracker;

pub use deploy::{Deploy, DeployMark};
pub use histogram::{BUCKET_BOUNDS_MS, BUCKETS, Histogram};
pub use query::{
    Counts, Granularity, JobMetrics, MAX_HOURS, MAX_MINUTES, Period, Query, QueryResult, SortBy,
};
pub use tracker::{Outcome, Tracker};
