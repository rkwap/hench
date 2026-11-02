//! `soak worker`: one hench worker OS process (`Hench::run`, so real SIGTERM/SIGINT/SIGTSTP
//! handling), running the soak jobs and recording every execution in its own append-only
//! log (outside Redis).
//!
//! Records (one JSON object per line, `t` in epoch ms, `w` the worker instance, `p` its pid):
//! * `boot` / `quiet` / `shutdown` / `exit` lifecycle events;
//! * `s` (an execution starts: nonce `n`, jid `j`, class `c`, queue `q`, attempt `a` =
//!   retry_count + 1, or 0 on the first run);
//!   With `--pro`, also the tick `pt` of a periodic job, the batch `b` of a member and the
//!   batch and event `cb` of a callback;
//! * `e` (it ended: outcome `o` = ok | err | int (iterable interrupted) | shutdown | ol (a
//!   limiter's refusal: rescheduled) | skip (a member of a cancelled batch), error class `x`),
//!   or `x` (the execution future was dropped or unwound without ending: hard shutdown or
//!   panic);
//! * `pp` (a planned panic is about to happen), `i` (an iterable item `k` was processed),
//!   `lg` (a pro member was granted its window limiter's slot), `death` (death handler), `err`
//!   (error handler, other than ordinary job failures).

use std::sync::Arc;
use std::time::Duration;

use hench::iterable::{Enumerator, IterableJob, Iteration};
use hench::prelude::*;
use hench::{FetchStrategy, RedisConfig, ServerNext};
use serde_json::{Value, json};

use crate::jsonlog::{JsonLog, now_ms};

/// The nonce of a soak job: its third argument (both job types).
fn nonce_of(args: &[Value]) -> u64 {
    args.get(2).and_then(Value::as_u64).unwrap_or(0)
}

/// The workers' `max_recoveries`: a job recovered from processes killed while they held it
/// more often than this is a poison pill (the morgue, `hench::CrashLoop`); the checker
/// verifies that such a job was really in flight in that many killed processes.
pub const MAX_RECOVERIES: u32 = 3;

#[derive(Clone)]
pub struct Recorder {
    pub log: Arc<JsonLog>,
    pub wid: Arc<str>,
    pub pid: u32,
}

impl Recorder {
    pub fn write(&self, mut value: Value) {
        if let Some(object) = value.as_object_mut() {
            object.insert("w".into(), Value::from(&*self.wid));
            object.insert("p".into(), Value::from(self.pid));
            object.insert("t".into(), Value::from(now_ms()));
        }
        self.log.write(&value);
    }
}

/// The soak's ordinary job. Args: (plan, param, nonce).
///
/// Plans: `ok`/`slow`/`vslow` (sleep `param` ms, succeed), `coop` (sleep `param` ms unless the
/// process stops: then fail with `hench::Shutdown`, which requeues it), `errk`/`panick` (fail
/// or panic while attempt < `param`, then succeed), `aerr`/`apanic` (always fail/panic: dead
/// set after its retries), `noretry` (fail, pushed with retry false), `deadfalse` (always fail,
/// pushed with dead false).
pub struct SoakJob {
    rec: Recorder,
}

impl Job for SoakJob {
    const NAME: &'static str = "SoakJob";
    type Args = (String, u64, u64);

    async fn perform(
        &self,
        ctx: &JobContext,
        (plan, param, nonce): (String, u64, u64),
    ) -> Result<(), JobError> {
        let attempt = u64::from(ctx.retry_count().map_or(0, |count| count + 1));
        let fail = |what: &str| {
            Err(JobError::new(
                "soak::PlannedFailure",
                format!("{what} (attempt {attempt})"),
            ))
        };
        match plan.as_str() {
            "ok" | "slow" | "vslow" => {
                tokio::time::sleep(Duration::from_millis(param)).await;
                Ok(())
            }
            "coop" => {
                let sleep = tokio::time::sleep(Duration::from_millis(param));
                tokio::select! {
                    () = sleep => Ok(()),
                    () = ctx.stopping() => Err(JobError::new(JobError::SHUTDOWN, "stopping early")),
                }
            }
            "errk" => {
                tokio::time::sleep(Duration::from_millis(5)).await;
                if attempt < param {
                    fail("planned failure")
                } else {
                    Ok(())
                }
            }
            "panick" => {
                tokio::time::sleep(Duration::from_millis(5)).await;
                if attempt < param {
                    planned_panic(&self.rec, nonce, attempt)
                }
                Ok(())
            }
            "aerr" | "noretry" | "deadfalse" => {
                tokio::time::sleep(Duration::from_millis(5)).await;
                fail("planned permanent failure")
            }
            "apanic" => {
                tokio::time::sleep(Duration::from_millis(5)).await;
                planned_panic(&self.rec, nonce, attempt)
            }
            other => Err(JobError::new("soak::UnknownPlan", other.to_owned())),
        }
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        // Keep retries within the soak's time scale (hench adds its jitter on top).
        RetryIn::After(Duration::from_secs(1))
    }
}

/// Records the planned panic, then panics.
fn planned_panic(rec: &Recorder, nonce: u64, attempt: u64) -> ! {
    rec.write(json!({"e": "pp", "n": nonce, "a": attempt}));
    panic!("planned panic (attempt {attempt})");
}

/// An iterable job: `items` items of `ms` milliseconds each. Args: (items, ms, nonce).
pub struct SoakIter {
    rec: Recorder,
}

impl IterableJob for SoakIter {
    const NAME: &'static str = "SoakIter";
    type Args = (u64, u64, u64);
    type Item = u64;
    type Cursor = u64;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        args: &'a (u64, u64, u64),
        cursor: Option<u64>,
    ) -> Result<Enumerator<'a, u64, u64>, JobError> {
        Ok(Enumerator::range(0..args.0, cursor))
    }

    async fn each_iteration(&self, item: u64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        let (_, ms, nonce) = *it.args();
        tokio::time::sleep(Duration::from_millis(ms)).await;
        self.rec
            .write(json!({"e": "i", "n": nonce, "k": item, "x": it.executions()}));
        Ok(())
    }

    fn checkpoint_interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn retry_in(&self, _count: u32, _error: &JobError, _job: &JobPayload) -> RetryIn {
        RetryIn::After(Duration::from_secs(1))
    }
}

/// Server middleware recording each execution's start and end.
struct ExecLog {
    rec: Recorder,
}

struct EndGuard {
    rec: Recorder,
    nonce: u64,
    jid: String,
    attempt: u64,
    done: bool,
}

impl Drop for EndGuard {
    fn drop(&mut self) {
        if !self.done {
            let how = if std::thread::panicking() {
                "panic"
            } else {
                "drop"
            };
            self.rec.write(json!({
                "e": "x", "n": self.nonce, "j": self.jid, "a": self.attempt, "o": how,
            }));
        }
    }
}

#[async_trait]
impl ServerMiddleware for ExecLog {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        let nonce = nonce_of(&job.args);
        let attempt = u64::from(job.retry_count.map_or(0, |count| count + 1));
        let mut start = json!({
            "e": "s", "n": nonce, "j": job.jid, "c": job.class, "q": ctx.queue(), "a": attempt,
            "eq": job.enqueued_at,
        });
        // The features' fields (the `pro` scenario): the tick, the batch, the callback.
        if let Some(meta) = &job.periodic {
            start["pt"] = json!(meta.tick);
        }
        if let Some(bid) = &job.bid {
            start["b"] = json!(bid);
        }
        if let Some(callback) = &job.batch_callback {
            start["cb"] = json!([callback.bid(), callback.event().to_string()]);
        }
        self.rec.write(start);
        let mut guard = EndGuard {
            rec: self.rec.clone(),
            nonce,
            jid: job.jid.clone(),
            attempt,
            done: false,
        };
        let result = next.run(ctx, job).await;
        guard.done = true;
        let (outcome, class) = match &result {
            Ok(()) => ("ok", String::new()),
            Err(error) if error.is_interrupted() => ("int", error.class().to_owned()),
            // A limiter's refusal (rescheduled) and a member of a cancelled batch (skipped):
            // neither is a failure.
            Err(error) if error.is_over_limit() => ("ol", error.class().to_owned()),
            Err(error) if error.is_batch_cancelled() => ("skip", error.class().to_owned()),
            Err(error) if error.class() == JobError::SHUTDOWN => {
                ("shutdown", error.class().to_owned())
            }
            Err(error) => ("err", error.class().to_owned()),
        };
        self.rec.write(json!({
            "e": "e", "n": nonce, "j": job.jid, "a": attempt, "o": outcome, "x": class,
        }));
        result
    }
}

pub struct WorkerOptions {
    pub wid: String,
    pub url: String,
    pub log: std::path::PathBuf,
    pub concurrency: usize,
    pub fetch: String,
    pub timeout: u64,
    pub orphan_check: u64,
    pub heartbeat: u64,
    /// The pro scenario's jobs, throttle and schedule (`pro.rs`).
    pub pro: bool,
}

pub async fn run(options: WorkerOptions) -> anyhow::Result<()> {
    // Planned panics are expected by the thousands: keep them out of the worker's log.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let planned = info
            .payload()
            .downcast_ref::<String>()
            .is_some_and(|message| message.starts_with("planned panic"));
        if !planned {
            default_hook(info);
        }
    }));

    let rec = Recorder {
        log: Arc::new(JsonLog::open(&options.log)?),
        wid: Arc::from(options.wid.as_str()),
        pid: std::process::id(),
    };
    rec.write(json!({"e": "boot", "fetch": options.fetch, "url": options.url}));

    let fetch = if options.fetch == "basic" {
        FetchStrategy::Basic
    } else {
        FetchStrategy::Reliable
    };
    let (death, error, quiet, shutdown, exit) = (
        rec.clone(),
        rec.clone(),
        rec.clone(),
        rec.clone(),
        rec.clone(),
    );
    let mut builder = Hench::builder()
        .redis_config(RedisConfig::new(options.url.clone()))
        .auto_register(false)
        .register(SoakJob { rec: rec.clone() })
        .register(SoakIter { rec: rec.clone() })
        .server_middleware(ExecLog { rec: rec.clone() })
        .concurrency(options.concurrency)
        .queues(["critical,3", "default,2", "low,1"])
        .timeout(Duration::from_secs(options.timeout))
        .fetch(fetch)
        .orphan_check_interval(Duration::from_secs(options.orphan_check))
        .max_recoveries(MAX_RECOVERIES)
        .heartbeat_interval(Duration::from_secs(options.heartbeat))
        .max_iteration_runtime(Duration::from_secs(4))
        .job_logging(false)
        .tag(options.wid.clone())
        .death_handler(move |job, err| {
            death.write(json!({
                "e": "death", "n": nonce_of(&job.args), "j": job.jid, "c": job.class,
                "x": err.class(), "rc": job.retry_count, "dead": job.dead,
            }));
        })
        .error_handler(move |err, ctx| {
            if ctx.context != "job failed" {
                let mut message = err.message();
                message.truncate(300);
                error.write(json!({
                    "e": "err", "ctx": ctx.context, "x": err.class(), "m": message,
                    "n": ctx.job.map(|job| nonce_of(&job.args)),
                }));
            }
        })
        .on_quiet(move || {
            let quiet = quiet.clone();
            async move { quiet.write(json!({"e": "quiet"})) }
        })
        .on_shutdown(move || {
            let shutdown = shutdown.clone();
            async move { shutdown.write(json!({"e": "shutdown"})) }
        })
        .on_exit(move || {
            let exit = exit.clone();
            async move { exit.write(json!({"e": "exit"})) }
        });
    if options.pro {
        builder = crate::pro::configure(builder, &rec);
    }
    let hench = builder.build().await?;
    let result = hench.run().await;
    rec.write(json!({"e": "returned", "ok": result.is_ok()}));
    result?;
    Ok(())
}
