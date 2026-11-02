//! Simple workflows on top of batches (`docs/design/batches.md` §2.17): stages of parallel
//! jobs, each starting when the previous one succeeded, and the internal job that starts the
//! next stage (`hench::WorkflowNext`, a reserved name).
//!
//! A workflow is one overall batch `W` holding the user's callbacks and the templates of stages
//! 2 to N (its field `wf`), with one child batch per stage. Stage `k`'s batch has an `on_success`
//! callback `hench::WorkflowNext(W, k + 1)`, which, being a child's callback, is a member of
//! `W`: `W` cannot succeed before it has run. It reopens `W` with the commit id `wf-<k + 1>`
//! (a re-run adds nothing) and defines the next stage's batch. Once the last stage succeeded,
//! `W` has nothing pending and every child succeeded: it fires `success`.
//!
//! Until its last stage is defined, `W` is held open (its field `open`): between two stages it
//! has no member of its own and its defined stages may all have completed (a member failed once
//! and waits for its retry), which must not fire `W`'s `complete` before the later stages ran.
//! The commit of the last stage, or a cancel, releases the hold.
//!
//! `hench::WorkflowNext` goes to the workflow's `callback_queue`, else to the queue of the first
//! job of the stage that just succeeded: a queue some worker polls, since that job ran.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::batch::collect::CHILD_WEIGHT;
use crate::batch::define::{BuildJob, capture, typed_push};
use crate::batch::model::HOLD;
use crate::batch::{Batch, BatchError, BatchEvent, Committed, WORKFLOW_NEXT};
use crate::client::{Client, ClientMode};
use crate::error::{Error, JobError, Result};
use crate::job::{ErasedJob, Job, JobContext, JobSetter};
use crate::keys;
use crate::payload::JobPayload;

/// Most stages of a workflow.
const MAX_STAGES: usize = 100;

/// Most jobs of one stage (a larger stage holds a loader job that adds to its stage batch).
const MAX_STAGE_JOBS: usize = 1_000;

/// Largest size of the stored stages (the `wf` field), in bytes.
const MAX_STAGES_BYTES: usize = 1024 * 1024;

/// The queue of `hench::WorkflowNext` for a stage without jobs (not built by [`Workflow`]).
const DEFAULT_QUEUE: &str = "default";

/// Stages of parallel jobs, each starting when the previous stage succeeded: chains,
/// fan-out/fan-in and Sidekiq Pro's "complex workflows" without hand-written callbacks.
///
/// ```no_run
/// use hench::batch::Workflow;
/// use hench::prelude::*;
///
/// # #[hench::job] async fn reserve_stock(order: i64) -> Result<(), JobError> { Ok(()) }
/// # #[hench::job] async fn charge_card(order: i64) -> Result<(), JobError> { Ok(()) }
/// # #[hench::job] async fn print_label(order: i64) -> Result<(), JobError> { Ok(()) }
/// # #[hench::job] async fn ship(order: i64) -> Result<(), JobError> { Ok(()) }
/// # #[hench::job] async fn order_shipped(order: i64) -> Result<(), JobError> { Ok(()) }
/// # async fn example(client: &Client) -> Result<(), hench::Error> {
/// let order = 42;
/// let started = Workflow::new(client)
///     .description(format!("Fulfil order {order}"))
///     .stage(|s| s.job::<reserve_stock>((order,)))
///     .stage(|s| s.job::<charge_card>((order,)).job::<print_label>((order,)))
///     .stage(|s| s.job::<ship>((order,)))
///     .on_success::<order_shipped>((order,))
///     .start()
///     .await?;
/// tracing::info!(bid = %started.bid, "order workflow started");
/// # Ok(()) }
/// ```
///
/// A member that dies makes its stage batch die, and the workflow with it (`on_death`); once
/// the member is revived and succeeds, or an operator skips it, the workflow continues.
/// Cancelling the workflow's batch skips the jobs not run yet and starts no further stage.
///
/// Between two stages, the internal job `hench::WorkflowNext` (known to every worker, nothing to
/// register) starts the next stage. It runs in the workflow's
/// [`callback_queue`](Workflow::callback_queue) when one is set, else in the queue of the first
/// job of the stage that just succeeded.
///
/// A job that starts a workflow and may run twice starts it once with
/// [`Workflow::with_bid`] and [`commit_id`](Workflow::commit_id) (`ctx.jid()` for both).
#[must_use = "a workflow starts only with `start`"]
pub struct Workflow {
    batch: Batch,
    stages: Vec<Stage>,
    callback_queue: Option<String>,
}

/// One stage of a [`Workflow`]: jobs that run in parallel.
#[must_use]
#[derive(Default)]
pub struct Stage {
    description: Option<String>,
    jobs: Vec<Result<BuildJob>>,
}

/// The stages of a workflow kept in its batch (`wf`), for `hench::WorkflowNext`.
#[derive(Debug, Serialize, Deserialize)]
struct Stored {
    /// The number of stages.
    n: usize,
    /// The queue of `hench::WorkflowNext` (the `callback_queue`; `None`: the queue of the first
    /// job of the stage that succeeded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue: Option<String>,
    /// Stages 2 to N.
    stages: Vec<StoredStage>,
}

/// One stage kept in `wf`.
#[derive(Debug, Serialize, Deserialize)]
struct StoredStage {
    /// The description of its batch.
    desc: String,
    /// Its jobs, normalized, without jid (each gets one when its stage starts).
    jobs: Vec<JobPayload>,
}

impl Workflow {
    /// A workflow pushing to `client`'s Redis (or testing store); on a collecting client it is
    /// a child of that definition's batch.
    pub fn new(client: &Client) -> Self {
        Self {
            batch: Batch::new(client),
            stages: Vec::new(),
            callback_queue: None,
        }
    }

    /// A workflow whose overall batch has a chosen bid (see [`Batch::with_bid`]), for an
    /// idempotent start from a job that may run twice: with the job's jid as both the bid and
    /// the [`commit_id`](Workflow::commit_id), a re-run's [`start`](Workflow::start) writes
    /// nothing and reports [`Committed::already_applied`]. Fails with
    /// [`BatchError::InvalidBid`] if `bid` is not 24 lowercase hexadecimal characters.
    ///
    /// ```no_run
    /// use hench::batch::Workflow;
    /// use hench::prelude::*;
    ///
    /// # #[hench::job] async fn capture_payment(order: i64) -> Result<(), JobError> { Ok(()) }
    /// # #[hench::job] async fn send_parcel(order: i64) -> Result<(), JobError> { Ok(()) }
    /// #[hench::job]
    /// async fn fulfil_order(ctx: &JobContext, order: i64) -> Result<(), JobError> {
    ///     Workflow::with_bid(ctx.client(), ctx.jid())?
    ///         .commit_id(ctx.jid())
    ///         .stage(|s| s.job::<capture_payment>((order,)))
    ///         .stage(|s| s.job::<send_parcel>((order,)))
    ///         .start()
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn with_bid(client: &Client, bid: impl Into<String>) -> Result<Self> {
        Ok(Self {
            batch: Batch::with_bid(client, bid)?,
            stages: Vec::new(),
            callback_queue: None,
        })
    }

    /// The id of the workflow's overall batch.
    pub fn bid(&self) -> &str {
        self.batch.bid()
    }

    /// Text shown in the dashboard.
    pub fn description(mut self, text: impl Into<String>) -> Self {
        self.batch = self.batch.description(text);
        self
    }

    /// Adds a stage, built by `build` (`|s| s.job::<J>(args)`).
    pub fn stage(mut self, build: impl FnOnce(Stage) -> Stage) -> Self {
        self.stages.push(build(Stage::default()));
        self
    }

    /// Callback job `J` when every job of every stage ran at least once (see
    /// [`Batch::on_complete`]): never before the last stage started. A dead job stops the
    /// workflow at its stage, so `complete` waits until it is revived and succeeds or is skipped
    /// (`on_death` fires meanwhile). A cancelled workflow completes once the jobs it had
    /// started ran (or were skipped).
    pub fn on_complete<J: Job>(mut self, args: J::Args) -> Self {
        self.batch = self.batch.on_complete::<J>(args);
        self
    }

    /// Callback job `J` when the last stage succeeded.
    pub fn on_success<J: Job>(mut self, args: J::Args) -> Self {
        self.batch = self.batch.on_success::<J>(args);
        self
    }

    /// Callback job `J` when a job of a stage dies.
    pub fn on_death<J: Job>(mut self, args: J::Args) -> Self {
        self.batch = self.batch.on_death::<J>(args);
        self
    }

    /// Callback job `J` with per-callback options (see [`Batch::on`]).
    pub fn on<J: Job>(mut self, event: BatchEvent, setter: JobSetter<J>, args: J::Args) -> Self {
        self.batch = self.batch.on(event, setter, args);
        self
    }

    /// Queue of the workflow's callbacks and of its internal `hench::WorkflowNext` jobs, which
    /// start each next stage. Default: each callback job's own queue, and for
    /// `hench::WorkflowNext` the queue of the first job of the stage that just succeeded (a
    /// queue workers poll, since that job ran).
    pub fn callback_queue(mut self, queue: impl Into<String>) -> Self {
        let queue = queue.into();
        self.callback_queue = Some(queue.clone());
        self.batch = self.batch.callback_queue(queue);
        self
    }

    /// Expiry of the unfinished workflow after its last activity (see [`Batch::expires_in`]).
    pub fn expires_in(mut self, ttl: Duration) -> Self {
        self.batch = self.batch.expires_in(ttl);
        self
    }

    /// How long the data stays once the workflow finished (see [`Batch::linger`]).
    pub fn linger(mut self, linger: Duration) -> Self {
        self.batch = self.batch.linger(linger);
        self
    }

    /// Idempotency key of the workflow's start (see [`Batch::commit_id`]). With a bid chosen by
    /// [`Workflow::with_bid`], a second start with the same commit id writes nothing and
    /// reports [`Committed::already_applied`]; with [`Workflow::new`] the bid is new each time,
    /// so a commit id cannot recognize an earlier start.
    pub fn commit_id(mut self, id: impl Into<String>) -> Self {
        self.batch = self.batch.commit_id(id);
        self
    }

    /// Writes the workflow and starts its first stage, in one commit: the overall batch, the
    /// first stage's batch and its jobs. Checks: 1 to 100 stages, 1 to 1 000 jobs per stage, each
    /// stage small enough for the commit that starts it (its jobs plus its batch, which counts as
    /// 10, at most `batch_max_commit_jobs`: [`BatchError::StageTooLarge`]; the later stages start
    /// from workers, which should have the same setting), at most 1 MiB of stored stages, no
    /// scheduled job (`at`/`delay`). After
    /// [`BatchError::Unconfirmed`], starting it again with the same
    /// [`Workflow::with_bid`] bid and [`commit_id`](Workflow::commit_id) settles it.
    pub async fn start(self) -> Result<Committed> {
        let Self {
            batch,
            stages,
            callback_queue,
        } = self;
        if !(1..=MAX_STAGES).contains(&stages.len()) {
            return Err(Error::InvalidJob(format!(
                "a workflow has 1 to {MAX_STAGES} stages"
            )));
        }
        let client = batch.client();
        let root = root_client(&client);
        let count = stages.len();
        // Each stage is one commit when it starts: its batch counts as CHILD_WEIGHT members.
        let limit = root.batch_defaults().max_commit_jobs;
        for (index, stage) in stages.iter().enumerate() {
            let jobs = stage.jobs.len();
            if jobs.saturating_add(CHILD_WEIGHT) > limit {
                return Err(Error::Batch(BatchError::StageTooLarge {
                    stage: index + 1,
                    jobs,
                    limit,
                }));
            }
        }
        let mut prepared = Vec::with_capacity(count);
        for (index, stage) in stages.into_iter().enumerate() {
            prepared.push(prepare(&root, stage, index + 1, count).await?);
        }
        let mut prepared = prepared.into_iter();
        let first = prepared
            .next()
            .ok_or_else(|| Error::InvalidJob("a workflow has at least one stage".to_owned()))?;
        let stored = Stored {
            n: count,
            queue: callback_queue,
            stages: prepared.collect(),
        };
        let wf = serde_json::to_string(&stored)?;
        if wf.len() > MAX_STAGES_BYTES {
            return Err(Error::InvalidJob(format!(
                "the stages of a workflow take at most {} MiB",
                MAX_STAGES_BYTES / (1024 * 1024)
            )));
        }
        let bid = batch.bid().to_owned();
        let queue = stored.queue;
        let mut batch = batch.extra_field("wf", wf);
        if count > 1 {
            // Held open until the last stage is defined (released by its WorkflowNext).
            batch = batch.extra_field(HOLD, "1".to_owned());
        }
        batch
            .jobs(|jobs| async move {
                stage_batch(&jobs, &bid, first, 1, count, queue.as_deref()).await?;
                Ok::<_, Error>(())
            })
            .await
    }
}

impl Stage {
    /// Adds job `J` with `args`.
    pub fn job<J: Job>(self, args: J::Args) -> Self {
        self.job_with(JobSetter::<J>::new(), args)
    }

    /// Adds job `J` with per-job options (`J::set().queue("low")`); `at`/`delay` are refused
    /// when the workflow starts.
    pub fn job_with<J: Job>(mut self, setter: JobSetter<J>, args: J::Args) -> Self {
        self.jobs.push(typed_push(setter, args));
        self
    }

    /// Text shown for the stage's batch in the dashboard (after "stage k of N").
    pub fn description(mut self, text: impl Into<String>) -> Self {
        self.description = Some(text.into());
        self
    }
}

impl std::fmt::Debug for Workflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Workflow")
            .field("batch", &self.batch)
            .field("stages", &self.stages.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stage")
            .field("description", &self.description)
            .field("jobs", &self.jobs.len())
            .finish()
    }
}

/// The client a collecting client commits through (the client itself otherwise).
fn root_client(client: &Client) -> Client {
    match client.mode() {
        ClientMode::Collect(collector) => collector.definition.client.clone(),
        _ => client.clone(),
    }
}

/// Normalizes the jobs of stage `number` of `count` (no jid, no times: each gets them when its
/// stage starts).
async fn prepare(
    client: &Client,
    stage: Stage,
    number: usize,
    count: usize,
) -> Result<StoredStage> {
    if !(1..=MAX_STAGE_JOBS).contains(&stage.jobs.len()) {
        return Err(Error::InvalidJob(format!(
            "stage {number} of a workflow has 1 to {MAX_STAGE_JOBS} jobs"
        )));
    }
    let mut jobs = Vec::with_capacity(stage.jobs.len());
    for build in stage.jobs {
        let Some(mut job) = capture(client, build?, true).await? else {
            continue;
        };
        if job.at.is_some() {
            return Err(Error::InvalidJob(format!(
                "job {} of stage {number} of a workflow is scheduled (at or delay): a stage's \
                 jobs start when the stage does",
                job.class
            )));
        }
        job.jid = String::new();
        job.created_at = None;
        job.enqueued_at = None;
        jobs.push(job);
    }
    let desc = match stage.description {
        Some(text) => format!("stage {number} of {count}: {text}"),
        None => format!("stage {number} of {count}"),
    };
    Ok(StoredStage { desc, jobs })
}

/// Defines the batch of stage `number` of `count` of workflow `workflow` on `jobs` (the
/// workflow's collecting client), with its jobs and, unless it is the last stage, the
/// `hench::WorkflowNext` callback that starts the next one, in `queue` (the `callback_queue`),
/// else in the queue of the stage's first job.
async fn stage_batch(
    jobs: &Client,
    workflow: &str,
    stage: StoredStage,
    number: usize,
    count: usize,
    queue: Option<&str>,
) -> Result<()> {
    let mut batch = Batch::new(jobs).description(stage.desc);
    if number < count {
        let queue = queue
            .or_else(|| stage.jobs.first().map(|job| job.queue.as_str()))
            .unwrap_or(DEFAULT_QUEUE);
        batch = batch.on(
            BatchEvent::Success,
            JobSetter::<WorkflowNext>::new().queue(queue),
            (workflow.to_owned(), number + 1),
        );
    }
    let templates = stage.jobs;
    batch
        .jobs(|stage_jobs| async move {
            for job in templates {
                stage_jobs.push(job).await?;
            }
            Ok::<_, Error>(())
        })
        .await?;
    Ok(())
}

/// The job behind [`WORKFLOW_NEXT`]: starts stage `n` of workflow `W` (its arguments `[W, n]`).
/// Every worker resolves the name to it (it is not in the registry, whose names are the
/// application's).
struct WorkflowNext;

impl Job for WorkflowNext {
    const NAME: &'static str = WORKFLOW_NEXT;
    type Args = (String, usize);

    async fn perform(
        &self,
        ctx: &JobContext,
        (workflow, number): Self::Args,
    ) -> Result<(), JobError> {
        let Some(raw) = stored_stages(ctx.client(), &workflow).await? else {
            tracing::warn!(
                bid = %workflow,
                "the workflow's batch is gone (expired or deleted); its next stage is not started"
            );
            return Ok(());
        };
        let mut stored: Stored = serde_json::from_str(&raw).map_err(|error| {
            JobError::from(Error::InvalidJob(format!(
                "the stages of workflow {workflow} are unreadable: {error}"
            )))
        })?;
        let Some(index) = number
            .checked_sub(2)
            .filter(|index| *index < stored.stages.len())
        else {
            tracing::warn!(bid = %workflow, stage = number, "a workflow has no such stage");
            return Ok(());
        };
        let stage = stored.stages.swap_remove(index);
        let (count, queue) = (stored.n, stored.queue);
        let mut reopen = Batch::open(ctx.client(), &workflow)?.commit_id(format!("wf-{number}"));
        if number == count {
            // The last stage: the workflow may complete once it did.
            reopen = reopen.extra_field(HOLD, "0".to_owned());
        }
        let opened = reopen
            .jobs(|jobs| async move {
                stage_batch(&jobs, &workflow, stage, number, count, queue.as_deref()).await
            })
            .await;
        match opened {
            Ok(committed) => {
                tracing::info!(
                    bid = %committed.bid,
                    stage = number,
                    stages = count,
                    already_applied = committed.already_applied,
                    "workflow stage started"
                );
                Ok(())
            }
            Err(Error::Batch(BatchError::Cancelled { bid })) => {
                tracing::info!(bid = %bid, "the workflow was cancelled; its next stage is not started");
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// The stored stages (`wf`) of workflow `workflow`: its batch's field, in Redis or in a testing
/// client's table; `None` once the batch is gone.
async fn stored_stages(client: &Client, workflow: &str) -> Result<Option<String>> {
    match client.mode() {
        ClientMode::Fake(_) | ClientMode::Inline(_) => Ok(client
            .tables()
            .batches
            .lock()
            .batches
            .get(workflow)
            .and_then(|batch| batch.extra.get("wf").cloned())),
        ClientMode::Redis | ClientMode::Collect(_) => {
            let hash = keys::batch(workflow);
            let hash = &hash;
            client
                .redis()
                .with_retry(|mut conn| async move {
                    redis::cmd("HGET")
                        .arg(hash)
                        .arg("wf")
                        .query_async::<Option<String>>(&mut conn)
                        .await
                })
                .await
        }
    }
}

/// The job registered under [`WORKFLOW_NEXT`] in every process.
pub(crate) fn job() -> Arc<dyn ErasedJob> {
    Arc::new(WorkflowNext)
}
