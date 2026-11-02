//! [`Batch`]: the definition of a batch, written by its commit (`docs/design/batches.md`
//! §2.3.1).

use std::fmt;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::future::BoxFuture;

use crate::batch::collect::{Definition, SlotState, collecting_client};
use crate::batch::model::event_index;
use crate::batch::scripts::{CommitMode, Template};
use crate::batch::{
    BatchCallback, BatchDefaults, BatchError, BatchEvent, commit, new_bid, valid_bid,
};
use crate::client::{Client, ClientMode};
use crate::error::{Error, Result};
use crate::job::{Job, JobSetter, args_to_values};

/// Most callbacks of one event.
pub(crate) const MAX_CALLBACKS: usize = 16;

/// Largest size of one event's callback templates, in bytes.
pub(crate) const MAX_TEMPLATES_BYTES: usize = 64 * 1024;

/// Longest description, in characters.
pub(crate) const MAX_DESCRIPTION_CHARS: usize = 1_000;

/// Longest expiry of an unfinished batch (`expires_in`).
pub(crate) const MAX_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Longest linger of a finished batch.
pub(crate) const MAX_LINGER: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// The field of a root batch created with a chosen bid: its commit ids outlive its linger by its
/// `ttl` (`hb_finish`).
const CHOSEN_BID: &str = "xbid";

/// A batch being defined: a new batch, or an existing one reopened to add members. Nothing is
/// written until it is committed ([`Batch::commit`], or [`Batch::jobs`] with a closure).
///
/// Jobs join the batch by being pushed through its collecting client ([`Batch::client`], or
/// the client handed to the [`Batch::jobs`] closure): every push is buffered (and goes through
/// normalization and client middleware at once, returning its jid), and the commit writes the
/// batch, the registration of every member and the members themselves in one atomic step.
///
/// A batch created from a collecting client (`Batch::new(&jobs)` inside another definition) is
/// a **child** of that batch: its commit only stages it into its root's commit. A batch dropped
/// without a commit writes nothing (a child is then left out of its root's commit, with its
/// members and children).
///
/// ```no_run
/// use hench::prelude::*;
///
/// #[hench::job(queue = "imports")]
/// async fn import_row(import_id: i64, row: u32) -> Result<(), JobError> {
///     Ok(())
/// }
///
/// #[hench::job(queue = "critical")]
/// async fn import_finished(ctx: &JobContext, import_id: i64) -> Result<(), JobError> {
///     let callback = ctx.batch_callback().ok_or_else(|| JobError::msg("not a callback"))?;
///     if let Some(status) = callback.status(ctx).await? {
///         tracing::info!(bid = callback.bid(), failures = status.failures, "import finished");
///     }
///     Ok(())
/// }
///
/// # async fn example(client: &Client) -> Result<(), hench::Error> {
/// let committed = Batch::new(client)
///     .description("Import #7")
///     .on_success::<import_finished>((7,))
///     .jobs(|jobs| async move {
///         for row in 0..100 {
///             import_row::perform_async(&jobs, (7, row)).await?;
///         }
///         Ok::<_, hench::Error>(())
///     })
///     .await?;
/// tracing::info!(bid = %committed.bid, jobs = committed.jids.len(), "import started");
/// # Ok(()) }
/// ```
#[must_use = "a batch is written only by `commit` or `jobs`"]
pub struct Batch {
    /// The client given (a collecting client for a child).
    client: Client,
    bid: String,
    mode: CommitMode,
    description: Option<String>,
    callback_queue: Option<String>,
    ttl: Option<Duration>,
    linger: Option<Duration>,
    commit_id: Option<String>,
    callbacks: Vec<CallbackDef>,
    extra: Vec<(String, String)>,
    /// Created by [`Batch::with_bid`]: as a root, its commit ids outlive its linger.
    chosen_bid: bool,
    place: Place,
}

/// Builds a job by pushing it through a capturing client (callbacks, workflow stages).
pub(crate) type BuildJob = Box<dyn FnOnce(Client) -> BoxFuture<'static, Result<()>> + Send>;

/// The push of job `J` with `setter` and `args`, for [`capture`] (or why its arguments are
/// invalid: they are checked at once).
pub(crate) fn typed_push<J: Job>(setter: JobSetter<J>, args: J::Args) -> Result<BuildJob> {
    args_to_values(J::NAME, &args)?;
    Ok(Box::new(move |client: Client| {
        Box::pin(async move {
            setter.perform_async(&client, args).await?;
            Ok(())
        })
    }))
}

/// The job `build` pushes, as a push through `client` would normalize it (the job type's
/// options, `client`'s default job options; uniqueness kept only with `unique`), captured
/// instead of written: no client middleware runs. `None` if nothing was pushed.
pub(crate) async fn capture(
    client: &Client,
    build: BuildJob,
    unique: bool,
) -> Result<Option<crate::payload::JobPayload>> {
    let capture = Client::fake()
        .with_default_job_options(client.default_job_options().clone())
        .with_unique_jobs(unique && client.unique_jobs());
    build(capture.clone()).await?;
    Ok(capture.all_jobs().pop())
}

/// One callback of a batch, built at its commit.
struct CallbackDef {
    event: BatchEvent,
    /// The push of the callback job (or why its arguments are invalid).
    build: Result<BuildJob>,
    /// The setter named a queue (it then wins over `callback_queue`).
    explicit_queue: bool,
}

/// Where a batch's definition lives.
enum Place {
    /// The root of its own definition, created when its client is first asked for.
    Root(OnceLock<SlotGuard>),
    /// A child of another definition (its slot, `None` when that definition was already
    /// closed).
    Child {
        definition: Arc<Definition>,
        guard: Option<SlotGuard>,
    },
}

/// Closes (a root) or discards (a child) its slot when it is dropped before the commit: a
/// batch dropped without a commit writes nothing.
struct SlotGuard {
    held: Option<(Arc<Definition>, usize)>,
}

impl SlotGuard {
    fn new(definition: Arc<Definition>, slot: usize) -> Self {
        Self {
            held: Some((definition, slot)),
        }
    }

    /// The definition and slot, without discarding them.
    fn disarm(mut self) -> Option<(Arc<Definition>, usize)> {
        self.held.take()
    }

    fn slot(&self) -> Option<(&Arc<Definition>, usize)> {
        self.held
            .as_ref()
            .map(|(definition, slot)| (definition, *slot))
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if let Some((definition, slot)) = self.held.take() {
            let mut state = definition.lock();
            if slot == 0 {
                state.closed = true;
            } else {
                state.discard(slot);
            }
        }
    }
}

/// What a commit wrote.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Committed<T = ()> {
    /// The batch defined (the root of the commit).
    pub bid: String,
    /// Members pushed by this commit, children's included, in push order (empty when the commit
    /// id had already been applied: nothing was written now).
    pub jids: Vec<String>,
    /// Jobs not pushed because another job held their unique lock (`OnConflict::Drop`), or
    /// because their own jid already held it (that job was in Redis outside the batch).
    pub duplicates: Vec<String>,
    /// Child batches created by this commit.
    pub children: Vec<String>,
    /// The commit id had been applied before this call (an explicit commit id used again):
    /// nothing was written now.
    pub already_applied: bool,
    /// What the definition's closure returned.
    pub value: T,
}

impl Committed<()> {
    /// The same report carrying `value`.
    fn with_value<T>(self, value: T) -> Committed<T> {
        Committed {
            bid: self.bid,
            jids: self.jids,
            duplicates: self.duplicates,
            children: self.children,
            already_applied: self.already_applied,
            value,
        }
    }
}

/// A child's definition, staged into its root's commit.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Staged {
    /// Description (`""`: none).
    pub(crate) description: String,
    /// Expiry after the last activity, seconds.
    pub(crate) ttl_secs: u64,
    /// Linger once finished, seconds.
    pub(crate) linger_secs: u64,
    /// The callback templates of `complete`, `success` and `death` (JSON arrays or `""`).
    pub(crate) callbacks: [String; 3],
    /// More fields of the hash.
    pub(crate) extra: Vec<(String, String)>,
}

impl Batch {
    /// A new batch pushing to `client`'s Redis (or testing store). If `client` is the
    /// collecting client of another definition, the new batch is a child of that batch,
    /// committed with it.
    pub fn new(client: &Client) -> Self {
        Self::create(client, new_bid())
    }

    /// Reopens batch `bid` to add members and child batches (checked at the commit: the batch
    /// must exist, not have succeeded and not be cancelled). Fails with
    /// [`BatchError::InvalidBid`] if `bid` is not 24 lowercase hexadecimal characters, and with
    /// [`Error::InvalidJob`] on a collecting client (a commit has one root: reopen the batch
    /// outside the definition).
    pub fn open(client: &Client, bid: impl Into<String>) -> Result<Self> {
        let bid = checked(bid.into())?;
        if matches!(client.mode(), ClientMode::Collect(_)) {
            return Err(Error::InvalidJob(format!(
                "batch {bid} cannot be reopened inside another batch's definition: a commit has \
                 one root batch"
            )));
        }
        let mut batch = Self::create(client, bid);
        batch.mode = CommitMode::Open;
        Ok(batch)
    }

    /// A new batch with a chosen bid (24 lowercase hexadecimal characters, else
    /// [`BatchError::InvalidBid`]), for idempotent creation by a job that may run twice: with
    /// the job's jid as both the bid and the [`commit_id`](Batch::commit_id), a re-run's commit
    /// finds its commit id applied and writes nothing ([`Committed::already_applied`]); a bid
    /// that exists from another commit is [`BatchError::Exists`].
    ///
    /// The commit ids of such a root batch stay recorded after it finished for its
    /// [`expires_in`](Batch::expires_in) (30 days by default; at least its linger), so that a
    /// re-run days later (a retry, a job revived from the morgue) still writes nothing. A
    /// re-run after that, or after the batch was deleted or expired unfinished, creates the
    /// batch and pushes its members again.
    ///
    /// ```no_run
    /// # use hench::prelude::*;
    /// # #[hench::job] async fn rollup(day: u32) -> Result<(), JobError> { Ok(()) }
    /// #[hench::job]
    /// async fn nightly(ctx: &JobContext) -> Result<(), JobError> {
    ///     Batch::with_bid(ctx.client(), ctx.jid())?
    ///         .commit_id(ctx.jid())
    ///         .jobs(|jobs| async move {
    ///             rollup::perform_async(&jobs, (1,)).await?;
    ///             Ok::<_, JobError>(())
    ///         })
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn with_bid(client: &Client, bid: impl Into<String>) -> Result<Self> {
        let bid = checked(bid.into())?;
        let mut batch = Self::create(client, bid);
        batch.chosen_bid = true;
        Ok(batch)
    }

    fn create(client: &Client, bid: String) -> Self {
        let place = match client.mode() {
            ClientMode::Collect(collector) => {
                let slot = collector
                    .definition
                    .lock()
                    .add_child(collector.slot, bid.clone());
                Place::Child {
                    definition: Arc::clone(&collector.definition),
                    guard: slot.map(|slot| SlotGuard::new(Arc::clone(&collector.definition), slot)),
                }
            }
            _ => Place::Root(OnceLock::new()),
        };
        Self {
            client: client.clone(),
            bid,
            mode: CommitMode::New,
            description: None,
            callback_queue: None,
            ttl: None,
            linger: None,
            commit_id: None,
            callbacks: Vec::new(),
            extra: Vec::new(),
            chosen_bid: false,
            place,
        }
    }

    /// The batch id (generated by [`Batch::new`]).
    pub fn bid(&self) -> &str {
        &self.bid
    }

    /// Text shown in the dashboard (new batches; at most 1 000 characters, checked at the
    /// commit).
    pub fn description(mut self, text: impl Into<String>) -> Self {
        self.description = Some(text.into());
        self
    }

    /// Queue of every callback of this batch (default: each callback job's own queue); a
    /// callback whose setter names a queue keeps it.
    pub fn callback_queue(mut self, queue: impl Into<String>) -> Self {
        self.callback_queue = Some(queue.into());
        self
    }

    /// Expiry of the unfinished batch after its last activity (default `batch_ttl`, 30 days;
    /// from 1 hour to 365 days).
    pub fn expires_in(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// How long the data stays once the batch finished (default `batch_linger`, 24 hours; from
    /// 30 s to 30 days).
    pub fn linger(mut self, linger: Duration) -> Self {
        self.linger = Some(linger);
        self
    }

    /// Idempotency key of this commit (default: random). A commit whose id the root batch has
    /// already applied writes nothing and reports [`Committed::already_applied`]: a loader job
    /// that reopens its batch with a commit id of its own adds its members once, however often
    /// it runs. Root batches only (a child is committed with its root).
    pub fn commit_id(mut self, id: impl Into<String>) -> Self {
        self.commit_id = Some(id.into());
        self
    }

    /// Callback job `J` with `args`, pushed when `complete` fires (every member ran at least
    /// once). At most 16 per event; arguments and options are checked at the commit.
    pub fn on_complete<J: Job>(self, args: J::Args) -> Self {
        self.on(BatchEvent::Complete, JobSetter::<J>::new(), args)
    }

    /// Callback job `J` with `args`, pushed when `success` fires (every member succeeded).
    pub fn on_success<J: Job>(self, args: J::Args) -> Self {
        self.on(BatchEvent::Success, JobSetter::<J>::new(), args)
    }

    /// Callback job `J` with `args`, pushed when `death` fires (the first member that will not
    /// run again).
    pub fn on_death<J: Job>(self, args: J::Args) -> Self {
        self.on(BatchEvent::Death, JobSetter::<J>::new(), args)
    }

    /// Callback job `J` with per-callback options (`J::set().queue("critical").retry(5)`):
    /// the job type's options apply, then [`callback_queue`](Batch::callback_queue), then the
    /// setter's. A setter with `at` or `delay` is an error at the commit (callbacks run when
    /// their event is dispatched).
    pub fn on<J: Job>(mut self, event: BatchEvent, setter: JobSetter<J>, args: J::Args) -> Self {
        let explicit_queue = setter.explicit_options().queue.is_some();
        let build = typed_push(setter, args);
        self.callbacks.push(CallbackDef {
            event,
            build,
            explicit_queue,
        });
        self
    }

    /// Adds a field to the batch's hash: the workflow's `wf` and its hold `open` (written over
    /// the existing field when the batch is reopened).
    pub(crate) fn extra_field(mut self, field: &str, value: String) -> Self {
        self.extra.push((field.to_owned(), value));
        self
    }

    /// A client whose pushes join this batch, buffered until the commit. Clones share the
    /// buffer; pushes after the commit (or once the batch was dropped) fail with
    /// [`BatchError::Closed`].
    pub fn client(&self) -> Client {
        match &self.place {
            Place::Root(definition) => {
                let guard = definition.get_or_init(|| {
                    SlotGuard::new(Definition::new(self.client.clone(), self.bid.clone()), 0)
                });
                match guard.slot() {
                    Some((definition, slot)) => collecting_client(definition, slot, &self.bid),
                    None => self.client.clone(),
                }
            }
            Place::Child { definition, guard } => {
                let slot = guard
                    .as_ref()
                    .and_then(SlotGuard::slot)
                    .map_or(usize::MAX, |(_, slot)| slot);
                collecting_client(definition, slot, &self.bid)
            }
        }
    }

    /// Writes the definition: for a root batch, one script writes it all (resent until a
    /// definite reply, at most `batch_commit_timeout`; then [`BatchError::Unconfirmed`], and
    /// committing again with the same [`commit_id`](Batch::commit_id) settles it). For a child
    /// (created from a collecting client) it only stages the definition into its root's commit
    /// and returns at once: nothing of it exists before the root's commit succeeds.
    pub async fn commit(self) -> Result<Committed> {
        match self.place {
            Place::Root(_) => self.commit_root().await,
            Place::Child { .. } => self.stage().await,
        }
    }

    /// [`Batch::client`], then `define` with it, then [`Batch::commit`]. An error from `define`
    /// discards the definition (nothing is written) and is returned as is. The closure gets
    /// the collecting client by value, so that the future stays `Send`.
    pub async fn jobs<T, E, F, Fut>(self, define: F) -> std::result::Result<Committed<T>, E>
    where
        F: FnOnce(Client) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>> + Send,
        T: Send,
        E: From<Error>,
    {
        let client = self.client();
        // On an error `self` is dropped: its guard discards the definition.
        let value = define(client).await?;
        let committed = self.commit().await.map_err(E::from)?;
        Ok(committed.with_value(value))
    }

    /// The batch settings of the client this batch commits through.
    fn defaults(&self) -> BatchDefaults {
        match &self.place {
            Place::Child { definition, .. } => definition.client.batch_defaults(),
            Place::Root(_) => self.client.batch_defaults(),
        }
    }

    /// Checks the settings of a new batch and resolves its expiry and linger (seconds).
    fn settings(&self) -> Result<(u64, u64)> {
        if self.mode == CommitMode::Open
            && (self.description.is_some()
                || self.callback_queue.is_some()
                || self.ttl.is_some()
                || self.linger.is_some()
                || !self.callbacks.is_empty())
        {
            return Err(Error::InvalidJob(format!(
                "batch {} is reopened: a reopened batch only gains members and children (its \
                 description, callbacks, callback queue, expiry and linger belong to its \
                 creation)",
                self.bid
            )));
        }
        if let Some(description) = &self.description
            && description.chars().count() > MAX_DESCRIPTION_CHARS
        {
            return Err(Error::InvalidJob(format!(
                "a batch description is at most {MAX_DESCRIPTION_CHARS} characters"
            )));
        }
        let defaults = self.defaults();
        let ttl = self.ttl.unwrap_or(defaults.ttl);
        if !(BatchDefaults::MIN_TTL..=MAX_TTL).contains(&ttl) {
            return Err(Error::InvalidJob(
                "a batch's expiry (expires_in) must be from 1 hour to 365 days".to_owned(),
            ));
        }
        let linger = self.linger.unwrap_or(defaults.linger);
        if !(BatchDefaults::MIN_LINGER..=MAX_LINGER).contains(&linger) {
            return Err(Error::InvalidJob(
                "a batch's linger must be from 30 seconds to 30 days".to_owned(),
            ));
        }
        Ok((ttl.as_secs(), linger.as_secs()))
    }

    /// Builds the callback templates of this batch (its callbacks pushed through a capturing
    /// client, then through `client`'s middleware), as stored in `cb_complete`, `cb_success`
    /// and `cb_death`. Their `bid` is `parent` (callbacks of a child are members of its
    /// parent).
    async fn templates(&mut self, client: &Client, parent: Option<&str>) -> Result<[String; 3]> {
        let mut templates: [Vec<Template>; 3] = Default::default();
        for callback in std::mem::take(&mut self.callbacks) {
            let build = callback.build?;
            let Some(template) = build_template(
                client,
                build,
                callback.event,
                &self.bid,
                parent,
                (!callback.explicit_queue)
                    .then_some(self.callback_queue.as_deref())
                    .flatten(),
            )
            .await?
            else {
                continue;
            };
            templates[event_index(callback.event)].push(template);
        }
        let mut stored: [String; 3] = Default::default();
        for (slot, (event, templates)) in stored.iter_mut().zip(
            [BatchEvent::Complete, BatchEvent::Success, BatchEvent::Death]
                .into_iter()
                .zip(templates),
        ) {
            if templates.len() > MAX_CALLBACKS {
                return Err(Error::InvalidJob(format!(
                    "a batch has at most {MAX_CALLBACKS} {event} callbacks"
                )));
            }
            if templates.is_empty() {
                continue;
            }
            let json = serde_json::to_string(&templates)?;
            if json.len() > MAX_TEMPLATES_BYTES {
                return Err(Error::InvalidJob(format!(
                    "the {event} callbacks of a batch take at most {} KiB",
                    MAX_TEMPLATES_BYTES / 1024
                )));
            }
            *slot = json;
        }
        Ok(stored)
    }

    /// Commits a root batch.
    async fn commit_root(mut self) -> Result<Committed> {
        let (ttl_secs, linger_secs) = self.settings()?;
        let client = self.client.clone();
        let callbacks = self.templates(&client, None).await?;
        let Place::Root(definition) = &mut self.place else {
            return Err(Error::InvalidJob("not a root batch".to_owned()));
        };
        let definition = definition.take().and_then(SlotGuard::disarm).map_or_else(
            || Definition::new(client.clone(), self.bid.clone()),
            |(definition, _)| definition,
        );
        let mut extra = std::mem::take(&mut self.extra);
        if self.chosen_bid && self.mode == CommitMode::New {
            extra.push((CHOSEN_BID.to_owned(), "1".to_owned()));
        }
        let root = commit::Root {
            mode: self.mode,
            bid: self.bid.clone(),
            ttl_secs,
            linger_secs,
            description: self.description.clone().unwrap_or_default(),
            callbacks,
            extra,
        };
        let commit_id = self
            .commit_id
            .clone()
            .unwrap_or_else(|| format!("c-{}", new_bid()));
        commit::commit(&client, &definition, root, &commit_id).await
    }

    /// Stages a child into its root's commit.
    async fn stage(mut self) -> Result<Committed> {
        let (ttl_secs, linger_secs) = self.settings()?;
        if self.commit_id.is_some() {
            return Err(Error::InvalidJob(format!(
                "batch {} is a child: it is committed with its root, whose commit id applies",
                self.bid
            )));
        }
        let Place::Child { definition, guard } = &self.place else {
            return Err(Error::InvalidJob("not a child batch".to_owned()));
        };
        let definition = Arc::clone(definition);
        let Some(slot) = guard
            .as_ref()
            .and_then(SlotGuard::slot)
            .map(|(_, slot)| slot)
        else {
            return Err(Error::Batch(BatchError::Closed));
        };
        let parent = {
            let state = definition.lock();
            state
                .slots
                .get(slot)
                .and_then(|slot| slot.parent)
                .and_then(|parent| state.slots.get(parent))
                .map(|parent| parent.bid.clone())
        };
        let root_client = definition.client.clone();
        let callbacks = self.templates(&root_client, parent.as_deref()).await?;
        let staged = Staged {
            description: self.description.clone().unwrap_or_default(),
            ttl_secs,
            linger_secs,
            callbacks,
            extra: std::mem::take(&mut self.extra),
        };
        let limit = definition.max_jobs();
        let mut state = definition.lock();
        if !state.is_open(slot) {
            return Err(Error::Batch(BatchError::Closed));
        }
        state.check_size(0, limit)?;
        if let Some(entry) = state.slots.get_mut(slot) {
            entry.state = SlotState::Staged(Box::new(staged));
        }
        let below = state.descendants(slot);
        let jids = state
            .members
            .iter()
            .filter(|member| member.slot == slot || below.contains(&member.slot))
            .map(|member| member.job.jid.clone())
            .collect();
        let children = below
            .iter()
            .filter_map(|index| state.slots.get(*index))
            .filter(|child| matches!(child.state, SlotState::Staged(_)))
            .map(|child| child.bid.clone())
            .collect();
        drop(state);
        if let Place::Child { guard, .. } = &mut self.place
            && let Some(guard) = guard.take()
        {
            guard.disarm();
        }
        tracing::debug!(bid = %self.bid, "child batch staged into its root's commit");
        Ok(Committed {
            bid: self.bid.clone(),
            jids,
            duplicates: Vec::new(),
            children,
            already_applied: false,
            value: (),
        })
    }
}

/// Builds one callback template: pushes the callback through a client that captures it (the
/// job type's options, the default job options, uniqueness removed), sets its queue (unless the
/// setter named one), `bid` and `batch_callback`, then runs `client`'s middleware on it.
/// `None` when middleware stopped it (dropped, with a warning).
async fn build_template(
    client: &Client,
    build: BuildJob,
    event: BatchEvent,
    bid: &str,
    parent: Option<&str>,
    queue: Option<&str>,
) -> Result<Option<Template>> {
    let Some(mut job) = capture(client, build, false).await? else {
        return Ok(None);
    };
    if job.at.is_some() {
        return Err(Error::InvalidJob(format!(
            "the {event} callback {} of batch {bid} is scheduled (at or delay): callbacks run \
             when their event is dispatched",
            job.class
        )));
    }
    job.enqueued_at = None;
    job.unique = None;
    if let Some(queue) = queue {
        job.queue = queue.to_owned();
    }
    job.bid = parent.map(str::to_owned);
    job.batch_callback = Some(BatchCallback::new(bid, event));
    let Some(mut job) = client.prepare_push(job).await? else {
        tracing::warn!(
            bid,
            event = %event,
            "client middleware stopped a batch callback; it is left out"
        );
        return Ok(None);
    };
    if job.at.is_some() {
        return Err(Error::InvalidJob(format!(
            "client middleware scheduled the {event} callback {} of batch {bid}: callbacks run \
             when their event is dispatched",
            job.class
        )));
    }
    job.enqueued_at = None;
    Ok(Some(Template {
        q: job.queue.clone(),
        jid: job.jid.clone(),
        j: job.to_json()?,
    }))
}

fn checked(bid: String) -> Result<String> {
    if valid_bid(&bid) {
        Ok(bid)
    } else {
        Err(Error::Batch(BatchError::InvalidBid { bid }))
    }
}

impl fmt::Debug for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("bid", &self.bid)
            .field("mode", &self.mode)
            .field("description", &self.description)
            .field("callbacks", &self.callbacks.len())
            .field("child", &matches!(self.place, Place::Child { .. }))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn batches_check_their_bid_and_settings() {
        let client = Client::fake();
        let batch = Batch::new(&client);
        assert!(valid_bid(batch.bid()));
        assert!(matches!(
            Batch::open(&client, "nope"),
            Err(Error::Batch(BatchError::InvalidBid { .. }))
        ));
        let chosen = Batch::with_bid(&client, "0123456789abcdef01234567").unwrap();
        assert_eq!(chosen.bid(), "0123456789abcdef01234567");
        // A reopened batch takes no creation settings.
        let reopened = Batch::open(&client, "0123456789abcdef01234567")
            .unwrap()
            .description("x");
        assert!(matches!(reopened.settings(), Err(Error::InvalidJob(_))));
        let too_long = Batch::new(&client).description("d".repeat(1_001));
        assert!(matches!(too_long.settings(), Err(Error::InvalidJob(_))));
        let short = Batch::new(&client).expires_in(Duration::from_secs(60));
        assert!(matches!(short.settings(), Err(Error::InvalidJob(_))));
        let ok = Batch::new(&client).linger(Duration::from_secs(30));
        assert_eq!(ok.settings().unwrap(), (30 * 86_400, 30));
    }

    #[test]
    fn children_are_defined_on_collecting_clients() {
        let client = Client::fake();
        let root = Batch::new(&client);
        let jobs = root.client();
        let child = Batch::new(&jobs);
        assert!(matches!(child.place, Place::Child { guard: Some(_), .. }));
        // A collecting client cannot reopen another batch.
        assert!(matches!(
            Batch::open(&jobs, "0123456789abcdef01234567"),
            Err(Error::InvalidJob(_))
        ));
        // Dropping the child discards its slot; dropping the root closes the definition.
        let Place::Root(definition) = &root.place else {
            panic!("a root");
        };
        let definition = Arc::clone(definition.get().unwrap().slot().unwrap().0);
        drop(child);
        assert!(matches!(
            definition.lock().slots[1].state,
            SlotState::Discarded
        ));
        drop(root);
        assert!(definition.lock().closed);
        assert!(format!("{:?}", Batch::new(&client)).contains("Batch"));
    }
}
