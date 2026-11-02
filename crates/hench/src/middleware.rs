//! Middleware around pushing jobs (client) and executing them (server).

use std::any::{TypeId, type_name};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{JobError, Result, across_boundary, capturing_origins};
use crate::job::{ErasedJob, JobContext, catch_panic};
use crate::payload::JobPayload;
use crate::redis::RedisPool;

/// Middleware around job execution in workers.
///
/// Middleware is code that runs around pushing a job (client middleware) or around executing
/// it (server middleware). It is "around"-style, like tower or Rack: each middleware receives the job and a
/// `next` handle, and decides whether (and with what) to continue the chain.
///
/// * **Client** middleware ([`ClientMiddleware`]) runs inside [`Client::push`](crate::Client::push)
///   after the job has been normalized. It may modify the job, return `Ok(None)` without
///   calling `next` to stop the push (`push` then returns `Ok(None)`), or fail the push with
///   an error.
/// * **Server** middleware ([`ServerMiddleware`]) runs in the worker around `perform`. Not
///   calling `next` means the job is considered done (and acknowledged) without running.
///
/// Middleware instances are shared by every invocation; keep per-job state in locals.
///
/// ```
/// use hench::prelude::*;
/// use hench::{RedisPool, ServerChain};
///
/// struct Timing;
///
/// #[async_trait]
/// impl ServerMiddleware for Timing {
///     async fn call(
///         &self,
///         ctx: &JobContext,
///         job: &mut JobPayload,
///         next: ServerNext<'_>,
///     ) -> Result<(), JobError> {
///         let started = std::time::Instant::now();
///         let result = next.run(ctx, job).await;
///         tracing::info!(jid = ctx.jid(), elapsed = ?started.elapsed(), "job finished");
///         result
///     }
/// }
///
/// let mut chain = ServerChain::new();
/// chain.add(Timing);
/// assert!(chain.exists::<Timing>());
/// ```
#[async_trait]
pub trait ServerMiddleware: Send + Sync + 'static {
    /// Handles one job. Call `next.run(ctx, job)` to continue the chain (and eventually run
    /// the job); return without calling it to skip the job.
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError>;
}

/// Middleware around pushing a job: runs inside [`Client::push`](crate::Client::push) after
/// the job has been normalized. See [`ServerMiddleware`] for the general model.
///
/// It may modify the job and continue with `next.run(job).await`, stop the push by returning
/// `Ok(None)` (then `push` returns `Ok(None)`), or fail the push with an error.
#[async_trait]
pub trait ClientMiddleware: Send + Sync + 'static {
    /// Handles one push. Return `next.run(job).await` to continue (possibly after modifying
    /// the job), `Ok(None)` to stop the push, or an error to fail it. `redis` is the pool the
    /// job is being pushed to.
    async fn call(
        &self,
        job: JobPayload,
        redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> Result<Option<JobPayload>>;
}

/// The innermost step of a server chain, run when every middleware called `next`.
#[async_trait]
pub trait ServerEndpoint: Send + Sync {
    /// Runs the job.
    async fn call(&self, ctx: &JobContext, job: &mut JobPayload) -> Result<(), JobError>;
}

/// A [`ServerEndpoint`] that performs a job: it hands the job, as modified by middleware, to
/// [`ErasedJob::perform_json`]. Panics are not caught here; callers wrap the whole chain.
///
/// Before `perform`, the features take their steps, in this order, on the job as fetched
/// (`ctx.payload()`: server middleware passes the context it received to `next`): a member of
/// a cancelled batch stops with [`JobError::batch_cancelled`], then the lock of a unique job
/// whose policy is `start` is released.
pub struct PerformEndpoint {
    job: Arc<dyn ErasedJob>,
}

impl PerformEndpoint {
    /// An endpoint performing `job`.
    pub fn new(job: Arc<dyn ErasedJob>) -> Self {
        Self { job }
    }
}

#[async_trait]
impl ServerEndpoint for PerformEndpoint {
    async fn call(&self, ctx: &JobContext, job: &mut JobPayload) -> Result<(), JobError> {
        crate::batch::endpoint::check(ctx).await?;
        crate::unique::endpoint::release_start(ctx).await;
        let ctx = ctx.clone().with_payload(job.clone());
        across_boundary(self.job.perform_json(&ctx, job.args.clone())).await
    }
}

impl fmt::Debug for PerformEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PerformEndpoint")
            .field(&self.job.name())
            .finish()
    }
}

/// The endpoint for a job whose class is not registered: it fails with
/// [`JobError::UNKNOWN_JOB`] once the chain reaches it, so server middleware sees (and can
/// report) the job like any other failure.
struct UnknownJobEndpoint<'a> {
    /// Makes the error from the job's class.
    error: &'a (dyn Fn(&str) -> JobError + Send + Sync),
}

#[async_trait]
impl ServerEndpoint for UnknownJobEndpoint<'_> {
    async fn call(&self, _ctx: &JobContext, job: &mut JobPayload) -> Result<(), JobError> {
        Err((self.error)(&job.class))
    }
}

/// Runs a job the way workers do; shared by the processor and the testing modes so that both
/// execute jobs identically. The job goes through the server middleware `chain`, then
/// `job_impl` performs it as middleware left it; a panic anywhere (middleware or job) is
/// caught and returned as a [`JobError::PANIC`] error. Without `job_impl` (an unregistered
/// class) the chain still runs and its endpoint fails with [`JobError::UNKNOWN_JOB`].
pub(crate) async fn execute_job(
    chain: &ServerChain,
    ctx: &JobContext,
    job: &mut JobPayload,
    job_impl: Option<Arc<dyn ErasedJob>>,
) -> Result<(), JobError> {
    execute_job_or(chain, ctx, job, job_impl, &JobError::unknown_job).await
}

/// [`execute_job`], failing an unregistered class with the error `unknown` makes from it.
pub(crate) async fn execute_job_or(
    chain: &ServerChain,
    ctx: &JobContext,
    job: &mut JobPayload,
    job_impl: Option<Arc<dyn ErasedJob>>,
    unknown: &(dyn Fn(&str) -> JobError + Send + Sync),
) -> Result<(), JobError> {
    // Jobs that store their backtrace capture where their errors and panics come from.
    let capture = job
        .backtrace
        .is_some_and(|backtrace| backtrace.select(Vec::new()).is_some());
    match job_impl {
        Some(job_impl) => {
            let endpoint = PerformEndpoint::new(job_impl);
            catch_panic(capturing_origins(
                capture,
                across_boundary(chain.invoke(ctx, job, &endpoint)),
            ))
            .await
        }
        None => {
            let endpoint = UnknownJobEndpoint { error: unknown };
            catch_panic(capturing_origins(
                capture,
                across_boundary(chain.invoke(ctx, job, &endpoint)),
            ))
            .await
        }
    }
}

/// The rest of a server chain, handed to [`ServerMiddleware::call`].
pub struct ServerNext<'a> {
    rest: &'a [Entry<dyn ServerMiddleware>],
    endpoint: &'a dyn ServerEndpoint,
}

impl ServerNext<'_> {
    /// Runs the remaining middleware and then the job.
    pub async fn run(self, ctx: &JobContext, job: &mut JobPayload) -> Result<(), JobError> {
        match self.rest.split_first() {
            Some((first, rest)) => {
                let next = ServerNext {
                    rest,
                    endpoint: self.endpoint,
                };
                across_boundary(first.middleware.call(ctx, job, next)).await
            }
            None => self.endpoint.call(ctx, job).await,
        }
    }
}

impl fmt::Debug for ServerNext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerNext")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// The rest of a client chain, handed to [`ClientMiddleware::call`].
pub struct ClientNext<'a> {
    rest: &'a [Entry<dyn ClientMiddleware>],
    redis: &'a RedisPool,
}

impl ClientNext<'_> {
    /// Runs the remaining middleware. Returns the job to push, or `None` if a later
    /// middleware stopped the push.
    pub async fn run(self, job: JobPayload) -> Result<Option<JobPayload>> {
        match self.rest.split_first() {
            Some((first, rest)) => {
                let next = ClientNext {
                    rest,
                    redis: self.redis,
                };
                first.middleware.call(job, self.redis, next).await
            }
            None => Ok(Some(job)),
        }
    }
}

impl fmt::Debug for ClientNext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientNext")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// One middleware in a chain, identified by its concrete type.
struct Entry<M: ?Sized> {
    type_id: TypeId,
    type_name: &'static str,
    middleware: Arc<M>,
}

impl<M: ?Sized> Clone for Entry<M> {
    fn clone(&self) -> Self {
        Self {
            type_id: self.type_id,
            type_name: self.type_name,
            middleware: Arc::clone(&self.middleware),
        }
    }
}

/// An ordered list of middleware, at most one per concrete type (like Sidekiq's
/// `Middleware::Chain`). See [`ServerChain`] and [`ClientChain`].
///
/// * [`add`](Chain::add) appends (replacing an existing entry of the same type),
/// * [`prepend`](Chain::prepend) inserts first,
/// * [`insert_before::<T>`](Chain::insert_before) / [`insert_after::<T>`](Chain::insert_after)
///   position relative to the middleware of type `T` (front / back if `T` is absent),
/// * [`remove::<T>`](Chain::remove) and [`exists::<T>`](Chain::exists) look up by type.
pub struct Chain<M: ?Sized> {
    entries: Vec<Entry<M>>,
}

/// A chain of [`ServerMiddleware`].
pub type ServerChain = Chain<dyn ServerMiddleware>;

/// A chain of [`ClientMiddleware`].
pub type ClientChain = Chain<dyn ClientMiddleware>;

impl<M: ?Sized> Chain<M> {
    /// An empty chain.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Number of middleware in the chain.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Removes every middleware.
    pub fn clear(&mut self) -> &mut Self {
        self.entries.clear();
        self
    }

    /// Removes the middleware of type `T`, if present.
    pub fn remove<T: 'static>(&mut self) -> &mut Self {
        let type_id = TypeId::of::<T>();
        self.entries.retain(|entry| entry.type_id != type_id);
        self
    }

    /// `true` if a middleware of type `T` is in the chain.
    pub fn exists<T: 'static>(&self) -> bool {
        self.position(TypeId::of::<T>()).is_some()
    }

    /// Type names of the middleware, in order.
    pub fn names(&self) -> Vec<&'static str> {
        self.entries.iter().map(|entry| entry.type_name).collect()
    }

    fn position(&self, type_id: TypeId) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| entry.type_id == type_id)
    }

    fn push_entry(&mut self, entry: Entry<M>) {
        self.entries
            .retain(|existing| existing.type_id != entry.type_id);
        self.entries.push(entry);
    }

    fn prepend_entry(&mut self, entry: Entry<M>) {
        self.entries
            .retain(|existing| existing.type_id != entry.type_id);
        self.entries.insert(0, entry);
    }

    fn insert_before_entry(&mut self, before: TypeId, entry: Entry<M>) {
        self.entries
            .retain(|existing| existing.type_id != entry.type_id);
        let index = self.position(before).unwrap_or(0);
        self.entries.insert(index, entry);
    }

    fn insert_after_entry(&mut self, after: TypeId, entry: Entry<M>) {
        self.entries
            .retain(|existing| existing.type_id != entry.type_id);
        let index = self
            .position(after)
            .map_or(self.entries.len(), |index| index + 1);
        self.entries.insert(index, entry);
    }
}

impl<M: ?Sized> Default for Chain<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: ?Sized> Clone for Chain<M> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<M: ?Sized> fmt::Debug for Chain<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.names()).finish()
    }
}

macro_rules! typed_chain {
    ($middleware:ident) => {
        impl Chain<dyn $middleware> {
            fn entry<T: $middleware>(middleware: T) -> Entry<dyn $middleware> {
                Entry {
                    type_id: TypeId::of::<T>(),
                    type_name: type_name::<T>(),
                    middleware: Arc::new(middleware),
                }
            }

            /// Appends `middleware`, replacing an existing middleware of the same type.
            pub fn add<T: $middleware>(&mut self, middleware: T) -> &mut Self {
                self.push_entry(Self::entry(middleware));
                self
            }

            /// Inserts `middleware` first, replacing an existing middleware of the same type.
            pub fn prepend<T: $middleware>(&mut self, middleware: T) -> &mut Self {
                self.prepend_entry(Self::entry(middleware));
                self
            }

            /// Inserts `middleware` just before the middleware of type `Before` (or first if
            /// there is none), replacing an existing middleware of its own type.
            pub fn insert_before<Before: 'static>(
                &mut self,
                middleware: impl $middleware,
            ) -> &mut Self {
                self.insert_before_entry(TypeId::of::<Before>(), Self::entry(middleware));
                self
            }

            /// Inserts `middleware` just after the middleware of type `After` (or last if
            /// there is none), replacing an existing middleware of its own type.
            pub fn insert_after<After: 'static>(
                &mut self,
                middleware: impl $middleware,
            ) -> &mut Self {
                self.insert_after_entry(TypeId::of::<After>(), Self::entry(middleware));
                self
            }
        }
    };
}

typed_chain!(ServerMiddleware);
typed_chain!(ClientMiddleware);

impl Chain<dyn ServerMiddleware> {
    /// Runs `job` through the chain and then `endpoint`.
    pub async fn invoke(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        endpoint: &dyn ServerEndpoint,
    ) -> Result<(), JobError> {
        let next = ServerNext {
            rest: &self.entries,
            endpoint,
        };
        next.run(ctx, job).await
    }
}

impl Chain<dyn ClientMiddleware> {
    /// Runs `job` through the chain. Returns the job to push, or `None` if a middleware
    /// stopped the push.
    pub async fn invoke(&self, job: JobPayload, redis: &RedisPool) -> Result<Option<JobPayload>> {
        let next = ClientNext {
            rest: &self.entries,
            redis,
        };
        next.run(job).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Client;
    use std::sync::Mutex;

    type Log = Arc<Mutex<Vec<String>>>;

    struct Recorder(&'static str, Log);
    struct Other(&'static str, Log);
    struct Halt;

    #[async_trait]
    impl ServerMiddleware for Recorder {
        async fn call(
            &self,
            ctx: &JobContext,
            job: &mut JobPayload,
            next: ServerNext<'_>,
        ) -> Result<(), JobError> {
            self.1.lock().unwrap().push(format!("{} before", self.0));
            let result = next.run(ctx, job).await;
            self.1.lock().unwrap().push(format!("{} after", self.0));
            result
        }
    }

    #[async_trait]
    impl ServerMiddleware for Other {
        async fn call(
            &self,
            ctx: &JobContext,
            job: &mut JobPayload,
            next: ServerNext<'_>,
        ) -> Result<(), JobError> {
            self.1.lock().unwrap().push(format!("{} before", self.0));
            next.run(ctx, job).await
        }
    }

    #[async_trait]
    impl ServerMiddleware for Halt {
        async fn call(
            &self,
            _ctx: &JobContext,
            _job: &mut JobPayload,
            _next: ServerNext<'_>,
        ) -> Result<(), JobError> {
            Ok(())
        }
    }

    struct Endpoint(Log);

    #[async_trait]
    impl ServerEndpoint for Endpoint {
        async fn call(&self, _ctx: &JobContext, job: &mut JobPayload) -> Result<(), JobError> {
            self.0
                .lock()
                .unwrap()
                .push(format!("perform {}", job.class));
            Ok(())
        }
    }

    fn ctx() -> JobContext {
        JobContext::new(JobPayload::new("J", vec![]), Client::fake())
    }

    #[test]
    fn add_replaces_same_type() {
        let log = Log::default();
        let mut chain = ServerChain::new();
        chain.add(Recorder("0", log.clone()));
        chain.add(Recorder("1", log.clone()));
        assert_eq!(chain.len(), 1);
        chain.add(Halt).prepend(Other("o", log));
        assert_eq!(chain.len(), 3);
        assert!(chain.names()[0].ends_with("Other"));
        assert!(chain.names()[2].ends_with("Halt"));
        chain.remove::<Halt>();
        assert!(!chain.exists::<Halt>());
        assert!(chain.exists::<Recorder>());
        chain.clear();
        assert!(chain.is_empty());
    }

    #[tokio::test]
    async fn order_and_halting() {
        let log = Log::default();
        let mut chain = ServerChain::new();
        chain.add(Recorder("1", log.clone()));
        chain.insert_before::<Recorder>(Other("2", log.clone()));
        let endpoint = Endpoint(log.clone());
        let mut job = JobPayload::new("J", vec![]);
        chain.invoke(&ctx(), &mut job, &endpoint).await.unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            ["2 before", "1 before", "perform J", "1 after"]
        );

        log.lock().unwrap().clear();
        chain.insert_after::<Other>(Halt);
        chain.invoke(&ctx(), &mut job, &endpoint).await.unwrap();
        assert_eq!(*log.lock().unwrap(), ["2 before"]);
        assert!(format!("{chain:?}").contains("Halt"));
    }

    #[test]
    fn insert_positions_when_target_is_missing() {
        let log = Log::default();
        let mut chain = ServerChain::new();
        chain.add(Recorder("r", log.clone()));
        chain.insert_before::<Halt>(Other("o", log.clone()));
        assert!(chain.names()[0].ends_with("Other"));
        chain.insert_after::<String>(Halt);
        assert!(chain.names()[2].ends_with("Halt"));
        // Moving an existing type keeps a single entry.
        chain.insert_before::<Other>(Halt);
        assert_eq!(chain.len(), 3);
        assert!(chain.names()[0].ends_with("Halt"));
    }

    #[tokio::test]
    async fn empty_chain_calls_endpoint() {
        let log = Log::default();
        let mut job = JobPayload::new("J", vec![]);
        ServerChain::default()
            .invoke(&ctx(), &mut job, &Endpoint(log.clone()))
            .await
            .unwrap();
        assert_eq!(*log.lock().unwrap(), ["perform J"]);
    }

    #[tokio::test]
    async fn client_chain_passes_job_through() {
        let chain = ClientChain::new();
        let redis = RedisPool::detached();
        let job = chain
            .invoke(JobPayload::new("J", vec![]), &redis)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.class, "J");
    }
}
