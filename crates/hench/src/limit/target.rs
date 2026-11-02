//! Where a limiter runs ([`LimitTarget`]): Redis, the in-memory table of a testing client, or
//! nowhere (unlimited); and the operations every backend offers.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::client::{Client, ClientMode};
use crate::config::Hench;
use crate::error::Result;
use crate::job::JobContext;
use crate::limit::memory::LimitTable;
use crate::limit::rule::Rule;
use crate::limit::scripts::{
    self, AcquireArgs, Acquired, LeaseRef, ReleaseOutcome, RenewOutcome, UsageArgs, UsageReply,
};
use crate::redis::RedisPool;
use crate::testing::{Tables, TestLimits};

/// Where a limiter takes its permits: Redis (a [`RedisPool`], a [`Client`] or a configured
/// [`Hench`]: their Redis), or a job's context (its client, and the process's shutdown, which
/// ends waits at once with the refusal). A testing client ([`Client::fake`], [`Client::inline`])
/// grants everything unless its limits run in memory
/// ([`TestLimits::InMemory`](crate::testing::TestLimits::InMemory)).
///
/// Implemented for references too: `limiter.acquire(ctx)` and `limiter.acquire(&pool)` both
/// work. Sealed: hench decides where limits live.
pub trait LimitTarget: sealed::Sealed {}

pub(crate) mod sealed {
    use super::{Backend, CancellationToken};

    /// The private side of [`LimitTarget`](super::LimitTarget).
    pub trait Sealed {
        /// Where the limits live.
        fn backend(&self) -> Backend;
        /// A signal that ends waits at once (a job's shutdown).
        fn stopping(&self) -> Option<CancellationToken> {
            None
        }
        /// Whether a refusal brings the caller back at its slot whatever it is: a job, which
        /// workers reschedule for its slot. Such a caller takes a place in the wait line with
        /// every refusal of [`acquire`](crate::limit::Limiter::acquire); others only for a slot
        /// they wait for in process.
        fn comes_back(&self) -> bool {
            false
        }
        /// The place the caller already holds in the wait line of one of `members` (the slot
        /// in ms): a job rescheduled for its place by a refusal of that limit, once per
        /// execution.
        fn ticket(&self, _members: &[&str]) -> Option<i64> {
            None
        }
    }
}

/// Where the limits of a call live: a store, or nowhere (everything is granted). Opaque
/// outside hench.
pub struct Backend(pub(crate) Option<Store>);

impl Backend {
    /// Nowhere: everything is granted.
    pub(crate) fn unlimited() -> Self {
        Backend(None)
    }

    /// `store`.
    pub(crate) fn store(store: Store) -> Self {
        Backend(Some(store))
    }
}

impl std::fmt::Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            None => f.write_str("Unlimited"),
            Some(Store::Redis(pool)) => write!(f, "Redis({pool:?})"),
            Some(Store::Memory(_)) => f.write_str("Memory"),
        }
    }
}

/// A store of limits: Redis, or the in-memory table of testing clients.
#[derive(Clone)]
pub(crate) enum Store {
    /// Redis, through the limiter scripts.
    Redis(RedisPool),
    /// The in-memory table of a testing client (shared by its clones).
    Memory(Arc<Tables>),
}

impl Store {
    /// The in-memory table, for a memory store.
    fn table(&self) -> Option<&LimitTable> {
        match self {
            Store::Memory(tables) => Some(&tables.limits),
            Store::Redis(_) => None,
        }
    }

    /// Checks and grants `args`' rules (all or none).
    pub(crate) async fn acquire(&self, args: &AcquireArgs<'_>) -> Result<Acquired> {
        match (self, self.table()) {
            (_, Some(table)) => Ok(table.acquire(args)),
            (Store::Redis(pool), None) => scripts::acquire(pool, args).await,
            (Store::Memory(_), None) => Ok(Acquired::Granted {
                reclaimed: 0,
                held: false,
                mismatch: None,
                now_ms: 0,
            }),
        }
    }

    /// Gives leases back.
    pub(crate) async fn release(&self, leases: &[LeaseRef]) -> Result<Vec<ReleaseOutcome>> {
        match (self, self.table()) {
            (_, Some(table)) => Ok(table.release(leases, None)),
            (Store::Redis(pool), None) => scripts::release(pool, leases, None).await,
            (Store::Memory(_), None) => Ok(Vec::new()),
        }
    }

    /// Extends leases.
    pub(crate) async fn renew(&self, leases: &[LeaseRef]) -> Result<Vec<RenewOutcome>> {
        match (self, self.table()) {
            (_, Some(table)) => Ok(table.renew(leases, None)),
            (Store::Redis(pool), None) => scripts::renew(pool, leases, None).await,
            (Store::Memory(_), None) => Ok(Vec::new()),
        }
    }

    /// Corrects a points grant by `delta` units.
    pub(crate) async fn adjust(
        &self,
        rule: &Rule,
        delta: i64,
        stats_ttl: Option<Duration>,
    ) -> Result<f64> {
        match (self, self.table()) {
            (_, Some(table)) => Ok(table.adjust(rule, delta, None)),
            (Store::Redis(pool), None) => scripts::adjust(pool, rule, delta, stats_ttl, None).await,
            (Store::Memory(_), None) => Ok(0.0),
        }
    }

    /// Reads the use of identities.
    pub(crate) async fn usage(&self, reads: &[UsageArgs]) -> Result<Vec<UsageReply>> {
        match (self, self.table()) {
            (_, Some(table)) => Ok(table.usage(reads, None)),
            (Store::Redis(pool), None) => scripts::usage(pool, reads, None).await,
            (Store::Memory(_), None) => Ok(Vec::new()),
        }
    }

    /// Deletes the state and counters of rules' identities.
    pub(crate) async fn reset(&self, rules: &[Rule]) -> Result<u64> {
        match (self, self.table()) {
            (_, Some(table)) => Ok(table.reset(rules)),
            (Store::Redis(pool), None) => {
                let targets: Vec<_> = rules.iter().map(scripts::reset_target).collect();
                scripts::reset(pool, &targets).await
            }
            (Store::Memory(_), None) => Ok(0),
        }
    }
}

/// The backend of a client: its Redis, or for a testing client its in-memory table when its
/// limits run in memory (unlimited otherwise).
pub(crate) fn client_backend(client: &Client) -> Backend {
    match client.mode() {
        ClientMode::Fake(_) | ClientMode::Inline(_) => {
            let tables = client.tables();
            if tables.limits.mode() == TestLimits::InMemory {
                Backend::store(Store::Memory(Arc::clone(tables)))
            } else {
                Backend::unlimited()
            }
        }
        ClientMode::Redis | ClientMode::Collect(_) => {
            Backend::store(Store::Redis(client.redis().clone()))
        }
    }
}

impl LimitTarget for RedisPool {}
impl sealed::Sealed for RedisPool {
    fn backend(&self) -> Backend {
        Backend::store(Store::Redis(self.clone()))
    }
}

impl LimitTarget for Client {}
impl sealed::Sealed for Client {
    fn backend(&self) -> Backend {
        client_backend(self)
    }
}

impl LimitTarget for Hench {}
impl sealed::Sealed for Hench {
    fn backend(&self) -> Backend {
        Backend::store(Store::Redis(self.redis().clone()))
    }
}

impl LimitTarget for JobContext {}
impl sealed::Sealed for JobContext {
    fn backend(&self) -> Backend {
        client_backend(self.client())
    }

    fn stopping(&self) -> Option<CancellationToken> {
        Some(self.stopping_token().clone())
    }

    fn comes_back(&self) -> bool {
        true
    }

    fn ticket(&self, members: &[&str]) -> Option<i64> {
        self.take_limit_ticket(members)
    }
}

impl<T: LimitTarget + ?Sized> LimitTarget for &T {}
impl<T: sealed::Sealed + ?Sized> sealed::Sealed for &T {
    fn backend(&self) -> Backend {
        (**self).backend()
    }

    fn stopping(&self) -> Option<CancellationToken> {
        (**self).stopping()
    }

    fn comes_back(&self) -> bool {
        (**self).comes_back()
    }

    fn ticket(&self, members: &[&str]) -> Option<i64> {
        (**self).ticket(members)
    }
}
