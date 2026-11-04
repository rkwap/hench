//! [`Queue`]: one job queue.

use std::collections::BTreeMap;
use std::fmt;

use futures::Stream;

use crate::api::page::{list_page, normalize};
use crate::api::{Api, JobRecord, Page, SortOrder, batched, json_escaped, latency_of_raw};
use crate::error::{Error, Result};
use crate::keys;
use crate::limit::Limit;
use crate::limit::queue as queue_limits;
use crate::redis::RedisPool;

/// Jobs fetched per round trip while iterating a queue.
const ITERATION_PAGE: i64 = 50;

/// Jobs fetched per round trip while searching a queue.
const SEARCH_PAGE: i64 = 500;

/// A queue: a Redis list of jobs, pushed on the left and consumed from the right (Sidekiq's
/// `Queue`). The contents change while you look at them; every method reads live data.
#[derive(Clone)]
pub struct Queue {
    api: Api,
    name: String,
    key: String,
}

impl Queue {
    /// The queue called `name` (which may not exist yet).
    pub fn new(api: impl Into<Api>, name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            api: api.into(),
            key: keys::queue(&name),
            name,
        }
    }

    /// Every known queue (the `queues` set, read with `SSCAN`), sorted by name.
    pub async fn all(api: impl Into<Api>) -> Result<Vec<Queue>> {
        let api = api.into();
        let names = scan_set(api.redis(), keys::QUEUES).await?;
        Ok(names
            .into_iter()
            .map(|name| Queue::new(&api, name))
            .collect())
    }

    /// The queue's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The number of jobs in the queue.
    pub async fn size(&self) -> Result<u64> {
        let key = &self.key;
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("LLEN").arg(key).query_async(&mut conn).await
            })
            .await
    }

    /// How long, in seconds, the oldest job has been waiting (`0.0` for an empty queue or a
    /// job without timestamps).
    pub async fn latency(&self) -> Result<f64> {
        let key = &self.key;
        let oldest: Option<String> = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("LINDEX")
                    .arg(key)
                    .arg(-1)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(oldest.as_deref().map_or(0.0, latency_of_raw))
    }

    /// `true` if the queue is known: it is in the `queues` set, holds jobs, or is paused or
    /// limited (a queue can be paused or limited before its first job). One round trip. A
    /// queue that was [`clear`](Self::clear)ed and is neither paused nor limited no longer
    /// exists.
    pub async fn exists(&self) -> Result<bool> {
        let mut pipe = ::redis::pipe();
        pipe.cmd("SISMEMBER")
            .arg(keys::QUEUES)
            .arg(&self.name)
            .cmd("EXISTS")
            .arg(&self.key)
            .cmd("SISMEMBER")
            .arg(keys::PAUSED)
            .arg(&self.name)
            .cmd("HEXISTS")
            .arg(keys::QUEUE_LIMITS)
            .arg(&self.name);
        let pipe = &pipe;
        let (known, stored, paused, limited): (bool, bool, bool, bool) = self
            .api
            .redis()
            .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
            .await?;
        Ok(known || stored || paused || limited)
    }

    /// `true` if the queue is in the `paused` set.
    pub async fn is_paused(&self) -> Result<bool> {
        let name = &self.name;
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("SISMEMBER")
                    .arg(keys::PAUSED)
                    .arg(name)
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// Pauses the queue: workers stop fetching its jobs (a worker already blocked waiting on
    /// it may still take one job that arrives within its fetch timeout, 2 s by default) until
    /// [`unpause`](Self::unpause). Jobs can still be pushed to a paused queue; they wait.
    /// Pausing is by name and outlives [`clear`](Self::clear). Returns `false` if the queue
    /// was already paused.
    pub async fn pause(&self) -> Result<bool> {
        let name = &self.name;
        let added: u64 = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("SADD")
                    .arg(keys::PAUSED)
                    .arg(name)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(added > 0)
    }

    /// Resumes a paused queue: workers fetch its jobs again at their next fetch. Returns
    /// `false` if the queue was not paused.
    pub async fn unpause(&self) -> Result<bool> {
        let name = &self.name;
        let removed: u64 = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("SREM")
                    .arg(keys::PAUSED)
                    .arg(name)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(removed > 0)
    }

    /// One page of jobs (numbered from 1; at most 1000 per page). [`SortOrder::Descending`]
    /// lists the newest jobs first, [`SortOrder::Ascending`] the next jobs to run first.
    pub async fn page(
        &self,
        page: usize,
        page_size: usize,
        order: SortOrder,
    ) -> Result<Page<JobRecord>> {
        let (number, size) = normalize(page, page_size);
        let (total, values) = list_page(&self.api, &self.key, number, size, order).await?;
        Ok(Page {
            items: values
                .into_iter()
                .map(|value| JobRecord::new(&self.api, value, Some(&self.name)))
                .collect(),
            number,
            size,
            total,
        })
    }

    /// Every job, newest first, fetched 50 at a time (Sidekiq's `Queue#each`). The stream is
    /// not `Unpin`: pin it to read it (`std::pin::pin!`, see the [module docs](super)).
    ///
    /// Deleting the jobs being iterated ([`JobRecord::delete`]) does not make the iteration
    /// skip jobs: each page is offset by how much the queue shrank. Jobs consumed by workers
    /// in the meantime can make a job appear twice.
    pub fn jobs(&self) -> impl Stream<Item = Result<JobRecord>> + Send + 'static {
        struct State {
            queue: Queue,
            page: i64,
            initial_size: Option<i64>,
        }
        let state = State {
            queue: self.clone(),
            page: 0,
            initial_size: None,
        };
        batched(state, |mut state| async move {
            let queue = &state.queue;
            let start = match state.initial_size {
                None => {
                    let size = i64::try_from(queue.size().await?).unwrap_or(i64::MAX);
                    state.initial_size = Some(size);
                    0
                }
                Some(initial) => {
                    let current = i64::try_from(queue.size().await?).unwrap_or(i64::MAX);
                    let deleted = initial.saturating_sub(current);
                    state
                        .page
                        .saturating_mul(ITERATION_PAGE)
                        .saturating_sub(deleted)
                        .max(0)
                }
            };
            let values = queue.range(start, start + ITERATION_PAGE - 1).await?;
            if values.is_empty() {
                return Ok(None);
            }
            state.page += 1;
            let records = values
                .into_iter()
                .map(|value| JobRecord::new(&queue.api, value, Some(&queue.name)))
                .collect();
            Ok(Some((records, state)))
        })
    }

    /// The job with this jid, if it is in the queue. This reads the whole queue (in pages of
    /// 500): an O(n) operation for admin tools, not for application logic.
    pub async fn find_job(&self, jid: &str) -> Result<Option<JobRecord>> {
        if jid.is_empty() {
            return Ok(None);
        }
        // Cheap substring check (on the jid as written in JSON) before parsing.
        let needle = json_escaped(jid);
        let mut start = 0;
        loop {
            let values = self.range(start, start + SEARCH_PAGE - 1).await?;
            if values.is_empty() {
                return Ok(None);
            }
            for value in values {
                if value.contains(&needle) {
                    let record = JobRecord::new(&self.api, value, Some(&self.name));
                    if record.jid() == Some(jid) {
                        return Ok(Some(record));
                    }
                }
            }
            start += SEARCH_PAGE;
        }
    }

    /// The limits on every fetch of the queue (see [`set_limits`](Self::set_limits)); empty
    /// when it has none. Fails with [`Error::Config`] when its stored limits are not valid
    /// (workers then leave the queue alone until they are set again or cleared).
    pub async fn limits(&self) -> Result<Vec<Limit>> {
        let name = &self.name;
        match self.stored_limits().await? {
            None => Ok(Vec::new()),
            Some(text) => queue_limits::parse(name, &text).map_err(|error| {
                let reason = match error {
                    Error::Config(reason) => reason,
                    other => other.to_string(),
                };
                Error::Config(format!(
                    "the stored limits of the queue `{}` (`{}`) are not valid: {reason}",
                    name.escape_debug(),
                    text.escape_debug()
                ))
            }),
        }
    }

    /// Limits every fetch of the queue, replacing its limits: concurrency, window, bucket and
    /// leaky limits, at most one of each kind (see [`crate::limit::Limit`]). A queue over its
    /// limits is skipped like a paused one: its jobs keep their order and the other queues keep
    /// flowing. Every process applies the new limits within a heartbeat interval (10 s by
    /// default); a process that starts with limits for the queue in its configuration
    /// (`HenchBuilder::queue_limits`, the worker file's `[queue_limits]`) writes its own back.
    ///
    /// Returns the limits as stored: each limit's spec in the long form, in the order
    /// concurrency, window, bucket, leaky, joined by `;` (`concurrency=3;window=100/1m`).
    ///
    /// Fails with [`Error::Config`] for an empty list, a points limit, two limits of one kind or
    /// an invalid limit.
    pub async fn set_limits(&self, limits: &[Limit]) -> Result<String> {
        queue_limits::check(&self.name, limits)?;
        let name = &self.name;
        let stored = queue_limits::stored_form(limits);
        let value = &stored;
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("HSET")
                    .arg(keys::QUEUE_LIMITS)
                    .arg(name)
                    .arg(value)
                    .exec_async(&mut conn)
                    .await
            })
            .await?;
        Ok(stored)
    }

    /// [`set_limits`](Self::set_limits) with limits written in the spec grammar, joined by
    /// `;` (`"concurrency=3; window=100/1m"`, see [`crate::limit`]): for limits typed by
    /// people (the dashboard, the `hench` command). Returns the limits as stored.
    pub async fn set_limits_from_str(&self, specs: &str) -> Result<String> {
        let limits = queue_limits::parse(&self.name, specs)?;
        self.set_limits(&limits).await
    }

    /// The queue's limits exactly as stored, valid or not (`None` without limits): what the
    /// dashboard shows and lets people edit; [`limits`](Self::limits) parses them.
    pub async fn stored_limits(&self) -> Result<Option<String>> {
        let name = &self.name;
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("HGET")
                    .arg(keys::QUEUE_LIMITS)
                    .arg(name)
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// Removes the queue's limits: every process fetches it freely again within a heartbeat
    /// interval. Returns `false` if it had none. The counters of its limits (`Api::limits`,
    /// identities `<kind>:@<queue>`) stay until they expire.
    pub async fn clear_limits(&self) -> Result<bool> {
        let name = &self.name;
        let removed: u64 = self
            .api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("HDEL")
                    .arg(keys::QUEUE_LIMITS)
                    .arg(name)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(removed > 0)
    }

    /// The limits of every limited queue, by queue name, as stored: each queue's specs joined by
    /// `;` (`concurrency=3;window=100/1m`); [`Queue::limits`] parses one.
    pub async fn all_limits(api: impl Into<Api>) -> Result<BTreeMap<String, String>> {
        let api = api.into();
        let stored: Vec<(Vec<u8>, Vec<u8>)> = api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("HGETALL")
                    .arg(keys::QUEUE_LIMITS)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        Ok(stored
            .into_iter()
            .map(|(queue, specs)| {
                (
                    String::from_utf8_lossy(&queue).into_owned(),
                    String::from_utf8_lossy(&specs).into_owned(),
                )
            })
            .collect())
    }

    /// Deletes every job in the queue and forgets the queue (removes it from `queues`), in
    /// one transaction. Its pause and its limits, set by name, stay.
    pub async fn clear(&self) -> Result<()> {
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("UNLINK")
            .arg(&self.key)
            .ignore()
            .cmd("SREM")
            .arg(keys::QUEUES)
            .arg(&self.name)
            .ignore();
        let pipe = &pipe;
        self.api
            .redis()
            .with_retry(|mut conn| async move { pipe.exec_async(&mut conn).await })
            .await
    }

    async fn range(&self, start: i64, end: i64) -> Result<Vec<String>> {
        let key = &self.key;
        self.api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("LRANGE")
                    .arg(key)
                    .arg(start)
                    .arg(end)
                    .query_async(&mut conn)
                    .await
            })
            .await
    }
}

impl fmt::Debug for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Queue").field(&self.name).finish()
    }
}

impl PartialEq for Queue {
    /// Queues are equal when they have the same name.
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

/// Every member of a set, sorted and without duplicates (`SSCAN` may return a member twice),
/// read with `SSCAN` in chunks so that a huge set never blocks Redis.
pub(crate) async fn scan_set(redis: &RedisPool, key: &str) -> Result<Vec<String>> {
    let mut members = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, chunk): (u64, Vec<String>) = redis
            .with_retry(|mut conn| async move {
                ::redis::cmd("SSCAN")
                    .arg(key)
                    .arg(cursor)
                    .arg("COUNT")
                    .arg(1_000)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        members.extend(chunk);
        if next == 0 {
            members.sort_unstable();
            members.dedup();
            return Ok(members);
        }
        cursor = next;
    }
}
