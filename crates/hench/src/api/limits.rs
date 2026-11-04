//! [`Limits`]: the limit identities of a Redis, for dashboards and admin tools
//! (`docs/design/ratelimit.md` §2.13): their use, counters, holders and wait lines, the
//! per-minute grants and refusals of each limiter, and resets.
//!
//! An identity is one limit of one limiter or throttle instance, written `<kind>:<id>`: the
//! letter of its state's kind (`c` concurrency, `w` window, `b` bucket, `g` leaky buckets and
//! points budgets) and its id, `<name>[:<key>]` (`w:stripe:42`). Identities are listed while
//! their counters live (the `limits` index, scored by the counters' expiry: a day after the
//! last use by default); orders other than [`LimitOrder::LastUsed`] and filters look at the
//! 10,000 most recently used ones.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use futures::stream::Stream;

use crate::api::events::{self, EventKind};
use crate::api::{Api, JobRecord, Page, SortedEntry, batched};
use crate::error::Result;
use crate::keys;
use crate::limit::rule::{self, parse_signature};
use crate::limit::scripts::{self, UsageArgs, UsageReply};
use crate::limit::{Limit, LimitKind, LimitOrder, Limited, ThrottleOption, ThrottleRef};
use crate::metrics::Period;
use crate::payload::{millis_to_system_time, now_secs_f64};
use crate::redis::RedisPool;

/// The largest page served.
const MAX_PAGE_SIZE: usize = 1_000;

/// The identities that orders other than "last used" and filters look at: the most recently
/// used ones.
pub(crate) const SORT_WINDOW: usize = 10_000;

/// Holders listed at most.
const MAX_HOLDERS: isize = 1_000;

/// Scheduled jobs [`Limits::waiting`] looks at, at most, and matches it returns at most.
const WAITING_SCAN: usize = 50_000;
const MAX_WAITING: usize = 1_000;

/// Identities read per round trip by [`Limits::entries`].
const ENTRIES_BATCH: usize = 100;

/// The limit identities of a Redis ([`Api::limits`]). Cheap to clone.
#[derive(Clone)]
pub struct Limits {
    api: Api,
}

impl fmt::Debug for Limits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Limits").finish_non_exhaustive()
    }
}

impl Api {
    /// The limit identities of rate limiting: limiters, throttles and their counters.
    pub fn limits(&self) -> Limits {
        Limits { api: self.clone() }
    }
}

/// One limit identity: its limit, current use and counters.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct LimitInfo {
    /// The shape of its state (points budgets are [`LimitKind::Leaky`] with a
    /// [`Limit::Points`] spec).
    pub kind: LimitKind,
    /// Its id: `<name>[:<key>]` (keys longer than 128 bytes are stored hashed, `#…`).
    pub id: String,
    /// The limiter's or throttle's name (`stripe`; a throttle's second limit of one kind and
    /// scope is `stripe~2`).
    pub name: String,
    /// The key, for per-key limits.
    pub key: Option<String>,
    /// The queue of a queue's limits (ids `@<queue>`, see
    /// [`Queue::set_limits`](crate::api::Queue::set_limits)); `None` for a limiter's or a
    /// throttle's.
    pub queue: Option<String>,
    /// The limit, read from the signature its last caller stored (`None` when unknown).
    pub spec: Option<Limit>,
    /// Units in use now (leaky buckets and points: the fractional level).
    pub used: f64,
    /// How long until a unit is free (`Some(ZERO)`: one is free now; concurrency: the earliest
    /// lease end, a worst case; `None` when unknown).
    pub next_free_in: Option<Duration>,
    /// Grants.
    pub granted: u64,
    /// Refusals (counted once per place in the wait line).
    pub refused: u64,
    /// Grants that waited first, and their total wait.
    pub waited: u64,
    /// The total wait of the grants that waited.
    pub wait_time: Duration,
    /// Concurrency: leases given back, and their total hold time.
    pub releases: u64,
    /// Concurrency: the total hold time of the leases given back.
    pub hold_time: Duration,
    /// Concurrency: ended leases taken back by grants (holders that did not give theirs back:
    /// a crash, or a release that never arrived).
    pub reclaimed: u64,
    /// Concurrency: holders that kept running after their lease ended and the slot was given
    /// to another (renewals did not keep up).
    pub overages: u64,
    /// Concurrency: ended leases restored by their holder's renewal (the slot was still free).
    pub restored: u64,
    /// Points: cost corrections ([`Permit::points_used`](crate::limit::Permit::points_used)).
    pub adjustments: u64,
    /// When the wait line clears (`None`: no one is waiting).
    pub line_ends_at: Option<SystemTime>,
    /// The last grant or refusal.
    pub last_used: Option<SystemTime>,
}

impl LimitInfo {
    /// The identity as written in the index and in `limited.by`: `<kind>:<id>`.
    pub fn identity(&self) -> String {
        format!("{}:{}", self.kind.letter(), self.id)
    }

    /// The capacity of the limit, when known.
    pub fn capacity(&self) -> Option<u32> {
        self.spec.as_ref().map(Limit::capacity)
    }

    /// The fraction of the capacity in use (at least 0; above 1 for points debt), when the
    /// capacity is known.
    pub fn fraction_used(&self) -> Option<f64> {
        self.capacity()
            .map(|capacity| (self.used / f64::from(capacity.max(1))).max(0.0))
    }

    /// The average wait of the grants that waited.
    pub fn average_wait(&self) -> Option<Duration> {
        u32::try_from(self.waited)
            .ok()
            .filter(|&waited| waited > 0)
            .map(|waited| self.wait_time / waited)
    }

    /// Concurrency: the average hold time of the leases given back.
    pub fn average_hold(&self) -> Option<Duration> {
        u32::try_from(self.releases)
            .ok()
            .filter(|&releases| releases > 0)
            .map(|releases| self.hold_time / releases)
    }

    /// The identity's record from its `LIMIT_USAGE` reply, `None` when its state and counters
    /// are both gone.
    fn from_reply(kind: LimitKind, id: &str, reply: &UsageReply) -> Option<Self> {
        let used = reply.used.unwrap_or(0.0);
        if reply.stats.is_empty() && used <= 0.0 {
            return None;
        }
        let stats = &reply.stats;
        let count = |field: &str| {
            stats
                .get(field)
                .and_then(|value| value.parse::<i64>().ok())
                .map_or(0, |value| u64::try_from(value).unwrap_or(0))
        };
        let millis = |field: &str| stats.get(field).and_then(|value| value.parse::<i64>().ok());
        let (name, key) = split_id(id);
        Some(Self {
            kind,
            id: id.to_owned(),
            name: name.to_owned(),
            key,
            queue: name.strip_prefix('@').map(str::to_owned),
            spec: stats
                .get("p")
                .and_then(|signature| parse_signature(signature)),
            used,
            next_free_in: reply.free_in,
            granted: count("g"),
            refused: count("r"),
            waited: count("w"),
            wait_time: Duration::from_millis(count("wms")),
            releases: count("h"),
            hold_time: Duration::from_millis(count("hms")),
            reclaimed: count("rc"),
            overages: count("ov"),
            restored: count("restored"),
            adjustments: count("adj"),
            line_ends_at: millis("line")
                .filter(|&line| line > reply.now_ms)
                .map(millis_to_system_time),
            last_used: millis("last").map(millis_to_system_time),
        })
    }
}

/// A holder of a concurrency limit: its token (for a throttled job, its jid followed by `~`
/// and 16 hex digits naming the execution; a random token for a
/// [`Limiter`](crate::limit::Limiter)'s permit) and the end of its lease.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Holder {
    /// The token.
    pub token: String,
    /// The jid of the throttled job holding it (`None` for a limiter's permit).
    pub jid: Option<String>,
    /// When its lease ends unless renewed (Redis's clock).
    pub lease_ends_at: SystemTime,
    /// Whether the lease has ended (the holder crashed or its renewals did not keep up; the
    /// next grant takes the slot back).
    pub ended: bool,
}

/// The grants and refusals of the limiters, per name and minute ([`Limits::events`]).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct LimitEvents {
    /// Grants and refusals over the period, all limiters together.
    pub totals: (u64, u64),
    /// Per limiter name: every minute of the period, oldest first, with its grants and
    /// refusals.
    pub by_name: BTreeMap<String, Vec<(SystemTime, u64, u64)>>,
}

impl LimitEvents {
    /// The grants and refusals of `name` over the period.
    pub fn totals_of(&self, name: &str) -> (u64, u64) {
        self.by_name.get(name).map_or((0, 0), |minutes| {
            minutes
                .iter()
                .fold((0, 0), |(g, r), &(_, granted, refused)| {
                    (g + granted, r + refused)
                })
        })
    }

    /// The `n` names with the most refusals over the period (only names with refusals), most
    /// first, with their grants and refusals.
    pub fn most_refused(&self, n: usize) -> Vec<(String, u64, u64)> {
        let mut names: Vec<(String, u64, u64)> = self
            .by_name
            .keys()
            .map(|name| {
                let (granted, refused) = self.totals_of(name);
                (name.clone(), granted, refused)
            })
            .filter(|(_, _, refused)| *refused > 0)
            .collect();
        names.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        names.truncate(n);
        names
    }
}

impl Limits {
    /// The number of identities in use (their counters live).
    pub async fn size(&self) -> Result<u64> {
        let live = live_bound();
        let live = live.as_str();
        self.redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZCOUNT")
                    .arg(keys::LIMITS)
                    .arg(live)
                    .arg("+inf")
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// One page of identities (numbered from 1, at most 1000 per page) in `order`. Identities
    /// found gone (their state and counters evicted or deleted) are skipped and dropped from the
    /// index.
    pub async fn page(
        &self,
        page: usize,
        size: usize,
        order: LimitOrder,
    ) -> Result<Page<LimitInfo>> {
        self.page_matching(page, size, order, "").await
    }

    /// [`Limits::page`] of the identities whose id (`<name>[:<key>]`) contains `filter`, `*`
    /// standing for any run of characters (`stripe*42`); an empty filter matches every
    /// identity. Filters look at the 10,000 most recently used identities.
    pub async fn page_matching(
        &self,
        page: usize,
        size: usize,
        order: LimitOrder,
        filter: &str,
    ) -> Result<Page<LimitInfo>> {
        let number = page.max(1);
        let size = size.min(MAX_PAGE_SIZE);
        let offset = (number - 1).saturating_mul(size);
        let (members, total) = if order == LimitOrder::LastUsed && filter.is_empty() {
            let total = self.size().await?;
            let members = if size == 0 {
                Vec::new()
            } else {
                self.recent(offset, size).await?
            };
            (members, total)
        } else {
            let mut members = self.recent(0, SORT_WINDOW).await?;
            if !filter.is_empty() {
                let pattern = Pattern::new(filter);
                members.retain(|member| {
                    split_member(member).is_some_and(|(_, id)| pattern.matches(id))
                });
            }
            match order {
                LimitOrder::Name => members.sort_by(|a, b| {
                    let (a, b) = (split_member(a), split_member(b));
                    a.map(|(kind, id)| (id, kind))
                        .cmp(&b.map(|(kind, id)| (id, kind)))
                }),
                LimitOrder::Refused => {
                    let refused = self.refusals(&members).await?;
                    // Stable: equal counts stay most recently used first.
                    let mut ranked: Vec<(u64, String)> = refused.into_iter().zip(members).collect();
                    ranked.sort_by_key(|(refused, _)| std::cmp::Reverse(*refused));
                    members = ranked.into_iter().map(|(_, member)| member).collect();
                }
                _ => {}
            }
            let total = u64::try_from(members.len()).unwrap_or(u64::MAX);
            let page: Vec<String> = members.into_iter().skip(offset).take(size).collect();
            (page, total)
        };
        let items = self.infos(&members).await?;
        Ok(Page {
            items,
            number,
            size,
            total,
        })
    }

    /// Every identity in use, most recently used first, read 100 at a time. Pin the stream to
    /// read it (`std::pin::pin!`). Identities used during the iteration may appear twice or be
    /// missed, as with any live data; each is yielded at most once. The stream leaves gone
    /// index entries in place (pages and grants drop them): removing them would move the
    /// entries after them into the positions it already read.
    pub fn entries(&self) -> impl Stream<Item = Result<LimitInfo>> + Send + 'static {
        struct State {
            limits: Limits,
            offset: usize,
            seen: HashSet<String>,
            done: bool,
        }
        let state = State {
            limits: self.clone(),
            offset: 0,
            seen: HashSet::new(),
            done: false,
        };
        batched(state, |mut state| async move {
            if state.done {
                return Ok(None);
            }
            let members = state.limits.recent(state.offset, ENTRIES_BATCH).await?;
            if members.len() < ENTRIES_BATCH {
                state.done = true;
            }
            if members.is_empty() {
                return Ok(None);
            }
            state.offset += members.len();
            let fresh: Vec<String> = members
                .into_iter()
                .filter(|member| state.seen.insert(member.clone()))
                .collect();
            let infos = state.limits.read(&fresh, false).await?;
            Ok(Some((infos, state)))
        })
    }

    /// The identity `<kind>:<id>`, `None` when it is not in use. A key longer than 128 bytes
    /// may be given as it was used or as stored (`#…`).
    pub async fn get(&self, kind: LimitKind, id: &str) -> Result<Option<LimitInfo>> {
        let id = stored_id(id);
        let reply = scripts::usage(
            self.redis(),
            &[UsageArgs::identity(kind.letter(), &id)],
            None,
        )
        .await?;
        Ok(reply
            .first()
            .and_then(|reply| LimitInfo::from_reply(kind, &id, reply)))
    }

    /// The holders of the concurrency limit `id` (at most 1000), earliest lease end first.
    pub async fn holders(&self, id: &str) -> Result<Vec<Holder>> {
        let state = keys::limit_state(LimitKind::Concurrency.letter(), &stored_id(id));
        let state = state.as_str();
        let (leases, time): (Vec<(String, f64)>, (String, String)) = self
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::pipe()
                    .cmd("ZRANGE")
                    .arg(state)
                    .arg(0)
                    .arg(MAX_HOLDERS - 1)
                    .arg("WITHSCORES")
                    .cmd("TIME")
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        let now_ms = redis_time_ms(&time);
        Ok(leases
            .into_iter()
            .map(|(token, ends)| {
                // Lease ends are whole milliseconds of Redis time.
                #[allow(
                    clippy::cast_possible_truncation,
                    reason = "lease ends are whole milliseconds, far below 2^53"
                )]
                let ends_ms = ends as i64;
                Holder {
                    jid: crate::limit::engine::token_jid(&token).map(str::to_owned),
                    token,
                    lease_ends_at: millis_to_system_time(ends_ms),
                    ended: now_ms.is_some_and(|now| ends_ms <= now),
                }
            })
            .collect())
    }

    /// Resets the identity `<kind>:<id>`: deletes its state and counters (every slot is free at
    /// once; holders keep running, and their releases count overages). `true` when it existed.
    pub async fn reset(&self, kind: LimitKind, id: &str) -> Result<bool> {
        let id = stored_id(id);
        let letter = kind.letter();
        let deleted = scripts::reset(
            self.redis(),
            &[(
                keys::limit_state(letter, &id),
                keys::limit_stats(letter, &id),
                format!("{letter}:{id}"),
            )],
        )
        .await?;
        Ok(deleted > 0)
    }

    /// Clears the wait line of `<kind>:<id>`: the next refused caller starts a new one (callers
    /// already parked keep their slots). `true` when there was one.
    pub async fn clear_line(&self, kind: LimitKind, id: &str) -> Result<bool> {
        let stats = keys::limit_stats(kind.letter(), &stored_id(id));
        scripts::clear_line(self.redis(), &stats).await
    }

    /// The grants and refusals per limiter name and minute over `period` (minutes only: the
    /// counters are kept 8 hours).
    pub async fn events(&self, period: Period) -> Result<LimitEvents> {
        let minutes = match period {
            Period::Minutes(minutes) => minutes,
            Period::Hours(_) => 0,
        };
        let read = events::read(self.redis(), EventKind::Limits, period, Utc::now()).await?;
        let mut out = LimitEvents::default();
        let Some(starts_at) = read.starts_at else {
            return Ok(out);
        };
        let names: BTreeSet<&str> = read.totals.keys().map(|(name, _)| name.as_str()).collect();
        for ((_, event), count) in &read.totals {
            match event.as_str() {
                "g" => out.totals.0 += count,
                "r" => out.totals.1 += count,
                _ => {}
            }
        }
        let minute_of = |index: u32| starts_at + chrono::Duration::minutes(i64::from(index));
        for name in names {
            let series = (0..minutes)
                .map(|index| {
                    let at = minute_of(index);
                    let counts = read.series.get(&at);
                    let count = |event: &str| {
                        counts
                            .and_then(|counts| counts.get(&(name.to_owned(), event.to_owned())))
                            .copied()
                            .unwrap_or(0)
                    };
                    (SystemTime::from(at), count("g"), count("r"))
                })
                .collect();
            out.by_name.insert(name.to_owned(), series);
        }
        Ok(out)
    }

    /// The scheduled jobs a limit identity rescheduled last (their `limited.by` is
    /// `<kind>:<id>`), earliest first: at most `max` (at most 1000), among the 50,000 earliest
    /// scheduled jobs. Redis looks for the label as the engine writes it (`"by":"<label>"`), a
    /// thousand jobs per call, and sends only the jobs holding it; each is then checked.
    pub async fn waiting(&self, kind: LimitKind, id: &str, max: usize) -> Result<Vec<SortedEntry>> {
        let label = format!("{}:{}", kind.letter(), stored_id(id));
        let max = max.min(MAX_WAITING);
        if max == 0 {
            return Ok(Vec::new());
        }
        let text = format!("\"by\":{}", serde_json::to_string(&label)?);
        self.api
            .scheduled()
            .first_containing(&text, WAITING_SCAN, max, |entry| {
                entry
                    .record()
                    .limited()
                    .is_some_and(|limited| limited.by == label)
            })
            .await
    }

    fn redis(&self) -> &RedisPool {
        self.api.redis()
    }

    /// Index members, most recently used first: `count` from `offset`.
    async fn recent(&self, offset: usize, count: usize) -> Result<Vec<String>> {
        let live = live_bound();
        let live = live.as_str();
        self.redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZREVRANGEBYSCORE")
                    .arg(keys::LIMITS)
                    .arg("+inf")
                    .arg(live)
                    .arg("LIMIT")
                    .arg(offset)
                    .arg(count)
                    .query_async(&mut conn)
                    .await
            })
            .await
    }

    /// The refusals of each member (0 when unknown).
    async fn refusals(&self, members: &[String]) -> Result<Vec<u64>> {
        let mut out = Vec::with_capacity(members.len());
        for chunk in members.chunks(MAX_PAGE_SIZE) {
            let mut pipe = ::redis::pipe();
            for member in chunk {
                let stats = split_member(member)
                    .map(|(kind, id)| keys::limit_stats(kind.letter(), id))
                    .unwrap_or_default();
                pipe.cmd("HGET").arg(stats).arg("r");
            }
            let pipe = &pipe;
            let counts: Vec<Option<String>> = self
                .redis()
                .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
                .await?;
            out.extend(
                counts
                    .into_iter()
                    .map(|count| count.and_then(|count| count.parse().ok()).unwrap_or(0)),
            );
        }
        Ok(out)
    }

    /// The records of `members` (in their order), skipping the gone ones, which `trim` drops
    /// from the index.
    async fn read(&self, members: &[String], trim: bool) -> Result<Vec<LimitInfo>> {
        let parsed: Vec<(&String, LimitKind, &str)> = members
            .iter()
            .filter_map(|member| split_member(member).map(|(kind, id)| (member, kind, id)))
            .collect();
        let reads: Vec<UsageArgs> = parsed
            .iter()
            .map(|(_, kind, id)| UsageArgs::identity(kind.letter(), id))
            .collect();
        let replies = scripts::usage(self.redis(), &reads, None).await?;
        let mut infos = Vec::with_capacity(parsed.len());
        let mut gone = Vec::new();
        for ((member, kind, id), reply) in parsed.into_iter().zip(&replies) {
            match LimitInfo::from_reply(kind, id, reply) {
                Some(info) => infos.push(info),
                None => gone.push((
                    keys::limit_state(kind.letter(), id),
                    keys::limit_stats(kind.letter(), id),
                    member.clone(),
                )),
            }
        }
        if trim
            && !gone.is_empty()
            && let Err(error) = scripts::trim(self.redis(), &gone).await
        {
            tracing::debug!(%error, "could not drop gone limit identities from the index");
        }
        Ok(infos)
    }

    /// [`Limits::read`], for a page.
    async fn infos(&self, members: &[String]) -> Result<Vec<LimitInfo>> {
        if members.is_empty() {
            return Ok(Vec::new());
        }
        self.read(members, true).await
    }
}

impl JobRecord {
    /// The `limited` field of a job a limiter or throttle rescheduled (reschedules so far, the
    /// identity that refused last, when): `None` when absent or malformed.
    pub fn limited(&self) -> Option<Limited> {
        serde_json::from_value(self.get("limited")?.clone()).ok()
    }

    /// The throttle the job names (`throttle: {"name", "key"}`): `None` when it names none, or
    /// the field is malformed.
    pub fn throttle(&self) -> Option<ThrottleRef> {
        match serde_json::from_value(self.get("throttle")?.clone()).ok()? {
            ThrottleOption::Enabled {
                name: Some(name),
                key,
            } => Some(ThrottleRef { name, key }),
            _ => None,
        }
    }
}

/// The kind and id of an index member (`w:stripe:42`).
pub(crate) fn split_member(member: &str) -> Option<(LimitKind, &str)> {
    let (letter, id) = member.split_once(':')?;
    let mut letters = letter.chars();
    let kind = LimitKind::from_letter(letters.next()?)?;
    (letters.next().is_none() && !id.is_empty()).then_some((kind, id))
}

/// `id` with its key as stored (hashed when longer than 128 bytes).
fn stored_id(id: &str) -> String {
    match split_id(id) {
        (name, Some(key)) => rule::id(name, Some(&rule::stored_key(&key))),
        (_, None) => id.to_owned(),
    }
}

/// The name and key of an id: the first `:` separates them (names have none), except in a
/// queue's limits (`@<queue>`), which have no key and keep every character of the queue's name.
fn split_id(id: &str) -> (&str, Option<String>) {
    if id.starts_with(crate::limit::queue::PREFIX) {
        return (id, None);
    }
    match id.split_once(':') {
        Some((name, key)) => (name, Some(key.to_owned())),
        None => (id, None),
    }
}

/// The lowest index score of a live identity: now (exclusive), in epoch seconds.
fn live_bound() -> String {
    format!("({:.3}", now_secs_f64())
}

/// Milliseconds of a `TIME` reply.
fn redis_time_ms((seconds, micros): &(String, String)) -> Option<i64> {
    let seconds: i64 = seconds.parse().ok()?;
    let micros: i64 = micros.parse().ok()?;
    Some(seconds * 1_000 + micros / 1_000)
}

/// A filter: text an id must contain, `*` standing for any run of characters (the dashboard's
/// rule).
struct Pattern {
    parts: Vec<String>,
}

impl Pattern {
    fn new(filter: &str) -> Self {
        Self {
            parts: filter
                .split('*')
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect(),
        }
    }

    fn matches(&self, text: &str) -> bool {
        let mut rest = text;
        for part in &self.parts {
            match rest.find(part.as_str()) {
                Some(start) => rest = &rest[start + part.len()..],
                None => return false,
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(used: Option<f64>, stats: &[(&str, &str)], now_ms: i64) -> UsageReply {
        UsageReply {
            used,
            free_in: Some(Duration::ZERO),
            stats: stats
                .iter()
                .map(|(field, value)| ((*field).to_owned(), (*value).to_owned()))
                .collect(),
            now_ms,
        }
    }

    #[test]
    fn members_split_into_kind_and_id() {
        assert_eq!(
            split_member("w:stripe:42"),
            Some((LimitKind::Window, "stripe:42"))
        );
        assert_eq!(split_member("g:erp"), Some((LimitKind::Leaky, "erp")));
        assert_eq!(split_member("x:erp"), None);
        assert_eq!(split_member("ww:erp"), None);
        assert_eq!(split_member("w:"), None);
        assert_eq!(split_member("w"), None);
    }

    #[test]
    fn records_are_read_from_the_counters() {
        let info = LimitInfo::from_reply(
            LimitKind::Concurrency,
            "stripe~2:acme:eu",
            &reply(
                Some(3.0),
                &[
                    ("p", "c|5"),
                    ("g", "10"),
                    ("r", "4"),
                    ("w", "2"),
                    ("wms", "3000"),
                    ("h", "8"),
                    ("hms", "16000"),
                    ("rc", "1"),
                    ("ov", "2"),
                    ("restored", "1"),
                    ("line", "1800000009000"),
                    ("last", "1800000000000"),
                ],
                1_800_000_001_000,
            ),
        )
        .unwrap();
        assert_eq!(info.name, "stripe~2");
        assert_eq!(info.key.as_deref(), Some("acme:eu"));
        assert_eq!(info.identity(), "c:stripe~2:acme:eu");
        assert_eq!(info.spec, Some(Limit::Concurrency { limit: 5 }));
        assert_eq!(info.capacity(), Some(5));
        assert_eq!(info.fraction_used(), Some(0.6));
        assert_eq!((info.granted, info.refused), (10, 4));
        assert_eq!(info.average_wait(), Some(Duration::from_millis(1_500)));
        assert_eq!(info.average_hold(), Some(Duration::from_secs(2)));
        assert_eq!((info.reclaimed, info.overages, info.restored), (1, 2, 1));
        assert_eq!(
            info.line_ends_at,
            Some(millis_to_system_time(1_800_000_009_000))
        );
        assert_eq!(
            info.last_used,
            Some(millis_to_system_time(1_800_000_000_000))
        );
        assert_eq!(info.queue, None);
        // A line in the past is no line.
        let info = LimitInfo::from_reply(
            LimitKind::Window,
            "@mailers",
            &reply(Some(0.0), &[("line", "5"), ("g", "x")], 10),
        )
        .unwrap();
        assert_eq!(info.line_ends_at, None);
        assert_eq!(info.queue.as_deref(), Some("mailers"));
        assert_eq!(info.granted, 0);
        assert_eq!(info.spec, None);
        assert_eq!(info.fraction_used(), None);
        // Gone: no counters, nothing used.
        assert!(LimitInfo::from_reply(LimitKind::Window, "x", &reply(None, &[], 10)).is_none());
        assert!(
            LimitInfo::from_reply(LimitKind::Concurrency, "x", &reply(Some(1.0), &[], 10))
                .is_some()
        );
    }

    #[test]
    fn long_keys_are_looked_up_as_stored() {
        let long = "k".repeat(200);
        let stored = stored_id(&format!("stripe:{long}"));
        assert!(stored.starts_with("stripe:#"));
        assert_eq!(stored_id(&stored), stored);
        assert_eq!(stored_id("stripe"), "stripe");
        assert_eq!(stored_id("stripe:42"), "stripe:42");
    }

    #[test]
    fn events_rank_names_by_refusals() {
        let at = SystemTime::UNIX_EPOCH;
        let mut events = LimitEvents::default();
        events
            .by_name
            .insert("a".into(), vec![(at, 5, 0), (at, 1, 0)]);
        events
            .by_name
            .insert("b".into(), vec![(at, 1, 3), (at, 0, 4)]);
        events.by_name.insert("c".into(), vec![(at, 0, 7)]);
        assert_eq!(events.totals_of("b"), (1, 7));
        assert_eq!(events.totals_of("nope"), (0, 0));
        assert_eq!(
            events.most_refused(5),
            [("b".to_owned(), 1, 7), ("c".to_owned(), 0, 7)]
        );
        assert_eq!(events.most_refused(1).len(), 1);
    }

    #[test]
    fn filters_match_pieces_in_order() {
        let pattern = Pattern::new("stripe*42");
        assert!(pattern.matches("stripe:42"));
        assert!(pattern.matches("stripe~2:acme-42"));
        assert!(!pattern.matches("42:stripe"));
        assert!(Pattern::new("").matches("anything"));
    }

    #[test]
    fn redis_times_are_read() {
        assert_eq!(
            redis_time_ms(&("1800000000".into(), "123456".into())),
            Some(1_800_000_000_123)
        );
        assert_eq!(redis_time_ms(&("x".into(), "1".into())), None);
    }
}
