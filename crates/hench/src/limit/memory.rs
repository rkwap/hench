//! Rate limiting in the testing modes (`docs/design/ratelimit.md` §2.14): unlimited by default,
//! or enforced in memory on tokio's clock ([`TestLimits::InMemory`]), so that
//! `tokio::time::pause()` and `advance()` step through windows deterministically.
//!
//! The in-memory table is a port of the limiter scripts: the same five algorithms, the same
//! wait line, the same leases, the same answers (a parity test feeds both one sequence).

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::client::Client;
use crate::error::JobError;
use crate::limit::rule::{Rule, parse_signature, period_of};
use crate::limit::scripts::{
    AcquireArgs, Acquired, DEFAULT_HOLD, LeaseRef, Place, ReleaseOutcome, RenewOutcome, UsageArgs,
    UsageReply,
};
use crate::limit::throttle::Throttles;
use crate::limit::{Limited, OverLimit, Reschedule, Throttle, reschedule, wakeups};
use crate::payload::JobPayload;
use crate::pro::{Classified, Fetched, Gate};
use crate::testing::TestLimits;

/// The in-memory limiter state of a testing client, its clones and its collecting clients.
#[derive(Debug, Default)]
pub(crate) struct LimitTable {
    inner: Mutex<TableState>,
}

/// The counters, signature, wait line and decaying average hold time of one identity.
#[derive(Debug, Clone, Default)]
struct Stats {
    signature: Option<String>,
    last: i64,
    line: Option<i64>,
    /// The decaying average of hold times (ms, one decimal, as the script stores it).
    hold: Option<f64>,
    counters: HashMap<&'static str, i64>,
}

impl Stats {
    fn count(&mut self, field: &'static str, by: i64) {
        if by != 0 {
            *self.counters.entry(field).or_default() += by;
        }
    }

    /// The hash as Redis would hold it.
    fn as_map(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = self
            .counters
            .iter()
            .map(|(field, value)| ((*field).to_owned(), value.to_string()))
            .collect();
        if let Some(signature) = &self.signature {
            map.insert("p".to_owned(), signature.clone());
            map.insert("last".to_owned(), self.last.to_string());
        }
        if let Some(line) = self.line {
            map.insert("line".to_owned(), line.to_string());
        }
        if let Some(hold) = self.hold {
            map.insert("hold".to_owned(), format!("{hold:.1}"));
        }
        map
    }
}

/// One decimal, as the release script stores the average hold time (`%.1f`).
fn round1(value: f64) -> f64 {
    format!("{value:.1}").parse().unwrap_or(value)
}

/// The leases the testing executor's throttle gate holds for a running job.
#[derive(Debug, Clone)]
struct HeldLeases {
    leases: Vec<LeaseRef>,
}

#[derive(Debug, Default)]
struct TableState {
    mode: TestLimits,
    throttles: Throttles,
    /// The origin of the table's clock (tokio's), set on first use.
    epoch: Option<tokio::time::Instant>,
    /// Concurrency states (by state key): token → lease end (ms).
    leases: HashMap<String, HashMap<String, i64>>,
    /// Window states: grant times, newest first, at most `limit`.
    grants: HashMap<String, VecDeque<i64>>,
    /// Bucket states: the bucket's index and the units used in it.
    buckets: HashMap<String, (i64, u64)>,
    /// Leaky and points states: the theoretical arrival time (ms, 3 decimals).
    levels: HashMap<String, f64>,
    stats: HashMap<String, Stats>,
    /// Leases held by the throttle gate, per throttle name and key (the latest last: an inline
    /// job running inside another one finishes first).
    held: HashMap<(String, Option<String>), Vec<HeldLeases>>,
    /// Numbers the tokens of the gate's leases.
    next_token: u64,
}

/// The check of one rule (`docs/design/ratelimit.md` §2.3), as the scripts compute it.
#[derive(Debug, Clone, Copy)]
struct Check {
    ok: bool,
    /// ms until the rule could grant (-1: never).
    wait: f64,
    held: bool,
    /// Units used in the bucket, or the new level of a leaky bucket.
    extra: f64,
    spacing: f64,
}

/// Three decimals, as the scripts store levels (`%.3f`).
fn round3(value: f64) -> f64 {
    format!("{value:.3}").parse().unwrap_or(value)
}

fn as_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

impl TableState {
    fn check(&self, rule: &Rule, args: &AcquireArgs<'_>, now: i64) -> Check {
        let limit = i64::from(rule.limit.capacity());
        let period = as_i64(period_of(&rule.limit, args.lease));
        let cost = if rule.is_concurrency() {
            1
        } else {
            i64::from(args.cost)
        };
        let refused = |wait: f64| Check {
            ok: false,
            wait,
            held: false,
            extra: 0.0,
            spacing: 0.0,
        };
        let granted = |extra: f64, held: bool| Check {
            ok: true,
            wait: 0.0,
            held,
            extra,
            spacing: 0.0,
        };
        match rule.letter() {
            'c' => {
                let leases = self.leases.get(&rule.state);
                if !args.token.is_empty()
                    && leases
                        .and_then(|leases| leases.get(args.token))
                        .is_some_and(|end| *end > now)
                {
                    return granted(0.0, true);
                }
                let live = leases.map_or(0, |leases| {
                    leases.values().filter(|end| **end > now).count()
                });
                if i64::try_from(live).unwrap_or(i64::MAX) < limit {
                    granted(0.0, false)
                } else {
                    refused(0.0)
                }
            }
            'w' => {
                if cost > limit {
                    return refused(-1.0);
                }
                let edge = self.grants.get(&rule.state).and_then(|grants| {
                    usize::try_from(limit - cost)
                        .ok()
                        .and_then(|at| grants.get(at))
                });
                match edge {
                    Some(edge) if *edge > now - period => refused((edge + period - now) as f64),
                    _ => granted(0.0, false),
                }
            }
            'b' => {
                if cost > limit {
                    return refused(-1.0);
                }
                let bucket = now.div_euclid(period);
                let used = match self.buckets.get(&rule.state) {
                    Some((index, used)) if *index == bucket => as_i64(*used),
                    _ => 0,
                };
                if used + cost <= limit {
                    granted(used as f64, false)
                } else {
                    refused(((bucket + 1) * period - now) as f64)
                }
            }
            _ => {
                if cost > limit {
                    return refused(-1.0);
                }
                let interval = period as f64 / limit as f64;
                let tat = self
                    .levels
                    .get(&rule.state)
                    .copied()
                    .unwrap_or(now as f64)
                    .max(now as f64);
                let new_tat = tat + cost as f64 * interval;
                if new_tat - now as f64 <= period as f64 {
                    granted(new_tat, false)
                } else {
                    refused((new_tat - period as f64 - now as f64).ceil())
                }
            }
        }
    }

    fn grant(&mut self, rule: &Rule, args: &AcquireArgs<'_>, now: i64, check: &Check) -> u64 {
        let period = as_i64(period_of(&rule.limit, args.lease));
        let limit = usize::try_from(rule.limit.capacity()).unwrap_or(usize::MAX);
        match rule.letter() {
            'c' => {
                let leases = self.leases.entry(rule.state.clone()).or_default();
                leases.insert(args.token.to_owned(), now + period);
                let before = leases.len();
                leases.retain(|_, end| *end > now);
                u64::try_from(before - leases.len()).unwrap_or(0)
            }
            'w' => {
                let grants = self.grants.entry(rule.state.clone()).or_default();
                for _ in 0..args.cost {
                    grants.push_front(now);
                }
                grants.truncate(limit);
                0
            }
            'b' => {
                self.buckets.insert(
                    rule.state.clone(),
                    (
                        now.div_euclid(period),
                        check.extra as u64 + u64::from(args.cost),
                    ),
                );
                0
            }
            _ => {
                self.levels.insert(rule.state.clone(), round3(check.extra));
                0
            }
        }
    }

    /// The bookkeeping of a use (the scripts' `hl_touch`); the signature stored before when it
    /// differed.
    fn touch(&mut self, rule: &Rule, now: i64, field: &'static str) -> Option<String> {
        let stats = self.stats.entry(rule.stats.clone()).or_default();
        let signature = rule.signature();
        let previous = stats
            .signature
            .as_ref()
            .filter(|stored| **stored != signature)
            .cloned();
        stats.count(field, 1);
        stats.signature = Some(signature);
        stats.last = now;
        previous
    }

    fn acquire(&mut self, args: &AcquireArgs<'_>, now: i64) -> Acquired {
        let keep_stats = args.stats;
        let keep_line = keep_stats && args.line;
        let mut checks: Vec<Check> = args
            .rules
            .iter()
            .map(|rule| self.check(rule, args, now))
            .collect();
        let held = checks.iter().any(|check| check.held);
        if checks.iter().all(|check| check.ok) {
            let mut reclaimed = Vec::with_capacity(args.rules.len());
            for (rule, check) in args.rules.iter().zip(&checks) {
                reclaimed.push(self.grant(rule, args, now, check));
            }
            let mut mismatch = None;
            if keep_stats {
                let waited = i64::try_from(args.waited.as_millis()).unwrap_or(i64::MAX);
                for (rule, reclaimed) in args.rules.iter().zip(&reclaimed) {
                    let previous = self.touch(rule, now, "g");
                    if mismatch.is_none() {
                        mismatch = previous;
                    }
                    let stats = self.stats.entry(rule.stats.clone()).or_default();
                    if waited > 0 {
                        stats.count("w", 1);
                        stats.count("wms", waited);
                    }
                    stats.count("rc", as_i64(*reclaimed));
                }
            }
            return Acquired::Granted {
                reclaimed: reclaimed.iter().sum(),
                held,
                mismatch,
                now_ms: now,
            };
        }
        // Refused: the scripts' computation, step by step.
        let mut own = -1.0_f64;
        let mut earliest = 0.0_f64;
        let mut widest = 0.0_f64;
        let mut rate_only = true;
        let mut limiting = 0;
        let hold = args.expected_hold.unwrap_or(DEFAULT_HOLD);
        for (index, (rule, check)) in args.rules.iter().zip(checks.iter_mut()).enumerate() {
            if check.ok {
                continue;
            }
            if check.wait < 0.0 {
                return Acquired::Refused {
                    wait_ms: 0,
                    limiting: index,
                    impossible: true,
                    slot_ms: -1,
                    now_ms: now,
                    place: Place::None,
                    mismatch: None,
                };
            }
            if rule.is_concurrency() {
                rate_only = false;
                let capacity = f64::from(rule.limit.capacity());
                let mut spacing = rule.default_spacing_ms(hold) as f64;
                if keep_stats && let Some(stats) = self.stats.get(&rule.stats) {
                    let holds = stats.counters.get("h").copied().unwrap_or(0);
                    let held_ms = stats.counters.get("hms").copied().unwrap_or(0);
                    match stats.hold {
                        Some(recent) if recent > 0.0 => spacing = recent / capacity,
                        _ if holds > 0 => spacing = held_ms as f64 / holds as f64 / capacity,
                        _ => {}
                    }
                }
                check.spacing = spacing.max(1.0);
                check.wait = check.spacing;
            } else {
                let period = period_of(&rule.limit, args.lease) as f64;
                check.spacing = period * f64::from(args.cost) / f64::from(rule.limit.capacity());
                earliest = earliest.max(check.wait);
            }
            widest = widest.max(check.spacing);
            if check.wait > own {
                own = check.wait;
                limiting = index;
            }
        }
        let mut start = now as f64 + own;
        let mut place = Place::None;
        if keep_line {
            if let Some(slot) = args.ticket {
                if slot as f64 >= now as f64 + earliest {
                    return Acquired::Refused {
                        wait_ms: u64::try_from(slot - now).unwrap_or(0),
                        limiting,
                        impossible: false,
                        slot_ms: slot,
                        now_ms: now,
                        place: Place::Kept,
                        mismatch: None,
                    };
                }
                if rate_only && slot as f64 >= now as f64 - (2.0 * widest).max(100.0) {
                    return Acquired::Refused {
                        wait_ms: own.floor().max(0.0) as u64,
                        limiting,
                        impossible: false,
                        slot_ms: (now as f64 + own).floor() as i64,
                        now_ms: now,
                        place: Place::Kept,
                        mismatch: None,
                    };
                }
            }
            let lines: Vec<f64> = args
                .rules
                .iter()
                .map(|rule| {
                    self.stats
                        .get(&rule.stats)
                        .and_then(|stats| stats.line)
                        .map_or(0.0, |line| line as f64)
                })
                .collect();
            for (check, line) in checks.iter().zip(&lines) {
                if !check.ok && *line > start {
                    start = *line;
                }
            }
            let within = args.place_within.map(|within| within.as_millis() as f64);
            if within.is_none_or(|within| start <= now as f64 + within) {
                place = Place::New;
                for ((rule, check), line) in args.rules.iter().zip(&checks).zip(&lines) {
                    if !check.ok {
                        let next = line.max(start + check.spacing).floor() as i64;
                        self.stats.entry(rule.stats.clone()).or_default().line = Some(next);
                    }
                }
            }
        }
        let mut mismatch = None;
        if keep_stats {
            for (rule, check) in args.rules.iter().zip(&checks) {
                if !check.ok {
                    let previous = self.touch(rule, now, "r");
                    if mismatch.is_none() {
                        mismatch = previous;
                    }
                }
            }
        }
        Acquired::Refused {
            wait_ms: (start - now as f64).floor().max(0.0) as u64,
            limiting,
            impossible: false,
            slot_ms: start.floor() as i64,
            now_ms: now,
            place,
            mismatch,
        }
    }

    fn release(&mut self, leases: &[LeaseRef], now: i64) -> Vec<ReleaseOutcome> {
        leases
            .iter()
            .map(|lease| {
                let end = self
                    .leases
                    .get_mut(&lease.state)
                    .and_then(|holders| holders.remove(&lease.token));
                if self.leases.get(&lease.state).is_some_and(HashMap::is_empty) {
                    self.leases.remove(&lease.state);
                }
                let keep = lease.stats_ttl_ms.is_some();
                let stats = self.stats.entry(lease.stats.clone()).or_default();
                match end {
                    None => {
                        if keep {
                            stats.count("ov", 1);
                        }
                        ReleaseOutcome::Missing
                    }
                    Some(end) => {
                        if keep && let Some(acquired) = lease.acquired_ms {
                            let held = (now - acquired).max(0);
                            stats.count("h", 1);
                            stats.count("hms", held);
                            let held = held as f64;
                            let average = match stats.hold {
                                Some(recent) => recent + (held - recent) / 5.0,
                                None => held,
                            };
                            stats.hold = Some(round1(average));
                        }
                        if end <= now {
                            if keep {
                                stats.count("ov", 1);
                            }
                            ReleaseOutcome::Late
                        } else {
                            ReleaseOutcome::Released
                        }
                    }
                }
            })
            .collect()
    }

    fn renew(&mut self, leases: &[LeaseRef], now: i64) -> Vec<RenewOutcome> {
        leases
            .iter()
            .map(|lease| {
                let lease_ms = as_i64(lease.lease_ms);
                let holders = self.leases.entry(lease.state.clone()).or_default();
                let result = match holders.get(&lease.token) {
                    Some(end) if *end > now => {
                        holders.insert(lease.token.clone(), now + lease_ms);
                        RenewOutcome::Renewed
                    }
                    _ => {
                        let live = holders.values().filter(|end| **end > now).count();
                        if u32::try_from(live).unwrap_or(u32::MAX) < lease.limit {
                            holders.insert(lease.token.clone(), now + lease_ms);
                            RenewOutcome::Restored
                        } else {
                            holders.remove(&lease.token);
                            RenewOutcome::Lost
                        }
                    }
                };
                if lease.stats_ttl_ms.is_some() {
                    let stats = self.stats.entry(lease.stats.clone()).or_default();
                    match result {
                        RenewOutcome::Restored => stats.count("restored", 1),
                        RenewOutcome::Lost => stats.count("ov", 1),
                        RenewOutcome::Renewed => {}
                    }
                }
                result
            })
            .collect()
    }

    fn adjust(&mut self, rule: &Rule, delta: i64, now: i64) -> f64 {
        let capacity = f64::from(rule.limit.capacity());
        let refill = period_of(&rule.limit, Duration::ZERO) as f64;
        let interval = refill / capacity;
        let tat = self
            .levels
            .get(&rule.state)
            .copied()
            .unwrap_or(now as f64)
            .max(now as f64);
        let mut new_tat = tat + delta as f64 * interval;
        if new_tat <= now as f64 {
            self.levels.remove(&rule.state);
            new_tat = now as f64;
        } else {
            self.levels.insert(rule.state.clone(), round3(new_tat));
        }
        if delta != 0 {
            self.stats
                .entry(rule.stats.clone())
                .or_default()
                .count("adj", 1);
        }
        round3((new_tat - now as f64) / interval)
    }

    fn usage(&self, read: &UsageArgs, now: i64) -> UsageReply {
        let stats = self.stats.get(&read.stats);
        let limit = read.limit.or_else(|| {
            let limit = parse_signature(stats?.signature.as_deref()?)?;
            Some((limit.capacity(), period_of(&limit, Duration::ZERO)))
        });
        let (used, free_in) = match (read.letter, limit) {
            ('c', limit) => {
                let mut ends: Vec<i64> = self
                    .leases
                    .get(&read.state)
                    .map_or_else(Vec::new, |holders| {
                        holders.values().copied().filter(|end| *end > now).collect()
                    });
                ends.sort_unstable();
                let live = ends.len();
                let free_in = limit.and_then(|(limit, _)| {
                    if u32::try_from(live).unwrap_or(u32::MAX) < limit {
                        Some(0)
                    } else {
                        ends.first().map(|end| end - now)
                    }
                });
                (Some(live as f64), free_in)
            }
            (_, None) => (None, None),
            ('w', Some((limit, period))) => {
                let period = as_i64(period);
                let limit = usize::try_from(limit).unwrap_or(usize::MAX);
                let grants: Vec<i64> = self
                    .grants
                    .get(&read.state)
                    .map_or_else(Vec::new, |grants| {
                        grants.iter().take(limit).copied().collect()
                    });
                let count = grants.iter().filter(|at| **at > now - period).count();
                let free_in = if count >= limit {
                    grants
                        .get(limit.saturating_sub(1))
                        .map(|at| (at + period - now).max(0))
                } else {
                    Some(0)
                };
                (Some(count as f64), free_in)
            }
            ('b', Some((limit, period))) => {
                let period = as_i64(period);
                let bucket = now.div_euclid(period);
                let used = match self.buckets.get(&read.state) {
                    Some((index, used)) if *index == bucket => *used,
                    _ => 0,
                };
                let free_in = if used >= u64::from(limit) {
                    (bucket + 1) * period - now
                } else {
                    0
                };
                (Some(used as f64), Some(free_in))
            }
            (_, Some((limit, period))) => {
                let interval = period as f64 / f64::from(limit);
                let tat = self
                    .levels
                    .get(&read.state)
                    .copied()
                    .unwrap_or(now as f64)
                    .max(now as f64);
                let next = tat + interval;
                let free_in = if next - now as f64 <= period as f64 {
                    0
                } else {
                    (next - period as f64 - now as f64).ceil() as i64
                };
                (Some(round3((tat - now as f64) / interval)), Some(free_in))
            }
        };
        UsageReply {
            used,
            free_in: free_in
                .and_then(|ms| u64::try_from(ms).ok())
                .map(Duration::from_millis),
            stats: stats.map(Stats::as_map).unwrap_or_default(),
            now_ms: now,
        }
    }
}

impl LimitTable {
    fn lock(&self) -> MutexGuard<'_, TableState> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets how the testing clients sharing this table apply limits, and their throttles (what
    /// `Testing::limits` and `Testing::throttle` configure).
    pub(crate) fn configure(
        &self,
        mode: TestLimits,
        throttles: impl IntoIterator<Item = Throttle>,
    ) {
        let mut state = self.lock();
        state.mode = mode;
        state.throttles = Throttles::new(throttles);
    }

    /// How the testing clients sharing this table apply limits.
    pub(crate) fn mode(&self) -> TestLimits {
        self.lock().mode
    }

    /// The time on the table's clock (tokio's), in ms since its first use.
    fn clock(state: &mut TableState) -> i64 {
        let now = tokio::time::Instant::now();
        let epoch = *state.epoch.get_or_insert(now);
        i64::try_from(now.saturating_duration_since(epoch).as_millis()).unwrap_or(i64::MAX)
    }

    /// The time of a call: a fixed time (parity tests), else the table's clock.
    fn time(state: &mut TableState, fixed: Option<i64>) -> i64 {
        fixed.unwrap_or_else(|| Self::clock(state))
    }

    /// `LIMIT_ACQUIRE`, in memory.
    pub(crate) fn acquire(&self, args: &AcquireArgs<'_>) -> Acquired {
        let mut state = self.lock();
        let now = Self::time(&mut state, args.now);
        state.acquire(args, now)
    }

    /// `LIMIT_RELEASE`, in memory (at `now`, else the table's clock).
    pub(crate) fn release(&self, leases: &[LeaseRef], now: Option<i64>) -> Vec<ReleaseOutcome> {
        let mut state = self.lock();
        let now = Self::time(&mut state, now);
        state.release(leases, now)
    }

    /// `LIMIT_RENEW`, in memory (at `now`, else the table's clock).
    pub(crate) fn renew(&self, leases: &[LeaseRef], now: Option<i64>) -> Vec<RenewOutcome> {
        let mut state = self.lock();
        let now = Self::time(&mut state, now);
        state.renew(leases, now)
    }

    /// `LIMIT_ADJUST`, in memory (at `now`, else the table's clock).
    pub(crate) fn adjust(&self, rule: &Rule, delta: i64, now: Option<i64>) -> f64 {
        let mut state = self.lock();
        let now = Self::time(&mut state, now);
        state.adjust(rule, delta, now)
    }

    /// `LIMIT_USAGE`, in memory (at `now`, else the table's clock).
    pub(crate) fn usage(&self, reads: &[UsageArgs], now: Option<i64>) -> Vec<UsageReply> {
        let mut state = self.lock();
        let now = Self::time(&mut state, now);
        reads.iter().map(|read| state.usage(read, now)).collect()
    }

    /// `LIMIT_RESET`, in memory.
    pub(crate) fn reset(&self, rules: &[Rule]) -> u64 {
        let mut state = self.lock();
        let mut deleted = 0;
        for rule in rules {
            let had_state = state.leases.remove(&rule.state).is_some()
                | state.grants.remove(&rule.state).is_some()
                | state.buckets.remove(&rule.state).is_some()
                | state.levels.remove(&rule.state).is_some();
            deleted += u64::from(had_state);
            deleted += u64::from(state.stats.remove(&rule.stats).is_some());
        }
        deleted
    }

    /// The throttle named `name`, when this table enforces limits.
    fn throttle(&self, name: &str) -> Option<Throttle> {
        let state = self.lock();
        if state.mode != TestLimits::InMemory {
            return None;
        }
        state.throttles.get(name).cloned()
    }
}

/// The throttle gate of the testing executor (before server middleware): every job runs unless
/// the client's limits run in memory; then `job`'s throttle is checked on tokio's clock (never
/// waiting) and a grant's concurrency leases are held until [`after`]. A refusal's
/// [`Reschedule`] continues the job's counts, as the workers' gate does.
pub(crate) fn gate(client: &Client, job: &JobPayload, fetched: &Fetched) -> Gate {
    let Some(reference) = &fetched.throttle else {
        return Gate::Run;
    };
    let table = &client.tables().limits;
    let Some(throttle) = table.throttle(&reference.name) else {
        return Gate::Run;
    };
    let rules = match throttle.rules(reference.key.as_deref()) {
        Ok(rules) if rules.is_empty() => return Gate::Run,
        Ok(rules) => rules,
        Err(error) => return Gate::Failed(JobError::from(error)),
    };
    let token = {
        let mut state = table.lock();
        state.next_token += 1;
        format!("testing-{}", state.next_token)
    };
    let stats_ttl = throttle.stats_ttl_duration();
    let members: Vec<&str> = rules.iter().map(|rule| rule.member.as_str()).collect();
    let args = AcquireArgs {
        rules: &rules,
        token: &token,
        cost: 1,
        lease: throttle.lease_duration(),
        ticket: Limited::ticket_for(job, &members),
        place_within: None,
        expected_hold: None,
        line: throttle.has_stats(),
        stats: throttle.has_stats(),
        stats_ttl,
        waited: Duration::ZERO,
        now: None,
    };
    match table.acquire(&args) {
        Acquired::Granted { now_ms, .. } => {
            let leases: Vec<LeaseRef> = rules
                .iter()
                .filter(|rule| rule.is_concurrency())
                .map(|rule| {
                    LeaseRef::of(
                        rule,
                        &token,
                        throttle.lease_duration(),
                        Some(stats_ttl),
                        Some(now_ms),
                    )
                })
                .collect();
            if !leases.is_empty() {
                table
                    .lock()
                    .held
                    .entry((reference.name.clone(), reference.key.clone()))
                    .or_default()
                    .push(HeldLeases { leases });
            }
            Gate::Run
        }
        Acquired::Refused {
            wait_ms,
            limiting,
            slot_ms,
            place,
            ..
        } => {
            let rule = &rules[limiting.min(rules.len() - 1)];
            let delay = reschedule::throttle_delay(
                Duration::from_millis(wait_ms),
                throttle.max_delay_duration(),
                args.line,
            );
            let place = match place {
                Place::New => Some(slot_ms),
                Place::Kept => args.ticket,
                Place::None => None,
            };
            Gate::Refused(Reschedule::throttled(
                job,
                rule.member.clone(),
                delay,
                place,
            ))
        }
    }
}

/// Releases the in-memory leases the gate holds for a job the testing executor ran.
pub(crate) fn after(client: &Client, fetched: &Fetched, _classified: &Classified) {
    let Some(reference) = &fetched.throttle else {
        return;
    };
    let table = &client.tables().limits;
    let held = {
        let mut state = table.lock();
        let key = (reference.name.clone(), reference.key.clone());
        let held = state.held.get_mut(&key).and_then(Vec::pop);
        if state.held.get(&key).is_some_and(Vec::is_empty) {
            state.held.remove(&key);
        }
        held
    };
    if let Some(held) = held {
        table.release(&held.leases, None);
        for lease in &held.leases {
            wakeups::notify(&lease.state);
        }
    }
}

/// What a testing client does with a job refused by its throttle (or by a limiter while it
/// ran), as a worker would: park it (capture it again, scheduled at its slot, `limited`
/// counted), or fail it once its budget is spent.
#[derive(Debug)]
pub(crate) enum Parked {
    /// The job to capture again: `at` its slot, `limited` updated, no `enqueued_at`.
    Again(Box<JobPayload>),
    /// The budget is spent: the job fails with this error (class `hench::OverLimit`).
    Exhausted(JobError),
}

/// The fate of `job` (as fetched) after a refusal decided by `reschedule` (the gate's, or an
/// over-limit decision, whose `limited` already continues the job's counts), at `now` (epoch
/// seconds) and `now_ms`; `budget` is the parkings a throttle allows (`None`: unlimited, a
/// throttle's default, or an over-limit decision, which checked its own budget).
pub(crate) fn park(
    job: &JobPayload,
    reschedule: &Reschedule,
    budget: Option<u32>,
    now: f64,
    now_ms: i64,
) -> Parked {
    if let Some(max) = budget
        && reschedule.limited.throttled > max
    {
        return Parked::Exhausted(reschedule::exhausted(
            &OverLimit::new(reschedule.label.clone()),
            max,
        ));
    }
    let mut parked = job.clone();
    let mut limited = reschedule.limited.clone();
    limited.at = Some(now_ms);
    parked.limited = Some(limited);
    parked.at = Some(now + reschedule.delay.as_secs_f64().max(0.001));
    parked.enqueued_at = None;
    Parked::Again(Box::new(parked))
}

/// The reschedule budget of the throttle a testing client's gate refused `fetched` with
/// (`None`: unlimited).
pub(crate) fn throttle_budget(client: &Client, fetched: &Fetched) -> Option<u32> {
    let reference = fetched.throttle.as_ref()?;
    client
        .tables()
        .limits
        .throttle(&reference.name)?
        .reschedule_budget()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limit::Limit;

    fn rule(spec: &str, id: &str) -> Rule {
        Rule::new(spec.parse().unwrap(), id.to_owned(), id.to_owned())
    }

    fn args<'a>(rules: &'a [Rule], token: &'a str, cost: u32, now: i64) -> AcquireArgs<'a> {
        AcquireArgs {
            rules,
            token,
            cost,
            lease: Duration::from_secs(30),
            ticket: None,
            place_within: None,
            expected_hold: None,
            line: false,
            stats: true,
            stats_ttl: Duration::from_secs(86_400),
            waited: Duration::ZERO,
            now: Some(now),
        }
    }

    fn granted(acquired: &Acquired) -> bool {
        matches!(acquired, Acquired::Granted { .. })
    }

    fn wait(acquired: &Acquired) -> u64 {
        match acquired {
            Acquired::Refused { wait_ms, .. } => *wait_ms,
            Acquired::Granted { .. } => panic!("granted"),
        }
    }

    #[test]
    fn windows_slide() {
        let table = LimitTable::default();
        let rules = [rule("w=3/1s", "w")];
        for _ in 0..3 {
            assert!(granted(&table.acquire(&args(&rules, "", 1, 0))));
        }
        assert_eq!(wait(&table.acquire(&args(&rules, "", 1, 0))), 1_000);
        assert_eq!(wait(&table.acquire(&args(&rules, "", 1, 999))), 1);
        assert!(granted(&table.acquire(&args(&rules, "", 1, 1_000))));
        let impossible = table.acquire(&args(&rules, "", 4, 5_000));
        assert!(matches!(
            impossible,
            Acquired::Refused {
                impossible: true,
                ..
            }
        ));
    }

    #[test]
    fn leaky_buckets_burst_then_drip() {
        let table = LimitTable::default();
        let rules = [Rule::new(
            Limit::Leaky {
                size: 3,
                drain: Duration::from_secs(3),
            },
            "l".into(),
            "l".into(),
        )];
        for _ in 0..3 {
            assert!(granted(&table.acquire(&args(&rules, "", 1, 0))));
        }
        assert_eq!(wait(&table.acquire(&args(&rules, "", 1, 0))), 1_000);
        assert!(granted(&table.acquire(&args(&rules, "", 1, 1_000))));
    }

    #[test]
    fn concurrency_counts_live_leases_by_token() {
        let table = LimitTable::default();
        let rules = [rule("c=2", "c")];
        assert!(granted(&table.acquire(&args(&rules, "a", 1, 0))));
        assert!(granted(&table.acquire(&args(&rules, "b", 1, 0))));
        assert!(!granted(&table.acquire(&args(&rules, "c", 1, 0))));
        // The same token again: granted without a new slot.
        assert!(matches!(
            table.acquire(&args(&rules, "a", 1, 10)),
            Acquired::Granted { held: true, .. }
        ));
        // After the leases ended: granted, two reclaimed.
        assert!(matches!(
            table.acquire(&args(&rules, "c", 1, 30_011)),
            Acquired::Granted { reclaimed: 2, .. }
        ));
    }

    /// A fetched job naming the throttle `name`, and its feature state.
    fn throttled(name: &str) -> (JobPayload, Fetched) {
        let mut job = JobPayload::new("Charge", vec![]);
        job.jid = "b".repeat(24);
        job.throttle = Some(crate::limit::ThrottleOption::named(name));
        let fetched = Fetched::from_payload(&job);
        (job, fetched)
    }

    #[tokio::test(start_paused = true)]
    async fn the_gate_enforces_throttles_in_memory_on_tokio_time() {
        let client = Client::fake();
        let table = &client.tables().limits;
        let window = Throttle::new("mem").window(2, Duration::from_secs(1));
        let (job, fetched) = throttled("mem");
        // Unlimited (the default): every job runs.
        table.configure(TestLimits::Unlimited, [window.clone()]);
        for _ in 0..3 {
            assert!(matches!(gate(&client, &job, &fetched), Gate::Run));
        }
        // In memory: two per second; a refusal parks the job until its slot.
        table.configure(TestLimits::InMemory, [window]);
        assert!(matches!(gate(&client, &job, &fetched), Gate::Run));
        assert!(matches!(gate(&client, &job, &fetched), Gate::Run));
        let Gate::Refused(reschedule) = gate(&client, &job, &fetched) else {
            panic!("refused");
        };
        assert_eq!(reschedule.label, "w:mem");
        // Parked until its slot (when the window frees), exactly: its place in the line.
        assert_eq!(reschedule.delay, Duration::from_secs(1));
        assert_eq!(reschedule.limited.throttled, 1);
        assert!(reschedule.limited.slot.is_some());
        // An unknown name runs, as in a worker.
        let (other, other_state) = throttled("other");
        assert!(matches!(gate(&client, &other, &other_state), Gate::Run));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(gate(&client, &job, &fetched), Gate::Run));
    }

    #[tokio::test(start_paused = true)]
    async fn the_gate_holds_concurrency_leases_until_the_outcome() {
        let client = Client::fake();
        let table = &client.tables().limits;
        table.configure(TestLimits::InMemory, [Throttle::new("one").concurrency(1)]);
        let (job, fetched) = throttled("one");
        assert!(matches!(gate(&client, &job, &fetched), Gate::Run));
        assert!(matches!(gate(&client, &job, &fetched), Gate::Refused(_)));
        // The outcome gives the lease back: the next job runs.
        after(&client, &fetched, &Classified::Success);
        assert!(matches!(gate(&client, &job, &fetched), Gate::Run));
        after(&client, &fetched, &Classified::Success);
        // A spent budget is a failure (class `hench::OverLimit`), not another reschedule.
        table.configure(
            TestLimits::InMemory,
            [Throttle::new("one").concurrency(1).reschedules(Some(0))],
        );
        assert!(matches!(gate(&client, &job, &fetched), Gate::Run));
        let Gate::Refused(reschedule) = gate(&client, &job, &fetched) else {
            panic!("refused");
        };
        let budget = throttle_budget(&client, &fetched);
        assert_eq!(budget, Some(0));
        assert!(matches!(
            park(&job, &reschedule, budget, 0.0, 0),
            Parked::Exhausted(_)
        ));
    }

    #[test]
    fn parked_jobs_continue_their_count() {
        let mut job = JobPayload::new("Charge", vec![]);
        job.jid = "a".repeat(24);
        job.enqueued_at = Some(1);
        let parking = |job: &JobPayload| {
            Reschedule::throttled(job, "c:stripe", Duration::from_secs(5), Some(7_000))
        };
        let Parked::Again(parked) = park(&job, &parking(&job), None, 100.0, 100_000) else {
            panic!("parked");
        };
        let limited = parked.limited.clone().unwrap();
        assert_eq!((limited.n, limited.throttled), (1, 1));
        assert_eq!((limited.at, limited.slot), (Some(100_000), Some(7_000)));
        assert_eq!(parked.at, Some(105.0));
        assert_eq!(parked.enqueued_at, None);
        let Parked::Again(again) = park(&parked, &parking(&parked), Some(2), 200.0, 200_000) else {
            panic!("parked");
        };
        assert_eq!(again.limited.as_ref().map(|limited| limited.n), Some(2));
        let Parked::Exhausted(error) = park(&again, &parking(&again), Some(2), 300.0, 300_000)
        else {
            panic!("exhausted");
        };
        assert!(error.is_over_limit());
        assert_eq!(error.class(), JobError::OVER_LIMIT);
    }
}

/// The in-memory algorithms against brute-force models (`docs/design/ratelimit.md` §3.1.7):
/// random arrival times and costs. The table must never grant more than the model allows and
/// must grant whenever the model would, with the same waits; so the two stay in step for the
/// whole sequence. (The scripts are held to the table by the parity test against Redis.)
#[cfg(test)]
mod properties {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::*;
    use crate::limit::{Interval, Limit};

    /// A call at a fixed time, without a wait line (refusals answer their own wait).
    fn args<'a>(
        rules: &'a [Rule],
        token: &'a str,
        cost: u32,
        now: i64,
        lease: u64,
    ) -> AcquireArgs<'a> {
        AcquireArgs {
            rules,
            token,
            cost,
            lease: Duration::from_millis(lease),
            ticket: None,
            place_within: None,
            expected_hold: None,
            line: false,
            stats: true,
            stats_ttl: Duration::from_secs(86_400),
            waited: Duration::ZERO,
            now: Some(now),
        }
    }

    fn rule(limit: Limit, id: &str) -> Rule {
        Rule::new(limit, id.to_owned(), id.to_owned())
    }

    /// `(gap, cost)` steps: arrivals at increasing times (gaps of 0 allowed: bursts).
    fn steps(max_gap: i64, max_cost: u32) -> impl Strategy<Value = Vec<(i64, u32)>> {
        prop::collection::vec((0..=max_gap, 1..=max_cost), 1..120)
    }

    /// The outcome of a call: `None` granted, `Some(wait)` refused.
    fn outcome(acquired: &Acquired) -> Option<u64> {
        match acquired {
            Acquired::Granted { .. } => None,
            Acquired::Refused {
                wait_ms,
                impossible,
                ..
            } => {
                assert!(!impossible, "{acquired:?}");
                Some(*wait_ms)
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// Window N per P: every unit granted at its time; a call of cost c is granted when the
        /// units of the last P ms plus c fit in N, else it waits until enough of them leave.
        #[test]
        fn windows_match_the_model(
            limit in 1_u32..=8,
            period in 50_i64..=2_000,
            steps in steps(400, 8),
        ) {
            let table = LimitTable::default();
            let rules = [rule(
                Limit::Window { limit, period: Duration::from_millis(period as u64) },
                "w",
            )];
            let mut units: Vec<i64> = Vec::new();
            let mut now = 0_i64;
            for (gap, cost) in steps {
                now += gap;
                let cost = cost.min(limit);
                let mut recent: Vec<i64> =
                    units.iter().copied().filter(|at| *at > now - period).collect();
                recent.sort_unstable_by(|a, b| b.cmp(a));
                let fits = recent.len() + cost as usize <= limit as usize;
                let got = outcome(&table.acquire(&args(&rules, "", cost, now, 0)));
                if fits {
                    prop_assert_eq!(got, None, "at {} with {:?}", now, recent);
                    units.extend(std::iter::repeat_n(now, cost as usize));
                } else {
                    // The unit that must leave: the (limit - cost)-th newest.
                    let edge = recent[(limit - cost) as usize];
                    prop_assert_eq!(got, Some((edge + period - now) as u64));
                }
                // Safety: no period ever holds more than the limit.
                let in_window = units.iter().filter(|at| **at > now - period).count();
                prop_assert!(in_window <= limit as usize);
            }
        }

        /// Bucket N per aligned interval I: units counted per `floor(t / I)`; a refusal waits
        /// for the next bucket.
        #[test]
        fn buckets_match_the_model(
            limit in 1_u32..=8,
            interval in 50_i64..=1_000,
            steps in steps(300, 8),
        ) {
            let table = LimitTable::default();
            let rules = [rule(
                Limit::Bucket {
                    limit,
                    interval: Interval::Every(Duration::from_millis(interval as u64)),
                },
                "b",
            )];
            let mut used: HashMap<i64, u32> = HashMap::new();
            let mut now = 0_i64;
            for (gap, cost) in steps {
                now += gap;
                let cost = cost.min(limit);
                let bucket = now.div_euclid(interval);
                let in_bucket = used.get(&bucket).copied().unwrap_or(0);
                let got = outcome(&table.acquire(&args(&rules, "", cost, now, 0)));
                if in_bucket + cost <= limit {
                    prop_assert_eq!(got, None);
                    *used.entry(bucket).or_default() += cost;
                } else {
                    prop_assert_eq!(got, Some(((bucket + 1) * interval - now) as u64));
                }
                prop_assert!(used.values().all(|units| *units <= limit));
            }
        }

        /// Leaky bucket of N draining one unit per I ms (D = N·I), as a quantity of drain left
        /// in ms, in integers: drains as time passes, a call of cost c fits when the drain left
        /// plus c·I is at most D, else it waits for the excess to drain.
        #[test]
        fn leaky_buckets_match_the_model(
            size in 1_u32..=8,
            unit in 1_i64..=200,
            steps in steps(400, 8),
        ) {
            let table = LimitTable::default();
            let drain = unit * i64::from(size);
            let rules = [rule(
                Limit::Leaky { size, drain: Duration::from_millis(drain as u64) },
                "g",
            )];
            let mut left = 0_i64;
            let mut last = 0_i64;
            let mut grants: Vec<(i64, u32)> = Vec::new();
            let mut now = 0_i64;
            for (gap, cost) in steps {
                now += gap;
                let cost = cost.min(size);
                left = (left - (now - last)).max(0);
                last = now;
                let needed = i64::from(cost) * unit;
                let got = outcome(&table.acquire(&args(&rules, "", cost, now, 0)));
                if left + needed <= drain {
                    prop_assert_eq!(got, None, "at {} with {} ms left", now, left);
                    left += needed;
                    grants.push((now, cost));
                } else {
                    prop_assert_eq!(got, Some((left + needed - drain) as u64));
                }
            }
            // The classic bound: over any span, at most a full bucket plus the drained units.
            for (i, (start, _)) in grants.iter().enumerate() {
                let mut units = 0_i64;
                for (at, cost) in &grants[i..] {
                    units += i64::from(*cost);
                    prop_assert!(units <= i64::from(size) + (at - start) / unit);
                }
            }
        }

        /// Concurrency N with leases: acquisitions (idempotent per token), releases and
        /// renewals by a handful of tokens; never more than N live leases.
        #[test]
        fn concurrency_matches_the_model(
            limit in 1_u32..=4,
            lease in 100_u64..=1_000,
            ops in prop::collection::vec((0_i64..=300, 0_u8..3, 0_u8..6), 1..150),
        ) {
            let table = LimitTable::default();
            let rules = [rule(Limit::Concurrency { limit }, "c")];
            let mut model: HashMap<String, i64> = HashMap::new();
            let lease_ms = lease as i64;
            let mut now = 0_i64;
            for (gap, op, token) in ops {
                now += gap;
                let token = format!("t{token}");
                let live = |model: &HashMap<String, i64>| {
                    model.values().filter(|end| **end > now).count()
                };
                let reference = LeaseRef::of(
                    &rules[0],
                    &token,
                    Duration::from_millis(lease),
                    Some(Duration::from_secs(86_400)),
                    Some(now),
                );
                match op {
                    0 => {
                        let acquired = table.acquire(&args(&rules, &token, 1, now, lease));
                        let holds = model.get(&token).is_some_and(|end| *end > now);
                        if holds || live(&model) < limit as usize {
                            let ended = model
                                .iter()
                                .filter(|(other, end)| **other != token && **end <= now)
                                .count() as u64;
                            prop_assert_eq!(
                                acquired,
                                Acquired::Granted {
                                    reclaimed: ended,
                                    held: holds,
                                    mismatch: None,
                                    now_ms: now,
                                }
                            );
                            model.insert(token, now + lease_ms);
                            model.retain(|_, end| *end > now);
                        } else {
                            prop_assert!(outcome(&acquired).is_some());
                        }
                    }
                    1 => {
                        let expected = match model.remove(&token) {
                            None => ReleaseOutcome::Missing,
                            Some(end) if end <= now => ReleaseOutcome::Late,
                            Some(_) => ReleaseOutcome::Released,
                        };
                        prop_assert_eq!(table.release(&[reference], Some(now)), vec![expected]);
                    }
                    _ => {
                        let expected = if model.get(&token).is_some_and(|end| *end > now) {
                            RenewOutcome::Renewed
                        } else if live(&model) < limit as usize {
                            RenewOutcome::Restored
                        } else {
                            RenewOutcome::Lost
                        };
                        if expected == RenewOutcome::Lost {
                            model.remove(&token);
                        } else {
                            model.insert(token, now + lease_ms);
                        }
                        prop_assert_eq!(table.renew(&[reference], Some(now)), vec![expected]);
                    }
                }
                prop_assert!(live(&model) <= limit as usize);
                let usage = table.usage(&[UsageArgs::of(&rules[0])], Some(now));
                prop_assert_eq!(usage[0].used, Some(live(&model) as f64));
            }
        }

        /// A window and a concurrency limit in one call: granted together or not at all; a
        /// refusal by either charges neither.
        #[test]
        fn composite_calls_charge_all_or_none(
            window in 1_u32..=6,
            concurrency in 1_u32..=3,
            ops in prop::collection::vec((0_i64..=200, any::<bool>()), 1..120),
        ) {
            let table = LimitTable::default();
            let period = 1_000_i64;
            let lease = 500_i64;
            let rules = [
                rule(Limit::Window { limit: window, period: Duration::from_millis(period as u64) }, "w"),
                rule(Limit::Concurrency { limit: concurrency }, "c"),
            ];
            let mut units: Vec<i64> = Vec::new();
            let mut holders: Vec<(String, i64)> = Vec::new();
            let mut now = 0_i64;
            for (index, (gap, release_first)) in ops.into_iter().enumerate() {
                now += gap;
                if release_first && let Some((token, _)) = holders.first().cloned() {
                    let reference = LeaseRef::of(
                        &rules[1],
                        &token,
                        Duration::from_millis(lease as u64),
                        None,
                        None,
                    );
                    table.release(&[reference], Some(now));
                    holders.remove(0);
                }
                let token = format!("call{index}");
                let window_fits =
                    units.iter().filter(|at| **at > now - period).count() < window as usize;
                let live = holders.iter().filter(|(_, end)| *end > now).count();
                let concurrency_fits = live < concurrency as usize;
                let got = outcome(&table.acquire(&args(&rules, &token, 1, now, lease as u64)));
                prop_assert_eq!(got.is_none(), window_fits && concurrency_fits);
                if got.is_none() {
                    units.push(now);
                    holders.retain(|(_, end)| *end > now);
                    holders.push((token, now + lease));
                }
                // Whatever the outcome, the window holds exactly the units of the grants.
                let usage = table.usage(&[UsageArgs::of(&rules[0])], Some(now));
                let counted = units.iter().filter(|at| **at > now - period).count();
                prop_assert_eq!(usage[0].used, Some(counted as f64));
            }
        }
    }
}
