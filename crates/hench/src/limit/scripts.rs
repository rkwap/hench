//! Calls of the limiter scripts (`limit/lua/*.lua`) and their replies.
//!
//! Every call goes through [`RedisPool::with_retry`]: a dropped connection resends it once
//! (harmless for concurrency tokens; a rate grant may then charge a unit twice, which only
//! under-admits), a timeout is never resent. Outside tests, `ARGV[1]` (a fixed time) is always
//! `''`: the scripts read Redis's clock.

use std::collections::HashMap;
use std::time::Duration;

use ::redis::{ErrorKind, RedisError, Value};
use chrono::Utc;

use crate::error::{Error, Result};
use crate::keys;
use crate::limit::lua::{
    LIMIT_ACQUIRE, LIMIT_ADJUST, LIMIT_RELEASE, LIMIT_RENEW, LIMIT_RESET, LIMIT_TRIM, LIMIT_USAGE,
};
use crate::limit::rule::{Rule, period_of};
use crate::lua::{LuaScript, ScriptCall};
use crate::redis::RedisPool;

/// Most permits renewed or released by one script call (bounds the time a call blocks Redis).
pub(crate) const CHUNK: usize = 256;

/// An acquisition: the rules checked and granted together, and how.
#[derive(Debug, Clone)]
pub(crate) struct AcquireArgs<'a> {
    /// The limits, checked and granted all or none.
    pub(crate) rules: &'a [Rule],
    /// The token of the concurrency leases (`""` without concurrency limits).
    pub(crate) token: &'a str,
    /// Units taken from every rate limit (concurrency ignores it).
    pub(crate) cost: u32,
    /// The lease of concurrency grants.
    pub(crate) lease: Duration,
    /// The slot of the place an earlier refusal gave this caller.
    pub(crate) ticket: Option<i64>,
    /// Keep a wait line.
    pub(crate) line: bool,
    /// How far ahead a slot may be for this caller to take a place in the line (`None`: any,
    /// the caller comes back for its slot whatever it is: a job parked or rescheduled at it).
    pub(crate) place_within: Option<Duration>,
    /// How long a concurrency permit is expected to be held, until the limit's own releases
    /// tell: the default spacing of its line is this divided by the limit (`None`:
    /// [`DEFAULT_HOLD`]).
    pub(crate) expected_hold: Option<Duration>,
    /// Keep the counters, signature and index entry.
    pub(crate) stats: bool,
    /// The expiry of the counters after the last use.
    pub(crate) stats_ttl: Duration,
    /// How long the caller has waited so far (counted on a grant).
    pub(crate) waited: Duration,
    /// A fixed time, for tests (`None`: Redis's clock).
    pub(crate) now: Option<i64>,
}

/// The hold time a concurrency limit's wait line assumes until releases measure it (and the
/// caller knows no better): short, because a line spaced too widely leaves slots idle while the
/// jobs parked for it wait, whereas one spaced too closely only brings waiters back early, to be
/// placed again with the measured spacing (the throttle gate doubles it with each parking of a
/// job, so that the waiters of long jobs nobody has measured yet come back less and less
/// often).
pub(crate) const DEFAULT_HOLD: Duration = Duration::from_millis(25);

/// What a refusal did with the caller's place in the wait line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Place {
    /// No place: no line, or a slot further than the caller waits (only told).
    None,
    /// A new place at the end of the line: the slot is the caller's ticket from now on.
    New,
    /// The caller kept the place of its ticket (the ticket stays the same).
    Kept,
}

/// The reply of an acquisition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Acquired {
    /// Every limit granted.
    Granted {
        /// Ended leases purged by the grant.
        reclaimed: u64,
        /// The token already held a live lease (an idempotent repeat).
        held: bool,
        /// The signature stored before for an identity whose definition differs.
        mismatch: Option<String>,
        /// The time of the grant (ms, Redis's clock).
        now_ms: i64,
    },
    /// Refused: nothing charged.
    Refused {
        /// How long until this caller's slot (ms).
        wait_ms: u64,
        /// The rule that decided the wait (an index into the call's rules).
        limiting: usize,
        /// The cost can never be granted (above the limit).
        impossible: bool,
        /// The slot (ms, Redis's clock).
        slot_ms: i64,
        /// The time of the refusal (ms, Redis's clock).
        now_ms: i64,
        /// What became of the caller's place in the line.
        place: Place,
        /// The signature stored before for a refusing identity whose definition differs.
        mismatch: Option<String>,
    },
}

/// A concurrency lease to renew or release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeaseRef {
    /// `limit:c:<id>`.
    pub(crate) state: String,
    /// `limit-stats:c:<id>`.
    pub(crate) stats: String,
    /// The holder's token.
    pub(crate) token: String,
    /// The lease, ms.
    pub(crate) lease_ms: u64,
    /// The limit (a lost lease is restored when fewer holders are live).
    pub(crate) limit: u32,
    /// The counters' expiry, ms (`None`: no bookkeeping).
    pub(crate) stats_ttl_ms: Option<u64>,
    /// When it was acquired (ms, Redis's clock; `None`: unknown, no hold time recorded).
    pub(crate) acquired_ms: Option<i64>,
}

impl LeaseRef {
    /// The lease of `rule` held by `token`.
    pub(crate) fn of(
        rule: &Rule,
        token: &str,
        lease: Duration,
        stats_ttl: Option<Duration>,
        acquired_ms: Option<i64>,
    ) -> Self {
        Self {
            state: rule.state.clone(),
            stats: rule.stats.clone(),
            token: token.to_owned(),
            lease_ms: millis(lease),
            limit: rule.limit.capacity(),
            stats_ttl_ms: stats_ttl.map(millis),
            acquired_ms,
        }
    }
}

/// What became of a lease when it was released.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReleaseOutcome {
    /// Given back.
    Released,
    /// Given back after it had ended (an overage).
    Late,
    /// Not held any more (reclaimed by another holder after it ended, or reset).
    Missing,
}

/// What became of a lease when it was renewed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RenewOutcome {
    /// Extended.
    Renewed,
    /// It had ended (or its key was lost) and a slot was free: taken again.
    Restored,
    /// It had ended and no slot was free: the holder runs without a lease.
    Lost,
}

/// The current use of one identity.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct UsageReply {
    /// Units in use (`None` when the limit is unknown).
    pub(crate) used: Option<f64>,
    /// Time until a unit is free (`None` when unknown).
    pub(crate) free_in: Option<Duration>,
    /// The stats hash.
    pub(crate) stats: HashMap<String, String>,
    /// The time of the read (ms, Redis's clock).
    pub(crate) now_ms: i64,
}

/// Milliseconds of a duration (saturating).
pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// `ARGV[1]`: a fixed time in tests, else `''` (Redis's clock).
fn now_arg(now: Option<i64>) -> String {
    now.map(|now| now.to_string()).unwrap_or_default()
}

/// Runs one of the embedded limiter scripts (`EVALSHA`, falling back to the source on
/// `NOSCRIPT`) with `keys` and `args`, through `with_retry`.
pub(crate) async fn run_script(
    pool: &RedisPool,
    script: &'static LuaScript,
    keys: &[String],
    args: &[String],
) -> Result<Value> {
    pool.with_retry(|mut conn| async move {
        let mut invocation = script.script().prepare_invoke();
        for key in keys {
            invocation.key(key);
        }
        for arg in args {
            invocation.arg(arg);
        }
        invocation.invoke_async(&mut conn).await
    })
    .await
}

/// The keys and arguments of [`LIMIT_ACQUIRE`].
pub(crate) fn acquire_call(args: &AcquireArgs<'_>) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::with_capacity(args.rules.len() * 2 + 2);
    let mut argv = Vec::with_capacity(6 + args.rules.len() * 9);
    argv.push(now_arg(args.now));
    argv.push(args.token.to_owned());
    argv.push(args.rules.len().to_string());
    argv.push(args.ticket.map(|slot| slot.to_string()).unwrap_or_default());
    let mut flags = String::new();
    if args.line {
        flags.push('L');
    }
    if !args.stats {
        flags.push('N');
    }
    argv.push(flags);
    argv.push(millis(args.waited).to_string());
    argv.push(
        args.place_within
            .map(|within| millis(within).to_string())
            .unwrap_or_default(),
    );
    let stats_ttl = millis(args.stats_ttl).to_string();
    let hold = args.expected_hold.unwrap_or(DEFAULT_HOLD);
    for rule in args.rules {
        keys.push(rule.state.clone());
        keys.push(rule.stats.clone());
        argv.push(rule.letter().to_string());
        argv.push(rule.limit.capacity().to_string());
        argv.push(period_of(&rule.limit, args.lease).to_string());
        argv.push(if rule.is_concurrency() { 1 } else { args.cost }.to_string());
        argv.push(stats_ttl.clone());
        argv.push(rule.member.clone());
        argv.push(rule.counter.clone());
        argv.push(rule.signature());
        argv.push(rule.default_spacing_ms(hold).to_string());
    }
    keys.push(keys::LIMITS.to_owned());
    keys.push(keys::limit_minute(Utc::now()));
    (keys, argv)
}

/// [`LIMIT_ACQUIRE`]: checks every rule, grants all or none.
pub(crate) async fn acquire(pool: &RedisPool, args: &AcquireArgs<'_>) -> Result<Acquired> {
    let (keys, argv) = acquire_call(args);
    let reply = run_script(pool, &LIMIT_ACQUIRE, &keys, &argv).await?;
    parse_acquired(reply)
}

/// The reply of [`LIMIT_ACQUIRE`].
pub(crate) fn parse_acquired(reply: Value) -> Result<Acquired> {
    let items = array(reply, "LIMIT_ACQUIRE")?;
    let field = |index: usize| {
        items
            .get(index)
            .ok_or_else(|| unexpected("LIMIT_ACQUIRE", "a short reply"))
    };
    if int(field(0)?)? == 1 {
        let mismatch = text(field(3)?)?;
        Ok(Acquired::Granted {
            reclaimed: u64::try_from(int(field(1)?)?).unwrap_or(0),
            held: int(field(2)?)? == 1,
            mismatch: (!mismatch.is_empty()).then_some(mismatch),
            now_ms: int(field(4)?)?,
        })
    } else {
        let limiting = usize::try_from(int(field(2)?)?.saturating_sub(1)).unwrap_or(0);
        Ok(Acquired::Refused {
            wait_ms: u64::try_from(int(field(1)?)?).unwrap_or(0),
            limiting,
            impossible: int(field(3)?)? == 1,
            slot_ms: int(field(4)?)?,
            now_ms: int(field(5)?)?,
            place: match int(field(6)?)? {
                1 => Place::New,
                2 => Place::Kept,
                _ => Place::None,
            },
            mismatch: Some(text(field(7)?)?).filter(|previous| !previous.is_empty()),
        })
    }
}

/// [`LIMIT_RELEASE`] of `leases` (in chunks).
pub(crate) async fn release(
    pool: &RedisPool,
    leases: &[LeaseRef],
    now: Option<i64>,
) -> Result<Vec<ReleaseOutcome>> {
    let mut out = Vec::with_capacity(leases.len());
    for chunk in leases.chunks(CHUNK) {
        let mut keys = Vec::with_capacity(chunk.len() * 2);
        let mut argv = vec![now_arg(now), chunk.len().to_string()];
        for lease in chunk {
            keys.push(lease.state.clone());
            keys.push(lease.stats.clone());
            argv.push(lease.token.clone());
            argv.push(
                lease
                    .acquired_ms
                    .map(|ms| ms.to_string())
                    .unwrap_or_default(),
            );
            argv.push(
                lease
                    .stats_ttl_ms
                    .map(|ms| ms.to_string())
                    .unwrap_or_default(),
            );
        }
        let reply = run_script(pool, &LIMIT_RELEASE, &keys, &argv).await?;
        for item in array(reply, "LIMIT_RELEASE")? {
            out.push(match int(&item)? {
                1 => ReleaseOutcome::Released,
                2 => ReleaseOutcome::Late,
                _ => ReleaseOutcome::Missing,
            });
        }
    }
    Ok(out)
}

/// [`LIMIT_RENEW`] of `leases` (in chunks).
pub(crate) async fn renew(
    pool: &RedisPool,
    leases: &[LeaseRef],
    now: Option<i64>,
) -> Result<Vec<RenewOutcome>> {
    let mut out = Vec::with_capacity(leases.len());
    for chunk in leases.chunks(CHUNK) {
        let call = renew_call(chunk, now);
        let reply = run_script(pool, call.script, &call.keys, &call.args).await?;
        out.extend(renew_reply(reply)?);
    }
    Ok(out)
}

/// The [`LIMIT_RENEW`] call of `leases` (at most [`CHUNK`]), for [`renew`] and for the
/// heartbeat's pipeline (`pro::on_beat`).
pub(crate) fn renew_call(leases: &[LeaseRef], now: Option<i64>) -> ScriptCall {
    let mut keys = Vec::with_capacity(leases.len() * 2);
    let mut args = vec![now_arg(now), leases.len().to_string()];
    for lease in leases {
        keys.push(lease.state.clone());
        keys.push(lease.stats.clone());
        args.push(lease.token.clone());
        args.push(lease.lease_ms.to_string());
        args.push(lease.limit.to_string());
        args.push(
            lease
                .stats_ttl_ms
                .map(|ms| ms.to_string())
                .unwrap_or_default(),
        );
    }
    ScriptCall {
        script: &LIMIT_RENEW,
        keys,
        args,
    }
}

/// The outcome of each lease of a [`renew_call`], in order.
pub(crate) fn renew_reply(reply: Value) -> Result<Vec<RenewOutcome>> {
    array(reply, "LIMIT_RENEW")?
        .iter()
        .map(|item| {
            Ok(match int(item)? {
                1 => RenewOutcome::Renewed,
                2 => RenewOutcome::Restored,
                _ => RenewOutcome::Lost,
            })
        })
        .collect()
}

/// [`LIMIT_ADJUST`]: corrects a points grant by `delta` units; returns the level after it.
pub(crate) async fn adjust(
    pool: &RedisPool,
    rule: &Rule,
    delta: i64,
    stats_ttl: Option<Duration>,
    now: Option<i64>,
) -> Result<f64> {
    let keys = vec![rule.state.clone(), rule.stats.clone()];
    let argv = vec![
        now_arg(now),
        delta.to_string(),
        rule.limit.capacity().to_string(),
        period_of(&rule.limit, Duration::ZERO).to_string(),
        stats_ttl
            .map(|ttl| millis(ttl).to_string())
            .unwrap_or_default(),
    ];
    let reply = run_script(pool, &LIMIT_ADJUST, &keys, &argv).await?;
    let level = text(&reply)?;
    level
        .parse()
        .map_err(|_| unexpected("LIMIT_ADJUST", "a level that is not a number"))
}

/// What [`usage`] reads for one identity: its keys, and its kind, limit and period when known
/// (else they come from its signature).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UsageArgs {
    /// The state key.
    pub(crate) state: String,
    /// The stats key.
    pub(crate) stats: String,
    /// The state's kind letter.
    pub(crate) letter: char,
    /// The limit and period (ms), when known.
    pub(crate) limit: Option<(u32, u64)>,
}

impl UsageArgs {
    /// The read of `rule` (its lease does not matter).
    pub(crate) fn of(rule: &Rule) -> Self {
        Self {
            state: rule.state.clone(),
            stats: rule.stats.clone(),
            letter: rule.letter(),
            limit: Some((
                rule.limit.capacity(),
                period_of(&rule.limit, Duration::ZERO),
            )),
        }
    }

    /// The read of the identity `<letter>:<id>`, whose limit comes from its signature.
    pub(crate) fn identity(letter: char, id: &str) -> Self {
        Self {
            state: keys::limit_state(letter, id),
            stats: keys::limit_stats(letter, id),
            letter,
            limit: None,
        }
    }
}

/// [`LIMIT_USAGE`] of `reads` (read only).
pub(crate) async fn usage(
    pool: &RedisPool,
    reads: &[UsageArgs],
    now: Option<i64>,
) -> Result<Vec<UsageReply>> {
    let mut out = Vec::with_capacity(reads.len());
    for chunk in reads.chunks(CHUNK) {
        let mut keys = Vec::with_capacity(chunk.len() * 2);
        let mut argv = vec![now_arg(now), chunk.len().to_string()];
        for read in chunk {
            keys.push(read.state.clone());
            keys.push(read.stats.clone());
            argv.push(read.letter.to_string());
            match read.limit {
                Some((limit, period)) => {
                    argv.push(limit.to_string());
                    argv.push(period.to_string());
                }
                None => {
                    argv.push(String::new());
                    argv.push(String::new());
                }
            }
        }
        let reply = run_script(pool, &LIMIT_USAGE, &keys, &argv).await?;
        for item in array(reply, "LIMIT_USAGE")? {
            out.push(parse_usage(item)?);
        }
    }
    Ok(out)
}

fn parse_usage(item: Value) -> Result<UsageReply> {
    let mut fields = array(item, "LIMIT_USAGE")?.into_iter();
    let mut next = || {
        fields
            .next()
            .ok_or_else(|| unexpected("LIMIT_USAGE", "a short reply"))
    };
    let used = text(&next()?)?;
    let free_in = int(&next()?)?;
    let flat = array(next()?, "LIMIT_USAGE")?;
    let now_ms = int(&next()?)?;
    let mut stats = HashMap::new();
    let mut pairs = flat.iter();
    while let (Some(field), Some(value)) = (pairs.next(), pairs.next()) {
        stats.insert(text(field)?, text(value)?);
    }
    Ok(UsageReply {
        used: used.parse().ok(),
        free_in: u64::try_from(free_in).ok().map(Duration::from_millis),
        stats,
        now_ms,
    })
}

/// [`LIMIT_RESET`] of `rules`' identities: deletes their state and counters; returns how many
/// keys existed.
pub(crate) async fn reset(
    pool: &RedisPool,
    identities: &[(String, String, String)],
) -> Result<u64> {
    let mut deleted = 0;
    for chunk in identities.chunks(CHUNK) {
        let mut keys = Vec::with_capacity(chunk.len() * 2 + 1);
        let mut argv = vec![chunk.len().to_string()];
        for (state, stats, member) in chunk {
            keys.push(state.clone());
            keys.push(stats.clone());
            argv.push(member.clone());
        }
        keys.push(keys::LIMITS.to_owned());
        let reply = run_script(pool, &LIMIT_RESET, &keys, &argv).await?;
        deleted += u64::try_from(int(&reply)?).unwrap_or(0);
    }
    Ok(deleted)
}

/// [`LIMIT_TRIM`] of `identities` (state, stats, index member): drops the index entries of
/// those whose state and counters are both gone; returns how many were dropped.
pub(crate) async fn trim(pool: &RedisPool, identities: &[(String, String, String)]) -> Result<u64> {
    let mut removed = 0;
    for chunk in identities.chunks(CHUNK) {
        let mut keys = Vec::with_capacity(chunk.len() * 2 + 1);
        let mut argv = vec![chunk.len().to_string()];
        for (state, stats, member) in chunk {
            keys.push(state.clone());
            keys.push(stats.clone());
            argv.push(member.clone());
        }
        keys.push(keys::LIMITS.to_owned());
        let reply = run_script(pool, &LIMIT_TRIM, &keys, &argv).await?;
        removed += u64::try_from(int(&reply)?).unwrap_or(0);
    }
    Ok(removed)
}

/// The keys and index member of `rule`, for [`reset`].
pub(crate) fn reset_target(rule: &Rule) -> (String, String, String) {
    (rule.state.clone(), rule.stats.clone(), rule.member.clone())
}

/// Clears the wait line of an identity (`HDEL line`); `true` when it had one.
pub(crate) async fn clear_line(pool: &RedisPool, stats: &str) -> Result<bool> {
    let removed: i64 = pool
        .with_retry(|mut conn| async move {
            ::redis::cmd("HDEL")
                .arg(stats)
                .arg("line")
                .query_async(&mut conn)
                .await
        })
        .await?;
    Ok(removed > 0)
}

fn unexpected(script: &str, what: &str) -> Error {
    Error::Redis(RedisError::from((
        ErrorKind::UnexpectedReturnType,
        "unexpected reply from a limiter script",
        format!("{script}: {what}"),
    )))
}

fn array(value: Value, script: &str) -> Result<Vec<Value>> {
    match value {
        Value::Array(items) | Value::Set(items) => Ok(items),
        Value::Nil => Ok(Vec::new()),
        _ => Err(unexpected(script, "not an array")),
    }
}

fn int(value: &Value) -> Result<i64> {
    match value {
        Value::Int(number) => Ok(*number),
        Value::BulkString(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| unexpected("limiter script", "not an integer")),
        Value::SimpleString(text) => text
            .parse()
            .map_err(|_| unexpected("limiter script", "not an integer")),
        Value::Double(number) if number.fract() == 0.0 => Ok(*number as i64),
        _ => Err(unexpected("limiter script", "not an integer")),
    }
}

fn text(value: &Value) -> Result<String> {
    match value {
        Value::BulkString(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Value::SimpleString(text) => Ok(text.clone()),
        Value::Int(number) => Ok(number.to_string()),
        Value::Nil => Ok(String::new()),
        Value::Okay => Ok("OK".to_owned()),
        _ => Err(unexpected("limiter script", "not a string")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(spec: &str, id: &str) -> Rule {
        Rule::new(spec.parse().unwrap(), id.to_owned(), id.to_owned())
    }

    #[test]
    fn production_calls_read_the_time_from_redis() {
        let rules = [rule("w=10/1s", "stripe"), rule("c=2", "erp")];
        let args = AcquireArgs {
            rules: &rules,
            token: "tok",
            cost: 3,
            lease: Duration::from_secs(30),
            ticket: None,
            place_within: None,
            expected_hold: None,
            line: true,
            stats: true,
            stats_ttl: Duration::from_secs(86_400),
            waited: Duration::ZERO,
            now: None,
        };
        let (keys, argv) = acquire_call(&args);
        assert_eq!(argv[0], "", "ARGV[1] is empty outside tests: Redis's clock");
        assert_eq!(
            &keys[..4],
            [
                "limit:w:stripe",
                "limit-stats:w:stripe",
                "limit:c:erp",
                "limit-stats:c:erp"
            ]
        );
        assert_eq!(keys[4], keys::LIMITS);
        assert!(keys[5].starts_with("l|"), "{}", keys[5]);
        // Token, n, ticket, flags, waited, and '' for "take a place whatever the slot".
        assert_eq!(&argv[1..7], ["tok", "2", "", "L", "0", ""]);
        // Per limit: kind, limit, period (lease), cost, stats TTL, member, name, signature,
        // default spacing (the default hold, 25 ms, shared by the limit's holders).
        assert_eq!(
            &argv[7..16],
            [
                "w",
                "10",
                "1000",
                "3",
                "86400000",
                "w:stripe",
                "stripe",
                "w|10|1000",
                "2"
            ]
        );
        assert_eq!(
            &argv[16..25],
            [
                "c", "2", "30000", "1", "86400000", "c:erp", "erp", "c|2", "12"
            ]
        );
        let lean = AcquireArgs {
            line: false,
            stats: false,
            ..args.clone()
        };
        assert_eq!(acquire_call(&lean).1[4], "N");
        // A caller that waits at most 250 ms for a place, with a hold time known to be 2 s.
        let bounded = AcquireArgs {
            place_within: Some(Duration::from_millis(250)),
            expected_hold: Some(Duration::from_secs(2)),
            ..args
        };
        let argv = acquire_call(&bounded).1;
        assert_eq!(argv[6], "250");
        assert_eq!(argv[24], "1000");
    }

    #[test]
    fn replies_are_parsed() {
        let granted = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(0),
            Value::BulkString(b"w|5|1000".to_vec()),
            Value::Int(1_000),
        ]);
        assert_eq!(
            parse_acquired(granted).unwrap(),
            Acquired::Granted {
                reclaimed: 2,
                held: false,
                mismatch: Some("w|5|1000".to_owned()),
                now_ms: 1_000
            }
        );
        let refused = Value::Array(vec![
            Value::Int(0),
            Value::Int(250),
            Value::Int(2),
            Value::Int(0),
            Value::Int(1_250),
            Value::Int(1_000),
            Value::Int(1),
            Value::BulkString(b"w|9|1000".to_vec()),
        ]);
        assert_eq!(
            parse_acquired(refused).unwrap(),
            Acquired::Refused {
                wait_ms: 250,
                limiting: 1,
                impossible: false,
                slot_ms: 1_250,
                now_ms: 1_000,
                place: Place::New,
                mismatch: Some("w|9|1000".to_owned()),
            }
        );
        assert!(parse_acquired(Value::Array(vec![Value::Int(1)])).is_err());
        assert!(parse_acquired(Value::Okay).is_err());
    }
}
