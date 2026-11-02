//! The limiter scripts against a private `redis-server` (`docs/design/ratelimit.md` §3.2): the
//! scenarios of the design's lab, run with fixed times so that every expectation is exact, and
//! the in-memory table fed the same sequences (parity).

use std::collections::HashMap;
use std::time::Duration;

use crate::keys;
use crate::limit::memory::LimitTable;
use crate::limit::rule::Rule;
use crate::limit::scripts::{
    self, AcquireArgs, Acquired, LeaseRef, Place, ReleaseOutcome, RenewOutcome, UsageArgs,
};
use crate::limit::{Interval, Limit};
use crate::redis::RedisPool;
use crate::test_util::TestRedis;

/// A fixed time far from Redis's clock, to make sure the scripts use the one they are given.
const T0: i64 = 1_800_000_000_000;

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

async fn acquire(pool: &RedisPool, args: &AcquireArgs<'_>) -> Acquired {
    scripts::acquire(pool, args).await.unwrap()
}

fn granted(reply: &Acquired) -> bool {
    matches!(reply, Acquired::Granted { .. })
}

#[track_caller]
fn wait(reply: &Acquired) -> u64 {
    match reply {
        Acquired::Refused {
            wait_ms,
            impossible: false,
            ..
        } => *wait_ms,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

async fn command<T: ::redis::FromRedisValue>(pool: &RedisPool, command: &::redis::Cmd) -> T {
    let mut conn = pool.conn().await.unwrap();
    command.query_async(&mut conn).await.unwrap()
}

async fn pttl(pool: &RedisPool, key: &str) -> i64 {
    command(pool, ::redis::cmd("PTTL").arg(key)).await
}

async fn stats(pool: &RedisPool, rule: &Rule) -> HashMap<String, String> {
    command(pool, ::redis::cmd("HGETALL").arg(&rule.stats)).await
}

fn lease(rule: &Rule, token: &str, acquired: i64) -> LeaseRef {
    LeaseRef::of(
        rule,
        token,
        Duration::from_secs(30),
        Some(Duration::from_secs(86_400)),
        Some(acquired),
    )
}

#[tokio::test]
async fn windows_slide_exactly() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=3/1s", "w")];
    for _ in 0..3 {
        assert!(granted(&acquire(&pool, &args(&rules, "", 1, T0)).await));
    }
    assert_eq!(wait(&acquire(&pool, &args(&rules, "", 1, T0)).await), 1_000);
    assert_eq!(
        wait(&acquire(&pool, &args(&rules, "", 1, T0 + 999)).await),
        1
    );
    assert!(granted(
        &acquire(&pool, &args(&rules, "", 1, T0 + 1_000)).await
    ));
    // Sliding: grants at 0, 400 and 800 of a fresh window.
    let rules = [rule("w=3/1s", "sliding")];
    for at in [0, 400, 800] {
        assert!(granted(
            &acquire(&pool, &args(&rules, "", 1, T0 + at)).await
        ));
    }
    assert!(granted(
        &acquire(&pool, &args(&rules, "", 1, T0 + 1_000)).await
    ));
    assert_eq!(
        wait(&acquire(&pool, &args(&rules, "", 1, T0 + 1_100)).await),
        300
    );
    // The list keeps at most `limit` grants, and expires a second after the window.
    let len: i64 = command(&pool, ::redis::cmd("LLEN").arg(&rules[0].state)).await;
    assert_eq!(len, 3);
    let ttl = pttl(&pool, &rules[0].state).await;
    assert!(ttl > 1_000 && ttl <= 2_000, "{ttl}");
}

#[tokio::test]
async fn costs_are_charged_whole_or_not_at_all() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=5/1s", "costs")];
    assert!(granted(&acquire(&pool, &args(&rules, "", 3, T0)).await));
    assert_eq!(
        wait(&acquire(&pool, &args(&rules, "", 3, T0 + 10)).await),
        990
    );
    assert!(granted(
        &acquire(&pool, &args(&rules, "", 2, T0 + 10)).await
    ));
    let impossible = acquire(&pool, &args(&rules, "", 6, T0 + 10)).await;
    assert!(
        matches!(
            impossible,
            Acquired::Refused {
                impossible: true,
                ..
            }
        ),
        "{impossible:?}"
    );
    // Large costs are pushed in chunks.
    let big = [rule("w=5000/1s", "big")];
    assert!(granted(&acquire(&pool, &args(&big, "", 4_500, T0)).await));
    let len: i64 = command(&pool, ::redis::cmd("LLEN").arg(&big[0].state)).await;
    assert_eq!(len, 4_500);
    assert_eq!(wait(&acquire(&pool, &args(&big, "", 501, T0)).await), 1_000);
}

#[tokio::test]
async fn buckets_follow_utc_boundaries() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    // T0 is a multiple of 1000: the bucket starts there.
    let rules = [rule("b=2/1000ms", "bucket")];
    assert!(granted(
        &acquire(&pool, &args(&rules, "", 1, T0 + 500)).await
    ));
    assert!(granted(
        &acquire(&pool, &args(&rules, "", 1, T0 + 900)).await
    ));
    assert_eq!(
        wait(&acquire(&pool, &args(&rules, "", 1, T0 + 950)).await),
        50
    );
    // The end of the bucket plus a second (from the first grant's time: never shortened).
    let ttl = pttl(&pool, &rules[0].state).await;
    assert!(ttl > 1_000 && ttl <= 1_500, "{ttl}");
    assert!(granted(
        &acquire(&pool, &args(&rules, "", 1, T0 + 1_000)).await
    ));
    // Day buckets reset at 00:00 UTC.
    let day = [Rule::new(
        Limit::Bucket {
            limit: 1,
            interval: Interval::Day,
        },
        "daily".into(),
        "daily".into(),
    )];
    let midnight: i64 = 1_798_761_600_000; // 2027-01-01T00:00:00Z
    assert_eq!(midnight % 86_400_000, 0);
    assert!(granted(
        &acquire(&pool, &args(&day, "", 1, midnight - 5_000)).await
    ));
    assert_eq!(
        wait(&acquire(&pool, &args(&day, "", 1, midnight - 1_000)).await),
        1_000
    );
    assert!(granted(&acquire(&pool, &args(&day, "", 1, midnight)).await));
}

#[tokio::test]
async fn leaky_buckets_burst_then_drip() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("l=3/3000ms", "leaky")];
    for _ in 0..3 {
        assert!(granted(&acquire(&pool, &args(&rules, "", 1, T0)).await));
    }
    assert_eq!(wait(&acquire(&pool, &args(&rules, "", 1, T0)).await), 1_000);
    assert!(granted(
        &acquire(&pool, &args(&rules, "", 1, T0 + 1_000)).await
    ));
    // Sidekiq's documented semantics of leaky(60, 60 s): a burst of 60, then one per second;
    // 5 more after 5 s; a full burst again after draining.
    let sidekiq = [rule("l=60/60s", "documented")];
    for _ in 0..60 {
        assert!(granted(&acquire(&pool, &args(&sidekiq, "", 1, T0)).await));
    }
    assert_eq!(
        wait(&acquire(&pool, &args(&sidekiq, "", 1, T0)).await),
        1_000
    );
    for _ in 0..5 {
        assert!(granted(
            &acquire(&pool, &args(&sidekiq, "", 1, T0 + 5_000)).await
        ));
    }
    assert!(!granted(
        &acquire(&pool, &args(&sidekiq, "", 1, T0 + 5_000)).await
    ));
    for _ in 0..60 {
        assert!(granted(
            &acquire(&pool, &args(&sidekiq, "", 1, T0 + 125_000)).await
        ));
    }
    assert!(!granted(
        &acquire(&pool, &args(&sidekiq, "", 1, T0 + 125_000)).await
    ));
    // The state expires once the bucket is empty, plus a second.
    let ttl = pttl(&pool, &sidekiq[0].state).await;
    assert!(ttl > 60_000 && ttl <= 61_000, "{ttl}");
}

#[tokio::test]
async fn points_take_estimates_and_corrections() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("p=1000/20s", "gql")];
    for _ in 0..5 {
        assert!(granted(&acquire(&pool, &args(&rules, "", 200, T0)).await));
    }
    assert_eq!(
        wait(&acquire(&pool, &args(&rules, "", 200, T0)).await),
        4_000
    );
    // A query estimated at 200 cost 50: refund 150.
    let ttl = Some(Duration::from_secs(86_400));
    let level = scripts::adjust(&pool, &rules[0], -150, ttl, Some(T0))
        .await
        .unwrap();
    assert!((level - 850.0).abs() < 1e-9, "{level}");
    assert_eq!(
        wait(&acquire(&pool, &args(&rules, "", 200, T0)).await),
        1_000
    );
    // An extra charge goes into debt.
    let level = scripts::adjust(&pool, &rules[0], 500, ttl, Some(T0))
        .await
        .unwrap();
    assert!((level - 1_350.0).abs() < 1e-9, "{level}");
    assert_eq!(wait(&acquire(&pool, &args(&rules, "", 1, T0)).await), 7_020);
    // A refund never goes below empty.
    let level = scripts::adjust(&pool, &rules[0], -100_000, ttl, Some(T0))
        .await
        .unwrap();
    assert_eq!(level, 0.0);
    let exists: i64 = command(&pool, ::redis::cmd("EXISTS").arg(&rules[0].state)).await;
    assert_eq!(exists, 0);
    assert_eq!(stats(&pool, &rules[0]).await["adj"], "3");
}

#[tokio::test]
async fn concurrency_leases_tokens_and_reclaims() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("c=2", "erp")];
    let c = &rules[0];
    assert!(granted(&acquire(&pool, &args(&rules, "a", 1, T0)).await));
    assert!(granted(&acquire(&pool, &args(&rules, "b", 1, T0)).await));
    let refused = acquire(&pool, &args(&rules, "c", 1, T0)).await;
    // Refused with one spacing (no hold time measured yet: the default hold of 25 ms shared
    // by 2 holders).
    assert_eq!(wait(&refused), 12);
    // The same token again: granted without a new slot.
    assert!(matches!(
        acquire(&pool, &args(&rules, "a", 1, T0 + 10)).await,
        Acquired::Granted {
            held: true,
            reclaimed: 0,
            ..
        }
    ));
    let holders: i64 = command(&pool, ::redis::cmd("ZCARD").arg(&c.state)).await;
    assert_eq!(holders, 2);
    // The leases end (30 s): granted, with both reclaimed.
    assert!(matches!(
        acquire(&pool, &args(&rules, "c", 1, T0 + 30_011)).await,
        Acquired::Granted { reclaimed: 2, .. }
    ));
    assert_eq!(stats(&pool, c).await["rc"], "2");
    // Releasing a reclaimed lease reports it missing (an overage).
    let released = scripts::release(&pool, &[lease(c, "a", T0)], Some(T0 + 30_100))
        .await
        .unwrap();
    assert_eq!(released, [ReleaseOutcome::Missing]);
    // Renewals: live → renewed; missing with a free slot → restored; full → lost.
    let renewed = scripts::renew(&pool, &[lease(c, "c", T0)], Some(T0 + 31_000))
        .await
        .unwrap();
    assert_eq!(renewed, [RenewOutcome::Renewed]);
    let renewed = scripts::renew(&pool, &[lease(c, "d", T0)], Some(T0 + 31_000))
        .await
        .unwrap();
    assert_eq!(renewed, [RenewOutcome::Restored]);
    let renewed = scripts::renew(&pool, &[lease(c, "e", T0)], Some(T0 + 31_000))
        .await
        .unwrap();
    assert_eq!(renewed, [RenewOutcome::Lost]);
    // Releases record the time held; the last one deletes the set.
    let released = scripts::release(
        &pool,
        &[lease(c, "c", T0 + 30_011), lease(c, "d", T0 + 31_000)],
        Some(T0 + 32_011),
    )
    .await
    .unwrap();
    assert_eq!(
        released,
        [ReleaseOutcome::Released, ReleaseOutcome::Released]
    );
    let exists: i64 = command(&pool, ::redis::cmd("EXISTS").arg(&c.state)).await;
    assert_eq!(exists, 0);
    let counters = stats(&pool, c).await;
    assert_eq!(counters["g"], "4");
    assert_eq!(counters["r"], "1");
    assert_eq!(counters["h"], "2");
    assert_eq!(counters["hms"], "3011");
    // The decaying average: the first hold (2000 ms), then a fifth of the way to the next.
    assert_eq!(counters["hold"], "1802.2");
    assert_eq!(counters["ov"], "2");
    assert_eq!(counters["restored"], "1");
    // Releasing after the lease ended: released late (an overage).
    assert!(granted(
        &acquire(&pool, &args(&rules, "f", 1, T0 + 40_000)).await
    ));
    let released = scripts::release(&pool, &[lease(c, "f", T0 + 40_000)], Some(T0 + 70_001))
        .await
        .unwrap();
    assert_eq!(released, [ReleaseOutcome::Late]);
}

#[tokio::test]
async fn composite_limits_are_charged_all_or_none() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=10/1s", "crm"), rule("c=1", "crm-c")];
    assert!(granted(&acquire(&pool, &args(&rules, "a", 1, T0)).await));
    // The concurrency limit refuses: the window is not charged (sidekiq-throttled's
    // starvation).
    for _ in 0..5 {
        let refused = acquire(&pool, &args(&rules, "b", 1, T0)).await;
        assert!(
            matches!(refused, Acquired::Refused { limiting: 1, .. }),
            "{refused:?}"
        );
    }
    let len: i64 = command(&pool, ::redis::cmd("LLEN").arg(&rules[0].state)).await;
    assert_eq!(len, 1);
    // The window refuses after its 10 grants; the latest start decides the wait.
    let window_only = [rule("w=2/1s", "x"), rule("w=1/5s", "y")];
    assert!(granted(
        &acquire(&pool, &args(&window_only, "", 1, T0)).await
    ));
    let refused = acquire(&pool, &args(&window_only, "", 1, T0 + 100)).await;
    assert!(
        matches!(
            refused,
            Acquired::Refused {
                limiting: 1,
                wait_ms: 4_900,
                ..
            }
        ),
        "{refused:?}"
    );
}

#[tokio::test]
async fn the_wait_line_spreads_refusals() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=10/60s", "line")];
    let lined = |now: i64, ticket: Option<i64>| AcquireArgs {
        line: true,
        ticket,
        place_within: None,
        expected_hold: None,
        ..args(&rules, "", 1, now)
    };
    for _ in 0..10 {
        assert!(granted(&acquire(&pool, &lined(T0, None)).await));
    }
    let mut slots = Vec::new();
    for _ in 0..5 {
        let refused = acquire(&pool, &lined(T0, None)).await;
        slots.push(wait(&refused));
    }
    assert_eq!(slots, [60_000, 66_000, 72_000, 78_000, 84_000]);
    // A caller coming back before its slot keeps its place without lengthening the line.
    let again = acquire(&pool, &lined(T0 + 1_000, Some(T0 + 72_000))).await;
    assert_eq!(wait(&again), 71_000);
    let line: String = command(&pool, ::redis::cmd("HGET").arg(&rules[0].stats).arg("line")).await;
    assert_eq!(line, (T0 + 90_000).to_string());
    // Refusals counted once per place given.
    assert_eq!(stats(&pool, &rules[0]).await["r"], "5");
    // Once in the past, the line no longer delays anybody.
    let later = T0 + 200_000;
    for _ in 0..10 {
        assert!(granted(&acquire(&pool, &lined(later, None)).await));
    }
    assert_eq!(wait(&acquire(&pool, &lined(later, None)).await), 60_000);
    // Without a line, refusals get the limit's own wait.
    assert_eq!(
        wait(&acquire(&pool, &args(&rules, "", 1, later)).await),
        60_000
    );
    assert_eq!(
        wait(&acquire(&pool, &args(&rules, "", 1, later)).await),
        60_000
    );
}

#[tokio::test]
async fn callers_that_will_not_wait_take_no_place_in_the_line() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=10/1s", "hint")];
    let call = |now: i64, within: Option<Duration>| AcquireArgs {
        line: true,
        place_within: within,
        ..args(&rules, "", 1, now)
    };
    for _ in 0..10 {
        assert!(granted(&acquire(&pool, &call(T0, None)).await));
    }
    // Callers that will not wait (`try_acquire`, an HTTP handler answering 429) are told when
    // the next unit frees, however many of them ask: they never push the line ahead.
    for at in 0..200 {
        let refused = acquire(&pool, &call(T0 + at, Some(Duration::ZERO))).await;
        assert!(
            matches!(
                refused,
                Acquired::Refused {
                    place: Place::None,
                    ..
                }
            ),
            "{refused:?}"
        );
        assert_eq!(wait(&refused), u64::try_from(1_000 - at).unwrap());
    }
    let line: Option<String> =
        command(&pool, ::redis::cmd("HGET").arg(&rules[0].stats).arg("line")).await;
    assert_eq!(line, None);
    // Each refusal is still counted.
    assert_eq!(stats(&pool, &rules[0]).await["r"], "200");
    // A job (rescheduled at its slot whatever it is) takes a place: the line moves one spacing.
    let job = acquire(&pool, &call(T0 + 200, None)).await;
    assert!(matches!(
        job,
        Acquired::Refused { place: Place::New, slot_ms, .. } if slot_ms == T0 + 1_000
    ));
    // A caller waiting in process takes one only for a slot within its wait.
    let patient = acquire(&pool, &call(T0 + 300, Some(Duration::from_secs(1)))).await;
    assert!(matches!(
        patient,
        Acquired::Refused { place: Place::New, slot_ms, .. } if slot_ms == T0 + 1_100
    ));
    let hasty = acquire(&pool, &call(T0 + 300, Some(Duration::from_millis(500)))).await;
    assert!(matches!(
        hasty,
        Acquired::Refused { place: Place::None, slot_ms, .. } if slot_ms == T0 + 1_200
    ));
    let line: String = command(&pool, ::redis::cmd("HGET").arg(&rules[0].stats).arg("line")).await;
    assert_eq!(line, (T0 + 1_200).to_string());
}

#[tokio::test]
async fn a_waiter_back_a_little_late_keeps_its_place() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=2/1s", "late")];
    let lined = |now: i64, ticket: Option<i64>| AcquireArgs {
        line: true,
        ticket,
        ..args(&rules, "", 1, now)
    };
    assert!(granted(&acquire(&pool, &lined(T0, None)).await));
    assert!(granted(&acquire(&pool, &lined(T0 + 500, None)).await));
    // Two waiters: when the units of T0 and T0 + 500 leave the window.
    let a = acquire(&pool, &lined(T0 + 600, None)).await;
    assert!(
        matches!(a, Acquired::Refused { slot_ms, place: Place::New, .. } if slot_ms == T0 + 1_000)
    );
    let b = acquire(&pool, &lined(T0 + 600, None)).await;
    assert!(
        matches!(b, Acquired::Refused { slot_ms, place: Place::New, .. } if slot_ms == T0 + 1_500)
    );
    // A caller with no place takes the unit of T0 the moment it frees.
    assert!(granted(&acquire(&pool, &lined(T0 + 1_000, None)).await));
    // The first waiter, back 5 ms after its slot (a timer, a scheduler), finds it taken: it
    // gets the next unit that frees, without a new place (the line stays, nothing counted).
    let late = acquire(&pool, &lined(T0 + 1_005, Some(T0 + 1_000))).await;
    assert!(
        matches!(late, Acquired::Refused { wait_ms: 495, slot_ms, place: Place::Kept, .. }
            if slot_ms == T0 + 1_500),
        "{late:?}"
    );
    let line: String = command(&pool, ::redis::cmd("HGET").arg(&rules[0].stats).arg("line")).await;
    assert_eq!(line, (T0 + 2_000).to_string());
    assert_eq!(stats(&pool, &rules[0]).await["r"], "2");
    // Back long after its slot (more than two spacings, at least 100 ms), it is a newcomer:
    // a new place at the end of the line.
    assert!(granted(&acquire(&pool, &lined(T0 + 1_600, None)).await));
    assert!(granted(&acquire(&pool, &lined(T0 + 2_050, None)).await));
    let stale = acquire(&pool, &lined(T0 + 2_100, Some(T0 + 1_000))).await;
    assert!(
        matches!(stale, Acquired::Refused { slot_ms, place: Place::New, .. } if slot_ms == T0 + 2_600),
        "{stale:?}"
    );
    let line: String = command(&pool, ::redis::cmd("HGET").arg(&rules[0].stats).arg("line")).await;
    assert_eq!(line, (T0 + 3_100).to_string());
}

#[tokio::test]
async fn concurrency_spacing_comes_from_measured_holds() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("c=2", "held")];
    let lined = |token: &'static str, now: i64| AcquireArgs {
        line: true,
        ..args(&rules, token, 1, now)
    };
    assert!(granted(&acquire(&pool, &lined("a", T0)).await));
    assert!(granted(&acquire(&pool, &lined("b", T0)).await));
    // `a` held its slot for 2 s: the average hold is 2 s, shared by 2 slots.
    scripts::release(&pool, &[lease(&rules[0], "a", T0)], Some(T0 + 2_000))
        .await
        .unwrap();
    assert!(granted(&acquire(&pool, &lined("c", T0 + 2_000)).await));
    assert_eq!(wait(&acquire(&pool, &lined("d", T0 + 2_000)).await), 1_000);
    assert_eq!(wait(&acquire(&pool, &lined("e", T0 + 2_000)).await), 2_000);
}

#[tokio::test]
async fn concurrency_waiters_keep_their_place_while_they_poll() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("c=1", "polled")];
    // Holders known to take a second each.
    let lined = |token: &'static str, now: i64, ticket: Option<i64>| AcquireArgs {
        line: true,
        ticket,
        place_within: None,
        expected_hold: Some(Duration::from_secs(1)),
        ..args(&rules, token, 1, now)
    };
    assert!(granted(&acquire(&pool, &lined("a", T0, None)).await));
    // The first waiter gets the slot a second ahead (the expected hold of one slot).
    let first = acquire(&pool, &lined("b", T0, None)).await;
    assert!(
        matches!(first, Acquired::Refused { wait_ms: 1_000, slot_ms, .. } if slot_ms == T0 + 1_000)
    );
    // Polling before its slot keeps its place without lengthening the line.
    for at in [25, 75, 175, 375, 775] {
        let poll = acquire(&pool, &lined("b", T0 + at, Some(T0 + 1_000))).await;
        assert!(
            matches!(poll, Acquired::Refused { slot_ms, .. } if slot_ms == T0 + 1_000),
            "{poll:?}"
        );
        assert_eq!(wait(&poll), (1_000 - at) as u64);
    }
    // The next waiter comes after it.
    assert_eq!(
        wait(&acquire(&pool, &lined("c", T0 + 800, None)).await),
        1_200
    );
    // Past its slot and still refused: a new place at the end of the line.
    let late = acquire(&pool, &lined("b", T0 + 1_001, Some(T0 + 1_000))).await;
    assert_eq!(wait(&late), 1_999);
    assert_eq!(stats(&pool, &rules[0]).await["r"], "3");
}

#[tokio::test]
async fn every_key_written_expires() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [
        rule("c=1", "ttl-c"),
        rule("w=1/1s", "ttl-w"),
        rule("b=1/1000ms", "ttl-b"),
        rule("l=1/1000ms", "ttl-l"),
        rule("p=10/1000ms", "ttl-p"),
    ];
    for rule in &rules {
        let single = std::slice::from_ref(rule);
        assert!(granted(&acquire(&pool, &args(single, "t", 1, T0)).await));
        // Refused: bookkeeping and the wait line.
        let lined = AcquireArgs {
            line: true,
            ..args(single, "u", 10, T0)
        };
        let _ = acquire(&pool, &lined).await;
    }
    scripts::renew(&pool, &[lease(&rules[0], "t", T0)], Some(T0 + 10))
        .await
        .unwrap();
    scripts::adjust(&pool, &rules[4], 3, Some(Duration::from_secs(60)), Some(T0))
        .await
        .unwrap();
    // A release of a reset limit still gives the counters it creates an expiry.
    command::<()>(&pool, ::redis::cmd("DEL").arg(&rules[0].stats)).await;
    scripts::release(&pool, &[lease(&rules[0], "nobody", T0)], Some(T0))
        .await
        .unwrap();
    let missing = redis.keys_without_ttl(&["limit*", "l|*"]).await;
    assert_eq!(
        missing,
        [keys::LIMITS],
        "only the index lives without expiry"
    );
    // The index is scored by the expiry of the stats it lists.
    let score: f64 = command(
        &pool,
        ::redis::cmd("ZSCORE").arg(keys::LIMITS).arg("w:ttl-w"),
    )
    .await;
    assert!(
        ((T0 + 86_400_000) as f64 / 1_000.0 - score).abs() < 1.0,
        "{score}"
    );
    // Stale index entries are trimmed by grants (10 at a time).
    for index in 0..12 {
        command::<()>(
            &pool,
            ::redis::cmd("ZADD")
                .arg(keys::LIMITS)
                .arg(1)
                .arg(format!("w:stale{index}")),
        )
        .await;
    }
    assert!(granted(
        &acquire(&pool, &args(&rules[1..2], "", 1, T0 + 5_000)).await
    ));
    let stale: i64 = command(
        &pool,
        ::redis::cmd("ZCOUNT").arg(keys::LIMITS).arg(0).arg(2),
    )
    .await;
    assert_eq!(stale, 2);
}

#[tokio::test]
async fn bookkeeping_can_be_turned_off() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=1/1s", "lean")];
    let lean = |now| AcquireArgs {
        stats: false,
        line: true,
        ..args(&rules, "", 1, now)
    };
    assert!(granted(&acquire(&pool, &lean(T0)).await));
    assert_eq!(wait(&acquire(&pool, &lean(T0)).await), 1_000);
    // No line either: the same wait twice.
    assert_eq!(wait(&acquire(&pool, &lean(T0)).await), 1_000);
    let exists: i64 = command(
        &pool,
        ::redis::cmd("EXISTS")
            .arg(&rules[0].stats)
            .arg(keys::LIMITS),
    )
    .await;
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn signatures_report_a_second_definition() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let first = [rule("w=5/1s", "shared")];
    let second = [rule("w=10/1s", "shared")];
    assert!(matches!(
        acquire(&pool, &args(&first, "", 1, T0)).await,
        Acquired::Granted { mismatch: None, .. }
    ));
    let reply = acquire(&pool, &args(&second, "", 1, T0)).await;
    assert!(
        matches!(&reply, Acquired::Granted { mismatch: Some(previous), .. } if previous == "w|5|1000"),
        "{reply:?}"
    );
}

#[tokio::test]
async fn usage_reads_every_kind() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [
        rule("c=2", "use-c"),
        rule("w=2/1s", "use-w"),
        rule("b=4/1000ms", "use-b"),
        rule("l=4/4000ms", "use-l"),
    ];
    for _ in 0..2 {
        for rule in &rules {
            let token = format!("t{}", rule.member.len());
            let unique = format!("{token}-{}", fastrand::u32(..));
            let _ = acquire(&pool, &args(std::slice::from_ref(rule), &unique, 1, T0)).await;
        }
    }
    let reads: Vec<UsageArgs> = rules.iter().map(UsageArgs::of).collect();
    let usage = scripts::usage(&pool, &reads, Some(T0 + 500)).await.unwrap();
    assert_eq!(usage[0].used, Some(2.0));
    assert_eq!(usage[0].free_in, Some(Duration::from_millis(29_500)));
    assert_eq!(usage[1].used, Some(2.0));
    assert_eq!(usage[1].free_in, Some(Duration::from_millis(500)));
    assert_eq!(usage[2].used, Some(2.0));
    assert_eq!(usage[2].free_in, Some(Duration::ZERO));
    assert_eq!(usage[3].used, Some(1.5));
    assert_eq!(usage[3].free_in, Some(Duration::ZERO));
    assert_eq!(usage[1].stats["g"], "2");
    // Read by identity alone: the limit comes from the signature.
    let by_identity = scripts::usage(&pool, &[UsageArgs::identity('w', "use-w")], Some(T0 + 500))
        .await
        .unwrap();
    assert_eq!(by_identity[0].used, Some(2.0));
    assert_eq!(by_identity[0].free_in, Some(Duration::from_millis(500)));
    // Unknown identities read as nothing.
    let unknown = scripts::usage(&pool, &[UsageArgs::identity('w', "nope")], Some(T0))
        .await
        .unwrap();
    assert_eq!(unknown[0].used, None);
    assert!(unknown[0].stats.is_empty());
    // Reset frees everything and forgets the identity; clearing a line keeps the rest.
    let deleted = scripts::reset(&pool, &[scripts::reset_target(&rules[0])])
        .await
        .unwrap();
    assert_eq!(deleted, 2);
    let listed: Option<f64> = command(
        &pool,
        ::redis::cmd("ZSCORE").arg(keys::LIMITS).arg("c:use-c"),
    )
    .await;
    assert_eq!(listed, None);
    command::<()>(
        &pool,
        ::redis::cmd("HSET").arg(&rules[1].stats).arg("line").arg(1),
    )
    .await;
    assert!(scripts::clear_line(&pool, &rules[1].stats).await.unwrap());
    assert!(!scripts::clear_line(&pool, &rules[1].stats).await.unwrap());
    // Trimming drops only the entries of identities whose keys are all gone.
    command::<()>(
        &pool,
        ::redis::cmd("ZADD")
            .arg(keys::LIMITS)
            .arg(9e9)
            .arg("w:gone")
            .arg(9e9)
            .arg("c:use-c"),
    )
    .await;
    let targets = [
        (
            keys::limit_state('w', "gone"),
            keys::limit_stats('w', "gone"),
            "w:gone".to_owned(),
        ),
        scripts::reset_target(&rules[1]),
        scripts::reset_target(&rules[0]),
    ];
    assert_eq!(scripts::trim(&pool, &targets).await.unwrap(), 2);
    let listed: Vec<String> = command(
        &pool,
        ::redis::cmd("ZRANGE").arg(keys::LIMITS).arg(0).arg(-1),
    )
    .await;
    assert!(listed.contains(&rules[1].member), "{listed:?}");
    assert!(
        !listed
            .iter()
            .any(|member| member == "w:gone" || member == "c:use-c")
    );
}

#[tokio::test]
async fn redis_time_is_used_outside_tests() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=1/60s", "clock"), rule("c=1", "clock-c")];
    let live = AcquireArgs {
        now: None,
        ..args(&rules, "tok", 1, 0)
    };
    let reply = acquire(&pool, &live).await;
    let Acquired::Granted { now_ms, .. } = reply else {
        panic!("{reply:?}");
    };
    let local = crate::payload::now_millis();
    assert!((now_ms - local).abs() < 5_000, "{now_ms} vs {local}");
    let refused = acquire(
        &pool,
        &AcquireArgs {
            token: "other",
            ..live.clone()
        },
    )
    .await;
    assert!(wait(&refused) > 50_000);
    let released = scripts::release(&pool, &[lease(&rules[1], "tok", now_ms)], None)
        .await
        .unwrap();
    assert_eq!(released, [ReleaseOutcome::Released]);
    // No script asks for script replication (deprecated; effects are the only mode).
    for script in crate::limit::lua::scripts() {
        assert!(!script.source().contains("replicate_commands"));
    }
}

#[tokio::test]
async fn under_oom_grants_are_refused_whole_and_refusals_still_answer() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let rules = [rule("w=1/60s", "oom"), rule("c=5", "oom-c")];
    assert!(granted(
        &acquire(&pool, &args(&rules[..1], "", 1, T0)).await
    ));
    command::<()>(
        &pool,
        ::redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg("1"),
    )
    .await;
    // A grant: refused with OOM, nothing written.
    let error = scripts::acquire(&pool, &args(&rules[1..], "a", 1, T0))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("OOM"), "{error}");
    let exists: i64 = command(&pool, ::redis::cmd("EXISTS").arg(&rules[1].state)).await;
    assert_eq!(exists, 0);
    // A refusal: answered.
    let lined = AcquireArgs {
        line: true,
        ..args(&rules[..1], "", 1, T0 + 10)
    };
    assert_eq!(wait(&acquire(&pool, &lined).await), 59_990);
    command::<()>(
        &pool,
        ::redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg("0"),
    )
    .await;
    assert!(granted(
        &acquire(&pool, &args(&rules[1..], "a", 1, T0)).await
    ));
}

/// One step of a parity sequence.
enum Step {
    Acquire(&'static str, u32, i64, bool),
    /// With a wait line, a ticket and how far ahead a place may be taken (ms).
    Lined(&'static str, i64, Option<i64>, Option<u64>),
    Release(&'static str, i64, i64),
    Renew(&'static str, i64),
    Adjust(i64, i64),
}

/// Feeds the same steps to Redis and to the in-memory table: same answers.
async fn parity(rules: &[Rule], steps: &[Step]) {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let table = LimitTable::default();
    for (index, step) in steps.iter().enumerate() {
        match *step {
            Step::Acquire(token, cost, at, line) => {
                let call = AcquireArgs {
                    line,
                    ..args(rules, token, cost, T0 + at)
                };
                let in_redis = acquire(&pool, &call).await;
                let in_memory = table.acquire(&call);
                assert_eq!(in_redis, in_memory, "step {index}");
            }
            Step::Lined(token, at, ticket, within) => {
                let call = AcquireArgs {
                    line: true,
                    ticket: ticket.map(|slot| T0 + slot),
                    place_within: within.map(Duration::from_millis),
                    ..args(rules, token, 1, T0 + at)
                };
                let in_redis = acquire(&pool, &call).await;
                let in_memory = table.acquire(&call);
                assert_eq!(in_redis, in_memory, "step {index}");
            }
            Step::Release(token, acquired, at) => {
                let leases: Vec<LeaseRef> = rules
                    .iter()
                    .filter(|rule| rule.is_concurrency())
                    .map(|rule| lease(rule, token, T0 + acquired))
                    .collect();
                let in_redis = scripts::release(&pool, &leases, Some(T0 + at))
                    .await
                    .unwrap();
                assert_eq!(
                    in_redis,
                    table.release(&leases, Some(T0 + at)),
                    "step {index}"
                );
            }
            Step::Renew(token, at) => {
                let leases: Vec<LeaseRef> = rules
                    .iter()
                    .filter(|rule| rule.is_concurrency())
                    .map(|rule| lease(rule, token, T0))
                    .collect();
                let in_redis = scripts::renew(&pool, &leases, Some(T0 + at)).await.unwrap();
                assert_eq!(
                    in_redis,
                    table.renew(&leases, Some(T0 + at)),
                    "step {index}"
                );
            }
            Step::Adjust(delta, at) => {
                let rule = rules.iter().find(|rule| rule.is_points()).unwrap();
                let ttl = Some(Duration::from_secs(60));
                let in_redis = scripts::adjust(&pool, rule, delta, ttl, Some(T0 + at))
                    .await
                    .unwrap();
                let in_memory = table.adjust(rule, delta, Some(T0 + at));
                assert!((in_redis - in_memory).abs() < 1e-9, "step {index}");
            }
        }
    }
    // The counters and lines agree too.
    let reads: Vec<UsageArgs> = rules.iter().map(UsageArgs::of).collect();
    let at = steps
        .iter()
        .map(|step| match *step {
            Step::Acquire(_, _, at, _)
            | Step::Lined(_, at, _, _)
            | Step::Release(_, _, at)
            | Step::Renew(_, at) => at,
            Step::Adjust(_, at) => at,
        })
        .max()
        .unwrap_or(0);
    let in_redis = scripts::usage(&pool, &reads, Some(T0 + at)).await.unwrap();
    let in_memory = table.usage(&reads, Some(T0 + at));
    for (redis, memory) in in_redis.iter().zip(&in_memory) {
        assert_eq!(redis.used, memory.used);
        assert_eq!(redis.free_in, memory.free_in);
        let mut redis_stats = redis.stats.clone();
        let mut memory_stats = memory.stats.clone();
        for stats in [&mut redis_stats, &mut memory_stats] {
            stats.remove("last");
        }
        assert_eq!(redis_stats, memory_stats);
    }
}

#[tokio::test]
async fn the_in_memory_table_answers_like_redis() {
    use Step::{Acquire, Adjust, Lined, Release, Renew};
    // Places in the wait line: callers that will not wait, places kept a little late, places
    // lost long after, the line of a concurrency limit spaced by measured holds.
    parity(
        &[rule("w=2/1s", "pt")],
        &[
            Acquire("", 1, 0, true),
            Acquire("", 1, 500, true),
            Lined("", 600, None, Some(0)),
            Lined("", 600, None, None),
            Lined("", 600, None, None),
            Lined("", 700, None, Some(1_000)),
            Lined("", 1_000, None, None),
            Lined("", 1_005, Some(1_000), None),
            Lined("", 1_600, None, None),
            Lined("", 2_100, Some(1_000), None),
            Lined("", 2_100, Some(3_000), None),
        ],
    )
    .await;
    parity(
        &[rule("c=1", "pct")],
        &[
            Acquire("a", 1, 0, true),
            Lined("b", 10, None, None),
            Release("a", 0, 700),
            Acquire("c", 1, 700, true),
            Lined("b", 800, Some(110), None),
            Release("c", 700, 1_000),
            Acquire("d", 1, 1_000, true),
            Lined("e", 1_010, None, None),
            Lined("b", 1_020, Some(110), Some(0)),
        ],
    )
    .await;
    parity(
        &[rule("w=3/1s", "pw")],
        &[
            Acquire("", 1, 0, true),
            Acquire("", 2, 100, true),
            Acquire("", 1, 200, true),
            Acquire("", 1, 300, true),
            Acquire("", 1, 999, true),
            Acquire("", 1, 1_000, true),
            Acquire("", 3, 1_500, false),
            Acquire("", 4, 1_500, false),
        ],
    )
    .await;
    parity(
        &[rule("b=3/1000ms", "pb"), rule("l=5/2000ms", "pl")],
        &[
            Acquire("", 1, 0, true),
            Acquire("", 2, 10, true),
            Acquire("", 1, 20, true),
            Acquire("", 1, 30, true),
            Acquire("", 1, 1_000, true),
            Acquire("", 2, 1_400, true),
            Acquire("", 2, 3_000, false),
        ],
    )
    .await;
    parity(
        &[rule("c=2", "pc"), rule("w=10/1s", "pcw")],
        &[
            Acquire("a", 1, 0, true),
            Acquire("b", 1, 0, true),
            Acquire("c", 1, 0, true),
            Acquire("a", 1, 10, true),
            Release("a", 0, 1_500),
            Acquire("c", 1, 1_500, true),
            Acquire("d", 1, 1_500, true),
            Acquire("e", 1, 1_500, true),
            Renew("b", 20_000),
            Renew("x", 20_000),
            Renew("y", 40_000),
            Acquire("f", 1, 60_000, true),
            Release("c", 1_500, 60_001),
        ],
    )
    .await;
    parity(
        &[rule("p=1000/20s", "pp")],
        &[
            Acquire("", 200, 0, true),
            Acquire("", 900, 10, true),
            Adjust(-150, 20),
            Acquire("", 900, 30, true),
            Adjust(400, 40),
            Acquire("", 10, 50, true),
            Acquire("", 10, 25_000, false),
        ],
    )
    .await;
}
