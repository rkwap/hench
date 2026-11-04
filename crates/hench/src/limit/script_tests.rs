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

/// `LIMIT_FETCH_GATE` (queue limits' fetch, `docs/design/ratelimit.md` §3.2.12).
mod fetch_gate {
    use super::*;
    use crate::limit::queue::{self, GateArgs, GateQueue, Gated, QueueEntry, Skipped};
    use std::sync::Arc;

    const INPROGRESS: &str = "host:1:abc:inprogress";
    const IDENTITY: &str = "host:1:abc";

    fn entry(queue: &str, specs: &str) -> QueueEntry {
        let limits = queue::parse(queue, specs).unwrap();
        QueueEntry {
            stored: queue::stored_form(&limits),
            rules: Some(Arc::from(queue::rules(queue, &limits))),
        }
    }

    async fn store(pool: &RedisPool, queue: &str, entry: &QueueEntry) {
        let _: i64 = command(
            pool,
            ::redis::cmd("HSET")
                .arg(keys::QUEUE_LIMITS)
                .arg(queue)
                .arg(&entry.stored),
        )
        .await;
    }

    async fn push(pool: &RedisPool, queue: &str, jobs: &[&str]) {
        for job in jobs {
            let _: i64 = command(
                pool,
                ::redis::cmd("LPUSH").arg(keys::queue(queue)).arg(*job),
            )
            .await;
        }
    }

    async fn gate(
        pool: &RedisPool,
        queues: &[(&str, Option<&QueueEntry>)],
        token: &str,
        reliable: bool,
        now: i64,
    ) -> Gated {
        let keys: Vec<String> = queues.iter().map(|(name, _)| keys::queue(name)).collect();
        let args = GateArgs {
            inprogress: INPROGRESS,
            identity: IDENTITY,
            reliable,
            token,
            lease: Duration::from_secs(60),
            queues: queues
                .iter()
                .zip(&keys)
                .map(|((name, entry), key)| GateQueue {
                    key,
                    name,
                    entry: *entry,
                })
                .collect(),
            now: Some(now),
        };
        queue::gate(pool, &args).await.unwrap()
    }

    #[track_caller]
    fn found(gated: &Gated) -> (&str, &[u8], Option<&str>) {
        match gated {
            Gated::Found {
                queue_key,
                job,
                token,
                ..
            } => (queue_key, job, token.as_deref()),
            other => panic!("expected a job, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unlimited_queues_are_fetched_like_the_plain_script() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        // Nothing anywhere: every queue is open for the blocking step.
        let gated = gate(&pool, &[("a", None), ("b", None)], "t", true, T0).await;
        assert_eq!(
            gated,
            Gated::Empty {
                open: vec!["queue:a".to_owned(), "queue:b".to_owned()],
                skipped: vec![],
            }
        );
        push(&pool, "b", &["b1", "b2"]).await;
        push(&pool, "a", &["a1"]).await;
        // In order: the first non-empty queue, its oldest job, to the in-progress list.
        let gated = gate(&pool, &[("a", None), ("b", None)], "t", true, T0).await;
        assert_eq!(found(&gated), ("queue:a", &b"a1"[..], None));
        let gated = gate(&pool, &[("a", None), ("b", None)], "t", true, T0).await;
        assert_eq!(found(&gated), ("queue:b", &b"b1"[..], None));
        let inprogress: Vec<String> =
            command(&pool, ::redis::cmd("LRANGE").arg(INPROGRESS).arg(0).arg(-1)).await;
        assert_eq!(inprogress, ["b1", "a1"]);
        let owners: Vec<String> =
            command(&pool, ::redis::cmd("SMEMBERS").arg(keys::INPROGRESS)).await;
        assert_eq!(owners, [IDENTITY]);
        // Paused queues are skipped and not open.
        let _: i64 = command(&pool, ::redis::cmd("SADD").arg(keys::PAUSED).arg("b")).await;
        let gated = gate(&pool, &[("a", None), ("b", None)], "t", true, T0).await;
        assert_eq!(
            gated,
            Gated::Empty {
                open: vec!["queue:a".to_owned()],
                skipped: vec![],
            }
        );
        // Basic fetch pops, nothing in progress.
        let _: i64 = command(&pool, ::redis::cmd("SREM").arg(keys::PAUSED).arg("b")).await;
        let gated = gate(&pool, &[("b", None)], "t", false, T0).await;
        assert_eq!(found(&gated), ("queue:b", &b"b2"[..], None));
        let len: i64 = command(&pool, ::redis::cmd("LLEN").arg(INPROGRESS)).await;
        assert_eq!(len, 2);
        // No time read, nothing of the limiters written.
        let keys: Vec<String> = command(&pool, ::redis::cmd("KEYS").arg("limit*")).await;
        assert!(keys.is_empty(), "{keys:?}");
    }

    #[tokio::test]
    async fn a_changed_limit_is_reported_before_anything_is_taken() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        push(&pool, "exports", &["e1"]).await;
        let believed = entry("exports", "c=2");
        // The process believes limits Redis does not have.
        let gated = gate(&pool, &[("exports", Some(&believed))], "t", true, T0).await;
        assert_eq!(gated, Gated::Stale("exports".to_owned(), vec![]));
        // Redis has limits the process does not know.
        store(&pool, "exports", &entry("exports", "c=3")).await;
        let gated = gate(&pool, &[("exports", None)], "t", true, T0).await;
        assert_eq!(gated, Gated::Stale("exports".to_owned(), vec![]));
        let gated = gate(&pool, &[("exports", Some(&believed))], "t", true, T0).await;
        assert_eq!(gated, Gated::Stale("exports".to_owned(), vec![]));
        let len: i64 = command(&pool, ::redis::cmd("LLEN").arg(keys::queue("exports"))).await;
        assert_eq!(len, 1, "nothing taken");
        // In agreement: fetched.
        let current = entry("exports", "c=3");
        let gated = gate(&pool, &[("exports", Some(&current))], "t", true, T0).await;
        assert_eq!(found(&gated).0, "queue:exports");
        // A paused queue is not even compared.
        let _: i64 = command(&pool, ::redis::cmd("SADD").arg(keys::PAUSED).arg("exports")).await;
        let gated = gate(&pool, &[("exports", Some(&believed))], "t", true, T0).await;
        assert_eq!(
            gated,
            Gated::Empty {
                open: vec![],
                skipped: vec![]
            }
        );
    }

    #[tokio::test]
    async fn a_full_concurrency_limit_skips_its_queue_and_serves_the_next() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let exports = entry("exports", "c=2");
        store(&pool, "exports", &exports).await;
        push(&pool, "exports", &["e1", "e2", "e3"]).await;
        push(&pool, "default", &["d1"]).await;
        let queues = [("exports", Some(&exports)), ("default", None)];
        // Two grants, each with its fetch's token and a lease of a minute; the second says the
        // limit is now full.
        for (token, job) in [("t1", "e1"), ("t2", "e2")] {
            let gated = gate(&pool, &queues, token, true, T0).await;
            assert_eq!(
                found(&gated),
                ("queue:exports", job.as_bytes(), Some(token))
            );
            let Gated::Found { now_ms, full, .. } = gated else {
                unreachable!()
            };
            assert_eq!(now_ms, T0);
            let expected = (token == "t2").then(|| Skipped {
                queue: "exports".to_owned(),
                wait: Duration::ZERO,
                busy: true,
                broken: false,
            });
            assert_eq!(full, expected);
        }
        let rules = exports.rules.clone().unwrap();
        let state = &rules[0].state;
        let leases: Vec<(String, i64)> = command(
            &pool,
            ::redis::cmd("ZRANGE")
                .arg(state)
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES"),
        )
        .await;
        assert_eq!(
            leases,
            [
                ("t1".to_owned(), T0 + 60_000),
                ("t2".to_owned(), T0 + 60_000)
            ]
        );
        // Full: the queue is skipped (its job stays first in line), the next queue served, and
        // the skipped queue reported so that the process leaves it out for a while.
        let gated = gate(&pool, &queues, "t3", true, T0 + 10).await;
        assert_eq!(found(&gated), ("queue:default", &b"d1"[..], None));
        let Gated::Found { skipped, .. } = &gated else {
            unreachable!()
        };
        assert_eq!(
            skipped,
            &[Skipped {
                queue: "exports".to_owned(),
                wait: Duration::ZERO,
                busy: true,
                broken: false,
            }]
        );
        let gated = gate(&pool, &queues, "t3", true, T0 + 10).await;
        assert_eq!(
            gated,
            Gated::Empty {
                open: vec!["queue:default".to_owned()],
                skipped: vec![Skipped {
                    queue: "exports".to_owned(),
                    wait: Duration::ZERO,
                    busy: true,
                    broken: false,
                }],
            }
        );
        let left: Vec<String> = command(
            &pool,
            ::redis::cmd("LRANGE")
                .arg(keys::queue("exports"))
                .arg(0)
                .arg(-1),
        )
        .await;
        assert_eq!(left, ["e3"]);
        // A released slot lets the next fetch in.
        let released = scripts::release(&pool, &[lease(&rules[0], "t1", T0)], Some(T0 + 20))
            .await
            .unwrap();
        assert_eq!(released, [ReleaseOutcome::Released]);
        let gated = gate(&pool, &queues, "t4", true, T0 + 30).await;
        assert_eq!(found(&gated), ("queue:exports", &b"e3"[..], Some("t4")));
        // Counters: three grants, and two refusals (each fetch that found the queue full: the
        // one that served `default` too), under `@exports`.
        let counters = stats(&pool, &rules[0]).await;
        assert_eq!(counters.get("g").map(String::as_str), Some("3"));
        assert_eq!(counters.get("r").map(String::as_str), Some("2"));
        assert_eq!(counters.get("p").map(String::as_str), Some("c|2"));
        let events: HashMap<String, String> = command(
            &pool,
            ::redis::cmd("HGETALL").arg(keys::limit_minute(chrono::Utc::now())),
        )
        .await;
        assert_eq!(events.get("@exports|g").map(String::as_str), Some("3"));
        assert_eq!(events.get("@exports|r").map(String::as_str), Some("2"));
        let indexed: Vec<String> = command(
            &pool,
            ::redis::cmd("ZRANGE").arg(keys::LIMITS).arg(0).arg(-1),
        )
        .await;
        assert_eq!(indexed, ["c:@exports"]);
    }

    #[tokio::test]
    async fn rate_limits_report_the_exact_wait_and_charge_all_or_none() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let reports = entry("reports", "c=10;w=2/1s");
        store(&pool, "reports", &reports).await;
        push(&pool, "reports", &["r1", "r2", "r3"]).await;
        let queues = [("reports", Some(&reports))];
        for (at, job) in [(0, "r1"), (400, "r2")] {
            let gated = gate(&pool, &queues, &format!("t{at}"), false, T0 + at).await;
            assert_eq!(found(&gated).1, job.as_bytes());
            // The second grant fills the window until the first leaves it, 1000 ms after it.
            let Gated::Found { full, .. } = gated else {
                unreachable!()
            };
            assert_eq!(
                full.map(|full| (full.wait, full.busy)),
                (at == 400).then_some((Duration::from_millis(600), false))
            );
        }
        // The window is full until the first grant leaves it, 1000 ms after it.
        let gated = gate(&pool, &queues, "t", false, T0 + 500).await;
        assert_eq!(
            gated,
            Gated::Empty {
                open: vec![],
                skipped: vec![Skipped {
                    queue: "reports".to_owned(),
                    wait: Duration::from_millis(500),
                    busy: false,
                    broken: false,
                }],
            }
        );
        // The refused fetch took no concurrency slot.
        let rules = reports.rules.clone().unwrap();
        let holders: i64 = command(&pool, ::redis::cmd("ZCARD").arg(&rules[0].state)).await;
        assert_eq!(holders, 2);
        let gated = gate(&pool, &queues, "t", false, T0 + 1_000).await;
        assert_eq!(found(&gated).1, b"r3");
        // Every key written expires.
        for key in [
            &rules[0].state,
            &rules[0].stats,
            &rules[1].state,
            &rules[1].stats,
        ] {
            assert!(pttl(&pool, key).await > 0, "{key}");
        }
    }

    #[tokio::test]
    async fn an_empty_limited_queue_is_open_and_not_charged() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let exports = entry("exports", "c=1;l=5/1s");
        store(&pool, "exports", &exports).await;
        let gated = gate(&pool, &[("exports", Some(&exports))], "t", true, T0).await;
        assert_eq!(
            gated,
            Gated::Empty {
                open: vec!["queue:exports".to_owned()],
                skipped: vec![],
            }
        );
        let keys: Vec<String> = command(&pool, ::redis::cmd("KEYS").arg("limit*")).await;
        assert!(keys.is_empty(), "{keys:?}");
        // Paused before limits: a paused limited queue is neither checked nor open.
        push(&pool, "exports", &["e1"]).await;
        let _: i64 = command(&pool, ::redis::cmd("SADD").arg(keys::PAUSED).arg("exports")).await;
        let gated = gate(&pool, &[("exports", Some(&exports))], "t", true, T0).await;
        assert_eq!(
            gated,
            Gated::Empty {
                open: vec![],
                skipped: vec![]
            }
        );
        let keys: Vec<String> = command(&pool, ::redis::cmd("KEYS").arg("limit*")).await;
        assert!(keys.is_empty(), "{keys:?}");
    }

    #[tokio::test]
    async fn an_ended_lease_frees_its_slot_for_the_next_fetch() {
        // A worker that crashed holding the only slot: its lease (a minute) ends, and the next
        // fetch takes the slot back and counts the reclaimed lease.
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let exports = entry("exports", "c=1");
        store(&pool, "exports", &exports).await;
        push(&pool, "exports", &["e1", "e2"]).await;
        let queues = [("exports", Some(&exports))];
        let gated = gate(&pool, &queues, "crashed", true, T0).await;
        assert_eq!(found(&gated).2, Some("crashed"));
        assert!(matches!(
            gate(&pool, &queues, "t", true, T0 + 59_999).await,
            Gated::Empty { .. }
        ));
        let gated = gate(&pool, &queues, "next", true, T0 + 60_001).await;
        assert_eq!(found(&gated), ("queue:exports", &b"e2"[..], Some("next")));
        let rules = exports.rules.clone().unwrap();
        let counters = stats(&pool, &rules[0]).await;
        assert_eq!(counters.get("rc").map(String::as_str), Some("1"));
        let holders: Vec<String> = command(
            &pool,
            ::redis::cmd("ZRANGE").arg(&rules[0].state).arg(0).arg(-1),
        )
        .await;
        assert_eq!(holders, ["next"]);
    }

    #[tokio::test]
    async fn every_kind_of_limit_is_enforced_at_the_fetch() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        for (specs, granted, wait) in [
            // A bucket of 2 per second: its third job waits for the next second.
            ("bucket=2/1s", 2, 1_000),
            // A leaky bucket of 2 drained in a second: a burst of 2, then one every 500 ms.
            ("leaky=2/1s", 2, 500),
        ] {
            let name = specs.split('=').next().unwrap();
            let queue = format!("q-{name}");
            let limits = entry(&queue, specs);
            store(&pool, &queue, &limits).await;
            push(&pool, &queue, &["a", "b", "c"]).await;
            let queues = [(queue.as_str(), Some(&limits))];
            for _ in 0..granted {
                let gated = gate(&pool, &queues, "t", false, T0).await;
                assert!(matches!(gated, Gated::Found { .. }), "{specs}: {gated:?}");
            }
            let gated = gate(&pool, &queues, "t", false, T0).await;
            let Gated::Empty { skipped, .. } = gated else {
                panic!("{specs}: expected a skip, got {gated:?}");
            };
            assert_eq!(skipped[0].wait, Duration::from_millis(wait), "{specs}");
            assert!(!skipped[0].busy);
        }
    }

    #[tokio::test]
    async fn the_gate_is_a_valid_feature_script() {
        let source = crate::limit::lua::LIMIT_FETCH_GATE.source();
        assert!(crate::lua::rule_violations(&source).is_empty());
        assert_eq!(source.matches("hl_now(ARGV[1])").count(), 1);
    }

    #[tokio::test]
    async fn a_limit_state_of_the_wrong_type_closes_its_queue_alone() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let exports = entry("exports", "c=2;w=10/1m");
        store(&pool, "exports", &exports).await;
        push(&pool, "exports", &["e1"]).await;
        push(&pool, "default", &["d1"]).await;
        // The window's state is a list: make it a string.
        let rules = exports.rules.clone().unwrap();
        let window = &rules[1].state;
        let _: () = command(&pool, ::redis::cmd("SET").arg(window).arg("oops")).await;
        let queues = [("exports", Some(&exports)), ("default", None)];
        let gated = gate(&pool, &queues, "t", true, T0).await;
        assert_eq!(found(&gated), ("queue:default", &b"d1"[..], None));
        let Gated::Found { skipped, .. } = &gated else {
            unreachable!()
        };
        assert_eq!(
            skipped,
            &[Skipped {
                queue: "exports".to_owned(),
                wait: Duration::ZERO,
                busy: false,
                broken: true,
            }]
        );
        // Nothing was taken or charged, and no refusal counted.
        let left: i64 = command(&pool, ::redis::cmd("LLEN").arg(keys::queue("exports"))).await;
        assert_eq!(left, 1);
        let held: i64 = command(&pool, ::redis::cmd("ZCARD").arg(&rules[0].state)).await;
        assert_eq!(held, 0);
        let refused: Option<i64> =
            command(&pool, ::redis::cmd("HGET").arg(&rules[0].stats).arg("r")).await;
        assert_eq!(refused, None);
        // Alone, it is reported as skipped too.
        let gated = gate(&pool, &queues[..1], "t", true, T0).await;
        assert!(
            matches!(&gated, Gated::Empty { open, skipped } if open.is_empty() && skipped[0].broken),
            "{gated:?}"
        );
    }

    #[tokio::test]
    async fn the_slots_of_a_failed_fetch_are_released_but_the_kept_one() {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let exports = entry("exports", "c=2");
        let reports = entry("reports", "c=1;w=10/1s");
        store(&pool, "exports", &exports).await;
        store(&pool, "reports", &reports).await;
        push(&pool, "exports", &["e1"]).await;
        push(&pool, "reports", &["r1"]).await;
        // One token granted on both queues (as a resent call could).
        let both = [("exports", Some(&exports)), ("reports", Some(&reports))];
        found(&gate(&pool, &both, "lost", true, T0).await);
        found(&gate(&pool, &both[1..], "lost", true, T0).await);
        let held = |rule: &crate::limit::rule::Rule| {
            let state = rule.state.clone();
            let pool = pool.clone();
            async move {
                let score: Option<f64> =
                    command(&pool, ::redis::cmd("ZSCORE").arg(state).arg("lost")).await;
                score.is_some()
            }
        };
        let exports_slot = &exports.rules.as_deref().unwrap()[0];
        let reports_slot = &reports.rules.as_deref().unwrap()[0];
        assert!(held(exports_slot).await && held(reports_slot).await);
        let keys: Vec<String> = both.iter().map(|(name, _)| keys::queue(name)).collect();
        let args = GateArgs {
            inprogress: INPROGRESS,
            identity: IDENTITY,
            reliable: true,
            token: "lost",
            lease: Duration::from_secs(60),
            queues: both
                .iter()
                .zip(&keys)
                .map(|((name, entry), key)| GateQueue {
                    key,
                    name,
                    entry: *entry,
                })
                .collect(),
            now: None,
        };
        // The job came from `reports`: the other slot goes back.
        queue::release_unused(&pool, &args, Some("queue:reports")).await;
        assert!(!held(exports_slot).await);
        assert!(held(reports_slot).await);
        // A failed call keeps none; releasing twice is harmless and counts nothing.
        queue::release_unused(&pool, &args, None).await;
        queue::release_unused(&pool, &args, None).await;
        assert!(!held(reports_slot).await);
        let overage: Option<i64> = command(
            &pool,
            ::redis::cmd("HGET").arg(&exports_slot.stats).arg("ov"),
        )
        .await;
        assert_eq!(overage, None);
    }
}
