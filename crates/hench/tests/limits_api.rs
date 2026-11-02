//! The `Limits` API against a private Redis (`docs/design/ratelimit.md` §2.13, §3.7.1): pages of
//! the identities in use with their use and counters, orders and filters, stale and gone index
//! entries, holders, resets, wait lines, the per-minute events and the jobs waiting for an
//! identity.

mod support;

use std::pin::pin;
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use hench::api::{Api, JobRecord};
use hench::limit::{Limit, LimitKind, LimitOrder, Limited, Limiter, Limits, Permit, ThrottleRef};
use hench::metrics::Period;
use hench::test_util::TestRedis;
use hench::{Error, JobPayload, RedisPool, keys};
use support::*;

const MINUTE: Duration = Duration::from_secs(60);

/// A limiter that never waits.
fn window(name: &str, limit: u32) -> Limiter {
    Limiter::window(name, limit, MINUTE).max_wait(Duration::ZERO)
}

/// A concurrency limiter that never waits.
fn concurrent(name: &str, limit: u32) -> Limiter {
    Limiter::concurrent(name, limit).max_wait(Duration::ZERO)
}

#[track_caller]
fn refused(result: Result<Permit, Error>) {
    assert!(
        matches!(result, Err(Error::OverLimit(_))),
        "expected a refusal, got {result:?}"
    );
}

/// A caller of `limiter` waiting in process for its slot, however far (a caller that will not
/// wait for its slot takes no place in the wait line): spawned, and once it holds its place in
/// the line of `stats` (a `limit-stats:` key), returned so that the test can stop it.
async fn wait_in_line(
    limiter: &Limiter,
    pool: &RedisPool,
    stats: &str,
) -> tokio::task::JoinHandle<()> {
    let patient = limiter.clone().max_wait(Duration::from_secs(3_600));
    let target = pool.clone();
    let waiter = tokio::spawn(async move {
        let _ = patient.acquire(&target).await;
    });
    wait_for("a place in the line", Duration::from_secs(10), || async {
        hgetall(pool, stats).await.contains_key("line")
    })
    .await;
    waiter
}

/// Index score of a member (`None` when absent).
async fn indexed(pool: &RedisPool, member: &str) -> Option<f64> {
    cmd(pool, &redis_cmd!("ZSCORE", keys::LIMITS, member)).await
}

fn ids(page: &[hench::limit::LimitInfo]) -> Vec<String> {
    page.iter().map(hench::limit::LimitInfo::identity).collect()
}

#[tokio::test]
async fn pages_list_the_identities_in_use_with_their_counters() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limits: Limits = Api::new(pool.clone()).limits();
    assert_eq!(limits.size().await.unwrap(), 0);
    assert!(
        limits
            .page(1, 10, LimitOrder::LastUsed)
            .await
            .unwrap()
            .items
            .is_empty()
    );

    let api = window("api", 2).key("acme");
    let _a = api.acquire(&pool).await.unwrap();
    let _b = api.acquire(&pool).await.unwrap();
    // Refused, and waiting in line for its slot.
    let waiter = wait_in_line(&api, &pool, &keys::limit_stats('w', "api:acme")).await;
    // The index orders by last use, in milliseconds: make the next use a later one.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let erp = concurrent("erp", 1);
    let held = erp.acquire(&pool).await.unwrap();
    refused(erp.acquire(&pool).await);
    refused(erp.acquire(&pool).await);

    assert_eq!(limits.size().await.unwrap(), 2);
    let page = limits.page(1, 10, LimitOrder::LastUsed).await.unwrap();
    assert_eq!(page.total, 2);
    assert_eq!(ids(&page.items), ["c:erp", "w:api:acme"]);

    let before = SystemTime::now();
    let info = limits
        .get(LimitKind::Window, "api:acme")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.kind, LimitKind::Window);
    assert_eq!(
        (info.name.as_str(), info.key.as_deref()),
        ("api", Some("acme"))
    );
    assert_eq!(
        info.spec,
        Some(Limit::Window {
            limit: 2,
            period: MINUTE
        })
    );
    assert_eq!(info.used, 2.0);
    assert_eq!(info.fraction_used(), Some(1.0));
    assert_eq!((info.granted, info.refused), (2, 1));
    let free_in = info.next_free_in.unwrap();
    assert!(
        free_in > Duration::from_secs(50) && free_in <= MINUTE,
        "{free_in:?}"
    );
    let line = info.line_ends_at.unwrap();
    assert!(line > before, "the waiting caller started a wait line");
    waiter.abort();
    let last = info.last_used.unwrap();
    assert!(last <= SystemTime::now() + Duration::from_secs(1));
    // The page lists the same record, read a moment earlier: its next free unit was then as
    // far away or a little further.
    let listed = page
        .items
        .iter()
        .find(|item| item.identity() == info.identity())
        .unwrap();
    assert!(listed.next_free_in >= info.next_free_in);
    let mut same = listed.clone();
    same.next_free_in = info.next_free_in;
    assert_eq!(same, info);

    let erp_info = limits
        .get(LimitKind::Concurrency, "erp")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(erp_info.spec, Some(Limit::Concurrency { limit: 1 }));
    assert_eq!(
        (erp_info.used, erp_info.granted, erp_info.refused),
        (1.0, 1, 2)
    );
    assert!(erp_info.next_free_in.unwrap() <= Limiter::LEASE);
    assert!(
        limits
            .get(LimitKind::Window, "erp")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        limits
            .get(LimitKind::Window, "nope")
            .await
            .unwrap()
            .is_none()
    );

    // Holders: the permit's lease, live.
    let holders = limits.holders("erp").await.unwrap();
    assert_eq!(holders.len(), 1);
    assert!(!holders[0].ended);
    assert!(holders[0].lease_ends_at > SystemTime::now());
    assert!(limits.holders("nope").await.unwrap().is_empty());
    held.release().await.unwrap();
    let erp_info = limits
        .get(LimitKind::Concurrency, "erp")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((erp_info.used, erp_info.releases), (0.0, 1));
    assert!(erp_info.average_hold().is_some());

    // Orders and filters.
    let by_name = limits.page(1, 10, LimitOrder::Name).await.unwrap();
    assert_eq!(ids(&by_name.items), ["w:api:acme", "c:erp"]);
    let by_refusals = limits.page(1, 10, LimitOrder::Refused).await.unwrap();
    assert_eq!(ids(&by_refusals.items), ["c:erp", "w:api:acme"]);
    let acme = limits
        .page_matching(1, 10, LimitOrder::LastUsed, "acme")
        .await
        .unwrap();
    assert_eq!(
        (ids(&acme.items), acme.total),
        (vec!["w:api:acme".to_owned()], 1)
    );
    let wild = limits
        .page_matching(1, 10, LimitOrder::Name, "e*p")
        .await
        .unwrap();
    assert_eq!(ids(&wild.items), ["c:erp"]);
    assert!(
        limits
            .page_matching(1, 10, LimitOrder::Name, "zzz")
            .await
            .unwrap()
            .items
            .is_empty()
    );

    // Pagination.
    let first = limits.page(1, 1, LimitOrder::LastUsed).await.unwrap();
    let second = limits.page(2, 1, LimitOrder::LastUsed).await.unwrap();
    let third = limits.page(3, 1, LimitOrder::LastUsed).await.unwrap();
    assert_eq!(
        (first.items.len(), second.items.len(), third.items.len()),
        (1, 1, 0)
    );
    assert_eq!((first.total, first.total_pages()), (2, 2));
    assert!(first.has_next() && !second.has_next());
    assert_ne!(first.items, second.items);
    let named = limits.page(2, 1, LimitOrder::Name).await.unwrap();
    assert_eq!(ids(&named.items), ["c:erp"]);
}

#[tokio::test]
async fn stale_and_gone_index_entries_are_skipped_and_dropped() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limits = Api::new(pool.clone()).limits();
    let _permit = window("live", 5).acquire(&pool).await.unwrap();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let _: () = cmd(
        &pool,
        &redis_cmd!(
            "ZADD",
            keys::LIMITS,
            now - 10.0,
            "w:stale",
            now + 3_600.0,
            "w:gone",
            now + 3_600.0,
            "junk"
        ),
    )
    .await;
    // Stale entries are not counted; gone ones are until a page finds them gone.
    assert_eq!(limits.size().await.unwrap(), 3);
    let page = limits.page(1, 10, LimitOrder::LastUsed).await.unwrap();
    assert_eq!(ids(&page.items), ["w:live"]);
    assert_eq!(
        indexed(&pool, "w:gone").await,
        None,
        "gone entries are dropped"
    );
    assert!(indexed(&pool, "w:live").await.is_some());
    assert_eq!(limits.size().await.unwrap(), 2);
    // A malformed member is skipped, never read.
    let page = limits.page(1, 10, LimitOrder::Name).await.unwrap();
    assert_eq!(ids(&page.items), ["w:live"]);
}

#[tokio::test]
async fn entries_stream_every_identity_once() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limits = Api::new(pool.clone()).limits();
    let api = window("bulk", 1);
    for key in 0..250 {
        let _permit = api.key(key).acquire(&pool).await.unwrap();
    }
    let mut entries = pin!(limits.entries());
    let mut seen = std::collections::HashSet::new();
    while let Some(entry) = entries.next().await {
        let entry = entry.unwrap();
        assert_eq!(entry.used, 1.0);
        assert!(seen.insert(entry.identity()), "{} twice", entry.identity());
    }
    assert_eq!(seen.len(), 250);
    assert!(seen.contains("w:bulk:0") && seen.contains("w:bulk:249"));
}

#[tokio::test]
async fn entries_step_over_gone_entries_without_missing_live_ones() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limits = Api::new(pool.clone()).limits();
    let api = window("bulk", 1);
    for key in 0..150 {
        let _permit = api.key(key).acquire(&pool).await.unwrap();
    }
    // Index entries whose state and counters are gone, ahead of every live one (most recent):
    // the stream's first read finds them gone.
    let later = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        + 2.0 * 86_400.0;
    for index in 0..60 {
        let _: () = cmd(
            &pool,
            &redis_cmd!("ZADD", keys::LIMITS, later, format!("w:gone-{index}")),
        )
        .await;
    }
    let mut entries = pin!(limits.entries());
    let mut seen = std::collections::HashSet::new();
    while let Some(entry) = entries.next().await {
        seen.insert(entry.unwrap().identity());
    }
    assert_eq!(seen.len(), 150, "every live identity, once");
    assert!(seen.iter().all(|identity| identity.starts_with("w:bulk:")));
    // The stream left the gone entries; a page drops them.
    assert!(indexed(&pool, "w:gone-0").await.is_some());
    limits.page(1, 100, LimitOrder::LastUsed).await.unwrap();
    assert_eq!(indexed(&pool, "w:gone-0").await, None);
}

#[tokio::test]
async fn reset_frees_a_full_concurrency_limit() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limits = Api::new(pool.clone()).limits();
    let erp = concurrent("erp", 1);
    let first = erp.acquire(&pool).await.unwrap();
    refused(erp.acquire(&pool).await);
    assert!(limits.reset(LimitKind::Concurrency, "erp").await.unwrap());
    assert_eq!(indexed(&pool, "c:erp").await, None);
    assert!(!limits.reset(LimitKind::Concurrency, "erp").await.unwrap());
    let second = erp.acquire(&pool).await.unwrap();
    // The first holder still runs; its release finds no lease and counts an overage.
    first.release().await.unwrap();
    let info = limits
        .get(LimitKind::Concurrency, "erp")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((info.used, info.granted, info.overages), (1.0, 1, 1));
    second.release().await.unwrap();

    // An ended lease is listed as ended until a grant takes its slot back.
    let state = keys::limit_state('c', "erp");
    let _: () = cmd(&pool, &redis_cmd!("ZADD", &state, 1_000, "crashed")).await;
    let holders = limits.holders("erp").await.unwrap();
    assert_eq!(holders.len(), 1);
    assert_eq!(holders[0].token, "crashed");
    assert!(holders[0].ended);
    assert_eq!(
        holders[0].lease_ends_at,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1)
    );
    let _third = erp.acquire(&pool).await.unwrap();
    let info = limits
        .get(LimitKind::Concurrency, "erp")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.reclaimed, 1);
}

#[tokio::test]
async fn clearing_a_wait_line() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limits = Api::new(pool.clone()).limits();
    let api = window("api", 1);
    let _permit = api.acquire(&pool).await.unwrap();
    // A caller that will not wait is told when the next unit frees, without a place in line.
    refused(api.acquire(&pool).await);
    let info = limits.get(LimitKind::Window, "api").await.unwrap().unwrap();
    assert_eq!(info.line_ends_at, None);
    // A caller waiting for its slot takes one.
    let waiter = wait_in_line(&api, &pool, &keys::limit_stats('w', "api")).await;
    waiter.abort();
    let info = limits.get(LimitKind::Window, "api").await.unwrap().unwrap();
    assert!(info.line_ends_at.is_some());
    assert!(limits.clear_line(LimitKind::Window, "api").await.unwrap());
    let info = limits.get(LimitKind::Window, "api").await.unwrap().unwrap();
    assert_eq!(info.line_ends_at, None);
    assert_eq!(info.refused, 2, "the counters stay");
    assert!(!limits.clear_line(LimitKind::Window, "api").await.unwrap());
    assert!(!limits.clear_line(LimitKind::Window, "nope").await.unwrap());
}

#[tokio::test]
async fn long_keys_are_found_as_used_or_as_stored() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limits = Api::new(pool.clone()).limits();
    let long = "k".repeat(300);
    let _permit = window("api", 5).key(&long).acquire(&pool).await.unwrap();
    let info = limits
        .get(LimitKind::Window, &format!("api:{long}"))
        .await
        .unwrap()
        .unwrap();
    let stored = info.key.clone().unwrap();
    assert!(stored.starts_with('#') && stored.len() == 23, "{stored}");
    let again = limits
        .get(LimitKind::Window, &info.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.identity(), info.identity());
}

#[tokio::test]
async fn events_count_grants_and_refusals_per_name_and_minute() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let limits = Api::new(pool.clone()).limits();
    let api = window("api", 2);
    let _a = api.acquire(&pool).await.unwrap();
    let _b = api.acquire(&pool).await.unwrap();
    refused(api.acquire(&pool).await);
    let _c = window("other", 5).acquire(&pool).await.unwrap();
    let events = limits.events(Period::Minutes(5)).await.unwrap();
    assert_eq!(events.totals, (3, 1));
    assert_eq!(events.totals_of("api"), (2, 1));
    assert_eq!(events.totals_of("other"), (1, 0));
    let series = &events.by_name["api"];
    assert_eq!(series.len(), 5);
    assert!(series.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(series.iter().map(|minute| minute.1).sum::<u64>(), 2);
    assert_eq!(events.most_refused(5), [("api".to_owned(), 2, 1)]);
    assert!(matches!(
        limits.events(Period::Hours(1)).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn waiting_finds_the_jobs_an_identity_rescheduled() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let limits = api.limits();
    let now = SystemTime::now();
    let mut jobs = Vec::new();
    for (index, by) in ["w:api:acme", "w:api:other", "w:api:acme", "c:api:acme"]
        .into_iter()
        .enumerate()
    {
        let mut job = JobPayload::new("Charge", vec![index.into()]);
        job.limited = Some(Limited::new(1, by, None));
        api.scheduled()
            .schedule(now + Duration::from_secs(60 + index as u64), &job)
            .await
            .unwrap();
        jobs.push(job);
    }
    api.scheduled()
        .schedule(now, &JobPayload::new("Plain", vec![]))
        .await
        .unwrap();
    let waiting = limits
        .waiting(LimitKind::Window, "api:acme", 10)
        .await
        .unwrap();
    let jids: Vec<&str> = waiting
        .iter()
        .filter_map(|entry| entry.record().jid())
        .collect();
    assert_eq!(jids, [jobs[0].jid.as_str(), jobs[2].jid.as_str()]);
    assert_eq!(
        limits
            .waiting(LimitKind::Window, "api:acme", 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        limits
            .waiting(LimitKind::Window, "api:acme", 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        limits
            .waiting(LimitKind::Concurrency, "api:acme", 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// `total_net_output_bytes` of the test's Redis: what it sent to its clients so far.
async fn bytes_sent(pool: &RedisPool) -> u64 {
    let info: String = cmd(pool, &redis_cmd!("INFO", "stats")).await;
    info.lines()
        .find_map(|line| line.strip_prefix("total_net_output_bytes:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or_default()
}

/// Round 3 (web-api-security-8): looking for the jobs an identity rescheduled reads the schedule
/// in Redis, which sends only the jobs that may match; it used to send every scheduled job it
/// looked at (up to 50,000: about 48 MiB per view of a limit's page, which refreshed itself).
/// Labels are matched as the engine writes them (escaped), and a job whose arguments merely
/// contain the label is not taken for one that waits.
#[tokio::test]
async fn waiting_reads_the_schedule_in_redis() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let api = Api::new(pool.clone());
    let limits = api.limits();
    let now = SystemTime::now();
    let key = "a\"b\\c";
    let label = format!("w:api:{key}");
    let padding = "y".repeat(1_000);
    for index in 0..3_000_u64 {
        let mut job = JobPayload::new("Report", vec![index.into(), padding.clone().into()]);
        if index == 7 {
            // Arguments holding the label as text: not a job waiting for it.
            job.args.push(serde_json::json!({ "by": label.clone() }));
        }
        api.scheduled()
            .schedule(now + Duration::from_secs(60 + index), &job)
            .await
            .unwrap();
    }
    let mut waiting_jobs = Vec::new();
    for offset in [30, 2_500] {
        let mut job = JobPayload::new("Charge", vec![offset.into()]);
        job.limited = Some(Limited::new(2, label.clone(), None));
        api.scheduled()
            .schedule(now + Duration::from_secs(60 + offset), &job)
            .await
            .unwrap();
        waiting_jobs.push(job.jid);
    }
    let before = bytes_sent(&pool).await;
    let waiting = limits
        .waiting(LimitKind::Window, &format!("api:{key}"), 100)
        .await
        .unwrap();
    let sent = bytes_sent(&pool).await - before;
    let jids: Vec<&str> = waiting
        .iter()
        .filter_map(|entry| entry.record().jid())
        .collect();
    assert_eq!(jids, [waiting_jobs[0].as_str(), waiting_jobs[1].as_str()]);
    assert!(
        waiting
            .windows(2)
            .all(|pair| pair[0].score() <= pair[1].score()),
        "earliest first"
    );
    assert!(sent < 64 * 1024, "Redis sent {sent} bytes for two matches");
    // `max` stops the search.
    let first = limits
        .waiting(LimitKind::Window, &format!("api:{key}"), 1)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].record().jid(), Some(waiting_jobs[0].as_str()));
}

#[test]
fn job_records_read_their_limit_fields_leniently() {
    let api = Api::new(RedisPool::detached());
    let record = JobRecord::new(
        &api,
        r#"{"class":"A","args":[],"jid":"1","queue":"q","throttle":{"name":"stripe","key":"42"},"limited":{"n":3,"by":"w:stripe:42","at":1800000000000}}"#,
        None,
    );
    assert_eq!(
        record.limited(),
        Some(Limited::new(3, "w:stripe:42", Some(1_800_000_000_000)))
    );
    let throttle = record.throttle().unwrap();
    assert_eq!(
        (throttle.name.as_str(), throttle.key.as_deref()),
        ("stripe", Some("42"))
    );
    for value in [
        r#"{"class":"A","throttle":false,"limited":"x"}"#,
        r#"{"class":"A","throttle":{"key":"42"},"limited":{"n":-1}}"#,
        r#"{"class":"A","throttle":[1],"limited":null}"#,
        "not json",
    ] {
        let record = JobRecord::new(&api, value, None);
        assert_eq!(record.limited(), None, "{value}");
        assert_eq!(record.throttle(), None::<ThrottleRef>, "{value}");
    }
}
