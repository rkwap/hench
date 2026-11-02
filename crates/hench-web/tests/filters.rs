//! Filters (`?substr=`) on queues and sets: literal text with `*` as the only wildcard, a
//! bounded amount of work per request, results in the list's own order, and summaries that
//! say what was searched.

mod common;

use common::*;
use hench::redis_rs;
use http::StatusCode;
use serde_json::json;

/// The `calls` and `usec` of a command in `INFO commandstats`.
fn command_stats(info: &str, command: &str) -> (u64, u64) {
    let Some(line) = info
        .lines()
        .find(|line| line.starts_with(&format!("cmdstat_{command}:")))
    else {
        return (0, 0);
    };
    let field = |name: &str| {
        line.split([':', ','])
            .find_map(|part| part.strip_prefix(&format!("{name}=")))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    };
    (field("calls"), field("usec"))
}

/// The Redis CPU (µs, every command) and the `ZSCAN`/`ZRANGE` calls one `GET path` costs.
async fn redis_cost(app: &App, path: &str) -> (u64, u64, u64) {
    let _: () = app.redis(redis_rs::cmd("CONFIG").arg("RESETSTAT")).await;
    app.get(path).await.assert_status(StatusCode::OK);
    let info: String = app.redis(redis_rs::cmd("INFO").arg("commandstats")).await;
    let usec = info
        .lines()
        .filter(|line| line.starts_with("cmdstat_"))
        .filter_map(|line| {
            line.split([':', ','])
                .find_map(|part| part.strip_prefix("usec="))
                .and_then(|value| value.parse::<u64>().ok())
        })
        .sum();
    (
        usec,
        command_stats(&info, "zscan").0,
        command_stats(&info, "zrange").0,
    )
}

/// Adds `count` retries of about 1.9 KB (`App::Job0`, `App::Job1`, ...), due one second
/// apart from `base`.
async fn add_retries(app: &App, count: u32, base: f64) {
    let filler: String = (0..1_800)
        .map(|i| char::from(b'a' + (i * 7 % 26) as u8))
        .collect();
    for chunk in 0..count.div_ceil(1_000) {
        let mut zadd = redis_rs::cmd("ZADD");
        zadd.arg("retry");
        for i in chunk * 1_000..((chunk + 1) * 1_000).min(count) {
            let job = json!({"class": format!("App::Job{i}"), "args": [filler], "jid": format!("{i:024x}"),
                             "queue": "default", "retry": true, "retry_count": 1,
                             "error_class": "E", "error_message": "m"});
            zadd.arg(base + f64::from(i)).arg(job.to_string());
        }
        let _: u64 = app.redis(&zadd).await;
    }
}

#[tokio::test]
async fn glob_characters_cost_no_more_than_plain_text() {
    // web-security-3: a `*` used to pass the filter to `ZSCAN MATCH` as a Redis glob, where
    // `?` and `[..]` are wildcards too: `*` + 254 `?` + `~` cost Redis O(length x pattern) per
    // entry (13 s of CPU for 20,000 retries, ~70x a plain filter).
    let app = App::new().await;
    add_retries(&app, 3_000, now_secs()).await;
    let (plain, _, _) = redis_cost(&app, "/retries?substr=nomatchxyz").await;
    let glob = format!("*{}~", "%3F".repeat(254));
    let (costly, zscans, _) = redis_cost(&app, &format!("/retries?substr={glob}")).await;
    assert_eq!(zscans, 0, "filters do not run Redis globs");
    assert!(
        costly < plain.max(1_000) * 3,
        "a 256-character glob filter cost {costly} µs of Redis CPU, a plain one {plain} µs"
    );
    // `?`, `[` and `]` are taken literally; `*` stands for any run of characters.
    let res = app.get("/retries?substr=App%3A%3AJob1%3F").await;
    assert_eq!(res.count("[data-check-item]"), 0);
    let res = app.get("/retries?substr=App%3A%3AJob%5B1%5D").await;
    assert_eq!(res.count("[data-check-item]"), 0);
    let res = app.get("/retries?substr=Job12*%22jid%22").await;
    // Job12, Job120..129, Job1200..1299: 1 + 10 + 100.
    assert_eq!(res.count("[data-check-item]"), 111);
    let res = app.get("/retries?substr=*").await;
    assert_eq!(res.count("[data-check-item]"), 500);
}

#[tokio::test]
async fn set_filters_examine_a_bounded_number_of_jobs() {
    let app = App::new().await;
    // 55,000 small retries; the filter matches none of them.
    for chunk in 0..55 {
        let mut zadd = redis_rs::cmd("ZADD");
        zadd.arg("retry");
        for i in chunk * 1_000..(chunk + 1) * 1_000 {
            zadd.arg(f64::from(i)).arg(
                json!({"class": "Small", "args": [i], "jid": format!("{i:024x}"), "queue": "q"})
                    .to_string(),
            );
        }
        let _: u64 = app.redis(&zadd).await;
    }
    let (_, zscans, zranges) = redis_cost(&app, "/retries?substr=nothing-like-this").await;
    assert_eq!(zscans, 0);
    assert!(
        zranges <= 50,
        "{zranges} pages of 1,000 read for one filter"
    );
    let res = app.get("/retries?substr=nothing-like-this").await;
    assert_eq!(
        res.texts(".filter-summary"),
        [
            "0 jobs matching “nothing-like-this” in the first 50,000 of 55,000 jobs; the others were not searched."
        ]
    );
    assert_eq!(
        res.texts("p.empty"),
        ["None of the jobs searched match this filter."]
    );
}

#[tokio::test]
async fn queue_filters_say_when_they_stopped_early() {
    // web-functional-5: a queue filter stops after 50,000 jobs, and used to say "No jobs
    // match this filter." although the rest of the queue was not searched.
    let app = App::new().await;
    let mut conn = app.pool.conn().await.expect("conn");
    for chunk in 0..60 {
        let mut cmd = redis_rs::cmd("LPUSH");
        cmd.arg("queue:bigq");
        for index in chunk * 1_000..(chunk + 1) * 1_000 {
            cmd.arg(json!({"class": "BigJob", "args": [format!("payload-{index}")]}).to_string());
        }
        let _: i64 = cmd.query_async(&mut conn).await.expect("lpush");
    }
    let _: i64 = app
        .redis(redis_rs::cmd("SADD").arg("queues").arg("bigq"))
        .await;
    // payload-42 is among the oldest: newest first, the search stops before it.
    let newest = app.get("/queues/bigq?substr=%22payload-42%22").await;
    assert_eq!(
        newest.texts(".filter-summary"),
        [
            "0 jobs matching “\"payload-42\"” in the first 50,000 of 60,000 jobs; the others were not searched."
        ]
    );
    assert_eq!(
        newest.texts("p.empty"),
        ["None of the jobs searched match this filter."]
    );
    // Oldest first, it is found, at its place in line.
    let oldest = app
        .get("/queues/bigq?substr=%22payload-42%22&direction=asc")
        .await;
    assert_eq!(
        oldest.texts(".filter-summary"),
        [
            "1 job matching “\"payload-42\"” in the first 50,000 of 60,000 jobs; the others were not searched."
        ]
    );
    assert_eq!(oldest.texts("tbody td.num"), ["43"]);
}

#[tokio::test]
async fn set_filters_show_the_first_matches_in_the_list_order() {
    // web-functional-5: the 500 matches shown used to be the first 500 `ZSCAN` returned (hash
    // order), sorted afterwards: the soonest jobs could be missing.
    let app = App::new().await;
    let base = now_secs() + 86_400.0;
    let mut conn = app.pool.conn().await.expect("conn");
    for chunk in 0..5 {
        let mut cmd = redis_rs::cmd("ZADD");
        cmd.arg("schedule");
        for index in chunk * 1_000..(chunk + 1) * 1_000 {
            cmd.arg(base + f64::from(index)).arg(
                json!({"class": "LaterJob", "args": [index], "queue": "default",
                       "jid": format!("later{index:05}")})
                .to_string(),
            );
        }
        let _: i64 = cmd.query_async(&mut conn).await.expect("zadd");
    }
    let res = app.get("/scheduled?substr=LaterJob").await;
    assert_eq!(
        res.texts(".filter-summary"),
        ["Showing the first 500 jobs matching “LaterJob”."]
    );
    let shown: Vec<String> = res.attrs("input[name=key]", "value");
    let soonest: Vec<String> = (0..500).map(|index| format!("later{index:05}")).collect();
    assert_eq!(
        shown
            .iter()
            .map(|key| key.rsplit('-').next().unwrap_or_default().to_owned())
            .collect::<Vec<_>>(),
        soonest
    );
    // The morgue lists the latest deaths first, and so does its filter.
    for index in 0..3 {
        add_to_set(
            &app,
            "dead",
            json!({"class": "Gone", "args": [index], "queue": "q", "jid": format!("dead{index:020}")}),
            now_secs() - 100.0 + f64::from(index),
        )
        .await;
    }
    let res = app.get("/morgue?substr=Gone").await;
    assert_eq!(res.texts("code.args"), ["2", "1", "0"]);
}

#[tokio::test]
async fn filtered_queues_show_places_in_line() {
    // web-functional-6: filtered rows were numbered 1, 2, 3... in display order, while "#" is
    // the place in line everywhere else (1 = next to run).
    let app = App::new().await;
    for index in 2..7 {
        push_raw(
            &app,
            "static",
            &json!({"class": "StaticJob", "args": [index], "jid": jid()}),
        )
        .await;
    }
    let rows = |res: &Res| -> Vec<(String, String)> {
        res.texts("tbody td.num")
            .into_iter()
            .zip(res.texts("tbody td.args-cell"))
            .collect()
    };
    let unfiltered = rows(&app.get("/queues/static").await);
    assert_eq!(unfiltered[0], ("5".to_owned(), "6".to_owned()));
    assert_eq!(
        rows(&app.get("/queues/static?substr=StaticJob").await),
        unfiltered
    );
    assert_eq!(
        rows(&app.get("/queues/static?substr=%5B5%5D").await),
        [("4".to_owned(), "5".to_owned())]
    );
    assert_eq!(
        rows(&app.get("/queues/static?substr=%5B5%5D&direction=asc").await),
        [("4".to_owned(), "5".to_owned())]
    );
    let oldest = rows(
        &app.get("/queues/static?substr=StaticJob&direction=asc")
            .await,
    );
    assert_eq!(oldest[0], ("1".to_owned(), "2".to_owned()));
}
