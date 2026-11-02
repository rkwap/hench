//! The Limits tab (`docs/design/ratelimit.md` §2.15, §3.7.2): the list of identities (escaped
//! names and keys, usage bars, sorting, filtering, summary, declared throttles), an identity's
//! page (facts, holders, waiting jobs), Reset and Clear wait line with their CSRF and read-only
//! refusals, and the Throttle and Limited rows of job pages.

mod common;

use std::time::Duration;

use common::{App, TOKEN, add_process, add_work, jid, now_secs, post_request};
use hench::limit::{LimitKind, Limiter, Permit};
use hench::{JobPayload, redis_rs};
use http::StatusCode;
use serde_json::json;

const MINUTE: Duration = Duration::from_secs(60);

/// A window limiter that never waits.
fn window(name: &str, limit: u32) -> Limiter {
    Limiter::window(name, limit, MINUTE).max_wait(Duration::ZERO)
}

/// A full window `api` keyed `acme` with one refusal, of a caller that waits in its line (a
/// caller that does not wait for its slot takes no place in the line), and a held concurrency
/// limit `erp`.
async fn seed(app: &App) -> Permit {
    let api = window("api", 2).key("acme");
    for _ in 0..2 {
        let _permit = api.acquire(&app.pool).await.unwrap();
    }
    let patient = api.clone().max_wait(Duration::from_secs(3_600));
    let pool = app.pool.clone();
    tokio::spawn(async move {
        let _ = patient.acquire(&pool).await;
    });
    let stats = hench::keys::limit_stats('w', "api:acme");
    for _ in 0..500 {
        let mut conn = app.pool.conn().await.unwrap();
        let line: Option<String> = redis_rs::cmd("HGET")
            .arg(&stats)
            .arg("line")
            .query_async(&mut conn)
            .await
            .unwrap();
        if line.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    let erp = Limiter::concurrent("erp", 2).max_wait(Duration::ZERO);
    erp.acquire(&app.pool).await.unwrap()
}

#[tokio::test]
async fn the_list_shows_every_identity_in_use() {
    let app = App::new().await;
    let empty = app.get("/limits").await;
    empty.assert_status(StatusCode::OK);
    assert_eq!(
        empty.texts(".empty"),
        ["No limiter or throttle has been used in the last day."]
    );
    let _held = seed(&app).await;

    let res = app.get("/limits").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.texts(".nav a[aria-current=page]"), ["Limits"]);
    assert_eq!(res.title(), "Limits (2) · Hench");
    assert!(
        res.body
            .contains("2 limits in use. Last hour: 3 granted, 1 refused."),
        "{}",
        res.body
    );
    assert_eq!(res.texts(".limits-refused .chip"), ["api: 1 refused"]);
    assert_eq!(res.texts(".limit-name"), ["c:erp", "w:api:acme"]);
    assert_eq!(res.texts(".limit-kind"), ["concurrency", "window"]);
    assert_eq!(res.texts(".usage-text"), ["1 / 2", "2 / 2"]);
    assert_eq!(
        res.attrs(".usage-fill", "class"),
        [
            "usage-fill fill-50 usage-ok",
            "usage-fill fill-100 usage-full"
        ]
    );
    let text = res.texts("tbody tr").join("\n");
    assert!(text.contains("window=2/1m"), "{text}");
    assert!(text.contains("concurrency=2"), "{text}");
    assert!(text.contains("line clears in"), "{text}");
    assert_eq!(
        res.attrs(".limit-name", "href"),
        ["/limits/c/erp", "/limits/w/api:acme"]
    );
    assert_eq!(
        res.attrs("input[name=identity]", "value"),
        ["c:erp", "w:api:acme"]
    );

    // Sorting and filtering.
    let by_name = app.get("/limits?sort=name").await;
    assert_eq!(by_name.texts(".limit-name"), ["w:api:acme", "c:erp"]);
    assert_eq!(by_name.texts(".limits-sort a[aria-current]"), ["Name"]);
    let by_refusals = app.get("/limits?sort=refused").await;
    assert_eq!(by_refusals.texts(".limit-name"), ["w:api:acme", "c:erp"]);
    let filtered = app.get("/limits?substr=acme").await;
    assert_eq!(filtered.texts(".limit-name"), ["w:api:acme"]);
    let none = app.get("/limits?substr=zzz").await;
    assert_eq!(none.texts(".empty"), ["No limit matches the filter."]);
    app.get("/limits?sort=size")
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    // Pages.
    let first = app.get("/limits?count=1").await;
    assert_eq!(first.texts(".limit-name"), ["c:erp"]);
    assert!(first.count(".pager") > 0);
    let second = app.get("/limits?count=1&page=2").await;
    assert_eq!(second.texts(".limit-name"), ["w:api:acme"]);
}

#[tokio::test]
async fn names_and_keys_are_escaped_and_control_characters_spelled_out() {
    let app = App::new().await;
    let hostile = window("x", 5).key("<script>alert(1)</script>\n\u{1b}[31m");
    let _permit = hostile.acquire(&app.pool).await.unwrap();
    let res = app.get("/limits").await;
    res.assert_status(StatusCode::OK);
    assert!(!res.body.contains("<script>alert(1)"));
    assert_eq!(
        res.texts(".limit-name"),
        ["w:x:<script>alert(1)</script>\\n\\u{1b}[31m"]
    );
    // The link carries the key as stored; the checkbox too, with `%` and control characters
    // percent-encoded so that a browser posts it back unchanged (it would turn the lone LF
    // into CRLF: round 3, web-api-security-5).
    let href = res.attrs(".limit-name", "href").remove(0);
    let page = app.get(&href).await;
    page.assert_status(StatusCode::OK);
    assert!(page.texts("h1")[0].contains("\\n\\u{1b}[31m"));
    let value = res.attrs("input[name=identity]", "value").remove(0);
    assert_eq!(value, "w:x:<script>alert(1)</script>%0A%1B[31m");
    let reset = app
        .post("/limits", &[("reset", "1"), ("identity", &value)])
        .await;
    assert!(
        reset
            .cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=l-reset.1.0.")
    );
    let id = "x:<script>alert(1)</script>\n\u{1b}[31m";
    assert!(
        app.api
            .limits()
            .get(LimitKind::Window, id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn an_identity_page_shows_its_facts_holders_and_waiting_jobs() {
    let app = App::new().await;
    let _held = seed(&app).await;
    // A holder that is a running job, and one whose lease ended.
    let running = jid();
    let state = hench::keys::limit_state('c', "erp");
    let far = (now_secs() * 1_000.0) as i64 + 60_000;
    let _: () = app
        .redis(
            redis_rs::cmd("ZADD")
                .arg(&state)
                .arg(far)
                .arg(&running)
                .arg(1_000)
                .arg("crashed"),
        )
        .await;
    add_process(&app, "host:1:abc", json!({"hostname": "host"}), 1).await;
    add_work(
        &app,
        "host:1:abc",
        "t1",
        "default",
        &json!({"class": "Sync", "args": [], "jid": running, "queue": "default"}),
    )
    .await;
    // Two jobs the window rescheduled, one by another limit.
    let api = app.api.clone();
    for (index, by) in ["w:api:acme", "w:api:other", "w:api:acme"]
        .into_iter()
        .enumerate()
    {
        let mut job = JobPayload::new("Charge", vec![index.into()]);
        job.limited = Some(hench::limit::Limited::new(2, by, None));
        api.scheduled()
            .schedule(
                std::time::SystemTime::now() + MINUTE + Duration::from_secs(index as u64),
                &job,
            )
            .await
            .unwrap();
    }

    let erp = app.get("/limits/c/erp").await;
    erp.assert_status(StatusCode::OK);
    // Round 3 (web-api-security-8): a limit's page does not refresh itself (each view searches
    // the schedule for the waiting jobs).
    assert_eq!(erp.count("body[data-live-page]"), 0);
    assert_eq!(erp.texts("h1")[0].split_whitespace().next(), Some("Limit"));
    let facts = erp.texts(".kv tr").join("\n");
    for expected in [
        "Identity",
        "c:erp",
        "concurrency=2",
        "Granted",
        "Overages",
        "Releases",
    ] {
        assert!(facts.contains(expected), "{expected}: {facts}");
    }
    let holders = erp.texts("#holders-heading ~ * th[scope=row], section:nth-of-type(2) tbody th");
    assert!(holders.iter().any(|token| token == &running), "{holders:?}");
    assert!(
        holders.iter().any(|token| token == "crashed"),
        "{holders:?}"
    );
    assert!(
        erp.attrs("tbody th a", "href")
            .contains(&"/busy".to_owned())
    );
    assert_eq!(erp.texts(".chip-warn"), ["lease ended"]);
    assert!(
        erp.texts(".empty")
            .contains(&"No scheduled job is waiting for this limit.".to_owned())
    );

    let window = app.get("/limits/w/api%3Aacme").await;
    window.assert_status(StatusCode::OK);
    assert_eq!(
        window.count("#holders-heading"),
        0,
        "no holders for windows"
    );
    let waiting = window.attrs("a[href*='/scheduled/']", "href");
    assert_eq!(waiting.len(), 2, "{}", window.body);
    let entry = app.get(&waiting[0]).await;
    entry.assert_status(StatusCode::OK);
    assert!(window.body.contains("Clear wait line"));
    assert!(window.body.contains("line clears in"));

    // Unknown identities and kinds.
    app.get("/limits/w/nope")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    app.get("/limits/x/erp")
        .await
        .assert_status(StatusCode::NOT_FOUND);
    app.get("/limits/cw/erp")
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn reset_and_clear_line_act_with_a_notice() {
    let app = App::new().await;
    let _held = seed(&app).await;
    let limits = app.api.limits();

    let res = app
        .post("/limits/w/api%3Aacme", &[("clear_line", "1")])
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/limits/w/api:acme");
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=l-cleared.0.0.w%3Aapi%3Aacme")
    );
    let info = limits
        .get(LimitKind::Window, "api:acme")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.line_ends_at, None);

    let res = app.post("/limits/c/erp", &[("reset", "1")]).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/limits");
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=l-reset.0.0.c%3Aerp")
    );
    assert!(
        limits
            .get(LimitKind::Concurrency, "erp")
            .await
            .unwrap()
            .is_none()
    );
    app.post("/limits/c/erp", &[("nothing", "1")])
        .await
        .assert_status(StatusCode::BAD_REQUEST);

    // Bulk: nothing selected, then the selected rows (a malformed one counts as failed).
    let res = app.post("/limits", &[("reset", "1")]).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=none.")
    );
    let res = app
        .post(
            "/limits",
            &[
                ("reset", "1"),
                ("sort", "refused"),
                ("identity", "w:api:acme"),
                ("identity", "junk"),
            ],
        )
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(res.location(), "/limits?sort=refused");
    assert!(
        res.cookie("hench_flash")
            .unwrap()
            .starts_with("hench_flash=l-reset.1.1.")
    );
    assert_eq!(limits.size().await.unwrap(), 0);
}

#[tokio::test]
async fn actions_need_a_token_and_are_refused_when_read_only() {
    let app = App::new().await;
    let _held = seed(&app).await;
    let limits = app.api.limits();
    // A wrong token.
    let res = app
        .send(post_request(
            "/limits/c/erp",
            &[("reset", "1")],
            TOKEN,
            &"f".repeat(64),
        ))
        .await;
    res.assert_status(StatusCode::FORBIDDEN);
    assert!(
        limits
            .get(LimitKind::Concurrency, "erp")
            .await
            .unwrap()
            .is_some()
    );

    let read_only = App::build(|web| web.read_only(true), "").await;
    let _held = seed(&read_only).await;
    let page = read_only.get("/limits").await;
    page.assert_status(StatusCode::OK);
    assert_eq!(page.count("form[method=post]"), 0);
    assert_eq!(page.count("input[name=identity]"), 0);
    let detail = read_only.get("/limits/c/erp").await;
    assert_eq!(detail.count("form[method=post]"), 0);
    read_only
        .post("/limits/c/erp", &[("reset", "1")])
        .await
        .assert_status(StatusCode::FORBIDDEN);
    read_only
        .post("/limits", &[("reset", "1"), ("identity", "c:erp")])
        .await
        .assert_status(StatusCode::FORBIDDEN);
    assert!(
        read_only
            .api
            .limits()
            .get(LimitKind::Concurrency, "erp")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn throttles_declared_by_live_processes_are_listed() {
    let app = App::new().await;
    add_process(
        &app,
        "a:1:x",
        json!({"hostname": "a", "throttles": {"stripe": "c=25;w=5/1000 per key", "erp": "c=3"}}),
        0,
    )
    .await;
    add_process(
        &app,
        "b:1:y",
        json!({"hostname": "b", "throttles": {"stripe": "c=20"}}),
        0,
    )
    .await;
    let res = app.get("/limits").await;
    res.assert_status(StatusCode::OK);
    let rows = res.texts("#throttles-heading ~ * tbody tr, section:last-of-type tbody tr");
    let text = rows.join("\n");
    assert!(text.contains("erp"), "{text}");
    assert!(text.contains("c=25;w=5/1000 per key"), "{text}");
    assert!(text.contains("c=20"), "{text}");
    assert_eq!(res.texts(".chip-warn"), ["differs between processes"]);
    assert!(
        res.attrs("section:last-of-type a", "href")
            .contains(&"/limits?substr=stripe".to_owned())
    );
}

#[tokio::test]
async fn job_pages_show_the_throttle_and_limited_rows() {
    let app = App::new().await;
    let job = json!({
        "class": "Charge",
        "args": [42],
        "jid": jid(),
        "queue": "billing",
        "throttle": {"name": "stripe", "key": "42"},
        "limited": {"n": 3, "by": "w:stripe:42", "at": 1_800_000_000_000_i64},
    });
    let (job, score) = common::add_to_set(&app, "schedule", job, now_secs() + 600.0).await;
    let page = app
        .get(&format!("/scheduled/{}", common::key(&job, score)))
        .await;
    page.assert_status(StatusCode::OK);
    let rows = page.texts(".kv tr");
    assert!(
        rows.iter()
            .any(|row| row.contains("Throttle") && row.contains("stripe, key 42")),
        "{rows:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row.contains("Limited")
                && row.contains("by w:stripe:42, rescheduled 3 times")),
        "{rows:?}"
    );
    let links = page.attrs(".kv a", "href");
    assert!(
        links.contains(&"/limits?substr=stripe".to_owned()),
        "{links:?}"
    );
    assert!(
        links.contains(&"/limits/w/stripe:42".to_owned()),
        "{links:?}"
    );
    // The fields are not repeated among the extra fields.
    assert!(!rows.iter().any(|row| row.starts_with("Extra")), "{rows:?}");

    // Malformed fields get no row and stay visible as extra fields.
    let odd = json!({
        "class": "Odd", "args": [], "jid": jid(), "queue": "q",
        "throttle": [1], "limited": "soon",
    });
    let (odd, score) = common::add_to_set(&app, "schedule", odd, now_secs() + 700.0).await;
    let page = app
        .get(&format!("/scheduled/{}", common::key(&odd, score)))
        .await;
    page.assert_status(StatusCode::OK);
    let rows = page.texts(".kv tr").join("\n");
    assert!(!rows.contains("rescheduled"), "{rows}");
    assert!(
        rows.contains("limited") && rows.contains("throttle"),
        "{rows}"
    );
}
