//! CSRF protection, cross-site refusal, read-only mode, escaping of hostile data, security
//! headers and parameter bounds.

mod common;

use axum::body::Body;
use common::*;
use http::{Request, StatusCode};
use serde_json::json;

const OTHER_TOKEN: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

#[tokio::test]
async fn posts_need_the_csrf_token_of_their_cookie() {
    let app = App::new().await;
    let (job, score) = add_retry(&app, &jid()).await;
    let key = key(&job, score);
    let fields = [("key", key.as_str()), ("delete", "1")];

    // No cookie, no field, a wrong field, a malformed token: refused, nothing changes.
    let mut no_cookie = post_request("/retries", &fields, TOKEN, TOKEN);
    no_cookie.headers_mut().remove("cookie");
    let attempts = [
        no_cookie,
        post_request("/retries", &fields, TOKEN, ""),
        post_request("/retries", &fields, TOKEN, OTHER_TOKEN),
        post_request("/retries", &fields, "short", "short"),
        post_request(
            "/retries",
            &fields,
            &TOKEN.to_uppercase(),
            &TOKEN.to_uppercase(),
        ),
    ];
    for request in attempts {
        let res = app.send(request).await;
        res.assert_status(StatusCode::FORBIDDEN);
        assert!(res.body.contains("Reload the page"), "{}", res.body);
    }
    assert_eq!(zcard(&app, "retry").await, 1);

    // The token of the page's own cookie is accepted.
    app.send(post_request("/retries", &fields, TOKEN, TOKEN))
        .await
        .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(zcard(&app, "retry").await, 0);
}

#[tokio::test]
async fn pages_issue_a_token_cookie_and_embed_it_in_forms() {
    let app = App::new().await;
    add_retry(&app, &jid()).await;
    let page = app.get("/retries").await;
    let cookie = page.cookie("hench_csrf").expect("a token cookie");
    assert!(cookie.contains("; Path=/;"), "{cookie}");
    assert!(
        cookie.contains("HttpOnly") && cookie.contains("SameSite=Lax"),
        "{cookie}"
    );
    assert!(!cookie.contains("Secure"), "{cookie}");
    let token = cookie
        .split(';')
        .next()
        .unwrap()
        .trim_start_matches("hench_csrf=")
        .to_owned();
    assert_eq!(token.len(), 64);
    let fields = page.attrs("input[name=_csrf]", "value");
    assert!(!fields.is_empty());
    assert!(fields.iter().all(|field| *field == token));

    // A browser that has the cookie keeps it: no new token, and forms carry the cookie's.
    let again = app
        .send(
            Request::get("/retries")
                .header("cookie", format!("hench_csrf={TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(again.cookie("hench_csrf").is_none());
    assert!(
        again
            .attrs("input[name=_csrf]", "value")
            .iter()
            .all(|field| field == TOKEN)
    );

    // Each new browser gets its own token.
    let other = app.get("/retries").await;
    assert_ne!(other.cookie("hench_csrf").unwrap(), cookie);

    // Behind an HTTPS proxy the cookie is Secure.
    let https = app
        .send(
            Request::get("/retries")
                .header("x-forwarded-proto", "https")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(https.cookie("hench_csrf").unwrap().contains("; Secure"));
}

#[tokio::test]
async fn cross_site_navigations_keep_the_token_of_open_pages() {
    // web-security-4: with a `SameSite=Strict` cookie, a link from another site (chat, a
    // ticket, an SSO redirect) arrived without it; the dashboard issued a new token whose
    // cookie replaced the one the forms of every open tab carry, and their POSTs got 403.
    let app = App::new().await;
    let (job, score) = add_retry(&app, &jid()).await;
    let page = app.get("/retries").await;
    let cookie = page.cookie("hench_csrf").expect("a token cookie");
    assert!(cookie.contains("SameSite=Lax"), "{cookie}");
    let token = page.attrs("input[name=_csrf]", "value").remove(0);

    // A Lax cookie comes along with a top-level navigation from another site: no new token.
    let navigation = Request::get("/queues")
        .header("sec-fetch-site", "cross-site")
        .header("sec-fetch-mode", "navigate")
        .header("cookie", format!("hench_csrf={token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.send(navigation).await;
    res.assert_status(StatusCode::OK);
    assert!(res.cookie("hench_csrf").is_none());

    // A refused cross-site POST (a form on another site, which Lax cookies do not reach) does
    // not hand out a cookie that would replace the open pages' token.
    let refused = Request::post("/queues/default")
        .header("sec-fetch-site", "cross-site")
        .header("origin", "http://evil.example")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("pause=1"))
        .unwrap();
    let res = app.send(refused).await;
    res.assert_status(StatusCode::FORBIDDEN);
    assert!(res.cookies().is_empty(), "{:?}", res.cookies());

    // The page opened first still works.
    let key = key(&job, score);
    app.send(post_request(
        "/retries",
        &[("key", key.as_str()), ("delete", "1")],
        &token,
        &token,
    ))
    .await
    .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(zcard(&app, "retry").await, 0);
}

#[tokio::test]
async fn cross_site_posts_are_refused() {
    // Reached as dash.example (the requests below name it).
    let app = App::build(
        |web| web.allowed_hosts(hench_web::AllowedHosts::new(["dash.example"])),
        "",
    )
    .await;
    let (job, score) = add_retry(&app, &jid()).await;
    let key = key(&job, score);
    let fields = [("key", key.as_str()), ("delete", "1")];
    for (header, value) in [
        ("sec-fetch-site", "cross-site"),
        ("sec-fetch-site", "same-site"),
        ("sec-fetch-site", "none"),
    ] {
        let mut request = post_request("/retries", &fields, TOKEN, TOKEN);
        request.headers_mut().insert(header, value.parse().unwrap());
        let res = app.send(request).await;
        res.assert_status(StatusCode::FORBIDDEN);
        assert!(res.body.contains("another site"));
    }
    // Without Sec-Fetch-Site, Origin decides.
    for (origin, allowed) in [
        ("https://evil.example", false),
        ("null", false),
        ("http://dash.example", true),
    ] {
        let mut request = post_request("/retries", &[("retry", "1")], TOKEN, TOKEN);
        request.headers_mut().remove("sec-fetch-site");
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        request
            .headers_mut()
            .insert("host", "dash.example".parse().unwrap());
        let res = app.send(request).await;
        let expected = if allowed {
            StatusCode::SEE_OTHER
        } else {
            StatusCode::FORBIDDEN
        };
        assert_eq!(res.status, expected, "{origin}");
    }
    assert_eq!(zcard(&app, "retry").await, 1);
}

#[tokio::test]
async fn read_only_dashboards_show_no_actions_and_refuse_posts() {
    let app = App::build(|web| web.read_only(true), "").await;
    let (job, score) = add_retry(&app, &jid()).await;
    add_scheduled(&app, &jid()).await;
    add_dead(&app, &jid()).await;
    push_raw(&app, "default", &json!({"class": "X", "args": []})).await;
    add_process(&app, "host:1:x", json!({"hostname": "host", "pid": 1}), 0).await;
    let entry = format!("/retries/{}", key(&job, score));
    for path in [
        "/",
        "/busy",
        "/queues",
        "/queues/default",
        "/retries",
        "/scheduled",
        "/morgue",
        "/metrics",
        "/metrics/HardJob",
        entry.as_str(),
    ] {
        let page = app.get(path).await;
        page.assert_status(StatusCode::OK);
        assert_eq!(page.count("form[method=post]"), 0, "{path}");
        // Action buttons have names; the filter's Search button (a GET form) does not.
        assert_eq!(page.count("button[name]"), 0, "{path}");
        assert_eq!(page.count("[formaction]"), 0, "{path}");
        assert_eq!(page.count("[data-check-item]"), 0, "{path}");
        assert_eq!(page.texts(".brand-badge"), ["read-only"], "{path}");
    }
    let key = key(&job, score);
    for (path, fields) in [
        ("/retries", vec![("key", key.as_str()), ("delete", "1")]),
        ("/retries/all/delete", vec![]),
        ("/queues/default", vec![("delete", "1")]),
        ("/busy", vec![("stop", "1")]),
    ] {
        let res = app.post(path, &fields).await;
        res.assert_status(StatusCode::FORBIDDEN);
        assert!(res.body.contains("read-only"), "{path}");
    }
    assert_eq!(zcard(&app, "retry").await, 1);
    assert_eq!(llen(&app, "default").await, 1);
    assert_eq!(pop_signal(&app, "host:1:x").await, None);
}

#[tokio::test]
async fn hostile_data_is_escaped_everywhere() {
    let app = App::new().await;
    let evil_job = json!({
        "class": "<script>alert('class')</script>",
        "args": ["<a>hello</a>", {"<img src=x onerror=alert(1)>": "\"quoted\""}],
        "queue": "<b>q</b>",
        "jid": "<i>jid</i>",
        "error_message": "fail message: <a>hello</a>",
        "error_class": "<em>RuntimeError</em>",
        "tags": ["<eviltag/>"],
        "retry_count": 0,
    });
    let (job, score) = add_to_set(&app, "retry", evil_job.clone(), now_secs()).await;
    add_to_set(&app, "dead", evil_job.clone(), now_secs()).await;
    add_to_set(&app, "schedule", evil_job.clone(), now_secs() + 60.0).await;
    push_raw(&app, "<b>q</b>", &evil_job).await;
    add_process(
        &app,
        "<p>host</p>:1:x",
        json!({"hostname": "<p>host</p>", "pid": 1, "labels": ["<u>label</u>"],
               "tag": "<s>tag</s>", "queues": ["<b>q</b>"]}),
        1,
    )
    .await;
    add_work(&app, "<p>host</p>:1:x", "<slot>", "<b>q</b>", &evil_job).await;

    let entry = format!("/retries/{}", hench_web_segment(&key(&job, score)));
    for path in [
        "/retries",
        "/morgue",
        "/scheduled",
        "/busy",
        "/queues",
        "/queues/%3Cb%3Eq%3C%2Fb%3E",
        entry.as_str(),
    ] {
        let page = app.get(path).await;
        page.assert_status(StatusCode::OK);
        for raw in [
            "<a>hello</a>",
            "<script>alert",
            "<b>q</b>",
            "<eviltag/>",
            "<img src=x",
            "<em>RuntimeError",
            "<i>jid</i>",
            "<p>host</p>",
            "<u>label</u>",
            "<s>tag</s>",
            "<slot>",
        ] {
            assert!(!page.body.contains(raw), "{path} contains {raw}");
        }
        // The hostile markup never becomes elements.
        for element in ["script", "img", "eviltag", "slot", "em", "u", "s", "i", "b"] {
            if element == "script" {
                // Only the dashboard's own scripts, which have a src.
                let scripts = page.count("script");
                assert_eq!(scripts, page.count("script[src]"), "{path}");
                continue;
            }
            assert_eq!(page.count(element), 0, "{path} has a <{element}>");
        }
        // But the text is there, escaped.
        assert!(
            page.body.contains("&#60;a&#62;hello&#60;/a&#62;")
                || page.body.contains("&#60;b&#62;q&#60;/b&#62;"),
            "{path}"
        );
    }
}

/// Mirrors the dashboard's path-segment escaping for a key with odd characters.
fn hench_web_segment(text: &str) -> String {
    text.replace('<', "%3C")
        .replace('>', "%3E")
        .replace('/', "%2F")
}

#[tokio::test]
async fn every_response_carries_the_security_headers() {
    let app = App::new().await;
    let page = app.get("/queues").await;
    let asset_path = page.attrs("link[rel=stylesheet]", "href")[0].clone();
    let responses = [
        app.get("/").await,
        page,
        app.get("/stats").await,
        app.get("/nowhere").await,
        app.get(&asset_path).await,
        app.post("/busy", &[]).await,
        app.send(post_request("/busy", &[("quiet", "1")], TOKEN, OTHER_TOKEN))
            .await,
    ];
    for res in responses {
        let csp = res.header("content-security-policy").unwrap_or_default();
        assert!(csp.contains("default-src 'self'"), "{csp}");
        assert!(!csp.contains("unsafe-inline"), "{csp}");
        assert!(csp.contains("frame-ancestors 'self'"), "{csp}");
        assert_eq!(res.header("x-content-type-options"), Some("nosniff"));
        assert_eq!(res.header("referrer-policy"), Some("same-origin"));
        assert_eq!(res.header("x-frame-options"), Some("SAMEORIGIN"));
        assert!(res.header("cache-control").is_some());
    }
    // Pages and data are never cached.
    let page = app.get("/retries").await;
    assert_eq!(page.header("cache-control"), Some("no-store"));
    assert_eq!(
        app.get("/stats").await.header("cache-control"),
        Some("no-store")
    );
}

#[tokio::test]
async fn pages_have_no_inline_scripts_or_styles() {
    let app = App::new().await;
    add_retry(&app, &jid()).await;
    let tracker = hench::metrics::Tracker::new();
    tracker.track(
        "HardJob",
        std::time::Duration::from_millis(5),
        hench::metrics::Outcome::Success,
    );
    tracker.flush(&app.pool).await.unwrap();
    for path in [
        "/",
        "/busy",
        "/queues",
        "/retries",
        "/metrics",
        "/metrics/HardJob",
        "/nowhere",
    ] {
        let page = app.get(path).await;
        // Every script is external; no style elements or attributes; no inline handlers.
        assert_eq!(page.count("script"), page.count("script[src]"), "{path}");
        assert_eq!(page.count("style"), 0, "{path}");
        assert_eq!(page.count("[style]"), 0, "{path}");
        for handler in ["onclick", "onload", "onerror", "onsubmit", "onchange"] {
            assert_eq!(page.count(&format!("[{handler}]")), 0, "{path} {handler}");
        }
    }
}

#[tokio::test]
async fn parameters_are_validated() {
    let app = App::new().await;
    let long = "x".repeat(257);
    app.get(&format!("/retries?substr={long}"))
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    app.get(&format!("/retries?substr={}", "x".repeat(256)))
        .await
        .assert_status(StatusCode::OK);
    for query in [
        "page=abc",
        "page=-1",
        "page=99999999999",
        "count=0",
        "count=abc",
    ] {
        app.get(&format!("/retries?{query}"))
            .await
            .assert_status(StatusCode::OK);
    }
    // Forms must be forms.
    let mut request = post_request("/retries", &[("retry", "1")], TOKEN, TOKEN);
    request
        .headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    app.send(request)
        .await
        .assert_status(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    // More keys than a page can show are refused.
    let keys: Vec<(&str, &str)> = (0..1_001)
        .map(|_| ("key", "1-x"))
        .chain([("delete", "1")])
        .collect();
    app.post("/retries", &keys)
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}

/// A page on a host name its author re-pointed at the dashboard's address (DNS rebinding):
/// the requests its scripts send carry its own name, a matching `Origin`, and
/// `Sec-Fetch-Site: same-origin`.
fn rebound(mut request: Request<Body>, host: &str) -> Request<Body> {
    let headers = request.headers_mut();
    headers.insert("host", host.parse().unwrap());
    headers.insert("origin", format!("http://{host}").parse().unwrap());
    headers.insert("sec-fetch-site", "same-origin".parse().unwrap());
    request
}

#[tokio::test]
async fn foreign_host_names_are_refused_by_default() {
    // security-supply-chain-3: an embedded dashboard (README §5, `examples/embedded.rs`: no
    // authentication, listening on 127.0.0.1) checked no `Host`, so a rebound page read every
    // page with its CSRF token and then passed every cross-site check: it emptied the dead
    // set and paused queues. Only `hench web` checked host names.
    let app = App::build(|web| web, "/jobs").await;
    let (dead, score) = add_dead(&app, &jid()).await;
    push_raw(
        &app,
        "images",
        &json!({"class": "Resize", "args": [1], "jid": jid()}),
    )
    .await;
    const EVIL: &str = "attacker.example:3000";

    for path in [
        "/jobs",
        "/jobs/morgue",
        "/jobs/queues/images",
        "/jobs/nowhere",
    ] {
        let res = app.send(rebound(get(path), EVIL)).await;
        res.assert_status(StatusCode::FORBIDDEN);
        assert!(!res.body.contains("_csrf"), "{path}: {}", res.body);
        assert!(res.cookies().is_empty(), "{path}");
        assert!(res.body.contains("HenchWeb::allowed_hosts"), "{}", res.body);
        assert_eq!(res.header("x-content-type-options"), Some("nosniff"));
        assert_eq!(res.header("cache-control"), Some("no-store"));
    }
    // Even with a token it got elsewhere, nothing changes.
    for (path, fields) in [
        ("/jobs/morgue/all/delete", vec![]),
        ("/jobs/queues/images", vec![("pause", "1")]),
        ("/jobs/morgue", vec![("key", "x"), ("delete", "1")]),
    ] {
        let request = rebound(post_request(path, &fields, TOKEN, TOKEN), EVIL);
        app.send(request).await.assert_status(StatusCode::FORBIDDEN);
    }
    assert_eq!(zcard(&app, "dead").await, 1);
    let paused: Vec<String> = app
        .redis(hench::redis_rs::cmd("SMEMBERS").arg("paused"))
        .await;
    assert!(paused.is_empty(), "{paused:?}");

    // Names nobody can re-point keep working: localhost and IP addresses, any port.
    for host in [
        "localhost:3000",
        "dash.localhost",
        "127.0.0.1:3000",
        "[::1]:3000",
    ] {
        let res = app.send(rebound(get("/jobs/morgue"), host)).await;
        res.assert_status(StatusCode::OK);
        assert!(res.body.contains(&key(&dead, score)), "{host}");
    }
    // HTTP/1.0 clients may send no Host at all (browsers always do).
    app.get("/morgue").await.assert_status(StatusCode::OK);
}

#[tokio::test]
async fn allowed_host_names_are_served() {
    let app = App::build(
        |web| {
            web.allowed_hosts(hench_web::AllowedHosts::new([
                "jobs.example.com",
                ".corp.example",
            ]))
        },
        "",
    )
    .await;
    for (host, status) in [
        ("jobs.example.com", StatusCode::OK),
        ("JOBS.example.com:8443", StatusCode::OK),
        ("a.corp.example", StatusCode::OK),
        ("127.0.0.1:3000", StatusCode::OK),
        ("evil.example", StatusCode::FORBIDDEN),
        ("jobs.example.com.evil.example", StatusCode::FORBIDDEN),
    ] {
        let res = app.send(rebound(get("/queues"), host)).await;
        assert_eq!(res.status, status, "{host}");
    }
    // Every name, for a dashboard behind authentication a rebound page cannot use.
    let app = App::build(|web| web.allowed_hosts(hench_web::AllowedHosts::any()), "").await;
    let res = app.send(rebound(get("/queues"), "evil.example")).await;
    res.assert_status(StatusCode::OK);
    // A refusal text of the application's own.
    let app = App::build(
        |web| {
            web.allowed_hosts(
                hench_web::AllowedHosts::local().refusal("Use https://jobs.example.com."),
            )
        },
        "",
    )
    .await;
    let res = app.send(rebound(get("/queues"), "evil.example")).await;
    res.assert_status(StatusCode::FORBIDDEN);
    assert_eq!(res.body, "Use https://jobs.example.com.\n");
}

#[tokio::test]
async fn routes_next_to_the_dashboard_can_check_host_names_too() {
    use axum::middleware;
    use axum::routing::get as route_get;
    let hosts = hench_web::AllowedHosts::new(["jobs.example.com"]);
    let app = axum::Router::new()
        .route("/admin/secret", route_get(|| async { "secret" }))
        .layer(middleware::from_fn_with_state(
            hosts,
            hench_web::require_host,
        ));
    let send = |host: &'static str| {
        let app = app.clone();
        async move {
            use tower::ServiceExt;
            let response = app
                .oneshot(rebound(get("/admin/secret"), host))
                .await
                .unwrap();
            response.status()
        }
    };
    assert_eq!(send("jobs.example.com").await, StatusCode::OK);
    assert_eq!(send("localhost:3000").await, StatusCode::OK);
    assert_eq!(send("evil.example").await, StatusCode::FORBIDDEN);
}

/// A bare GET request.
fn get(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).unwrap()
}
