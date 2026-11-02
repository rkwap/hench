//! Pages of the host application inside the dashboard (Sidekiq's web extensions,
//! `webext_test`, and `web_test`'s "displays custom job info"): routes given to
//! `HenchWeb::routes` are guarded like the dashboard's own, render in its layout with
//! `Page`, read forms with `Form`, and job pages show the rows of `HenchWeb::job_info`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::response::{Redirect, Response};
use axum::routing::{get, post};
use common::*;
use hench_web::{Form, JobInfoRow, Page, escape_html};
use http::{Request, StatusCode};
use serde::Deserialize;

/// The routes of a "Reports" extension: a page, a form posting to it, and a purge action that
/// counts its calls.
fn reports(purges: Arc<AtomicUsize>) -> Router {
    async fn page(page: Page) -> Response {
        let form = if page.is_read_only() {
            String::new()
        } else {
            format!(
                "<form method=\"post\" action=\"{}\">{}<button class=\"btn\" name=\"purge\" \
                 value=\"1\">Purge</button></form>",
                page.url("/reports/purge"),
                page.csrf_field()
            )
        };
        let content = format!(
            "<section class=\"panel\"><h1 id=\"reports\">Reports</h1>\
             <p class=\"where\">{}|{}</p>{form}</section>",
            escape_html(page.prefix()),
            escape_html(page.path())
        );
        page.render("Reports", &content).await
    }
    Router::new().route("/reports", get(page)).route(
        "/reports/purge",
        post(move |page: Page, form: Form| {
            let purges = Arc::clone(&purges);
            async move {
                if form.has("purge") {
                    purges.fetch_add(1, Ordering::SeqCst);
                }
                Redirect::to(&page.url("/reports"))
            }
        }),
    )
}

/// A form POST as another site sends it.
fn cross_site_post(path: &str) -> Request<Body> {
    let mut request = post_request(path, &[("purge", "1")], TOKEN, TOKEN);
    let headers = request.headers_mut();
    headers.insert("sec-fetch-site", "cross-site".parse().unwrap());
    headers.insert("origin", "https://evil.example".parse().unwrap());
    request
}

#[tokio::test]
async fn host_routes_are_guarded_like_the_dashboards_own() {
    // parity-gaps-4: routes added to the dashboard's router (what `HenchWeb::tab`'s docs
    // said to do) came after its guard: read-only mode and the cross-site check did not
    // apply to them (a cross-site POST purged everything) and their pages had no security
    // headers.
    for read_only in [true, false] {
        let purges = Arc::new(AtomicUsize::new(0));
        let routes = reports(Arc::clone(&purges));
        let app = App::build(
            move |web| {
                web.read_only(read_only)
                    .tab("Reports", "reports")
                    .routes(routes)
            },
            "/hench",
        )
        .await;
        for path in ["/hench/busy", "/hench/reports/purge"] {
            let res = app.send(cross_site_post(path)).await;
            res.assert_status(StatusCode::FORBIDDEN);
        }
        // A same-origin POST with the token: refused when read-only, done otherwise.
        let res = app
            .send(post_request(
                "/hench/reports/purge",
                &[("purge", "1")],
                TOKEN,
                TOKEN,
            ))
            .await;
        if read_only {
            res.assert_status(StatusCode::FORBIDDEN);
            assert!(res.body.contains("read-only"), "{}", res.body);
            assert_eq!(purges.load(Ordering::SeqCst), 0);
        } else {
            res.assert_status(StatusCode::SEE_OTHER);
            assert_eq!(res.location(), "/hench/reports");
            assert_eq!(purges.load(Ordering::SeqCst), 1);
        }
        // The page has the dashboard's security headers, and no form when read-only.
        let res = app.get("/reports").await;
        res.assert_status(StatusCode::OK);
        for (header, value) in [
            ("x-frame-options", "SAMEORIGIN"),
            ("x-content-type-options", "nosniff"),
            ("cache-control", "no-store"),
        ] {
            assert_eq!(res.header(header), Some(value), "{header}");
        }
        assert!(
            res.header("content-security-policy")
                .is_some_and(|csp| csp.contains("default-src 'self'")),
            "{:?}",
            res.header("content-security-policy")
        );
        assert_eq!(
            res.count("form[action=\"/hench/reports/purge\"]"),
            usize::from(!read_only)
        );
        // Foreign host names are refused like everywhere in the dashboard.
        let mut request = Request::get("/hench/reports").body(Body::empty()).unwrap();
        request
            .headers_mut()
            .insert("host", "evil.example".parse().unwrap());
        app.send(request).await.assert_status(StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn host_pages_render_in_the_dashboards_layout() {
    let app = App::build(
        |web| {
            web.title("Acme jobs")
                .tab("Reports", "reports")
                .routes(reports(Arc::default()))
        },
        "/hench",
    )
    .await;
    push_raw(
        &app,
        "default",
        &serde_json::json!({"class": "X", "args": [], "jid": jid()}),
    )
    .await;
    let res = app.get("/reports").await;
    res.assert_status(StatusCode::OK);
    assert_eq!(res.title(), "Reports · Acme jobs");
    // The navigation, with the extension's tab highlighted.
    assert_eq!(res.texts(".nav a[aria-current=page]"), ["Reports"]);
    assert_eq!(
        res.attrs(".nav a[aria-current=page]", "href"),
        ["/hench/reports"]
    );
    // The totals, the content, the footer, the dashboard's assets.
    assert_eq!(res.texts("[data-total=enqueued]"), ["1"]);
    assert_eq!(res.texts("#reports"), ["Reports"]);
    assert_eq!(res.texts(".where"), ["/hench|/reports"]);
    assert_eq!(res.count(".app-footer"), 1);
    assert!(
        res.attrs("link[rel=stylesheet]", "href")[0].starts_with("/hench/assets/hench."),
        "{:?}",
        res.attrs("link[rel=stylesheet]", "href")
    );
    // The form carries the page's token, and a token cookie is issued with it.
    let token = res.attrs("input[name=_csrf]", "value");
    assert_eq!(token.len(), 1);
    let cookie = res.cookie("hench_csrf").expect("a token cookie");
    assert!(cookie.contains(&token[0]), "{cookie}");
}

#[tokio::test]
async fn host_forms_need_the_dashboards_token() {
    let purges = Arc::new(AtomicUsize::new(0));
    let routes = reports(Arc::clone(&purges));
    let app = App::build(move |web| web.routes(routes), "").await;
    const OTHER: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
    for request in [
        post_request("/reports/purge", &[("purge", "1")], TOKEN, OTHER),
        post_request("/reports/purge", &[("purge", "1")], TOKEN, ""),
    ] {
        let res = app.send(request).await;
        res.assert_status(StatusCode::FORBIDDEN);
        // The dashboard's own error page.
        assert!(res.body.contains("Reload the page"), "{}", res.body);
    }
    let mut json = post_request("/reports/purge", &[("purge", "1")], TOKEN, TOKEN);
    json.headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    app.send(json)
        .await
        .assert_status(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(purges.load(Ordering::SeqCst), 0);
    app.post("/reports/purge", &[("purge", "1")])
        .await
        .assert_status(StatusCode::SEE_OTHER);
    assert_eq!(purges.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn extensions_route_like_the_dashboard() {
    // Sidekiq's webext_test "allows web ui extensions": an index route and one with a path
    // parameter and a query parameter, here under a mount with a parameter of its own.
    #[derive(Deserialize)]
    struct Name {
        name: String,
    }
    #[derive(Deserialize)]
    struct Time {
        time: u64,
    }
    async fn index(page: Page) -> Response {
        page.render("My extension", "<p id=\"index\">My Index Route</p>")
            .await
    }
    async fn hello(
        page: Page,
        Path(Name { name }): Path<Name>,
        Query(time): Query<Time>,
    ) -> Response {
        let content = format!(
            "<p id=\"hello\">Hello, {}, it is {}</p>",
            escape_html(&name),
            time.time
        );
        page.render("Hello", &content).await
    }
    let routes = Router::new()
        .route("/foobar", get(index))
        .route("/foobar/{name}", get(hello));
    let redis = hench::test_util::TestRedis::start().await;
    let pool = redis.pool().await;
    let dashboard = hench_web::router(
        hench_web::HenchWeb::new(&pool)
            .tab("MyExt", "foobar")
            .routes(routes),
    );
    let app = Router::new().nest_service("/tenants/{tenant}/jobs", dashboard);
    let get = |path: String| {
        let app = app.clone();
        async move {
            use tower::ServiceExt;
            let response = app
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let body = http_body_util::BodyExt::collect(response.into_body())
                .await
                .unwrap()
                .to_bytes();
            (status, String::from_utf8_lossy(&body).into_owned())
        }
    };
    let (status, body) = get("/tenants/acme/jobs/foobar".to_owned()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("My Index Route"), "{body}");
    assert!(
        body.contains("href=\"/tenants/acme/jobs/foobar\""),
        "{body}"
    );
    let time = hench::now_secs_f64() as u64;
    let (status, body) = get(format!("/tenants/acme/jobs/foobar/%3Cmike%3E?time={time}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains(&format!("Hello, &lt;mike&gt;, it is {time}")),
        "{body}"
    );
    // The dashboard's own pages and its 404 page still answer.
    let (status, _) = get("/tenants/acme/jobs/queues".to_owned()).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = get("/tenants/acme/jobs/nowhere".to_owned()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("app-header"), "{body}");
}

#[tokio::test]
async fn page_and_form_need_the_dashboard() {
    // Outside the dashboard's router they cannot work (no context): a clear 500.
    let app = Router::new()
        .route(
            "/lost",
            get(|page: Page| async move { page.render("x", "y").await }),
        )
        .route("/form", post(|_form: Form| async { "never" }));
    use tower::ServiceExt;
    let response = app
        .clone()
        .oneshot(Request::get("/lost").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let response = app
        .oneshot(post_request("/form", &[], TOKEN, TOKEN))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn job_pages_show_the_host_applications_rows() {
    // Sidekiq's web_test "displays custom job info".
    let app = App::build(
        |web| {
            web.job_info(|job| {
                let jid = job.jid().unwrap_or_default();
                vec![
                    JobInfoRow::link(
                        "External Logs",
                        format!("Logs for {jid}"),
                        format!("https://example.com/logs/{jid}"),
                    ),
                    JobInfoRow::link("Unsafe", "<not a link>", "javascript:alert(1)"),
                ]
            })
            .job_info(|job| vec![JobInfoRow::text("Class again", job.class().unwrap_or("?"))])
            .job_info(|_| panic!("a broken callback"))
        },
        "",
    )
    .await;
    let retry_jid = jid();
    let (retry, retry_score) = add_retry(&app, &retry_jid).await;
    let (scheduled, scheduled_score) = add_scheduled(&app, &jid()).await;
    let (dead, dead_score) = add_dead(&app, &jid()).await;
    for (path, job, score) in [
        ("/retries", &retry, retry_score),
        ("/scheduled", &scheduled, scheduled_score),
        ("/morgue", &dead, dead_score),
    ] {
        let res = app.get(&format!("{path}/{}", key(job, score))).await;
        res.assert_status(StatusCode::OK);
        let jid = job["jid"].as_str().unwrap();
        let href = format!("https://example.com/logs/{jid}");
        assert_eq!(
            res.attrs(&format!("table.kv a[href=\"{href}\"]"), "href")
                .len(),
            1,
            "{path}"
        );
        let labels = res.texts("table.kv th");
        for label in ["External Logs", "Unsafe", "Class again"] {
            assert!(
                labels.iter().any(|text| text == label),
                "{path}: {labels:?}"
            );
        }
        // Text, never markup or a script link.
        assert!(
            res.texts("table.kv td")
                .iter()
                .any(|text| text == "<not a link>"),
            "{path}"
        );
        assert!(!res.body.contains("<not a link>"), "{path}");
        assert!(!res.body.contains("javascript:alert"), "{path}");
        assert!(
            res.texts("table.kv td")
                .iter()
                .any(|text| text == "HardJob")
        );
    }
    // No rows on the lists.
    let res = app.get("/retries").await;
    assert!(!res.body.contains("example.com/logs"));
    // The retry page itself is unaffected by the rows (Sidekiq's scenario checks the link).
    assert!(
        app.get(&format!("/retries/{}", key(&retry, retry_score)))
            .await
            .body
            .contains(&format!("https://example.com/logs/{retry_jid}"))
    );
}
