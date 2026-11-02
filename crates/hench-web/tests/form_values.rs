//! Forms that post a value read from Redis back to the server (a jid, a limit's identity, an
//! entry's key, a process identity) work whatever the value holds. Browsers do not post a
//! field's value as the page holds it: the HTML parser turns CR and CRLF into LF and NUL into
//! U+FFFD, and a form submission turns a lone CR or LF into CRLF. Jids are whatever the pushing
//! code chose and limiter keys any text without NUL, so the pages write such values encoded
//! and the server decodes them (round 3, web-api-security-5). The values are read from the
//! rendered pages and posted the way a browser would.

mod common;

use std::time::Duration;

use common::*;
use hench::PushOutcome;
use hench::limit::{LimitKind, Limiter};
use hench::prelude::*;
use http::StatusCode;
use serde_json::json;

/// A job that never runs (its queue has no worker).
#[derive(Default)]
struct Parked;

impl Job for Parked {
    const NAME: &'static str = "forms::Parked";
    type Args = (u32,);

    async fn perform(&self, _ctx: &JobContext, _args: Self::Args) -> Result<(), JobError> {
        Ok(())
    }
}

/// Jids an application may choose: a lone LF, a lone CR, CRLF, NUL, and `%` (the escape of the
/// fields' encoding).
const JIDS: [&str; 5] = ["lone\nlf", "lone\rcr", "crlf\r\nok", "nul\0x", "100% %0A"];

/// What a browser posts for a value as the page wrote it: the HTML parser's preprocessing has
/// already happened (the scraper's attributes hold the DOM's values), then the form submission
/// turns a lone CR or LF into CRLF.
fn submitted(value: &str) -> String {
    value
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\n', "\r\n")
}

/// The flash cookie's code (`kind.count.failed.subject`) of a response.
fn flash(res: &Res) -> String {
    let cookie = res.cookie("hench_flash").unwrap_or_default();
    cookie
        .trim_start_matches("hench_flash=")
        .split(';')
        .next()
        .unwrap_or_default()
        .to_owned()
}

#[tokio::test]
async fn locks_held_by_any_jid_are_released_from_their_page_and_the_list() {
    let app = App::new().await;
    let client = Client::new(app.pool.clone());
    let locks = app.api.unique_locks();
    let mut digests = Vec::new();
    for (n, jid) in JIDS.iter().enumerate() {
        let outcome = Parked::set()
            .jid(*jid)
            .queue("parked")
            .unique_for(Duration::from_secs(600))
            .push_outcome(&client, (u32::try_from(n).unwrap(),))
            .await
            .unwrap();
        assert!(matches!(outcome, PushOutcome::Pushed { .. }), "{outcome:?}");
        let page = locks
            .page(1, 100, hench::api::SortOrder::Ascending)
            .await
            .unwrap();
        let lock = page.items.iter().find(|lock| lock.jid == *jid).unwrap();
        digests.push(lock.digest.clone());
    }
    // The lock's page: Release names the holder it showed.
    for (digest, jid) in digests.iter().zip(JIDS).take(3) {
        let page = app.get(&format!("/locks/{digest}")).await;
        page.assert_status(StatusCode::OK);
        let shown = page.attrs("form input[name=jid]", "value");
        assert_eq!(shown.len(), 1, "{shown:?}");
        let res = app
            .post(
                &format!("/locks/{digest}"),
                &[("release", "1"), ("jid", &submitted(&shown[0]))],
            )
            .await;
        res.assert_status(StatusCode::SEE_OTHER);
        // The subject (the digest) is percent-encoded in the cookie.
        assert!(flash(&res).starts_with("u-released.1.0."), "{jid:?}");
        assert_eq!(locks.get(digest).await.unwrap(), None, "{jid:?}");
    }
    // The list: the rest, selected together.
    let list = app.get("/locks").await;
    let keys = list.attrs("input[name=key]", "value");
    assert_eq!(keys.len(), 2, "{keys:?}");
    let fields: Vec<(&str, String)> = keys.iter().map(|key| ("key", submitted(key))).collect();
    let mut form: Vec<(&str, &str)> = vec![("release", "1")];
    form.extend(fields.iter().map(|(name, value)| (*name, value.as_str())));
    let res = app.post("/locks", &form).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(flash(&res), "u-released.2.0.");
    assert_eq!(locks.size().await.unwrap(), 0);
}

#[tokio::test]
async fn limits_with_any_key_are_reset_from_the_list_which_counts_real_resets() {
    let app = App::new().await;
    let mut permits = Vec::new();
    for key in ["lone\nlf", "lone\rcr", "100%"] {
        let limiter = Limiter::concurrent("forms", 5)
            .key(key)
            .max_wait(Duration::ZERO);
        permits.push(limiter.acquire(&app.pool).await.unwrap());
    }
    let limits = app.api.limits();
    let list = app.get("/limits").await;
    let identities = list.attrs("input[name=identity]", "value");
    assert_eq!(identities.len(), 3, "{identities:?}");
    let fields: Vec<String> = identities.iter().map(|value| submitted(value)).collect();
    let mut form: Vec<(&str, &str)> = vec![("reset", "1")];
    form.extend(fields.iter().map(|value| ("identity", value.as_str())));
    let res = app.post("/limits", &form).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(flash(&res), "l-reset.3.0.");
    for key in ["lone\nlf", "lone\rcr", "100%"] {
        let id = format!("forms:{key}");
        assert!(
            limits
                .get(LimitKind::Concurrency, &id)
                .await
                .unwrap()
                .is_none(),
            "{key:?} is still in use"
        );
    }
    // Identities not in use (expired meanwhile, or never were) are not counted as reset.
    let res = app
        .post(
            "/limits",
            &[
                ("reset", "1"),
                ("identity", "c:nope"),
                ("identity", "w:nada"),
            ],
        )
        .await;
    assert_eq!(flash(&res), "l-reset.0.0.");
    drop(permits);
}

#[tokio::test]
async fn batch_members_with_any_jid_are_skipped_from_the_batch_page() {
    let app = App::new().await;
    let client = Client::new(app.pool.clone());
    let bid = Batch::new(&client)
        .description("odd jids")
        .jobs(|jobs| async move {
            for (n, jid) in ["member\nlf", "dead\rcr"].into_iter().enumerate() {
                Parked::set()
                    .jid(jid)
                    .queue("parked")
                    .perform_async(&jobs, (u32::try_from(n).unwrap(),))
                    .await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await
        .unwrap()
        .bid;
    // One member deleted from its queue: dead in its batch (a failure row with Skip).
    let deleted = app
        .api
        .queue("parked")
        .find_job("dead\rcr")
        .await
        .unwrap()
        .unwrap();
    assert!(deleted.delete().await.unwrap());
    let batches = app.api.batches();
    let page = app.get(&format!("/batches/{bid}")).await;
    page.assert_status(StatusCode::OK);
    // The jids are shown with their line breaks spelled out.
    assert_eq!(
        page.texts("section[aria-labelledby=failures-heading] tbody th"),
        ["dead\\rcr"]
    );
    let shown = page.texts("section[aria-labelledby=pending-heading] li code");
    assert_eq!(shown.len(), 2, "{shown:?}");
    assert!(shown.contains(&"member\\nlf".to_owned()), "{shown:?}");
    let dead = page.attrs(
        "section[aria-labelledby=failures-heading] input[name=jid]",
        "value",
    );
    assert_eq!(dead.len(), 1, "{dead:?}");
    let res = app
        .post(
            &format!("/batches/{bid}"),
            &[("skip", "1"), ("jid", &submitted(&dead[0]))],
        )
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(flash(&res), "b-skipped.1.0.");
    let pending = batches.pending_jids(&bid, 10).await.unwrap();
    assert_eq!(pending, ["member\nlf"]);

    let page = app.get(&format!("/batches/{bid}")).await;
    let waiting = page.attrs(
        "section[aria-labelledby=pending-heading] input[name=jid]",
        "value",
    );
    assert_eq!(waiting.len(), 1, "{waiting:?}");
    let res = app
        .post(
            &format!("/batches/{bid}"),
            &[("skip", "1"), ("jid", &submitted(&waiting[0]))],
        )
        .await;
    assert_eq!(flash(&res), "b-skipped.1.0.");
    assert!(batches.pending_jids(&bid, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn set_entries_with_any_jid_are_acted_on_from_the_list() {
    let app = App::new().await;
    for jid in JIDS {
        add_retry(&app, jid).await;
    }
    let list = app.get("/retries").await;
    let keys = list.attrs("input[name=key]", "value");
    assert_eq!(keys.len(), JIDS.len(), "{keys:?}");
    let fields: Vec<String> = keys.iter().map(|key| submitted(key)).collect();
    let mut form: Vec<(&str, &str)> = vec![("delete", "1")];
    form.extend(fields.iter().map(|key| ("key", key.as_str())));
    let res = app.post("/retries", &form).await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(flash(&res), format!("deleted.{}.0.", JIDS.len()));
    assert_eq!(zcard(&app, "retry").await, 0);
}

#[tokio::test]
async fn processes_with_any_identity_are_signalled_from_the_busy_page() {
    let app = App::new().await;
    let identity = "odd\nhost:1:abc";
    add_process(
        &app,
        identity,
        json!({"hostname": "odd\nhost", "identity": identity}),
        0,
    )
    .await;
    let page = app.get("/busy").await;
    let shown = page.attrs("input[name=identity]", "value");
    assert_eq!(shown.len(), 1, "{shown:?}");
    let res = app
        .post(
            "/busy",
            &[("quiet", "1"), ("identity", &submitted(&shown[0]))],
        )
        .await;
    res.assert_status(StatusCode::SEE_OTHER);
    assert_eq!(flash(&res), "quieted.1.0.");
    assert_eq!(pop_signal(&app, identity).await.as_deref(), Some("TSTP"));
}
