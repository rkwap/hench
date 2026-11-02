//! The options of the pro features: unique jobs and throttles, generated into `Job::options`,
//! `Job::unique_key`, `Job::throttle_key` and `Job::throttle`.

#![deny(warnings)]

use hench::prelude::*;

/// Unique for ten minutes, by user, released when it starts; replaces a scheduled duplicate.
#[hench::job(
    queue = "mailers",
    unique_for = 600,
    unique_until = "start",
    on_conflict = "replace",
    unique_across_queues = true,
    unique_by = [user_id]
)]
async fn digest(ctx: &JobContext, user_id: u64, template: String) -> Result<(), JobError> {
    let _ = (ctx.jid(), user_id, template);
    Ok(())
}

/// Throttled by a named throttle, keyed by the account.
#[hench::job(throttle = "stripe", throttle_by = [account])]
async fn charge(account: String, cents: u64) {
    let _ = (account, cents);
}

/// An inline throttle, named after the job, keyed by two arguments.
#[hench::job(
    name = "Billing::Sync",
    throttle(concurrency = 3, window_per_key = "5/1s"),
    throttle_by = [region, account]
)]
async fn sync((low, high): (u32, u32), region: String, account: u64) {
    let _ = (low, high, region, account);
}

fn main() {
    let options = <digest as Job>::options();
    assert_eq!(options.queue.as_deref(), Some("mailers"));
    let key = <digest as Job>::unique_key(&(7, "welcome".to_owned()));
    assert_eq!(key, Some(serde_json::json!([7])));

    let key = <charge as Job>::throttle_key(&("acme".to_owned(), 100));
    assert_eq!(key.as_deref(), Some("acme"));
    assert!(<charge as Job>::throttle().is_none());

    let throttle = <sync as Job>::throttle().expect("an inline throttle");
    assert_eq!(throttle.name(), "Billing.Sync");
    let key = <sync as Job>::throttle_key(&((1, 2), "eu".to_owned(), 42));
    assert_eq!(key.as_deref(), Some(r#"["eu",42]"#));
}
