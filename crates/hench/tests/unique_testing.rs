//! Unique jobs in the testing modes (`docs/design/unique.md` §3.6 T1-T3): fake and inline
//! clients enforce locks in memory, per client, with the scripts' rules; `JobContext` shows a
//! job its uniqueness and releases its lock early. No Redis.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use hench::prelude::*;
use hench::testing::Testing;
use hench::{PushOutcome, Registry};
use serde_json::{Value, json};

/// What the jobs of a test saw.
#[derive(Default)]
struct Seen {
    runs: Mutex<Vec<String>>,
    pushes: Mutex<Vec<String>>,
}

/// A unique job keyed by its first argument; its second says what it does.
#[derive(Default)]
struct Sync;

impl Job for Sync {
    const NAME: &'static str = "testing::Sync";
    type Args = (i64, String);

    fn options() -> JobOptions {
        JobOptions::new().unique_for(Duration::from_secs(60))
    }

    fn unique_key((key, _): &Self::Args) -> Option<Value> {
        Some(json!([key]))
    }

    async fn perform(&self, ctx: &JobContext, (key, mode): Self::Args) -> Result<(), JobError> {
        let seen = ctx.data::<Arc<Seen>>()?;
        seen.runs.lock().unwrap().push(format!("{key}:{mode}"));
        match mode.as_str() {
            "fail" => Err(JobError::msg("boom")),
            "push_self" => {
                let until = ctx
                    .unique()
                    .and_then(|unique| unique.until)
                    .unwrap_or_default();
                let outcome = Sync::set()
                    .unique_until(until)
                    .push_outcome(ctx.client(), (key, "ok".to_owned()))
                    .await?;
                seen.pushes.lock().unwrap().push(match outcome {
                    PushOutcome::Pushed { .. } => "pushed".to_owned(),
                    PushOutcome::Duplicate { .. } => "duplicate".to_owned(),
                    other => format!("{other:?}"),
                });
                Ok(())
            }
            "release" => {
                assert!(ctx.release_unique_lock().await?);
                assert!(!ctx.release_unique_lock().await?, "already released");
                let outcome = Sync::set()
                    .push_outcome(ctx.client(), (key, "ok".to_owned()))
                    .await?;
                seen.pushes
                    .lock()
                    .unwrap()
                    .push(format!("{}", outcome.jid().is_some()));
                Ok(())
            }
            "interrupt" => {
                if ctx.retry_count().is_none() && seen.runs.lock().unwrap().len() == 1 {
                    Err(JobError::interrupted())
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

fn testing(seen: &Arc<Seen>) -> Testing {
    Testing::new()
        .auto_register(false)
        .register(Sync)
        .data(seen.clone())
}

#[tokio::test]
async fn fake_clients_enforce_locks_until_the_job_runs() {
    // T1.
    let seen = Arc::new(Seen::default());
    let client = testing(&seen).fake();
    let first = Sync::perform_async(&client, (1, "ok".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        Sync::perform_async(&client, (1, "again".into()))
            .await
            .unwrap(),
        None
    );
    let outcome = Sync::set()
        .push_outcome(&client, (1, "again".into()))
        .await
        .unwrap();
    let PushOutcome::Duplicate { holder, .. } = outcome else {
        panic!("not a duplicate: {outcome:?}");
    };
    assert_eq!(holder, first);
    assert!(matches!(
        Sync::set()
            .on_conflict(OnConflict::Error)
            .perform_async(&client, (1, "x".into()))
            .await,
        Err(hench::Error::DuplicateJob { .. })
    ));
    assert_eq!(client.jobs::<Sync>().len(), 1);
    assert_eq!(
        client.unique_holder::<Sync>(&(1, "any".into())),
        Some(first.clone())
    );
    assert_eq!(
        client.unique_holder_for(Sync::NAME, Some("default"), &json!([1])),
        Some(first)
    );
    // Running it releases the lock.
    assert_eq!(client.drain::<Sync>().await.unwrap(), 1);
    assert_eq!(client.unique_holder::<Sync>(&(1, "any".into())), None);
    assert!(
        Sync::perform_async(&client, (1, "ok".into()))
            .await
            .unwrap()
            .is_some()
    );
    // A failure releases it too (testing clients do not retry).
    client.clear_all();
    client.clear_unique_locks();
    Sync::perform_async(&client, (2, "fail".into()))
        .await
        .unwrap()
        .unwrap();
    assert!(client.drain::<Sync>().await.is_err());
    assert_eq!(client.unique_holder::<Sync>(&(2, "x".into())), None);
    // Clones share the locks; another client has its own.
    let clone = client.clone();
    Sync::perform_async(&clone, (3, "ok".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        Sync::perform_async(&client, (3, "ok".into()))
            .await
            .unwrap(),
        None
    );
    let other = testing(&seen).fake();
    assert!(
        Sync::perform_async(&other, (3, "ok".into()))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn start_locks_are_released_before_perform_and_expired_ones_stay() {
    // T1: `start` released when the job starts; `expired` kept after it ran.
    let seen = Arc::new(Seen::default());
    let client = testing(&seen).fake();
    Sync::set()
        .unique_until(UniqueUntil::Start)
        .perform_async(&client, (1, "push_self".into()))
        .await
        .unwrap()
        .unwrap();
    client.perform_one::<Sync>().await.unwrap().unwrap();
    assert_eq!(seen.pushes.lock().unwrap()[0], "pushed");
    let expired = Sync::set()
        .unique(Unique::new(Duration::from_secs(60)).until(UniqueUntil::Expired))
        .perform_async(&client, (2, "ok".into()))
        .await
        .unwrap()
        .unwrap();
    client.drain_all().await.unwrap();
    assert_eq!(
        client.unique_holder::<Sync>(&(2, "x".into())),
        Some(expired)
    );
}

#[tokio::test]
async fn the_switch_turns_uniqueness_off() {
    let seen = Arc::new(Seen::default());
    let client = testing(&seen).unique_jobs(false).fake();
    for _ in 0..2 {
        assert!(
            Sync::perform_async(&client, (1, "ok".into()))
                .await
                .unwrap()
                .is_some()
        );
    }
    let captured = client.jobs::<Sync>();
    assert_eq!(captured.len(), 2);
    assert!(captured.iter().all(|job| job.unique.is_none()));
}

#[tokio::test]
async fn debounce_pushes_replace_a_captured_scheduled_job() {
    let seen = Arc::new(Seen::default());
    let client = testing(&seen).fake();
    let debounce = Sync::set()
        .delay(Duration::from_secs(30))
        .on_conflict(OnConflict::Replace);
    let first = debounce
        .push_outcome(&client, (1, "v1".to_owned()))
        .await
        .unwrap();
    let second = debounce
        .push_outcome(&client, (1, "v2".to_owned()))
        .await
        .unwrap();
    let PushOutcome::Replaced { replaced, .. } = &second else {
        panic!("not replaced: {second:?}");
    };
    assert_eq!(Some(replaced.as_str()), first.jid());
    let captured = client.jobs::<Sync>();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].args[1], json!("v2"));
    // A queued holder is not replaced.
    Sync::perform_async(&client, (2, "queued".into()))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        Sync::set()
            .on_conflict(OnConflict::Replace)
            .push_outcome(&client, (2, "next".into()))
            .await
            .unwrap(),
        PushOutcome::Duplicate { .. }
    ));
}

#[tokio::test]
async fn inline_clients_run_jobs_inside_their_lock() {
    // T2: a job pushing itself: a duplicate with `success`, run with `start`.
    let seen = Arc::new(Seen::default());
    let client = testing(&seen).inline();
    Sync::perform_async(&client, (1, "push_self".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seen.pushes.lock().unwrap().as_slice(), ["duplicate"]);
    // The lock was released after the run.
    assert_eq!(client.unique_holder::<Sync>(&(1, "x".into())), None);
    Sync::set()
        .unique_until(UniqueUntil::Start)
        .perform_async(&client, (2, "push_self".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        seen.pushes.lock().unwrap().as_slice(),
        ["duplicate", "pushed"]
    );
    assert_eq!(seen.runs.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn jobs_can_release_their_lock_early() {
    let seen = Arc::new(Seen::default());
    let client = testing(&seen).fake();
    Sync::perform_async(&client, (1, "release".into()))
        .await
        .unwrap()
        .unwrap();
    client.perform_one::<Sync>().await.unwrap().unwrap();
    assert_eq!(seen.pushes.lock().unwrap().as_slice(), ["true"]);
    // The follow-up holds the lock now.
    assert!(client.unique_holder::<Sync>(&(1, "x".into())).is_some());
}

#[tokio::test]
async fn interrupted_jobs_keep_their_lock_on_a_fake_client() {
    let seen = Arc::new(Seen::default());
    let client = testing(&seen).fake();
    let jid = Sync::perform_async(&client, (1, "interrupt".into()))
        .await
        .unwrap()
        .unwrap();
    client.perform_one::<Sync>().await.unwrap().unwrap();
    // Pushed back to the client, still holding its lock.
    let captured = client.jobs::<Sync>();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].jid, jid);
    assert_eq!(client.unique_holder::<Sync>(&(1, "x".into())), Some(jid));
    assert_eq!(
        Sync::perform_async(&client, (1, "dup".into()))
            .await
            .unwrap(),
        None
    );
    client.drain::<Sync>().await.unwrap();
    assert_eq!(client.unique_holder::<Sync>(&(1, "x".into())), None);
}

#[tokio::test]
async fn jobs_see_their_uniqueness() {
    let job = {
        let mut payload = JobPayload::new(Sync::NAME, vec![json!(1), json!("x")]);
        let mut unique = Unique::new(Duration::from_secs(60)).until(UniqueUntil::Start);
        unique.digest = Some("d".to_owned());
        payload.unique = Some(unique.into());
        payload
    };
    let ctx = JobContext::new(job, Client::fake());
    let unique = ctx.unique().unwrap();
    assert_eq!(unique.until, Some(UniqueUntil::Start));
    assert_eq!(unique.digest.as_deref(), Some("d"));
    // Nothing held in this client: nothing released.
    assert!(!ctx.release_unique_lock().await.unwrap());
    let plain = JobContext::new(JobPayload::new("Plain", vec![]), Client::fake());
    assert!(plain.unique().is_none());
    assert!(!plain.release_unique_lock().await.unwrap());
}

#[tokio::test]
async fn an_explicit_registry_works_too() {
    let seen = Arc::new(Seen::default());
    let mut registry = Registry::new();
    registry.register(Sync).unwrap();
    let client = Testing::new()
        .registry(registry)
        .data(seen.clone())
        .inline();
    Sync::perform_async(&client, (9, "ok".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seen.runs.lock().unwrap().as_slice(), ["9:ok"]);
}
