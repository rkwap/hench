//! Iterable jobs (Sidekiq's `iterable_test` and `iterable_job_query_test` scenarios), in the
//! testing modes, against Redis and on real workers.

mod support;

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use hench::iterable::{Enumerator, INVALID_CURSOR, IterableJob, Iteration, IterationState};
use hench::metrics::{Period, Query};
use hench::prelude::*;
use hench::test_util::TestRedis;
use hench::testing::Testing;
use hench::{Data, FetchStrategy, keys};
use serde_json::{Value, json};
use support::{WAIT, hgetall, wait_for};

/// What the test jobs did, and how they should behave.
struct Recorder {
    items: Mutex<Vec<i64>>,
    hooks: Mutex<Vec<String>>,
    /// Items left before the job signals a shutdown (its own stopping token): `usize::MAX`
    /// never does.
    stop_after: AtomicUsize,
    /// Fail on this item, once.
    fail_on: Mutex<Option<i64>>,
    /// Panic on this item (or, for [`PanickyStream`], while producing it), once.
    panic_on: Mutex<Option<i64>>,
    /// Abort the iteration after this item.
    abort_after: Mutex<Option<i64>>,
    /// Call [`Iteration::abort`] from this hook (`"start"` or `"resume"`).
    abort_in: Mutex<Option<&'static str>>,
    /// Fail this hook (`"stop"` or `"complete"`), once.
    fail_hook: Mutex<Option<&'static str>>,
    /// Pause per item, in milliseconds.
    pause_ms: AtomicUsize,
    /// Arguments seen by the hooks.
    args: Mutex<Vec<(String, Value)>>,
    /// Cursors seen while processing items.
    cursors: Mutex<Vec<Option<usize>>>,
}

impl Default for Recorder {
    fn default() -> Self {
        Self {
            items: Mutex::default(),
            hooks: Mutex::default(),
            stop_after: AtomicUsize::new(usize::MAX),
            fail_on: Mutex::default(),
            panic_on: Mutex::default(),
            abort_after: Mutex::default(),
            abort_in: Mutex::default(),
            fail_hook: Mutex::default(),
            pause_ms: AtomicUsize::new(0),
            args: Mutex::default(),
            cursors: Mutex::default(),
        }
    }
}

impl Recorder {
    fn items(&self) -> Vec<i64> {
        self.items.lock().unwrap().clone()
    }

    fn hook_count(&self, hook: &str) -> usize {
        self.hooks
            .lock()
            .unwrap()
            .iter()
            .filter(|h| *h == hook)
            .count()
    }

    fn hooks(&self) -> Vec<String> {
        self.hooks.lock().unwrap().clone()
    }

    fn stop_after(&self, items: usize) {
        self.stop_after.store(items, Ordering::SeqCst);
    }

    fn hook<J: IterableJob>(&self, name: &str, it: &Iteration<'_, J>) {
        self.hooks.lock().unwrap().push(name.to_owned());
        let args = serde_json::to_value(it.args()).unwrap();
        self.args.lock().unwrap().push((name.to_owned(), args));
        if *self.abort_in.lock().unwrap() == Some(name) {
            it.abort();
        }
    }

    /// Fails, once, when `hook` is the hook to fail.
    fn hook_outcome(&self, hook: &str) -> Result<(), JobError> {
        let mut fail_hook = self.fail_hook.lock().unwrap();
        if *fail_hook == Some(hook) {
            *fail_hook = None;
            Err(JobError::new("HookError", format!("{hook} failed")))
        } else {
            Ok(())
        }
    }

    /// `true` once for `number` when it is the item to panic on.
    fn take_panic(&self, number: i64) -> bool {
        let mut panic_on = self.panic_on.lock().unwrap();
        if *panic_on == Some(number) {
            *panic_on = None;
            true
        } else {
            false
        }
    }

    /// Records one processed item; signals a shutdown when the countdown reaches zero.
    async fn process<J: IterableJob>(
        &self,
        number: i64,
        it: &Iteration<'_, J>,
    ) -> Result<(), JobError> {
        let pause = self.pause_ms.load(Ordering::SeqCst);
        if pause > 0 {
            tokio::time::sleep(Duration::from_millis(pause as u64)).await;
        }
        let fail = {
            let mut fail_on = self.fail_on.lock().unwrap();
            if *fail_on == Some(number) {
                *fail_on = None;
                true
            } else {
                false
            }
        };
        if fail {
            return Err(JobError::new("RuntimeError", format!("failed on {number}")));
        }
        if self.take_panic(number) {
            panic!("panicked on {number}");
        }
        self.items.lock().unwrap().push(number);
        let remaining = self.stop_after.load(Ordering::SeqCst);
        if remaining != usize::MAX {
            let remaining = remaining.saturating_sub(1);
            self.stop_after.store(remaining, Ordering::SeqCst);
            if remaining == 0 {
                it.stopping_token().cancel();
            }
        }
        if *self.abort_after.lock().unwrap() == Some(number) {
            it.abort();
        }
        Ok(())
    }
}

/// Iterates over the numbers 10 to 20.
#[derive(Default)]
struct Numbers;

impl IterableJob for Numbers {
    const NAME: &'static str = "Numbers";
    type Args = ();
    type Item = i64;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::vec((10..=20).collect(), cursor))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        let recorder = it.data::<Recorder>()?;
        recorder.cursors.lock().unwrap().push(it.cursor().copied());
        recorder.process(number, it).await
    }

    async fn on_start(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("start", it);
        Ok(())
    }

    async fn on_resume(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("resume", it);
        Ok(())
    }

    async fn on_stop(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        let recorder = it.data::<Recorder>()?;
        recorder.hook("stop", it);
        recorder.hook_outcome("stop")
    }

    async fn on_cancel(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("cancel", it);
        Ok(())
    }

    async fn on_complete(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        let recorder = it.data::<Recorder>()?;
        recorder.hook("complete", it);
        recorder.hook_outcome("complete")
    }
}

/// Iterates over `[0, 1]`, recording each number with the job's two arguments.
#[derive(Default)]
struct WithArguments;

impl IterableJob for WithArguments {
    const NAME: &'static str = "WithArguments";
    type Args = (String, Value);
    type Item = i64;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a Self::Args,
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::vec(vec![0, 1], cursor))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        let recorder = it.data::<Recorder>()?;
        let (first, second) = it.args();
        recorder
            .hooks
            .lock()
            .unwrap()
            .push(format!("{number} {first} {second}"));
        Ok(())
    }

    async fn on_start(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("start", it);
        Ok(())
    }

    async fn on_stop(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("stop", it);
        Ok(())
    }

    async fn on_complete(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("complete", it);
        Ok(())
    }
}

/// Nothing to iterate.
#[derive(Default)]
struct Empty;

impl IterableJob for Empty {
    const NAME: &'static str = "Empty";
    type Args = ();
    type Item = ();
    type Cursor = u32;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        _cursor: Option<u32>,
    ) -> Result<Enumerator<'a, (), u32>, JobError> {
        Ok(Enumerator::empty())
    }

    async fn each_iteration(&self, (): (), _it: &Iteration<'_, Self>) -> Result<(), JobError> {
        Err(JobError::msg("there is nothing to iterate"))
    }

    async fn on_start(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("start", it);
        Ok(())
    }

    async fn on_stop(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("stop", it);
        Ok(())
    }

    async fn on_complete(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("complete", it);
        Ok(())
    }
}

/// Runs for at most 30 ms per execution, over the numbers 0 to 19.
#[derive(Default)]
struct Limited;

impl IterableJob for Limited {
    const NAME: &'static str = "Limited";
    type Args = (String,);
    type Item = i64;
    type Cursor = i64;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (String,),
        cursor: Option<i64>,
    ) -> Result<Enumerator<'a, i64, i64>, JobError> {
        Ok(Enumerator::range(0..20, cursor))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Also run without data, by `perform_inline`.
        if let Ok(recorder) = it.data::<Recorder>() {
            recorder.items.lock().unwrap().push(number);
        }
        Ok(())
    }

    async fn on_resume(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        if let Ok(recorder) = it.data::<Recorder>() {
            recorder.hook("resume", it);
        }
        Ok(())
    }

    fn max_iteration_runtime(&self) -> Option<Duration> {
        Some(Duration::from_millis(30))
    }
}

/// Records its argument (shares the queue with iterable jobs).
#[derive(Default)]
struct Marker;

impl Job for Marker {
    const NAME: &'static str = "Marker";
    type Args = (String,);

    async fn perform(&self, ctx: &JobContext, (name,): (String,)) -> Result<(), JobError> {
        ctx.data::<Recorder>()?
            .hooks
            .lock()
            .unwrap()
            .push(format!("marker {name}"));
        Ok(())
    }
}

fn data(recorder: &Arc<Recorder>) -> Arc<Data> {
    let mut data = Data::new();
    data.insert_arc(recorder.clone());
    Arc::new(data)
}

/// One execution of `Numbers` with jid `jid`, keeping its state through `client` (Redis or
/// in memory).
async fn run_numbers(client: &Client, recorder: &Arc<Recorder>, jid: &str) -> Result<(), JobError> {
    let mut payload = JobPayload::new("Numbers", vec![]);
    payload.jid = jid.to_owned();
    payload.queue = "default".to_owned();
    let ctx = JobContext::new(payload, client.clone()).with_data(data(recorder));
    Job::perform(&Numbers, &ctx, ()).await
}

async fn state(redis: &RedisPool, jid: &str) -> HashMap<String, String> {
    hgetall(redis, &keys::iteration_state(jid)).await
}

#[tokio::test]
async fn iterates_over_vectors() {
    let recorder = Arc::new(Recorder::default());
    let client = Testing::new()
        .register(Numbers)
        .data_arc(recorder.clone())
        .inline();
    Numbers::perform_async(&client, ()).await.unwrap();
    assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
    assert_eq!(recorder.hooks(), ["start", "stop", "complete"]);
    // While processing an item, the cursor is its position.
    let cursors = recorder.cursors.lock().unwrap().clone();
    assert_eq!(cursors[0], None);
    assert_eq!(cursors[1], Some(1));
    assert_eq!(cursors[10], Some(10));
}

#[tokio::test]
async fn supports_jobs_with_arguments() {
    let recorder = Arc::new(Recorder::default());
    let client = Testing::new()
        .register(WithArguments)
        .data_arc(recorder.clone())
        .fake();
    let args = ("arg one".to_owned(), json!({"mike": 456, "bob": "string"}));
    WithArguments::perform_async(&client, args).await.unwrap();
    assert_eq!(client.drain::<WithArguments>().await.unwrap(), 1);
    assert_eq!(
        recorder.hooks(),
        [
            "start",
            r#"0 arg one {"bob":"string","mike":456}"#,
            r#"1 arg one {"bob":"string","mike":456}"#,
            "stop",
            "complete",
        ]
    );
    // Every hook saw the arguments after their JSON round trip.
    let args = recorder.args.lock().unwrap().clone();
    assert_eq!(args.len(), 3);
    for (hook, value) in args {
        assert_eq!(
            value,
            json!(["arg one", {"mike": 456, "bob": "string"}]),
            "{hook}"
        );
    }
}

#[tokio::test]
async fn empty_enumerators_complete_at_once() {
    let recorder = Arc::new(Recorder::default());
    let client = Testing::new()
        .register(Empty)
        .data_arc(recorder.clone())
        .inline();
    Empty::perform_async(&client, ()).await.unwrap();
    assert_eq!(recorder.hooks(), ["start", "stop", "complete"]);
}

#[tokio::test]
async fn aborting_completes_the_job() {
    let recorder = Arc::new(Recorder::default());
    *recorder.abort_after.lock().unwrap() = Some(11);
    let client = Client::fake();
    run_numbers(&client, &recorder, "abort").await.unwrap();
    assert_eq!(recorder.items(), [10, 11]);
    assert_eq!(recorder.hook_count("complete"), 1);
    assert_eq!(recorder.hook_count("stop"), 1);
    // The state is gone: the job is over.
    let again = Arc::new(Recorder::default());
    run_numbers(&client, &again, "abort").await.unwrap();
    assert_eq!(again.items().len(), 11);
    assert_eq!(again.hooks(), ["start", "stop", "complete"]);
}

#[tokio::test]
async fn resumes_from_the_saved_cursor() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let recorder = Arc::new(Recorder::default());

    recorder.stop_after(2);
    let error = run_numbers(&client, &recorder, "resume").await.unwrap_err();
    assert!(error.is_interrupted(), "{error:?}");
    assert_eq!(recorder.items(), [10, 11]);
    let saved = state(&pool, "resume").await;
    assert_eq!(saved["ex"], "1");
    assert_eq!(saved["c"], "2");
    assert!(saved["rt"].parse::<f64>().unwrap() >= 0.0);
    let ttl = support::pttl(&pool, &keys::iteration_state("resume")).await;
    assert!(ttl > 29 * 86_400 * 1_000, "{ttl}");

    recorder.stop_after(2);
    let error = run_numbers(&client, &recorder, "resume").await.unwrap_err();
    assert!(error.is_interrupted());
    assert_eq!(recorder.items(), [10, 11, 12, 13]);
    let saved = state(&pool, "resume").await;
    assert_eq!(saved["ex"], "2");
    assert_eq!(saved["c"], "4");
    assert!(saved["rt"].parse::<f64>().unwrap() > 0.0);

    recorder.stop_after(usize::MAX);
    run_numbers(&client, &recorder, "resume").await.unwrap();
    assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
    assert!(state(&pool, "resume").await.is_empty());
}

#[tokio::test]
async fn calls_the_hooks() {
    let client = Client::fake();
    let recorder = Arc::new(Recorder::default());
    recorder.stop_after(2);
    run_numbers(&client, &recorder, "hooks").await.unwrap_err();
    assert_eq!(recorder.hooks(), ["start", "stop"]);

    recorder.stop_after(2);
    run_numbers(&client, &recorder, "hooks").await.unwrap_err();
    assert_eq!(recorder.hooks(), ["start", "stop", "resume", "stop"]);

    recorder.stop_after(usize::MAX);
    run_numbers(&client, &recorder, "hooks").await.unwrap();
    assert_eq!(
        recorder.hooks(),
        [
            "start", "stop", "resume", "stop", "resume", "stop", "complete"
        ]
    );
    assert_eq!(recorder.items().len(), 11);
}

#[tokio::test]
async fn failures_save_the_cursor_and_retries_resume() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let recorder = Arc::new(Recorder::default());
    *recorder.fail_on.lock().unwrap() = Some(15);
    let error = run_numbers(&client, &recorder, "fails").await.unwrap_err();
    assert_eq!(error.message(), "failed on 15");
    assert_eq!(recorder.items(), [10, 11, 12, 13, 14]);
    let saved = state(&pool, "fails").await;
    assert_eq!(saved["ex"], "1");
    // The retry starts with the item that failed.
    assert_eq!(saved["c"], "5");
    // No stop hook on failure.
    assert_eq!(recorder.hooks(), ["start"]);

    run_numbers(&client, &recorder, "fails").await.unwrap();
    assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
    assert_eq!(recorder.hooks(), ["start", "resume", "stop", "complete"]);
}

/// Iterates over the numbers 0 to 9 from a stream that panics, once, while producing the item
/// the recorder's `panic_on` names.
#[derive(Default)]
struct PanickyStream;

impl IterableJob for PanickyStream {
    const NAME: &'static str = "PanickyStream";
    type Args = ();
    type Item = i64;
    type Cursor = i64;

    fn build_enumerator<'a>(
        &'a self,
        ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<i64>,
    ) -> Result<Enumerator<'a, i64, i64>, JobError> {
        let recorder = ctx.data_arc::<Recorder>()?;
        let numbers = futures::stream::iter(cursor.unwrap_or(0)..10);
        Ok(Enumerator::from_stream(numbers.map(move |number| {
            if recorder.take_panic(number) {
                panic!("the enumerator panicked on {number}");
            }
            (number, number + 1)
        })))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.items.lock().unwrap().push(number);
        Ok(())
    }

    async fn on_start(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("start", it);
        Ok(())
    }

    async fn on_resume(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("resume", it);
        Ok(())
    }
}

/// A panicking item saves the cursor before the panic goes on, as an error does: the retry
/// resumes with that item (`on_resume`) instead of starting over from the last checkpoint.
#[tokio::test]
async fn panics_save_the_cursor_like_errors() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let recorder = Arc::new(Recorder::default());
    *recorder.panic_on.lock().unwrap() = Some(15);
    let panicked = AssertUnwindSafe(run_numbers(&client, &recorder, "panics"))
        .catch_unwind()
        .await
        .unwrap_err();
    // The panic reaches the caller unchanged (workers turn it into a failure of class `panic`).
    assert_eq!(
        panicked.downcast_ref::<String>().map(String::as_str),
        Some("panicked on 15")
    );
    assert_eq!(recorder.items(), [10, 11, 12, 13, 14]);
    let saved = state(&pool, "panics").await;
    assert_eq!(saved["ex"], "1");
    assert_eq!(saved["c"], "5");
    // No stop hook on failure.
    assert_eq!(recorder.hooks(), ["start"]);

    run_numbers(&client, &recorder, "panics").await.unwrap();
    assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
    assert_eq!(recorder.hooks(), ["start", "resume", "stop", "complete"]);
}

/// Through the workers' execution path (an inline testing client): a panicking enumerator
/// fails the job with class `panic` after saving the cursor of the last item processed, and
/// the retry (the same job pushed again) resumes after it.
#[tokio::test]
async fn panicking_enumerators_save_the_cursor_too() {
    let recorder = Arc::new(Recorder::default());
    *recorder.panic_on.lock().unwrap() = Some(5);
    let client = Testing::new()
        .register(PanickyStream)
        .data_arc(recorder.clone())
        .inline();
    let mut payload = JobPayload::new("PanickyStream", vec![]);
    payload.jid = "0123456789abcdef01234567".to_owned();
    match client.push(payload.clone()).await {
        Err(hench::Error::Job(error)) => {
            assert_eq!(error.class(), JobError::PANIC);
            assert!(
                error.message().contains("the enumerator panicked on 5"),
                "{}",
                error.message()
            );
        }
        other => panic!("expected a panic, got {other:?}"),
    }
    assert_eq!(recorder.items(), [0, 1, 2, 3, 4]);
    assert_eq!(recorder.hooks(), ["start"]);

    client.push(payload).await.unwrap();
    assert_eq!(recorder.items(), (0..10).collect::<Vec<_>>());
    assert_eq!(recorder.hooks(), ["start", "resume"]);
}

/// `abort()` called from `on_start` or `on_resume` ends the iteration before its first item,
/// as completed; from the hooks that run once the iteration is over it changes nothing.
#[tokio::test]
async fn aborting_from_the_start_hooks_completes_before_the_first_item() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;

    let recorder = Arc::new(Recorder::default());
    *recorder.abort_in.lock().unwrap() = Some("start");
    run_numbers(&client, &recorder, "abort-start")
        .await
        .unwrap();
    assert!(recorder.items().is_empty(), "{:?}", recorder.items());
    assert_eq!(recorder.hooks(), ["start", "stop", "complete"]);
    assert!(state(&pool, "abort-start").await.is_empty());

    // From `on_resume`: only the items of the interrupted first execution ran.
    let recorder = Arc::new(Recorder::default());
    *recorder.abort_in.lock().unwrap() = Some("resume");
    recorder.stop_after(2);
    let error = run_numbers(&client, &recorder, "abort-resume")
        .await
        .unwrap_err();
    assert!(error.is_interrupted(), "{error:?}");
    recorder.stop_after(usize::MAX);
    run_numbers(&client, &recorder, "abort-resume")
        .await
        .unwrap();
    assert_eq!(recorder.items(), [10, 11]);
    assert_eq!(
        recorder.hooks(),
        ["start", "stop", "resume", "stop", "complete"]
    );
    assert!(state(&pool, "abort-resume").await.is_empty());

    // From `on_stop` of an interrupted execution: still interrupted, the state is kept.
    let recorder = Arc::new(Recorder::default());
    *recorder.abort_in.lock().unwrap() = Some("stop");
    recorder.stop_after(2);
    let error = run_numbers(&client, &recorder, "abort-stop")
        .await
        .unwrap_err();
    assert!(error.is_interrupted(), "{error:?}");
    assert_eq!(state(&pool, "abort-stop").await["c"], "2");
}

#[tokio::test]
async fn reschedules_itself_when_the_process_is_stopping() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;
    let recorder = Arc::new(Recorder::default());
    recorder.stop_after(2);
    run_numbers(&client, &recorder, "stopping")
        .await
        .unwrap_err();

    // The process is already stopping when the job starts: it saves and leaves at once.
    let mut payload = JobPayload::new("Numbers", vec![]);
    payload.jid = "stopping".to_owned();
    let stopping = hench::CancellationToken::new();
    stopping.cancel();
    let ctx = JobContext::new(payload, client.clone())
        .with_data(data(&recorder))
        .with_stopping(stopping);
    let error = Job::perform(&Numbers, &ctx, ()).await.unwrap_err();
    assert!(error.is_interrupted());
    assert_eq!(recorder.items(), [10, 11]);
    assert_eq!(state(&pool, "stopping").await["ex"], "2");
    assert_eq!(state(&pool, "stopping").await["c"], "2");

    recorder.stop_after(usize::MAX);
    run_numbers(&client, &recorder, "stopping").await.unwrap();
    assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
}

#[tokio::test]
async fn runs_no_longer_than_the_runtime_limit() {
    let recorder = Arc::new(Recorder::default());
    recorder.pause_ms.store(10, Ordering::SeqCst);
    let mut payload = JobPayload::new("Numbers", vec![]);
    payload.jid = "limited".to_owned();
    let ctx = JobContext::new(payload, Client::fake())
        .with_data(data(&recorder))
        .with_max_iteration_runtime(Some(Duration::from_millis(5)));
    let error = Job::perform(&Numbers, &ctx, ()).await.unwrap_err();
    assert!(error.is_interrupted());
    // At least one item per execution, and far from all of them.
    let processed = recorder.items().len();
    assert!((1..11).contains(&processed), "{processed}");

    // `perform_inline` returns the interruption too (here the job's own limit).
    let error = Limited.perform_inline(("x".to_owned(),)).await.unwrap_err();
    assert_eq!(error.class(), JobError::INTERRUPTED);
}

/// Three items, the last one longer than an execution may run.
#[derive(Default)]
struct SlowLastItem;

impl SlowLastItem {
    /// Far more than the first two items take, even on a loaded machine.
    const LIMIT: Duration = Duration::from_secs(1);
}

impl IterableJob for SlowLastItem {
    const NAME: &'static str = "SlowLastItem";
    type Args = ();
    type Item = i64;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::vec(vec![0, 1, 2], cursor))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        if number == 2 {
            tokio::time::sleep(Self::LIMIT + Duration::from_millis(300)).await;
        }
        it.data::<Recorder>()?.items.lock().unwrap().push(number);
        Ok(())
    }

    async fn on_stop(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("stop", it);
        Ok(())
    }

    async fn on_complete(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("complete", it);
        Ok(())
    }

    fn max_iteration_runtime(&self) -> Option<Duration> {
        Some(Self::LIMIT)
    }
}

/// An execution stops before an item only when there is one (like Sidekiq, which checks once
/// its enumerator yields another item): the time running out, or a shutdown starting, during
/// the last item completes the job instead of sending it back to its queue for an execution
/// with nothing left to do.
#[tokio::test]
async fn the_last_item_completes_the_job_whatever_the_time_or_a_shutdown() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = redis.client().await;

    // The time runs out during the last item.
    let recorder = Arc::new(Recorder::default());
    let mut payload = JobPayload::new("SlowLastItem", vec![]);
    payload.jid = "slow-last".to_owned();
    let ctx = JobContext::new(payload, client.clone()).with_data(data(&recorder));
    Job::perform(&SlowLastItem, &ctx, ()).await.unwrap();
    assert_eq!(recorder.items(), [0, 1, 2]);
    assert_eq!(recorder.hooks(), ["stop", "complete"]);
    assert!(state(&pool, "slow-last").await.is_empty());

    // The process starts stopping during the last item.
    let recorder = Arc::new(Recorder::default());
    recorder.stop_after(11);
    run_numbers(&client, &recorder, "stopping-last")
        .await
        .unwrap();
    assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
    assert_eq!(recorder.hooks(), ["start", "stop", "complete"]);
    assert!(state(&pool, "stopping-last").await.is_empty());

    // During the one before: the execution stops before the last item, which the enumerator
    // yields again to the next execution (after the saved cursor).
    let recorder = Arc::new(Recorder::default());
    recorder.stop_after(10);
    let error = run_numbers(&client, &recorder, "stopping-before-last")
        .await
        .unwrap_err();
    assert!(error.is_interrupted(), "{error:?}");
    assert_eq!(state(&pool, "stopping-before-last").await["c"], "10");
    recorder.stop_after(usize::MAX);
    run_numbers(&client, &recorder, "stopping-before-last")
        .await
        .unwrap();
    assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
    assert_eq!(
        recorder.hooks(),
        ["start", "stop", "resume", "stop", "complete"]
    );
}

#[tokio::test]
async fn cancels_before_the_first_item_through_the_client() {
    for redis_backed in [false, true] {
        let redis = TestRedis::start().await;
        let client = if redis_backed {
            redis.client().await
        } else {
            Client::fake()
        };
        let recorder = Arc::new(Recorder::default());
        let cancelled = client.cancel("cancel-early").await.unwrap();
        if redis_backed {
            let pool = redis.pool().await;
            let saved = state(&pool, "cancel-early").await;
            assert_eq!(saved.keys().collect::<Vec<_>>(), ["cancelled"]);
            let state = IterationState::fetch(&pool, "cancel-early")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(state.cancelled, Some(cancelled));
            assert_eq!(state.executions, 0);
        }
        run_numbers(&client, &recorder, "cancel-early")
            .await
            .unwrap();
        assert!(recorder.items().is_empty());
        assert_eq!(recorder.hooks(), ["start", "cancel", "stop", "complete"]);
        if redis_backed {
            let pool = redis.pool().await;
            assert!(state(&pool, "cancel-early").await.is_empty());
        }
    }
}

#[tokio::test]
async fn cancels_midway() {
    let client = Client::fake();
    let recorder = Arc::new(Recorder::default());
    recorder.stop_after(2);
    run_numbers(&client, &recorder, "midway").await.unwrap_err();
    assert_eq!(recorder.items(), [10, 11]);
    assert_eq!(recorder.hooks(), ["start", "stop"]);

    let first = client.cancel("midway").await.unwrap();
    // Cancelling again keeps the first time.
    assert_eq!(client.cancel("midway").await.unwrap(), first);

    recorder.stop_after(usize::MAX);
    run_numbers(&client, &recorder, "midway").await.unwrap();
    assert_eq!(recorder.items(), [10, 11]);
    assert_eq!(
        recorder.hooks(),
        ["start", "stop", "resume", "cancel", "stop", "complete"]
    );
}

/// Cancels itself (through its client) on 12, with a checkpoint before every item.
#[derive(Default)]
struct SelfCancelling;

impl IterableJob for SelfCancelling {
    const NAME: &'static str = "SelfCancelling";
    type Args = ();
    type Item = i64;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::vec((10..=20).collect(), cursor))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.items.lock().unwrap().push(number);
        if number == 12 {
            it.client().cancel(it.jid()).await?;
        }
        Ok(())
    }

    async fn on_cancel(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("cancel", it);
        Ok(())
    }

    fn checkpoint_interval(&self) -> Duration {
        Duration::ZERO
    }
}

#[tokio::test]
async fn cancellation_is_seen_at_the_next_checkpoint() {
    let redis = TestRedis::start().await;
    let recorder = Arc::new(Recorder::default());
    let client = Testing::new()
        .register(SelfCancelling)
        .data_arc(recorder.clone())
        .fake();
    SelfCancelling::perform_async(&client, ()).await.unwrap();
    client.drain::<SelfCancelling>().await.unwrap();
    assert_eq!(recorder.items(), [10, 11, 12]);
    assert_eq!(recorder.hooks(), ["cancel"]);

    // The same against Redis.
    let recorder = Arc::new(Recorder::default());
    let mut payload = JobPayload::new("SelfCancelling", vec![]);
    payload.jid = "self".to_owned();
    let ctx = JobContext::new(payload, redis.client().await).with_data(data(&recorder));
    Job::perform(&SelfCancelling, &ctx, ()).await.unwrap();
    assert_eq!(recorder.items(), [10, 11, 12]);
    assert!(state(&redis.pool().await, "self").await.is_empty());
}

#[tokio::test]
async fn unreadable_cursors_fail_the_job() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let () = support::cmd(
        &pool,
        &redis_cmd!(
            "HSET",
            keys::iteration_state("bad"),
            "c",
            "\"text\"",
            "ex",
            1
        ),
    )
    .await;
    let recorder = Arc::new(Recorder::default());
    let error = run_numbers(&redis.client().await, &recorder, "bad")
        .await
        .unwrap_err();
    assert_eq!(error.class(), INVALID_CURSOR);
    assert!(recorder.hooks().is_empty());
}

#[tokio::test]
async fn testing_clients_push_interrupted_jobs_back() {
    // Fake: the job goes back to the end of its fake queue and draining continues it.
    let recorder = Arc::new(Recorder::default());
    let client = Testing::new()
        .register(Limited)
        .register(Marker)
        .data_arc(recorder.clone())
        .fake();
    Limited::perform_async(&client, ("a".to_owned(),))
        .await
        .unwrap();
    assert!(client.drain::<Limited>().await.unwrap() > 1);
    assert_eq!(recorder.items(), (0..20).collect::<Vec<_>>());
    assert!(recorder.hook_count("resume") >= 1);
    assert!(client.jobs::<Limited>().is_empty());

    // `perform_one` runs one execution; the job is back in the queue afterwards.
    let recorder = Arc::new(Recorder::default());
    let client = Testing::new()
        .register(Limited)
        .data_arc(recorder.clone())
        .fake();
    let jid = Limited::perform_async(&client, ("b".to_owned(),))
        .await
        .unwrap()
        .unwrap();
    client.perform_one::<Limited>().await.unwrap().unwrap();
    let pushed_back = client.jobs::<Limited>();
    assert_eq!(pushed_back.len(), 1);
    assert_eq!(pushed_back[0].jid, jid);
    assert!(recorder.items().len() < 20);

    // Inline: the job runs again at once until it is done.
    let recorder = Arc::new(Recorder::default());
    let client = Testing::new()
        .register(Limited)
        .data_arc(recorder.clone())
        .inline();
    Limited::perform_async(&client, ("c".to_owned(),))
        .await
        .unwrap();
    assert_eq!(recorder.items(), (0..20).collect::<Vec<_>>());

    // The process-wide limit applies too.
    let recorder = Arc::new(Recorder::default());
    recorder.pause_ms.store(5, Ordering::SeqCst);
    let client = Testing::new()
        .register(Numbers)
        .data_arc(recorder.clone())
        .max_iteration_runtime(Duration::from_millis(12))
        .fake();
    Numbers::perform_async(&client, ()).await.unwrap();
    let executions = client.drain::<Numbers>().await.unwrap();
    assert!(executions > 1, "{executions}");
    assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
}

/// Iterates over `0..count`, one item per execution (a zero run-time limit interrupts every
/// execution after its first item).
#[derive(Default)]
struct OneAtATime;

impl IterableJob for OneAtATime {
    const NAME: &'static str = "OneAtATime";
    type Args = (i64,);
    type Item = i64;
    type Cursor = i64;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        (count,): &'a (i64,),
        cursor: Option<i64>,
    ) -> Result<Enumerator<'a, i64, i64>, JobError> {
        Ok(Enumerator::range(0..*count, cursor))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.items.lock().unwrap().push(number);
        Ok(())
    }

    fn max_iteration_runtime(&self) -> Option<Duration> {
        Some(Duration::ZERO)
    }
}

/// Counts the jobs a client pushes.
struct CountPushes(Arc<AtomicUsize>);

#[async_trait]
impl ClientMiddleware for CountPushes {
    async fn call(
        &self,
        job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        next.run(job).await
    }
}

/// An inline client runs an interrupted job again in a loop, not from within the execution
/// that was interrupted: hundreds of interruptions fit in the 2 MiB stack of a test thread (a
/// nested re-run overflowed it after a few dozen in debug builds, aborting the test binary).
#[test]
fn inline_clients_run_interrupted_jobs_again_without_nesting() {
    const ITEMS: i64 = 500;
    let recorder = Arc::new(Recorder::default());
    let pushes = Arc::new(AtomicUsize::new(0));
    let run = {
        let recorder = recorder.clone();
        let pushes = pushes.clone();
        move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let client = Testing::new()
                    .register(OneAtATime)
                    .data_arc(recorder)
                    .client_middleware(CountPushes(pushes))
                    .inline();
                OneAtATime::perform_async(&client, (ITEMS,)).await
            })
        }
    };
    // The stack of a `#[tokio::test]` thread, whatever RUST_MIN_STACK says.
    let jid = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(run)
        .unwrap()
        .join()
        .unwrap()
        .unwrap();
    assert!(jid.is_some());
    assert_eq!(recorder.items(), (0..ITEMS).collect::<Vec<_>>());
    // The push, then a push back per interrupted execution, each through client middleware as
    // when workers push an interrupted job back: every execution but the last, which processes
    // the last item and completes.
    assert_eq!(pushes.load(Ordering::SeqCst), ITEMS as usize);
}

// --- IterationState (Sidekiq's IterableJobQuery) ---

async fn seed(pool: &RedisPool, jid: &str, fields: &[(&str, &str)]) {
    let mut command = hench::redis_rs::cmd("HSET");
    command.arg(keys::iteration_state(jid));
    for (field, value) in fields {
        command.arg(*field).arg(*value);
    }
    let () = support::cmd(pool, &command).await;
}

#[tokio::test]
async fn iteration_states() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let none: [&str; 0] = [];
    assert!(
        IterationState::fetch_many(&pool, none)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        IterationState::fetch_many(&pool, ["missing"])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(IterationState::fetch(&pool, "missing").await.unwrap(), None);

    seed(
        &pool,
        "jid-1",
        &[("ex", "5"), ("rt", "1.5"), ("c", r#"{"offset":100}"#)],
    )
    .await;
    seed(&pool, "jid-2", &[("ex", "1"), ("cancelled", "1700000000")]).await;
    seed(&pool, "jid-3", &[("c", "not-json")]).await;
    seed(&pool, "jid-4", &[("ex", "2")]).await;
    let states = IterationState::fetch_many(
        &pool,
        ["jid-1", "jid-2", "jid-3", "jid-4", "jid-4", "", "nope"],
    )
    .await
    .unwrap();
    assert_eq!(states.len(), 4);
    let first = &states["jid-1"];
    assert_eq!(first.jid, "jid-1");
    assert_eq!(first.executions, 5);
    assert_eq!(first.runtime, Duration::from_millis(1_500));
    assert_eq!(first.cursor, Some(json!({"offset": 100})));
    assert_eq!(
        states["jid-2"].cancelled,
        Some(std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
    );
    assert_eq!(states["jid-3"].cursor, Some(json!("not-json")));
    assert_eq!(states["jid-4"].executions, 2);
}

// --- on real workers ---

#[tokio::test]
async fn workers_push_interrupted_jobs_to_the_back_of_the_queue() {
    support::init_logging();
    for fetch in [FetchStrategy::Reliable, FetchStrategy::Basic] {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let recorder = Arc::new(Recorder::default());
        let hench = redis
            .builder()
            .await
            .fetch(fetch)
            .concurrency(1)
            .register(Limited)
            .register(Marker)
            .data_arc(recorder.clone())
            .build()
            .await
            .unwrap();
        let client = hench.client().clone();
        let jid = Limited::perform_async(&client, ("x".to_owned(),))
            .await
            .unwrap()
            .unwrap();
        Marker::perform_async(&client, ("after".to_owned(),))
            .await
            .unwrap();
        let launcher = hench.start().await.unwrap();
        wait_for("the iterable job to finish", WAIT, || async {
            recorder.items().len() == 20 && state(&pool, &jid).await.is_empty()
        })
        .await;
        // Every item exactly once, in order, across several executions.
        assert_eq!(recorder.items(), (0..20).collect::<Vec<_>>(), "{fetch:?}");
        let resumes = recorder.hook_count("resume");
        assert!(resumes >= 1, "{fetch:?}");
        // The first interruption sent the job behind the marker.
        let hooks = recorder.hooks();
        assert_eq!(
            hooks.iter().position(|hook| hook == "marker after"),
            Some(0),
            "{fetch:?}: {hooks:?}"
        );
        launcher.stop().await.unwrap();

        // Interruptions count as processed jobs, not as failures, and use no retry.
        let stats = hench::api::Stats::fetch(&hench).await.unwrap();
        assert_eq!(stats.processed, resumes as u64 + 2, "{fetch:?}");
        assert_eq!(stats.failed, 0, "{fetch:?}");
        assert_eq!(stats.retry_size, 0, "{fetch:?}");
        let metrics = Query::new(&pool)
            .for_job("Limited", Period::Minutes(5))
            .await
            .unwrap();
        let totals = metrics.jobs["Limited"].totals;
        assert_eq!(totals.processed, resumes as u64 + 1, "{fetch:?}");
        assert_eq!(totals.failed, 0, "{fetch:?}");
        assert!(totals.ms >= 20 * 10, "{fetch:?}: {totals:?}");
        assert_eq!(support::queue_size(&pool, "default").await, 0);
        assert_eq!(
            support::llen(&pool, &keys::inprogress(launcher.identity())).await,
            0
        );
    }
}

/// Processes 0 to 999 slowly; never finishes within a test.
#[derive(Default)]
struct Slow;

impl IterableJob for Slow {
    const NAME: &'static str = "Slow";
    type Args = ();
    type Item = i64;
    type Cursor = i64;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<i64>,
    ) -> Result<Enumerator<'a, i64, i64>, JobError> {
        Ok(Enumerator::range(0..1_000, cursor))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        it.data::<Recorder>()?.items.lock().unwrap().push(number);
        Ok(())
    }
}

#[tokio::test]
async fn a_shutdown_saves_the_cursor_and_the_next_process_resumes() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let recorder = Arc::new(Recorder::default());
    let hench = redis
        .builder()
        .await
        .register(Slow)
        .data_arc(recorder.clone())
        .build()
        .await
        .unwrap();
    let jid = Slow::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    let launcher = hench.start().await.unwrap();
    wait_for("a few items", WAIT, || async {
        recorder.items().len() >= 5
    })
    .await;
    launcher.stop().await.unwrap();

    // Back in the queue (same jid), with its cursor saved.
    let queued = support::queue_jobs(&pool, "default").await;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].jid, jid);
    let saved = IterationState::fetch(&pool, &jid).await.unwrap().unwrap();
    let done = recorder.items();
    assert_eq!(saved.cursor, Some(json!(done.len())));
    assert_eq!(saved.executions, 1);

    // The next process continues after the last item done.
    let hench = redis
        .builder()
        .await
        .register(Slow)
        .data_arc(recorder.clone())
        .build()
        .await
        .unwrap();
    let launcher = hench.start().await.unwrap();
    wait_for("more items", WAIT, || async {
        recorder.items().len() >= done.len() + 5
    })
    .await;
    launcher.stop().await.unwrap();
    let items = recorder.items();
    let expected: Vec<i64> = (0..items.len() as i64).collect();
    assert_eq!(items, expected, "no item twice, none skipped");
}

#[tokio::test]
async fn jobs_cancelled_while_queued_complete_without_iterating() {
    let redis = TestRedis::start().await;
    let recorder = Arc::new(Recorder::default());
    let hench = redis
        .builder()
        .await
        .register(Numbers)
        .data_arc(recorder.clone())
        .build()
        .await
        .unwrap();
    let jid = Numbers::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    hench.client().cancel(&jid).await.unwrap();
    let launcher = hench.start().await.unwrap();
    wait_for("the job to complete", WAIT, || async {
        recorder.hook_count("complete") == 1
    })
    .await;
    launcher.stop().await.unwrap();
    assert!(recorder.items().is_empty());
    assert_eq!(recorder.hooks(), ["start", "cancel", "stop", "complete"]);
}

#[tokio::test]
async fn interrupted_jobs_keep_flowing_through_client_middleware() {
    use std::sync::atomic::AtomicUsize;

    /// Counts pushes of iterable jobs and can drop them (or panic).
    struct CountPushes {
        pushes: Arc<AtomicUsize>,
        drop_pushes: Arc<AtomicBool>,
        panic: bool,
    }

    #[async_trait]
    impl ClientMiddleware for CountPushes {
        async fn call(
            &self,
            job: JobPayload,
            _redis: &RedisPool,
            next: ClientNext<'_>,
        ) -> hench::Result<Option<JobPayload>> {
            self.pushes.fetch_add(1, Ordering::SeqCst);
            if self.drop_pushes.load(Ordering::SeqCst) {
                if self.panic {
                    panic!("middleware bug");
                }
                return Ok(None);
            }
            next.run(job).await
        }
    }

    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let recorder = Arc::new(Recorder::default());
    let pushes = Arc::new(AtomicUsize::new(0));
    let drop_pushes = Arc::new(AtomicBool::new(false));
    let hench = redis
        .builder()
        .await
        .register(Limited)
        .data_arc(recorder.clone())
        .client_middleware(CountPushes {
            pushes: pushes.clone(),
            drop_pushes: drop_pushes.clone(),
            panic: false,
        })
        .build()
        .await
        .unwrap();
    let jid = Limited::perform_async(hench.client(), ("x".to_owned(),))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pushes.load(Ordering::SeqCst), 1);
    // Middleware dropping the push back drops the job, as it would drop a new push.
    drop_pushes.store(true, Ordering::SeqCst);
    let launcher = hench.start().await.unwrap();
    wait_for("the push back", WAIT, || async {
        pushes.load(Ordering::SeqCst) >= 2
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(support::queue_size(&pool, "default").await, 0);
    let processed = recorder.items().len();
    assert!(processed > 0 && processed < 20, "{processed}");
    // Its state stays, so the job could still be pushed again by hand.
    assert!(IterationState::fetch(&pool, &jid).await.unwrap().is_some());

    // A panicking middleware rejects the push back: the job fails and is retried from its
    // cursor, and the error handlers hear about it.
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let errors = support::Errors::default();
    let pushes = Arc::new(AtomicUsize::new(0));
    let drop_pushes = Arc::new(AtomicBool::new(false));
    let hench = redis
        .builder()
        .await
        .register(Limited)
        .data_arc(Arc::new(Recorder::default()))
        .clear_error_handlers()
        .error_handler(errors.handler())
        .client_middleware(CountPushes {
            pushes: pushes.clone(),
            drop_pushes: drop_pushes.clone(),
            panic: true,
        })
        .build()
        .await
        .unwrap();
    let jid = Limited::perform_async(hench.client(), ("y".to_owned(),))
        .await
        .unwrap()
        .unwrap();
    drop_pushes.store(true, Ordering::SeqCst);
    let launcher = hench.start().await.unwrap();
    wait_for("the failed push back", WAIT, || async {
        support::zcard(&pool, keys::RETRY).await == 1
    })
    .await;
    launcher.stop().await.unwrap();
    let retries = support::sorted_jobs(&pool, keys::RETRY).await;
    assert_eq!(retries[0].0.jid, jid);
    assert_eq!(retries[0].0.error_class.as_deref(), Some(JobError::PANIC));
    let message = retries[0].0.error_message.clone().unwrap_or_default();
    assert!(
        message.contains("could not push an interrupted job back"),
        "{message}"
    );
    assert!(message.contains("middleware bug"), "{message}");
    assert_eq!(errors.with_context("job failed").len(), 1);
    assert!(IterationState::fetch(&pool, &jid).await.unwrap().is_some());
}

// --- the end of an iteration ---

/// A finishing hook that fails makes the job fail, but the iteration is over: its retry runs
/// the finishing hooks again, not the items (since the last checkpoint, or all of them).
#[tokio::test]
async fn after_a_failing_finishing_hook_only_the_finishing_hooks_run_again() {
    for redis_backed in [false, true] {
        let redis = TestRedis::start().await;
        let pool = redis.pool().await;
        let client = if redis_backed {
            redis.client().await
        } else {
            Client::fake()
        };

        let recorder = Arc::new(Recorder::default());
        *recorder.fail_hook.lock().unwrap() = Some("complete");
        let error = run_numbers(&client, &recorder, "hook-fails")
            .await
            .unwrap_err();
        assert_eq!(error.message(), "complete failed");
        assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
        if redis_backed {
            // What the retry (or another process, after a crash) finds.
            let saved = state(&pool, "hook-fails").await;
            assert_eq!(saved["done"], "completed", "{saved:?}");
            assert_eq!(saved["c"], "11");
        }
        run_numbers(&client, &recorder, "hook-fails").await.unwrap();
        assert_eq!(recorder.items(), (10..=20).collect::<Vec<_>>());
        assert_eq!(
            recorder.hooks(),
            ["start", "stop", "complete", "stop", "complete"]
        );
        if redis_backed {
            assert!(state(&pool, "hook-fails").await.is_empty());
        }

        // A cancelled job finishes as cancelled.
        let recorder = Arc::new(Recorder::default());
        *recorder.fail_hook.lock().unwrap() = Some("stop");
        client.cancel("cancelled-hook-fails").await.unwrap();
        run_numbers(&client, &recorder, "cancelled-hook-fails")
            .await
            .unwrap_err();
        run_numbers(&client, &recorder, "cancelled-hook-fails")
            .await
            .unwrap();
        assert!(recorder.items().is_empty());
        assert_eq!(
            recorder.hooks(),
            ["start", "cancel", "stop", "cancel", "stop", "complete"]
        );
    }
}

/// Pauses every client of the test Redis (`CLIENT PAUSE ... ALL`) the first time `on_complete`
/// runs: Redis stalls (a slow fork, a failover) for longer than the response timeout just
/// before the job deletes its state.
struct Stall {
    url: String,
    pause: Duration,
    done: AtomicBool,
}

/// Ten quick items; stalls Redis from `on_complete` (see [`Stall`]).
#[derive(Default)]
struct StallsAtTheEnd;

impl IterableJob for StallsAtTheEnd {
    const NAME: &'static str = "StallsAtTheEnd";
    type Args = ();
    type Item = i64;
    type Cursor = i64;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<i64>,
    ) -> Result<Enumerator<'a, i64, i64>, JobError> {
        Ok(Enumerator::range(0..10, cursor))
    }

    async fn each_iteration(&self, number: i64, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.items.lock().unwrap().push(number);
        Ok(())
    }

    async fn on_start(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("start", it);
        Ok(())
    }

    async fn on_resume(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("resume", it);
        Ok(())
    }

    async fn on_complete(&self, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?.hook("complete", it);
        let stall = it.data::<Stall>()?;
        if !stall.done.swap(true, Ordering::SeqCst) {
            // On a connection of its own.
            let pool = RedisPool::connect(&stall.url).await?;
            let mut conn = pool.conn().await?;
            hench::redis_rs::cmd("CLIENT")
                .arg("PAUSE")
                .arg(stall.pause.as_millis() as u64)
                .arg("ALL")
                .exec_async(&mut conn)
                .await
                .map_err(hench::Error::from)?;
        }
        Ok(())
    }
}

/// A pool on `redis` whose replies time out after 300 ms (3 s by default).
async fn impatient_pool(redis: &TestRedis) -> RedisPool {
    RedisPool::from_config(redis.config().response_timeout(Duration::from_millis(300)))
        .await
        .unwrap()
}

fn stall(redis: &TestRedis) -> Arc<Stall> {
    Arc::new(Stall {
        url: redis.url(),
        pause: Duration::from_millis(1_500),
        done: AtomicBool::new(false),
    })
}

/// Once the iteration is over, deleting the state is housekeeping: an execution whose deletion
/// gets no reply (which does not mean Redis did not run it) completes all the same, instead of
/// failing a finished job, whose retry would find no state and start over from the first item.
#[tokio::test]
async fn deleting_the_state_of_a_finished_iteration_cannot_fail_the_job() {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let client = Client::new(impatient_pool(&redis).await);
    let recorder = Arc::new(Recorder::default());
    let mut data = Data::new();
    data.insert_arc(recorder.clone());
    data.insert_arc(stall(&redis));
    let mut payload = JobPayload::new("StallsAtTheEnd", vec![]);
    payload.jid = "stalls".to_owned();
    let ctx = JobContext::new(payload, client).with_data(Arc::new(data));
    Job::perform(&StallsAtTheEnd, &ctx, ()).await.unwrap();
    assert_eq!(recorder.items(), (0..10).collect::<Vec<_>>());
    assert_eq!(recorder.hooks(), ["start", "complete"]);
    // Redis deletes it once it continues.
    wait_for("the state to go", WAIT, || async {
        state(&pool, "stalls").await.is_empty()
    })
    .await;
}

/// The same on a worker: the job is not failed, reported or retried, and runs once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workers_complete_iterations_whose_state_deletion_times_out() {
    support::init_logging();
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let recorder = Arc::new(Recorder::default());
    let errors = support::Errors::default();
    let hench = redis
        .builder()
        .await
        .redis(impatient_pool(&redis).await)
        .concurrency(1)
        .register(StallsAtTheEnd)
        .data_arc(recorder.clone())
        .data_arc(stall(&redis))
        .clear_error_handlers()
        .error_handler(errors.handler())
        .build()
        .await
        .unwrap();
    let jid = StallsAtTheEnd::perform_async(hench.client(), ())
        .await
        .unwrap()
        .unwrap();
    let launcher = hench.start().await.unwrap();
    wait_for("the job to be acknowledged", WAIT, || async {
        recorder.hook_count("complete") == 1
            && state(&pool, &jid).await.is_empty()
            && support::llen(&pool, &keys::inprogress(launcher.identity())).await == 0
    })
    .await;
    launcher.stop().await.unwrap();
    assert_eq!(recorder.items(), (0..10).collect::<Vec<_>>());
    assert_eq!(recorder.hooks(), ["start", "complete"]);
    // (Fetches and beats time out during the stall too; they are reported.)
    assert!(
        errors.with_context("job failed").is_empty(),
        "{:?}",
        errors.all()
    );
    let stats = hench::api::Stats::fetch(&pool).await.unwrap();
    assert_eq!((stats.processed, stats.failed), (1, 0));
    assert_eq!(stats.retry_size, 0);
    assert_eq!(support::queue_size(&pool, "default").await, 0);
}

// --- where panicking iterable jobs failed ---

/// Panics on its third item, saying on which line (its backtrace must say the same).
#[derive(Default)]
struct PanicsHere;

impl IterableJob for PanicsHere {
    const NAME: &'static str = "PanicsHere";
    type Args = ();
    type Item = i64;
    type Cursor = usize;

    fn options() -> JobOptions {
        JobOptions::new().backtrace(true)
    }

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::vec(vec![0, 1, 2, 3], cursor))
    }

    async fn each_iteration(&self, number: i64, _it: &Iteration<'_, Self>) -> Result<(), JobError> {
        tokio::task::yield_now().await;
        if number == 2 {
            panic!("panicked on line {}", line!());
        }
        Ok(())
    }
}

/// [`PanicsHere`] panicking on another line.
#[derive(Default)]
struct PanicsThere;

impl IterableJob for PanicsThere {
    const NAME: &'static str = "PanicsThere";
    type Args = ();
    type Item = i64;
    type Cursor = usize;

    fn options() -> JobOptions {
        JobOptions::new().backtrace(true)
    }

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, i64, usize>, JobError> {
        Ok(Enumerator::vec(vec![0, 1, 2, 3], cursor))
    }

    async fn each_iteration(&self, number: i64, _it: &Iteration<'_, Self>) -> Result<(), JobError> {
        tokio::task::yield_now().await;
        if number == 2 {
            panic!("panicked on line {}", line!());
        }
        Ok(())
    }
}

/// Runs `per_class` jobs of [`PanicsHere`] and [`PanicsThere`] on `concurrency` processors and
/// checks that the backtrace of each retry starts where that job panicked.
async fn check_panic_origins(concurrency: usize, per_class: usize) {
    let redis = TestRedis::start().await;
    let pool = redis.pool().await;
    let hench = redis
        .builder()
        .await
        .concurrency(concurrency)
        .register(PanicsHere)
        .register(PanicsThere)
        .build()
        .await
        .unwrap();
    for _ in 0..per_class {
        PanicsHere::perform_async(hench.client(), ()).await.unwrap();
        PanicsThere::perform_async(hench.client(), ())
            .await
            .unwrap();
    }
    let launcher = hench.start().await.unwrap();
    wait_for("every job to fail", WAIT, || async {
        support::zcard(&pool, keys::RETRY).await == 2 * per_class
    })
    .await;
    launcher.stop().await.unwrap();
    let mut wrong = Vec::new();
    for (job, _) in support::sorted_jobs(&pool, keys::RETRY).await {
        let message = job.error_message.clone().unwrap_or_default();
        let line = message.rsplit(' ').next().unwrap_or_default().to_owned();
        let lines = job
            .error_backtrace
            .as_deref()
            .map(|backtrace| hench::decompress_backtrace(backtrace).unwrap())
            .unwrap_or_default();
        let expected = format!("panicked at {}:{line}:", file!());
        if !lines
            .first()
            .is_some_and(|first| first.starts_with(&expected))
        {
            wrong.push(format!("{} ({message}): {:?}", job.class, lines.first()));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} backtraces do not start where their job panicked: {wrong:?}",
        wrong.len(),
        2 * per_class
    );
}

/// A panic in `each_iteration` is stored with where it happened, as for any job that enables
/// `backtrace`, although the iteration saves its cursor before the panic goes on: during that
/// round trip to Redis the job may move to another thread, and other jobs panic on this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_iterable_jobs_store_where_they_panicked() {
    check_panic_origins(16, 40).await;
}

/// The same on a single thread, where the jobs take turns during the save.
#[tokio::test(flavor = "current_thread")]
async fn panicking_iterable_jobs_store_where_they_panicked_on_one_thread() {
    check_panic_origins(16, 20).await;
}

/// Registered with the other `#[hench::job]` jobs.
#[derive(Default)]
struct Registered;

#[hench::job]
impl IterableJob for Registered {
    const NAME: &'static str = "Registered";
    type Args = (u32,);
    type Item = u32;
    type Cursor = u32;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        (count,): &'a (u32,),
        cursor: Option<u32>,
    ) -> Result<Enumerator<'a, u32, u32>, JobError> {
        Ok(Enumerator::range(0..*count, cursor))
    }

    async fn each_iteration(&self, number: u32, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        it.data::<Recorder>()?
            .items
            .lock()
            .unwrap()
            .push(i64::from(number));
        Ok(())
    }
}

#[tokio::test]
async fn the_job_attribute_registers_iterable_jobs() {
    let registry = hench::Registry::from_inventory().unwrap();
    assert_eq!(registry.names(), ["Registered"]);
    // Testing clients and workers find it without an explicit registration.
    let recorder = Arc::new(Recorder::default());
    let client = Testing::new().data_arc(recorder.clone()).inline();
    Registered::perform_async(&client, (4,)).await.unwrap();
    assert_eq!(recorder.items(), [0, 1, 2, 3]);
}
