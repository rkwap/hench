//! Property tests: job payloads, typed arguments and the backtrace codec survive round trips.

use hench::{
    BacktraceOption, JobPayload, RetryOption, args_from_values, args_to_values, compress_backtrace,
    decompress_backtrace,
};
use proptest::prelude::*;
use serde_json::{Map, Value, json};

fn finite_f64() -> impl Strategy<Value = f64> {
    any::<f64>().prop_filter("finite", |value| value.is_finite())
}

/// JSON scalars, including arbitrary finite floats (hench enables serde_json's
/// `float_roundtrip`, so they parse back exactly).
fn json_leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        any::<u64>().prop_map(|n| json!(n)),
        finite_f64().prop_map(|n| json!(n)),
        ".{0,12}".prop_map(Value::String),
    ]
}

fn json_value() -> impl Strategy<Value = Value> {
    json_leaf().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map("[a-z]{0,6}", inner, 0..4)
                .prop_map(|map| Value::Object(map.into_iter().collect::<Map<_, _>>())),
        ]
    })
}

fn retry_option() -> impl Strategy<Value = RetryOption> {
    prop_oneof![
        any::<bool>().prop_map(RetryOption::Bool),
        any::<u32>().prop_map(RetryOption::Count),
    ]
}

fn backtrace_option() -> impl Strategy<Value = BacktraceOption> {
    prop_oneof![
        any::<bool>().prop_map(BacktraceOption::Bool),
        any::<u32>().prop_map(BacktraceOption::Lines),
    ]
}

fn extra_map() -> impl Strategy<Value = Map<String, Value>> {
    prop::collection::btree_map(
        "[a-z_]{1,12}".prop_filter("reserved key", |key| !JobPayload::is_reserved_key(key)),
        json_value(),
        0..4,
    )
    .prop_map(|map| map.into_iter().collect())
}

prop_compose! {
    fn identity()(
        class in "[A-Za-z][A-Za-z0-9_:]{0,20}",
        args in prop::collection::vec(json_value(), 0..5),
        jid in prop_oneof![Just(String::new()), "[0-9a-f]{24}"],
        queue in "[a-z_]{0,10}",
    ) -> (String, Vec<Value>, String, String) {
        (class, args, jid, queue)
    }
}

prop_compose! {
    fn options()(
        retry in proptest::option::of(retry_option()),
        backtrace in proptest::option::of(backtrace_option()),
        dead in proptest::option::of(any::<bool>()),
        tags in prop::collection::vec(".{0,8}", 0..3),
        retry_for in proptest::option::of(any::<u64>()),
        retry_queue in proptest::option::of("[a-z]{1,8}"),
        bid in proptest::option::of("[a-z0-9]{1,8}"),
    ) -> (Option<RetryOption>, Option<BacktraceOption>, Option<bool>, Vec<String>, Option<u64>, Option<String>, Option<String>) {
        (retry, backtrace, dead, tags, retry_for, retry_queue, bid)
    }
}

prop_compose! {
    fn timestamps()(
        created_at in proptest::option::of(any::<i64>()),
        enqueued_at in proptest::option::of(any::<i64>()),
        at in proptest::option::of(finite_f64()),
        retry_count in proptest::option::of(any::<u32>()),
        failed_at in proptest::option::of(any::<i64>()),
        retried_at in proptest::option::of(any::<i64>()),
        discarded_at in proptest::option::of(any::<i64>()),
    ) -> (Option<i64>, Option<i64>, Option<f64>, Option<u32>, Option<i64>, Option<i64>, Option<i64>) {
        (created_at, enqueued_at, at, retry_count, failed_at, retried_at, discarded_at)
    }
}

prop_compose! {
    fn payload()(
        (class, args, jid, queue) in identity(),
        (retry, backtrace, dead, tags, retry_for, retry_queue, bid) in options(),
        (created_at, enqueued_at, at, retry_count, failed_at, retried_at, discarded_at) in timestamps(),
        error_message in proptest::option::of(".{0,40}"),
        error_class in proptest::option::of("[A-Za-z:]{1,20}"),
        error_backtrace in proptest::option::of("[A-Za-z0-9+/=]{0,20}"),
        extra in extra_map(),
    ) -> JobPayload {
        // `JobPayload` is non-exhaustive: every field is set one by one.
        let mut payload = JobPayload::new(class, args);
        payload.jid = jid;
        payload.queue = queue;
        payload.retry = retry;
        payload.backtrace = backtrace;
        payload.dead = dead;
        payload.tags = tags;
        payload.retry_for = retry_for;
        payload.retry_queue = retry_queue;
        payload.created_at = created_at;
        payload.enqueued_at = enqueued_at;
        payload.at = at;
        payload.retry_count = retry_count;
        payload.failed_at = failed_at;
        payload.retried_at = retried_at;
        payload.discarded_at = discarded_at;
        payload.error_message = error_message;
        payload.error_class = error_class;
        payload.error_backtrace = error_backtrace;
        payload.bid = bid;
        payload.extra = extra;
        payload
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn payloads_round_trip_through_json(payload in payload()) {
        let json = payload.to_json().unwrap();
        let parsed = JobPayload::from_json(&json).unwrap();
        prop_assert_eq!(&parsed, &payload);
        // And the serialization is stable.
        prop_assert_eq!(parsed.to_json().unwrap(), json);
    }

    #[test]
    fn payloads_round_trip_through_values(payload in payload()) {
        let value = payload.to_value().unwrap();
        let parsed: JobPayload = serde_json::from_value(value).unwrap();
        prop_assert_eq!(parsed, payload);
    }

    #[test]
    fn typed_arguments_round_trip(
        args in (any::<i64>(), ".{0,10}", any::<bool>(), prop::collection::vec(any::<u32>(), 0..4), proptest::option::of(any::<i64>()))
    ) {
        let values = args_to_values("Job", &args).unwrap();
        prop_assert_eq!(values.len(), 5);
        let back: (i64, String, bool, Vec<u32>, Option<i64>) = args_from_values(values).unwrap();
        prop_assert_eq!(back, args);
    }

    #[test]
    fn float_arguments_survive_the_trip_through_redis(values in prop::collection::vec(finite_f64(), 1..4)) {
        // Exactly what a push and a fetch do: typed args -> payload JSON text -> typed args.
        let job = JobPayload::new("FloatJob", args_to_values("FloatJob", &values).unwrap());
        let fetched = JobPayload::from_json(&job.to_json().unwrap()).unwrap();
        let back: Vec<f64> = args_from_values(fetched.args).unwrap();
        let bits = |floats: &[f64]| floats.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
        prop_assert_eq!(bits(&back), bits(&values));
    }

    #[test]
    fn backtraces_round_trip(lines in prop::collection::vec(".{0,60}", 0..20)) {
        let encoded = compress_backtrace(&lines);
        prop_assert!(!encoded.contains('\n'));
        prop_assert_eq!(decompress_backtrace(&encoded).unwrap(), lines);
    }
}
