//! The limit spec grammar exists twice: in `hench::limit` (`Limit: FromStr + Display`) and in
//! `hench-macros`, which parses `#[hench::job(throttle(..))]` at compile time and cannot depend
//! on `hench`. This test feeds both one corpus: they must accept the same specs and print them
//! the same way (`docs/design/pro-features.md` §2.4).

#[allow(dead_code, unreachable_pub)]
#[path = "../../hench-macros/src/spec.rs"]
mod macro_spec;

use hench::limit::Limit;

/// Specs both parsers must agree on: valid ones in every form, and mistakes.
const CORPUS: &[&str] = &[
    "concurrency=10",
    "c=1",
    " c=25 ",
    "window=100/1s",
    "w=100/1000ms",
    "w=5/0.5",
    "w=90/90",
    "window=3/1h30m",
    "window=7/1d",
    "window=7/1d2h3m4s5ms",
    "bucket=10000/day",
    "b=3/second",
    "b=3/minute",
    "b=3/hour",
    "b=3/1500ms",
    "leaky=60/1m",
    "l=1/1",
    "points=1000/2h",
    "p=5/30s",
    "window=5/1s per key",
    "c=3 per key",
    "bucket=10/day per key",
    "c=4294967295",
    "",
    "c",
    "c=",
    "c=0",
    "c=-1",
    "c=1.5",
    "c=4294967296",
    "c=5/1s",
    "w=5",
    "w=5/",
    "w=5/0s",
    "w=5/0",
    "w=5/1x",
    "w=5/s",
    "w=5/0.0001",
    "w=5/1sper key",
    "b=5/week",
    "q=5/1s",
    "window5/1s",
    "per key",
];

/// The macro's verdict on a spec, in the library's terms: the display of the limit, with
/// ` per key` when it applies per key.
fn from_macro(text: &str) -> Option<String> {
    macro_spec::parse(text).ok().map(|spec| spec.to_string())
}

/// The library's verdict: `Limit` refuses `per key` (a throttle's scope), so it is split off as
/// the library's throttle parser does.
fn from_library(text: &str) -> Option<String> {
    let trimmed = text.trim();
    let (body, suffix) = match trimmed.strip_suffix("per key") {
        Some(body) if body.ends_with(char::is_whitespace) => (body.trim_end(), " per key"),
        _ => (trimmed, ""),
    };
    body.parse::<Limit>()
        .ok()
        .map(|limit| format!("{limit}{suffix}"))
}

#[test]
fn both_parsers_agree_on_the_corpus() {
    for text in CORPUS {
        assert_eq!(from_macro(text), from_library(text), "spec `{text}`");
    }
}

#[test]
fn printed_specs_parse_back_to_themselves() {
    for text in CORPUS {
        if let Some(printed) = from_library(text) {
            assert_eq!(from_library(&printed).as_deref(), Some(printed.as_str()));
            assert_eq!(from_macro(&printed).as_deref(), Some(printed.as_str()));
        }
    }
}
