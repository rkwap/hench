//! Compile tests for `#[hench::job]` (trybuild): the signatures and options it accepts, and
//! the error messages for the ones it rejects. Runtime behaviour is tested in `job.rs`.
//!
//! * `ui/pass`: must compile (without warnings where the file denies them).
//! * `ui/fail`: errors produced by the macro itself, whose text is stable.
//! * `ui/typeck`: type errors reported by the compiler at the user's code. Their help text
//!   changes between compiler versions, so they only run on the version recorded in
//!   `ui/typeck/rustc-version.txt`. It also lists some of the types that do implement a
//!   missing trait, which depends on the features enabled across the dependency graph:
//!   trybuild builds these files with hench's dev-dependencies too (for example `axum` turns
//!   on `serde_json`'s `raw_value`, adding `RawValue` to the `Deserialize` list).
//!
//! After changing a message, regenerate the expected output with
//! `TRYBUILD=overwrite cargo test -p hench --test macros`, review the diff, and update
//! `rustc-version.txt` (`rustc -V | cut -d' ' -f2`) if the `typeck` files changed.
//!
//! CI runs this file in a job pinned to the recorded version, with
//! `HENCH_REQUIRE_TYPECK=1`: there a version mismatch fails instead of skipping the `typeck`
//! cases, so moving that job to a new compiler forces re-recording them (and the version file).

use std::process::Command;

/// When set, a compiler other than the recorded one is an error, not a reason to skip.
const REQUIRE_TYPECK: &str = "HENCH_REQUIRE_TYPECK";

#[test]
fn ui() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass/*.rs");
    tests.compile_fail("tests/ui/fail/*.rs");
    if typeck_output_is_comparable() {
        tests.compile_fail("tests/ui/typeck/*.rs");
    }
}

/// `true` if the compiler is the one that produced the `ui/typeck` expectations.
fn typeck_output_is_comparable() -> bool {
    let recorded = include_str!("ui/typeck/rustc-version.txt").trim();
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let current = Command::new(rustc)
        .arg("-V")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|version| version.split_whitespace().nth(1).map(str::to_owned));
    let comparable = current.as_deref() == Some(recorded);
    if !comparable && std::env::var_os(REQUIRE_TYPECK).is_some() {
        panic!(
            "{REQUIRE_TYPECK} is set but ui/typeck was recorded with rustc {recorded}, this is \
             {}: re-record it (see this file's docs) or run the recorded compiler",
            current.as_deref().unwrap_or("an unknown version")
        );
    }
    if !comparable {
        eprintln!(
            "skipping ui/typeck: expectations were recorded with rustc {recorded}, this is {}",
            current.as_deref().unwrap_or("an unknown version")
        );
    }
    comparable
}
