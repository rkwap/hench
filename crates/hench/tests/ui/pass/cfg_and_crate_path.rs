//! `cfg` attributes remove every generated item; `crate = "..."` handles a renamed crate.

extern crate hench as renamed;

use renamed::Job;

/// Compiled out together with its implementation and registration.
#[cfg(any())]
#[renamed::job(crate = "::renamed")]
async fn compiled_out() {}

/// Kept.
#[cfg(all())]
#[renamed::job(crate = "::renamed", queue = "kept")]
async fn kept(value: u8) {
    let _ = value;
}

fn main() {
    assert_eq!(<kept as Job>::NAME, "kept");
    assert_eq!(kept::options().queue.as_deref(), Some("kept"));
}
