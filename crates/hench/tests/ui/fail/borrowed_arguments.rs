#![allow(unused)]

use std::borrow::Cow;

#[hench::job]
async fn anonymous_lifetime(text: Cow<'_, str>) {}

#[hench::job]
async fn elided_lifetime(text: Cow<str>) {}

#[hench::job]
async fn nested(texts: Vec<Option<Cow<str>>>) {}

#[hench::job]
async fn static_context(ctx: &'static hench::JobContext, id: u64) {}

type Ctx = hench::JobContext;

#[hench::job]
async fn aliased_context(ctx: &Ctx, id: u64) {}

#[hench::job]
async fn opaque_return() -> impl Sized {}

#[hench::job]
async fn opaque_error(fail: bool) -> Result<(), impl std::error::Error + Send + Sync + 'static> {
    if fail { Err(std::io::Error::other("no")) } else { Ok(()) }
}

/// `'static` borrows nothing: fine.
#[hench::job]
async fn static_cow(text: Cow<'static, str>) {}

fn main() {}
