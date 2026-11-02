//! Crates that forbid `dead_code` or `deprecated` can define jobs: a forbidden lint cannot be
//! allowed, so the generated code must not need to allow them (even for a job nobody
//! enqueues, whose generated helpers stay unused).
#![deny(warnings)]
#![forbid(dead_code, deprecated)]

/// A private job nobody enqueues.
#[hench::job]
async fn never_enqueued(id: u64) {
    let _ = id;
}

/// A public job with options and a context.
#[hench::job(queue = "reports", retry = 3)]
pub async fn public_job(ctx: &hench::JobContext, name: String) -> Result<(), hench::JobError> {
    let _ = (ctx, name);
    Ok(())
}

mod inner {
    /// A crate-visible job in a module.
    #[hench::job]
    pub(crate) async fn crate_job() {}
}

fn main() {}
