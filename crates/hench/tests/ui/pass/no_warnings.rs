//! Generated code is documented and warning free, even for jobs that are never enqueued.
#![deny(warnings, missing_docs, unused, unreachable_pub)]

//! A crate with jobs.

/// A public job.
#[hench::job]
pub async fn public_job(id: u64) -> Result<(), hench::JobError> {
    let _ = id;
    Ok(())
}

/// A private job nobody enqueues.
#[hench::job(queue = "private")]
async fn private_job(ctx: &hench::JobContext) {
    let _ = ctx;
}

/// A job being phased out: using it warns, defining it does not.
#[deprecated(note = "use public_job")]
#[hench::job]
pub async fn old_job() {}

/// A job called `args`, like the generated `perform`'s arguments.
#[hench::job]
pub async fn args(value: u8) {
    let _ = value;
}

/// Named like the parameters of the generated helpers.
#[hench::job]
pub async fn client() {}

/// Named like the parameters of the generated helpers.
#[hench::job]
pub async fn delay() {}

/// Named like the parameters of the generated helpers.
#[hench::job]
pub async fn at() {}

#[expect(missing_docs, reason = "lint expectations apply to the generated items")]
#[hench::job]
pub async fn undocumented() {}

mod inner {
    /// A crate-visible job.
    #[hench::job]
    pub(crate) async fn crate_job() {}
}

fn main() {}
