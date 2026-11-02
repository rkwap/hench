//! `#[hench::job]` on `impl Job` and `impl IterableJob` blocks registers the job.

use hench::iterable::{Enumerator, IterableJob, Iteration};
use hench::{Job, JobContext, JobError, JobOptions, Registry};

/// A job implemented by hand.
#[derive(Default)]
struct ByHand;

#[hench::job]
impl Job for ByHand {
    const NAME: &'static str = "ByHand";
    type Args = (u32,);

    fn options() -> JobOptions {
        JobOptions::new().queue("manual")
    }

    async fn perform(&self, _ctx: &JobContext, (_value,): (u32,)) -> Result<(), JobError> {
        Ok(())
    }
}

/// An iterable job.
#[derive(Default)]
struct Walk;

#[hench::job(crate = "::hench")]
impl IterableJob for Walk {
    const NAME: &'static str = "Walk";
    type Args = ();
    type Item = u32;
    type Cursor = usize;

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        _args: &'a (),
        cursor: Option<usize>,
    ) -> Result<Enumerator<'a, u32, usize>, JobError> {
        Ok(Enumerator::vec(vec![1, 2, 3], cursor))
    }

    async fn each_iteration(&self, _item: u32, _it: &Iteration<'_, Self>) -> Result<(), JobError> {
        Ok(())
    }
}

/// Compiled out together with its registration.
#[derive(Default)]
#[cfg(any())]
struct Gone;

#[cfg(any())]
#[hench::job]
impl Job for Gone {
    const NAME: &'static str = "Gone";
    type Args = ();

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {
    let registry = Registry::from_inventory().unwrap();
    assert_eq!(registry.names(), ["ByHand", "Walk"]);
    assert_eq!(<ByHand as Job>::options().queue.as_deref(), Some("manual"));
}
