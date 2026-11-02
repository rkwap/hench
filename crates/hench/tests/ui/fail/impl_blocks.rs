use hench::{Job, JobContext, JobError};

#[derive(Default)]
struct WithOptions;

#[hench::job(queue = "nope")]
impl Job for WithOptions {
    const NAME: &'static str = "WithOptions";
    type Args = ();

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        Ok(())
    }
}

struct Inherent;

#[hench::job]
impl Inherent {
    fn helper(&self) {}
}

#[derive(Default)]
struct OtherTrait;

#[hench::job]
impl Clone for OtherTrait {
    fn clone(&self) -> Self {
        OtherTrait
    }
}

#[derive(Default)]
struct Generic<T>(T);

#[hench::job]
impl<T: Send + Sync + Default + 'static> Job for Generic<T> {
    const NAME: &'static str = "Generic";
    type Args = ();

    async fn perform(&self, _ctx: &JobContext, (): ()) -> Result<(), JobError> {
        Ok(())
    }
}

fn main() {}
