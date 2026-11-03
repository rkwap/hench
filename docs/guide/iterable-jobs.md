# Iterable jobs

An iterable job works through a long sequence (rows, pages, files) one item at a time. It
saves its cursor regularly, so it resumes where it stopped after a shutdown, a failure or
when it used up its run time and went back to its queue to let other jobs run:

```rust,no_run
use std::time::Duration;

use hench::iterable::{Enumerator, IterableJob, Iteration};
use hench::prelude::*;

/// Re-indexes every product, a page of ids at a time.
#[derive(Default)]
struct ReindexProducts;

impl IterableJob for ReindexProducts {
    const NAME: &'static str = "ReindexProducts";
    type Args = (u64,); // how many products
    type Item = Vec<u64>; // a page of product ids
    type Cursor = u64; // the last product id handled

    fn build_enumerator<'a>(
        &'a self,
        _ctx: &'a JobContext,
        (count,): &'a (u64,),
        cursor: Option<u64>,
    ) -> Result<Enumerator<'a, Vec<u64>, u64>, JobError> {
        let count = *count;
        // Keyset pagination: each page starts after the cursor (a database query in practice).
        Ok(Enumerator::batches(cursor, move |after| async move {
            let first = after.map_or(1, |id| id + 1);
            Ok((first..=count).take(100).map(|id| (id, id)).collect())
        }))
    }

    async fn each_iteration(&self, page: Vec<u64>, it: &Iteration<'_, Self>) -> Result<(), JobError> {
        println!("execution {}: page from {:?}", it.executions(), page.first());
        Ok(())
    }

    /// Give other jobs a turn every 30 seconds.
    fn max_iteration_runtime(&self) -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder().register(ReindexProducts).build().await?;
    let jid = ReindexProducts::perform_async(hench.client(), (1_000_000,)).await?;
    if let Some(jid) = jid {
        // Changed our mind: the job stops at its next checkpoint.
        hench.client().cancel(&jid).await?;
    }
    hench.run().await?;
    Ok(())
}
```

Items run at least once (a crash resumes from the last saved cursor), so make
`each_iteration` idempotent. Hooks (`on_start`, `on_resume`, `on_stop`, `on_cancel`,
`on_complete`) and `Iteration::abort` complete the picture; the dashboard shows each running
or retrying iterable job's cursor and executions.

---

[← Rate limiting and throttling](rate-limiting.md) · [Guide](README.md) · [Metrics →](metrics.md)
