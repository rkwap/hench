# Batches and workflows

A **batch** groups jobs and runs callback jobs when they are done (`hench::batch`): `complete`
once every job ran at least once, `success` once every job succeeded, `death` at the first job
that will not run again (the morgue, retries disabled, a discard, a delete or kill through the
API). Each job is counted once, however often it runs or is reported, and each event fires
once, even with several processes, lost replies, crashes and Redis failovers. A batch is
defined through a collecting client: every push is buffered, and the commit writes the batch,
its callbacks and every job in one atomic step (nothing is written when the definition fails).

```rust,no_run
use hench::prelude::*;

#[hench::job(queue = "imports")]
async fn import_row(import_id: i64, row: u32) -> Result<(), JobError> {
    println!("importing row {row} of import {import_id}");
    Ok(())
}

#[hench::job(queue = "critical")]
async fn import_finished(ctx: &JobContext, import_id: i64) -> Result<(), JobError> {
    let callback = ctx
        .batch_callback()
        .ok_or_else(|| JobError::msg("not a callback"))?;
    if let Some(status) = callback.status(ctx).await? {
        println!(
            "import {import_id}: {} of {} rows failed",
            status.failures, status.total
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `#[hench::job]` functions register themselves: no `register` call.
    let hench = Hench::builder()
        .queues(["imports", "critical"])
        .build()
        .await?;
    let committed = Batch::new(hench.client())
        .description("Import #7")
        .on_complete::<import_finished>((7,))
        .jobs(|jobs| async move {
            for row in 0..1_000 {
                import_row::perform_async(&jobs, (7, row)).await?;
            }
            Ok::<_, hench::Error>(())
        })
        .await?;
    println!("batch {} has {} jobs", committed.bid, committed.jids.len());
    hench.run().await?;
    Ok(())
}
```

- **Nesting**: a batch defined with the collecting client of another (`Batch::new(&jobs)`) is
  its child, committed with it; the child's callbacks are jobs of the parent, so the parent's
  `complete` and `success` wait for them, and a child's death is its parent's.
- **Adding jobs later**: `ctx.batch()` reopens a job's own batch (a job of a child batch's
  callback reopens the parent), `Batch::open(&client, bid)` any batch that has not succeeded or
  been cancelled. Give each loader its own `.commit_id(..)`: a loader that runs twice adds its
  jobs once. `Batch::with_bid(&client, ctx.jid())` with the same commit id creates a batch once
  from a job that may run twice: its commit ids stay recorded for `batch_ttl` (30 days) after
  the batch finished, so a re-run days later (a late retry, a job revived from the morgue)
  still writes nothing. One commit holds at most `batch_max_commit_jobs` jobs (10 000; at most
  100 000), each child batch counting as 10; larger batches use loader jobs (staged commits
  are planned for 0.2: see the [roadmap](../ROADMAP.md)). A commit is one
  Redis script, which every other client waits for: about 4 µs per job, so some 40 ms for
  10 000 jobs and 0.4 s for 100 000 (raise the limit only with response timeouts well above
  that).
- **Cancelling**: `client.cancel_batch(bid)` (or the dashboard) cancels a batch and its
  children: jobs not run yet are skipped (counted as processed), `success` never fires,
  `complete` fires once the jobs left have run; a long job can stop early with
  `ctx.check_batch().await?` (an iterable job checks at its checkpoints from 0.2: see the
  [roadmap](../ROADMAP.md)). A cancelled batch keeps its data until its last job left is
  skipped (a job waiting for a retry, or revived from the morgue, is skipped when it comes
  up), then lingers.
- **Workflows**: `hench::batch::Workflow` chains stages of parallel jobs, each stage starting
  once the previous one succeeded, with the batch callbacks of the whole (`on_complete` once
  every job of every stage ran, `on_success` once the last stage succeeded, `on_death` at the
  first job that will not run again, which stops the workflow at its stage). Between stages an
  internal job, `hench::WorkflowNext` (known to every worker, nothing to register), starts the
  next stage: it runs in the queue of the first job of the stage that just succeeded, or in
  the workflow's `callback_queue`. Each stage starts with one batch commit, so `start` refuses
  a stage whose jobs plus its batch (10) exceed `batch_max_commit_jobs`
  (`BatchError::StageTooLarge`; a loader job can push the rest of a large stage). A job that may
  run twice starts its workflow once with
  `Workflow::with_bid(ctx.client(), ctx.jid())?.commit_id(ctx.jid())`:

```rust,no_run
use hench::batch::Workflow;
use hench::prelude::*;

#[hench::job]
async fn reserve_stock(order: i64) -> Result<(), JobError> {
    println!("reserving the stock of order {order}");
    Ok(())
}

#[hench::job]
async fn charge_card(order: i64) -> Result<(), JobError> {
    println!("charging order {order}");
    Ok(())
}

#[hench::job]
async fn print_label(order: i64) -> Result<(), JobError> {
    println!("printing the label of order {order}");
    Ok(())
}

#[hench::job]
async fn order_shipped(order: i64) -> Result<(), JobError> {
    println!("order {order} shipped");
    Ok(())
}

async fn fulfil(client: &Client, order: i64) -> Result<(), hench::Error> {
    Workflow::new(client)
        .description(format!("Fulfil order {order}"))
        .stage(|s| s.job::<reserve_stock>((order,)))
        .stage(|s| s.job::<charge_card>((order,)).job::<print_label>((order,)))
        .on_success::<order_shipped>((order,))
        .start()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder().build().await?;
    fulfil(hench.client(), 42).await?;
    hench.run().await?;
    Ok(())
}
```

- **Expiry**: a batch's data expires 30 days after its last activity (`batch_ttl`; activity
  extends it once a quarter of it, an hour at most, went by) and 24 hours after it finished
  (`batch_linger`); a parent lives at least as long as each of its children.
- **Tests**: fake clients capture a batch's jobs and its callbacks (`drain_all` runs them in
  order); inline clients run the jobs at the commit and the callbacks after them, and a
  workflow's stages one after the other, however many there are.
- **Rollout**: workers count a batch's jobs only if they understand batches; upgrade every
  worker before defining batches.
- **Operations**: the dashboard's **Batches** tab lists the batches by state with their
  progress, and shows each batch's events, callbacks, failures (linked to their retries or
  morgue entries), children and pending jobs, with Cancel, Delete, Skip and Push callbacks
  now; `hench batches` (`list`, `show`, `cancel --yes`, `delete --yes`, `skip`, `dispatch`),
  `hench stats batches` and the JSON API (`GET <prefix>/api/v1/batches`, `GET
  <prefix>/api/v1/batches/{bid}`, `POST <prefix>/api/v1/batches/{bid}/cancel`; `hench web
  --api`) do the same from scripts; `hench::api::Api::batches()` from code.

---

[← Periodic jobs](periodic-jobs.md) · [Guide](README.md) · [Rate limiting and throttling →](rate-limiting.md)
