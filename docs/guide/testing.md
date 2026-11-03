# Testing

Testing clients run jobs without Redis. `Client::fake()` captures pushed jobs; `drain`
then runs them through the same execution path as the workers (middleware, the job span,
argument decoding):

```rust
use hench::prelude::*;

#[hench::job(queue = "mailers")]
async fn password_reset(email: String) -> Result<(), std::io::Error> {
    if email.contains('@') {
        Ok(())
    } else {
        Err(std::io::Error::other("not an email address"))
    }
}

#[tokio::main]
async fn main() -> hench::Result<()> {
    let client = Client::fake();
    password_reset::perform_async(&client, ("ada@example.com".to_owned(),)).await?;

    // What was enqueued, as stored.
    let jobs = client.jobs::<password_reset>();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].queue, "mailers");

    // Run them (jobs they enqueue are captured too).
    assert_eq!(client.drain::<password_reset>().await?, 1);
    assert!(client.jobs::<password_reset>().is_empty());

    // Or run one directly, errors included.
    assert!(password_reset::perform_inline(("nobody".to_owned(),)).await.is_err());
    Ok(())
}
```

- `hench::testing::Testing` builds testing clients with your registrations, shared data and
  middleware, in fake or inline mode (`Client::inline` runs each job as it is pushed).
  `Testing::from_builder(configure()).await?` takes them from the builder your program uses,
  without connecting to Redis (`Testing::from_hench(&hench)` copies a built `Hench`, but
  building one connects).
- With the `test-util` feature, `hench::test_util::TestRedis::start().await` runs a private
  `redis-server` for one test (on a Unix socket, killed when dropped), with `pool()`,
  `client()` and `builder()` (a `HenchBuilder` with short intervals) for end-to-end tests.
- The pro features in testing clients: what is decided at push time is enforced, what depends
  on time is not. Unique locks are held and released as workers would (a duplicate push
  returns `None`; `Testing::unique_jobs(false)` turns them off); a batch's commit captures its
  members, a drain counts their outcomes and then runs the callbacks the batch fired (an inline
  client runs the members at the commit). Limiters and throttles grant everything unless
  `Testing::limits(TestLimits::InMemory)` enforces them on tokio's clock (a refused job stays
  captured until its slot, with its `limited` field; `Client::limited_jobs` lists them).
  Schedules have no poller: `Periodic::payload(tick)` gives the job a tick would push, and
  `upcoming` the next ticks.

---

[← Middleware](middleware.md) · [Guide](README.md) · [Unique jobs →](unique-jobs.md)
