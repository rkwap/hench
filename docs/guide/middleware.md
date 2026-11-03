# Middleware

Middleware wraps pushes (client) and executions (server). Not calling `next` stops the job:
a client middleware returning `Ok(None)` cancels the push, a server middleware returning
early marks the job done.

```rust,no_run
use std::time::Instant;

use hench::prelude::*;

/// Logs how long every job takes.
struct Timing;

#[async_trait]
impl ServerMiddleware for Timing {
    async fn call(
        &self,
        ctx: &JobContext,
        job: &mut JobPayload,
        next: ServerNext<'_>,
    ) -> Result<(), JobError> {
        let started = Instant::now();
        let result = next.run(ctx, job).await;
        println!("{} took {:?}", job.class, started.elapsed());
        result
    }
}

/// Stamps every job with the tenant that enqueued it.
struct Tenant(&'static str);

#[async_trait]
impl ClientMiddleware for Tenant {
    async fn call(
        &self,
        mut job: JobPayload,
        _redis: &RedisPool,
        next: ClientNext<'_>,
    ) -> hench::Result<Option<JobPayload>> {
        job.extra.insert("tenant".to_owned(), self.0.into());
        next.run(job).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hench = Hench::builder()
        .server_middleware(Timing)
        .client_middleware(Tenant("acme"))
        .build()
        .await?;
    hench.run().await?;
    Ok(())
}
```

Chains hold one middleware per type and support `add`, `prepend`, `insert_before`,
`insert_after`, `remove` and `exists` (`configure_server_middleware(|chain| ..)`).

---

[← Retries and delivery guarantees](reliability.md) · [Guide](README.md) · [Testing →](testing.md)
