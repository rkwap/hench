# Metrics

Workers count every execution per job class (executions, failures, milliseconds and a
26-bucket histogram of durations) and add the counts to Redis on every heartbeat: minute
buckets kept 8 hours and ten-minute buckets kept 3 days. `.metrics(false)` turns it off.

```rust,no_run
use hench::metrics::{Deploy, Period, Query, SortBy};
use hench::RedisPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis = RedisPool::connect("redis://127.0.0.1:6379").await?;

    // Mark deploys: the dashboard's charts show them.
    let label = Deploy::git_label().unwrap_or_else(|| "manual deploy".to_owned());
    Deploy::new(&redis).mark(&label, std::time::SystemTime::now()).await?;

    // The busiest job classes of the last 4 hours.
    let result = Query::new(&redis).top_jobs(Period::Minutes(240), None).await?;
    for (class, metrics) in result.sorted(SortBy::TotalTime).into_iter().take(5) {
        println!(
            "{class}: {} runs, {} failed, {:.1} s, {:.1} ms on average",
            metrics.totals.processed,
            metrics.totals.failed,
            metrics.totals.seconds(),
            metrics.totals.average_ms()
        );
    }

    // One class, with its execution-time histogram.
    let one = Query::new(&redis).for_job("ImportFeed", Period::Minutes(60)).await?;
    println!("{:?}", one.jobs["ImportFeed"].histogram());
    Ok(())
}
```

The dashboard's Metrics pages chart the same data (execution time per class over 1 hour to 3
days, and per class the executions, the distribution of durations and a heat map of
durations per minute, with deploy marks), and `hench stats metrics` prints the busiest
classes of the last hour.

---

[← Iterable jobs](iterable-jobs.md) · [Guide](README.md) · [Using the features together →](combining-features.md)
