//! A worker's memory: what hench itself allocates at rest, per job and at the peak of a burst.
//!
//! Two steps, in two processes, so that the pushes do not count in the worker's memory:
//!
//! ```text
//! cargo run --release --example memory -- push redis://127.0.0.1:6379/ [jobs] [arg bytes]
//! cargo run --release --example memory -- work redis://127.0.0.1:6379/ [jobs] [concurrency] [job kB]
//! ```
//!
//! With `job kB`, each job also allocates and fills that much memory, holds it for two
//! milliseconds and frees it, like a job that builds a document or loads rows: the heap in use
//! still goes back to its idle value, and what the resident memory does afterwards is the
//! allocator's doing.
//!
//! `work` starts one worker process (reliable fetch, the queue `default`), lets it idle, runs
//! the jobs pushed before, lets it idle again and prints, one `name value` per line:
//!
//! * `idle_heap_kb`, `idle_rss_kb`: the heap in use and the resident memory of the started,
//!   idle worker;
//! * `peak_heap_kb`: the most heap in use at once while the jobs ran;
//! * `after_heap_kb`, `after_rss_kb`: the same as at rest, once the jobs are done. The heap in
//!   use going back to its idle value shows that hench keeps nothing per job; the resident
//!   memory staying up is the allocator keeping freed memory (see the guide's deployment page);
//! * `allocations_per_job`, `allocated_bytes_per_job`: what running one no-op job costs.
//!
//! The heap figures come from a counting allocator around the system one, so they do not depend
//! on the allocator's own bookkeeping; the resident memory does (`MALLOC_ARENA_MAX`, or
//! `LD_PRELOAD` of another allocator, change it). `ci/memory.sh` runs both steps against a
//! fresh `redis-server`. Point it at a Redis of its own.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use hench::{BulkPush, Hench, Job, JobContext, JobError, RedisConfig};
use serde_json::json;

/// The system allocator, counting what is in use.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);

impl Counting {
    fn grew(size: usize) {
        let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        PEAK.fetch_max(live, Ordering::Relaxed);
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED.fetch_add(size as u64, Ordering::Relaxed);
    }
}

// SAFETY: every call is forwarded unchanged to the system allocator; the counters do not
// allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, forwarded.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            Self::grew(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: the caller's contract, forwarded.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, forwarded.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            Self::grew(layout.size());
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract, forwarded.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            Self::grew(new_size);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Jobs run so far.
static DONE: AtomicU64 = AtomicU64::new(0);

/// Jobs pushed per bulk push.
const CHUNK: u64 = 1_000;

/// What each job allocates and frees (bytes; 0: nothing).
static JOB_BYTES: AtomicUsize = AtomicUsize::new(0);

/// A job that does nothing but count itself (and, if asked, use some memory for a moment).
struct Noop;

impl Job for Noop {
    const NAME: &'static str = "Noop";
    type Args = (u64, String);

    async fn perform(&self, _ctx: &JobContext, _args: (u64, String)) -> Result<(), JobError> {
        let bytes = JOB_BYTES.load(Ordering::Relaxed);
        if bytes > 0 {
            // In pieces, as real data comes: rows, strings, buffers.
            let pieces: Vec<Vec<u8>> = (0..bytes / 4096).map(|_| vec![1_u8; 4096]).collect();
            tokio::time::sleep(Duration::from_millis(2)).await;
            drop(pieces);
        }
        DONE.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// The positional argument at `index`, or `default`.
fn argument<T: std::str::FromStr>(
    args: &[String],
    index: usize,
    default: T,
) -> Result<T, Box<dyn std::error::Error>>
where
    T::Err: std::error::Error + 'static,
{
    match args.get(index) {
        Some(value) => Ok(value.parse()?),
        None => Ok(default),
    }
}

/// The resident memory of this process, in kB (0 where `/proc` does not say).
fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
        })
        .unwrap_or(0)
}

fn heap_kb() -> usize {
    LIVE.load(Ordering::Relaxed) / 1024
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const USAGE: &str = "usage: memory push <redis url> [jobs] [arg bytes]\n       \
                         memory work <redis url> [jobs] [concurrency] [job kB]";
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(step), Some(url)) = (args.first(), args.get(1)) else {
        return Err(USAGE.into());
    };
    let jobs: u64 = argument(&args, 2, 100_000)?;
    let builder = Hench::builder()
        .redis_config(RedisConfig::new(url.clone()))
        .auto_register(false)
        .register(Noop)
        .queues(["default"]);
    match step.as_str() {
        "push" => {
            let bytes: usize = argument(&args, 3, 100)?;
            let filler = "x".repeat(bytes);
            let hench = builder.build().await?;
            let mut next = 0;
            while next < jobs {
                let end = (next + CHUNK).min(jobs);
                let chunk = (next..end).map(|n| vec![json!(n), json!(filler)]).collect();
                hench
                    .client()
                    .push_bulk(BulkPush::new(Noop::NAME, chunk))
                    .await?;
                next = end;
            }
        }
        "work" => {
            let concurrency: usize = argument(&args, 3, 25)?;
            let job_kb: usize = argument(&args, 4, 0)?;
            JOB_BYTES.store(job_kb * 1024, Ordering::Relaxed);
            let hench = builder.concurrency(concurrency).build().await?;
            // The queue is held back while the worker settles.
            let api = hench::api::Api::new(hench.redis().clone());
            api.queue("default").pause().await?;
            let launcher = hench.start().await?;
            tokio::time::sleep(Duration::from_secs(3)).await;
            println!("idle_heap_kb {}", heap_kb());
            println!("idle_rss_kb {}", rss_kb());

            PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
            let (allocations, allocated) = (
                ALLOCATIONS.load(Ordering::Relaxed),
                ALLOCATED.load(Ordering::Relaxed),
            );
            api.queue("default").unpause().await?;
            while DONE.load(Ordering::Relaxed) < jobs {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let allocations = ALLOCATIONS.load(Ordering::Relaxed) - allocations;
            let allocated = ALLOCATED.load(Ordering::Relaxed) - allocated;
            println!("peak_heap_kb {}", PEAK.load(Ordering::Relaxed) / 1024);

            // Long enough for the statistics and the metrics to be flushed.
            tokio::time::sleep(Duration::from_secs(5)).await;
            println!("after_heap_kb {}", heap_kb());
            println!("after_rss_kb {}", rss_kb());
            println!(
                "allocations_per_job {:.1}",
                allocations as f64 / jobs as f64
            );
            println!(
                "allocated_bytes_per_job {:.0}",
                allocated as f64 / jobs as f64
            );
            launcher.stop().await?;
        }
        _ => return Err(USAGE.into()),
    }
    Ok(())
}
