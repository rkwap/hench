//! Procedural macros for [hench](https://docs.rs/hench): the `#[hench::job]` attribute.
//!
//! Use it through the `hench` crate (`hench::job`, also in `hench::prelude`), not directly.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod attrs;
mod expand;
mod spec;
mod throttle;
mod unique;

use proc_macro::TokenStream;

/// Turns an `async fn` into a hench job.
///
/// ```ignore
/// use hench::prelude::*;
///
/// /// Sends a templated email to a user.
/// #[hench::job(queue = "mailers", retry = 5)]
/// pub async fn send_email(ctx: &JobContext, user_id: i64, template: String) -> Result<(), JobError> {
///     let mailer = ctx.data::<Mailer>()?;
///     mailer.send(user_id, &template).await?;
///     Ok(())
/// }
///
/// // Enqueue it (the arguments are a tuple):
/// send_email::perform_async(&client, (42, "welcome".into())).await?;
/// send_email::perform_in(&client, Duration::from_secs(300), (42, "reminder".into())).await?;
/// send_email::set().queue("critical").perform_async(&client, (42, "urgent".into())).await?;
/// ```
///
/// The function is replaced by a unit struct with the same name and visibility (and its doc
/// comments), which implements `hench::Job` and has inherent `perform_async`, `perform_in`,
/// `perform_at`, `perform_bulk`, `perform_inline` and `set` functions, so no trait import is
/// needed to enqueue it. The job is registered automatically: workers built with
/// `Hench::builder()` and the testing clients find it by name.
///
/// # The function
///
/// * It must be a free, non-generic `async fn` (also when its body does not await: the
///   attribute allows clippy's `unused_async` there).
/// * Its first parameter may be the job context, `ctx: &JobContext` (any name, no lifetime).
///   The context is recognized by its type's name, `JobContext` (`&hench::JobContext` works
///   too), not through a type alias or a renamed import.
/// * The other parameters are the job's arguments: owned types that implement
///   `serde::Serialize` and `serde::de::DeserializeOwned` and serialize to JSON. They form
///   the job's `Args` tuple: `()` without arguments, `(A,)` for one, `(A, B, ..)` for more.
///   Arguments cannot borrow: no references, no lifetimes but `'static` (`Cow<'static, str>`
///   is fine).
/// * It returns `()` (it cannot fail) or `Result<(), E>` where `E` converts into
///   `hench::JobError` (any `std::error::Error + Send + Sync + 'static`, `anyhow::Error`,
///   `JobError` itself, ...), not `impl Trait`. The future must be `Send`.
/// * `#[cfg(...)]` attributes apply to everything generated; doc comments document the
///   struct; other attributes (such as `#[tracing::instrument]`) apply to the body.
///
/// The body keeps every lint of the crate: the generated code allows only
/// `non_camel_case_types` (the struct is named after the function) and, on the body,
/// `clippy::unused_async`. So deprecated items used in a job warn as anywhere else, and crates
/// that forbid `dead_code` or `deprecated` can define jobs; a crate that forbids
/// `non_camel_case_types` can only use the attribute on impl blocks (below).
///
/// # Options
///
/// | Option | Example | Meaning |
/// |---|---|---|
/// | `name` | `name = "billing::Charge"` | the name stored in jobs (default: the function name) |
/// | `queue` | `queue = "critical"` | the queue (default: `default`) |
/// | `retry` | `retry = false`, `retry = 5` | `true` (25 retries), `false` (no retries, no morgue) or a maximum number of retries |
/// | `backtrace` | `backtrace = true`, `backtrace = 20` | store the error backtrace (all or the first lines) |
/// | `dead` | `dead = false` | `false` keeps the job out of the morgue when its retries are exhausted |
/// | `tags` | `tags = ["billing"]` | tags shown in the dashboard |
/// | `retry_for` | `retry_for = 3600` | retry for this many seconds after the first failure instead of a number of times |
/// | `retry_queue` | `retry_queue = "slow"` | the queue used for retries |
/// | `log_level` | `log_level = "debug"` | the job's log level (`trace`, `debug`, `info`, `warn` or `error`), see `JobOptions::log_level` |
/// | `unique_for` | `unique_for = 600` | unique job: a push while an equal job is queued, scheduled or running (within this many seconds) is a duplicate (`hench::unique`) |
/// | `unique_until` | `unique_until = "start"` | when the lock goes: `"success"` (default), `"start"` or `"expired"` (at most one per period) |
/// | `on_conflict` | `on_conflict = "replace"` | what a duplicate push does: `"drop"` (default), `"error"` (`Error::DuplicateJob`) or `"replace"` the waiting job |
/// | `unique_across_queues` | `unique_across_queues = true` | equal jobs in different queues are duplicates too |
/// | `unique_by` | `unique_by = [account]` | only these arguments make jobs equal (`Job::unique_key`) |
/// | `throttle` | `throttle = "stripe"` | run under the throttle of that name, defined with `HenchBuilder::throttle` or the worker file (`hench::limit`) |
/// | `throttle(..)` | `throttle(concurrency = 3, window = "100/1m")` | define a throttle named after the job: `concurrency`, `window`, `bucket`, `leaky`, `points`, each also `*_per_key` |
/// | `throttle_by` | `throttle_by = [account]` | the throttle key, from these arguments; limits `*_per_key` need one |
/// | `crate` | `crate = "::my_hench"` | the path of the hench crate, if it was renamed |
///
/// Times are whole seconds everywhere in the attribute; the unique options need `unique_for`.
///
/// The name must be unique among the jobs of a process: duplicates are reported when the
/// workers are built.
///
/// Because the job is a unit struct, its name is also a constant pattern: in code where the
/// job is in scope, `let send_email = ...;` does not declare a variable but matches against
/// the struct (and fails to compile). Use another variable name.
///
/// # Jobs implemented by hand
///
/// On an `impl Job for T` or `impl IterableJob for T` block, the attribute keeps the block as
/// written and registers `T::default()` like the jobs above, so `T` must implement `Default`.
/// The name and options come from the implementation; only the `crate` option is accepted.
///
/// ```ignore
/// #[derive(Default)]
/// struct Reindex;
///
/// #[hench::job]
/// impl IterableJob for Reindex {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn job(args: TokenStream, item: TokenStream) -> TokenStream {
    expand::job(args.into(), item.into()).into()
}
