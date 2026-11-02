//! Log capture for the tests of the rate limiting module: what a closure logs at `warn`.

use std::sync::{LazyLock, Mutex, PoisonError};

/// Log output, for the warnings.
#[derive(Clone, Default)]
struct Buffer(std::sync::Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A second dispatcher for the whole test binary, so that each thread asks its own
/// dispatcher whether a callsite is enabled (see `job_logger`'s tests).
static SECOND_DISPATCHER: LazyLock<tracing::Dispatch> =
    LazyLock::new(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));

/// What `run` logs at `warn` and above.
pub(crate) fn warnings(run: impl FnOnce()) -> String {
    LazyLock::force(&SECOND_DISPATCHER);
    let buffer = Buffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::with_default(subscriber, run);
    let bytes = buffer
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    String::from_utf8_lossy(&bytes).into_owned()
}
