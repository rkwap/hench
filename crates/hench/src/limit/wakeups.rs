//! Local wake-ups of concurrency waiters: a process-wide map from a concurrency state key to the
//! [`Notify`] its waiters listen to, notified whenever this process releases a lease of it, so
//! that a waiter in the same process takes the slot within milliseconds instead of at its next
//! poll (waiters in other processes notice by polling, `docs/design/ratelimit.md` §2.10).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError, Weak};

use tokio::sync::Notify;

/// Waiters by state key; entries whose waiters are gone are pruned as the map grows.
static WAITERS: LazyLock<Mutex<HashMap<String, Weak<Notify>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Prune dead entries when the map reaches this size (then the next multiple).
const PRUNE_AT: usize = 256;

/// The notifier of `state`'s waiters (shared by every waiter of it while one holds it).
pub(crate) fn listen(state: &str) -> Arc<Notify> {
    let mut waiters = WAITERS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(notify) = waiters.get(state).and_then(Weak::upgrade) {
        return notify;
    }
    if waiters.len() >= PRUNE_AT && waiters.len().is_multiple_of(PRUNE_AT) {
        waiters.retain(|_, notify| notify.strong_count() > 0);
    }
    let notify = Arc::new(Notify::new());
    waiters.insert(state.to_owned(), Arc::downgrade(&notify));
    notify
}

/// Wakes the waiters of `state` (a lease of it was released in this process).
pub(crate) fn notify(state: &str) {
    let notify = WAITERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(state)
        .and_then(Weak::upgrade);
    if let Some(notify) = notify {
        notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn releases_wake_the_waiters_of_their_state() {
        let notify = listen("limit:c:wakeups-test");
        assert!(Arc::ptr_eq(&notify, &listen("limit:c:wakeups-test")));
        {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            notify_other_and_this();
            tokio::time::timeout(Duration::from_secs(1), notified)
                .await
                .expect("woken");
        }
        drop(notify);
        // Nobody listens any more: notifying is a no-op, and a new listener gets a new notifier.
        super::notify("limit:c:wakeups-test");
        let _again = listen("limit:c:wakeups-test");
    }

    fn notify_other_and_this() {
        super::notify("limit:c:someone-else");
        super::notify("limit:c:wakeups-test");
    }
}
