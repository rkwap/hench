//! One-shot notices ("flash messages") shown on the page a POST action redirects to.
//!
//! The notice travels in a short-lived cookie (`hench_flash`) as a compact code, never as
//! free text: `kind.count.failed.subject`, where the subject (a queue name) is
//! percent-encoded. The page that shows it clears the cookie.

use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

/// The name of the notice cookie.
pub(crate) const COOKIE: &str = "hench_flash";

/// How long a notice waits to be shown, in seconds.
pub(crate) const MAX_AGE_SECS: u64 = 60;

/// The longest subject kept (queue names are short; this bounds the cookie).
const MAX_SUBJECT_CHARS: usize = 200;

/// What an action did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoticeKind {
    /// Jobs moved back to their queues by a retry.
    Retried,
    /// Scheduled jobs pushed to their queues.
    Enqueued,
    /// Jobs deleted.
    Deleted,
    /// Jobs moved to the dead set.
    Killed,
    /// A queue deleted (subject: its name).
    QueueCleared,
    /// A queue paused (subject: its name).
    QueuePaused,
    /// A queue resumed (subject: its name).
    QueueUnpaused,
    /// Processes asked to quiet down.
    Quieted,
    /// Processes asked to stop.
    Stopped,
    /// A bulk action without any selected job.
    NothingSelected,
    /// The job acted on was no longer there.
    Gone,
    /// The process acted on was no longer running.
    ProcessGone,
    /// A periodic job enqueued now (subject: the schedule).
    PeriodicEnqueued,
    /// Periodic schedules paused (subject: the schedule, for one).
    PeriodicPaused,
    /// Periodic schedules resumed (subject: the schedule, for one).
    PeriodicResumed,
    /// Periodic schedules deleted (subject: the schedule, for one).
    PeriodicDeleted,
    /// Batches cancelled (subject: the batch, for one).
    BatchCancelled,
    /// Batches deleted (subject: the batch, for one).
    BatchDeleted,
    /// Pending batch members skipped.
    MemberSkipped,
    /// Batch callbacks dispatched.
    CallbacksDispatched,
    /// Limits reset (subject: the limit, for one).
    LimitsReset,
    /// The wait line of a limit cleared (subject: the limit).
    LineCleared,
    /// Unique locks released (subject: the digest, for one).
    LocksReleased,
    /// The limits of a queue set (subject: its name).
    QueueLimited,
    /// The limits of a queue cleared (subject: its name).
    QueueUnlimited,
}

impl NoticeKind {
    const ALL: [NoticeKind; 25] = [
        NoticeKind::Retried,
        NoticeKind::Enqueued,
        NoticeKind::Deleted,
        NoticeKind::Killed,
        NoticeKind::QueueCleared,
        NoticeKind::QueuePaused,
        NoticeKind::QueueUnpaused,
        NoticeKind::Quieted,
        NoticeKind::Stopped,
        NoticeKind::NothingSelected,
        NoticeKind::Gone,
        NoticeKind::ProcessGone,
        NoticeKind::PeriodicEnqueued,
        NoticeKind::PeriodicPaused,
        NoticeKind::PeriodicResumed,
        NoticeKind::PeriodicDeleted,
        NoticeKind::BatchCancelled,
        NoticeKind::BatchDeleted,
        NoticeKind::MemberSkipped,
        NoticeKind::CallbacksDispatched,
        NoticeKind::LimitsReset,
        NoticeKind::LineCleared,
        NoticeKind::LocksReleased,
        NoticeKind::QueueLimited,
        NoticeKind::QueueUnlimited,
    ];

    fn code(self) -> &'static str {
        match self {
            NoticeKind::Retried => "retried",
            NoticeKind::Enqueued => "enqueued",
            NoticeKind::Deleted => "deleted",
            NoticeKind::Killed => "killed",
            NoticeKind::QueueCleared => "cleared",
            NoticeKind::QueuePaused => "paused",
            NoticeKind::QueueUnpaused => "unpaused",
            NoticeKind::Quieted => "quieted",
            NoticeKind::Stopped => "stopped",
            NoticeKind::NothingSelected => "none",
            NoticeKind::Gone => "gone",
            NoticeKind::ProcessGone => "nopid",
            NoticeKind::PeriodicEnqueued => "p-enqueued",
            NoticeKind::PeriodicPaused => "p-paused",
            NoticeKind::PeriodicResumed => "p-resumed",
            NoticeKind::PeriodicDeleted => "p-deleted",
            NoticeKind::BatchCancelled => "b-cancelled",
            NoticeKind::BatchDeleted => "b-deleted",
            NoticeKind::MemberSkipped => "b-skipped",
            NoticeKind::CallbacksDispatched => "b-dispatched",
            NoticeKind::LimitsReset => "l-reset",
            NoticeKind::LineCleared => "l-cleared",
            NoticeKind::LocksReleased => "u-released",
            NoticeKind::QueueLimited => "q-limited",
            NoticeKind::QueueUnlimited => "q-unlimited",
        }
    }

    fn from_code(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.code() == code)
    }
}

/// A notice: what happened, to how many items, how many failed, and about what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Notice {
    /// What happened.
    pub(crate) kind: NoticeKind,
    /// How many items it happened to.
    pub(crate) count: u64,
    /// How many items it could not be done to.
    pub(crate) failed: u64,
    /// What it is about (a queue name), or empty.
    pub(crate) subject: String,
}

impl Notice {
    /// A notice about `count` items of which `failed` could not be changed.
    pub(crate) fn new(kind: NoticeKind, count: u64, failed: u64) -> Self {
        Self {
            kind,
            count,
            failed,
            subject: String::new(),
        }
    }

    /// A notice about a named thing (a queue).
    pub(crate) fn about(kind: NoticeKind, subject: &str) -> Self {
        Self {
            kind,
            count: 0,
            failed: 0,
            subject: subject.to_owned(),
        }
    }

    /// `true` when part of the action failed.
    pub(crate) fn is_warning(&self) -> bool {
        self.failed > 0
            || matches!(
                self.kind,
                NoticeKind::NothingSelected | NoticeKind::Gone | NoticeKind::ProcessGone
            )
    }

    /// The cookie value.
    pub(crate) fn encode(&self) -> String {
        let (subject, _) = crate::format::truncate(&self.subject, MAX_SUBJECT_CHARS);
        format!(
            "{}.{}.{}.{}",
            self.kind.code(),
            self.count,
            self.failed,
            utf8_percent_encode(&subject, NON_ALPHANUMERIC)
        )
    }

    /// Parses a cookie value; `None` for anything malformed.
    pub(crate) fn decode(value: &str) -> Option<Self> {
        let mut parts = value.splitn(4, '.');
        let kind = NoticeKind::from_code(parts.next()?)?;
        let count = parts.next()?.parse().ok()?;
        let failed = parts.next()?.parse().ok()?;
        let subject = percent_decode_str(parts.next()?).decode_utf8().ok()?;
        let (subject, _) = crate::format::truncate(&subject, MAX_SUBJECT_CHARS);
        Some(Self {
            kind,
            count,
            failed,
            subject,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notices_round_trip_through_the_cookie() {
        for kind in NoticeKind::ALL {
            let notice = Notice::new(kind, 3, 1);
            assert_eq!(Notice::decode(&notice.encode()), Some(notice));
        }
        let named = Notice::about(NoticeKind::QueuePaused, "a.b;c=d \"é\" <x>");
        let encoded = named.encode();
        assert!(
            encoded
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '%'),
            "{encoded}"
        );
        assert_eq!(Notice::decode(&encoded), Some(named));
    }

    #[test]
    fn malformed_cookies_are_ignored() {
        for value in [
            "",
            "retried",
            "retried.1",
            "retried.1.0",
            "bogus.1.0.",
            "retried.x.0.",
            "retried.1.-1.",
            "retried.1.0.%FF",
        ] {
            assert_eq!(Notice::decode(value), None, "{value}");
        }
        let long = Notice::about(NoticeKind::QueueCleared, &"q".repeat(1_000));
        let decoded = Notice::decode(&long.encode()).unwrap();
        assert!(decoded.subject.chars().count() <= MAX_SUBJECT_CHARS + 1);
    }

    #[test]
    fn warnings_are_flagged() {
        assert!(!Notice::new(NoticeKind::Deleted, 2, 0).is_warning());
        assert!(Notice::new(NoticeKind::Deleted, 2, 1).is_warning());
        assert!(Notice::new(NoticeKind::NothingSelected, 0, 0).is_warning());
    }
}
