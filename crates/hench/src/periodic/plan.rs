//! Catch-up planning: what a poller does with a due tick, given the catch-up policy, the
//! deadline and the time (`docs/design/periodic.md` §2.2.5). Pure: the poller reads the state,
//! plans, and sends one `PERIODIC_FIRE` with the plan.
//!
//! With `next` = T due (T ≤ now) and L the latest tick at or before now:
//!
//! | Policy | Enqueued now | Missed (counted) | New `next` |
//! |---|---|---|---|
//! | `Latest` | L, unless a deadline is set and L is later than it | ticks in [T, L), plus L if dropped | the first tick after now |
//! | `Skip` | L if within the deadline (5 minutes unless set) | as above | as above |
//! | `All { max }` | the oldest C of the `max` most recent ticks in [T, L] within the deadline | ticks in [T, C) | the first tick after C (due at once while the backlog lasts) |
//!
//! A T that is not a tick of the timetable as this process computes it (no tick in [T, now]:
//! another process computed it under other time-zone rules, or it was edited by hand) is read as
//! the nearest tick within three hours (the last one before it or the next one after now): it is
//! enqueued in that tick's place, unless that tick was handled already ([`Foreign`]).

use std::time::Duration;

use crate::periodic::CatchUp;
use crate::periodic::timing::{MISSED_CAP, Timing, TimingError};

/// The deadline of [`CatchUp::Skip`] when none is set: longer than a lease plus a poll, so that
/// a tick taken by a poller that crashed still runs.
pub(crate) const SKIP_DEADLINE: Duration = Duration::from_secs(300);

/// The largest `max` of [`CatchUp::All`].
pub(crate) const MAX_CATCH_UP: u32 = 10_000;

/// How far from a tick of the timetable a due `next` that is not one may lie and still stand for
/// it: two processes' rules for one zone (an offset, a daylight saving time rule, an older
/// version's DST rules) put a tick at most this far apart; Vixie cron treats clock jumps up to
/// three hours the same way.
pub(crate) const FOREIGN_TOLERANCE_SECS: i64 = 3 * 3_600;

/// A schedule's catch-up policy and deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Policy {
    /// What to do with missed ticks.
    pub(crate) catch_up: CatchUp,
    /// Never enqueue a tick later than this (whole seconds).
    pub(crate) deadline: Option<Duration>,
}

impl Policy {
    /// The deadline that applies: the one set, or [`SKIP_DEADLINE`] for `Skip`.
    pub(crate) fn effective_deadline(&self) -> Option<Duration> {
        match self.catch_up {
            CatchUp::Skip => Some(self.deadline.unwrap_or(SKIP_DEADLINE)),
            _ => self.deadline,
        }
    }

    /// Whether `tick` (epoch seconds) is within the deadline at `now_ms` (inclusive).
    fn within(&self, tick: i64, now_ms: i64) -> bool {
        match self.effective_deadline() {
            None => true,
            Some(deadline) => {
                let deadline_ms = i64::try_from(deadline.as_millis()).unwrap_or(i64::MAX);
                now_ms.saturating_sub(tick.saturating_mul(1_000)) <= deadline_ms
            }
        }
    }
}

/// What to do with a due tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Plan {
    /// The tick to enqueue now, if any.
    pub(crate) fire: Option<i64>,
    /// Ticks dropped by this plan (counted, never enqueued).
    pub(crate) missed: u64,
    /// `true` when counting the missed ticks stopped at [`MISSED_CAP`] (cron timetables).
    pub(crate) capped: bool,
    /// The schedule's next tick once this plan is applied.
    pub(crate) next: i64,
    /// Set when the due tick was not a tick of the timetable as this process computes it.
    pub(crate) foreign: Option<Foreign>,
}

/// How a plan read a due `next` that is not a tick of the timetable as this process computes it
/// (no tick between it and now): another process computed it under other time-zone rules (older
/// or newer tzdata, rules that changed, another version's DST rules), or it was edited by hand.
/// It stands for the nearest tick of the timetable within [`FOREIGN_TOLERANCE_SECS`], so that
/// processes that disagree on the rules still run each period once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Foreign {
    /// It stands for this tick (the last one before it, or the next one after now) and is
    /// enqueued in its place; the next tick after it follows.
    InPlaceOf(i64),
    /// It stands for this tick, which was handled already (`last_tick`): not enqueued again.
    Handled(i64),
    /// No tick of the timetable lies within the tolerance: enqueued as a tick of its own.
    Extra,
}

/// What a poller remembers between the plans of one `All` catch-up, so that working through a
/// backlog tick by tick costs a constant time per tick instead of walking the backlog again:
/// for the latest tick `last` and `max`, the oldest of the `max` most recent ticks
/// (`Oldest`), or that it is older than an instant the walk stopped at (`Before`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Boundary {
    last: i64,
    max: u32,
    found: Found,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Found {
    /// Nothing remembered.
    #[default]
    Nothing,
    /// The oldest of the `max` most recent ticks.
    Oldest(i64),
    /// The oldest of the `max` most recent ticks is before this instant.
    Before(i64),
}

/// Plans the due tick `due` (the schedule's `next`, at most `now`) at `now_ms` (Redis clock,
/// epoch milliseconds). `last_tick` is the last tick the schedule handled. `boundary` is the
/// poller's memory for `All` catch-ups (any value is correct; a matching one is faster).
pub(crate) fn plan(
    timing: &Timing,
    policy: &Policy,
    due: i64,
    last_tick: Option<i64>,
    now_ms: i64,
    boundary: &mut Boundary,
) -> Result<Plan, TimingError> {
    let now = now_ms.div_euclid(1_000);
    let latest = timing.latest_at_or_before(now)?;
    let after_now = timing.next_after(now)?;
    if latest < due {
        return foreign(timing, policy, due, last_tick, latest, after_now, now_ms);
    }
    if !policy.within(latest, now_ms) {
        // Even the latest tick is too late: every tick from `due` on is missed.
        let (missed, capped) = timing.count(due, latest + 1, MISSED_CAP)?;
        return Ok(Plan {
            fire: None,
            missed,
            capped,
            next: after_now,
            foreign: None,
        });
    }
    match policy.catch_up {
        CatchUp::All { max } => {
            let oldest = oldest_to_fire(timing, policy, due, latest, now_ms, max, boundary)?;
            let (missed, capped) = timing.count(due, oldest, MISSED_CAP)?;
            Ok(Plan {
                fire: Some(oldest),
                missed,
                capped,
                next: timing.next_after(oldest)?,
                foreign: None,
            })
        }
        CatchUp::Latest | CatchUp::Skip => {
            let (missed, capped) = timing.count(due, latest, MISSED_CAP)?;
            Ok(Plan {
                fire: Some(latest),
                missed,
                capped,
                next: after_now,
                foreign: None,
            })
        }
    }
}

/// The plan of a due `next` that is not a tick of the timetable (`latest` < `due` ≤ now <
/// `after_now`): see [`Foreign`]. It stands for the nearer of `latest` and `after_now` within
/// [`FOREIGN_TOLERANCE_SECS`] (`latest` on a tie): in place of `after_now`, it is enqueued and
/// the schedule goes on after `after_now`; in place of `latest`, it is enqueued unless
/// `last_tick` says `latest` was handled. Enqueued only within the policy's deadline (else one
/// tick missed).
fn foreign(
    timing: &Timing,
    policy: &Policy,
    due: i64,
    last_tick: Option<i64>,
    latest: i64,
    after_now: i64,
    now_ms: i64,
) -> Result<Plan, TimingError> {
    let (to_latest, to_next) = (due - latest, after_now - due);
    let (reading, next) = if to_next < to_latest && to_next <= FOREIGN_TOLERANCE_SECS {
        (Foreign::InPlaceOf(after_now), timing.next_after(after_now)?)
    } else if to_latest <= FOREIGN_TOLERANCE_SECS {
        if last_tick.is_some_and(|last| last >= latest) {
            (Foreign::Handled(latest), after_now)
        } else {
            (Foreign::InPlaceOf(latest), after_now)
        }
    } else {
        (Foreign::Extra, after_now)
    };
    let (fire, missed) = match reading {
        Foreign::Handled(_) => (None, 0),
        _ if !policy.within(due, now_ms) => (None, 1),
        _ => (Some(due), 0),
    };
    Ok(Plan {
        fire,
        missed,
        capped: false,
        next,
        foreign: Some(reading),
    })
}

/// The tick an `All { max }` catch-up enqueues now: the oldest of the `max` most recent ticks
/// up to `latest` that are at or after `due` and within the deadline.
fn oldest_to_fire(
    timing: &Timing,
    policy: &Policy,
    due: i64,
    latest: i64,
    now_ms: i64,
    max: u32,
    boundary: &mut Boundary,
) -> Result<i64, TimingError> {
    // The first tick at or after `due` that is within the deadline (`latest` is).
    let mut lower = due;
    if !policy.within(due, now_ms) {
        let deadline = policy.effective_deadline().unwrap_or_default();
        let deadline_ms = i64::try_from(deadline.as_millis()).unwrap_or(i64::MAX);
        // within(t) ⟺ t ≥ (now_ms − deadline_ms) / 1000, rounded up.
        let earliest = (now_ms - deadline_ms + 999).div_euclid(1_000);
        lower = timing.next_after(earliest.max(due) - 1)?;
    }
    if boundary.last != latest || boundary.max != max {
        *boundary = Boundary {
            last: latest,
            max,
            found: Found::Nothing,
        };
    }
    let oldest = match boundary.found {
        Found::Oldest(oldest) => oldest,
        Found::Before(stop) if stop <= lower => return Ok(lower),
        Found::Before(_) | Found::Nothing => {
            // Walk back from `latest` over at most `max` ticks, stopping below `lower`.
            let mut tick = latest;
            let mut seen = 1;
            let found = loop {
                if tick < lower {
                    break Found::Before(tick + 1);
                }
                if seen >= max {
                    break Found::Oldest(tick);
                }
                tick = timing.latest_at_or_before(tick - 1)?;
                seen += 1;
            };
            boundary.found = found;
            match found {
                Found::Oldest(oldest) => oldest,
                Found::Before(_) | Found::Nothing => return Ok(lower),
            }
        }
    };
    Ok(oldest.max(lower))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600;

    /// 2026-11-02 10:00:00 UTC.
    const TEN: i64 = 1_793_613_600;

    fn hourly() -> Timing {
        Timing::cron("0 * * * *", "UTC").unwrap()
    }

    fn policy(catch_up: CatchUp, deadline: Option<u64>) -> Policy {
        Policy {
            catch_up,
            deadline: deadline.map(Duration::from_secs),
        }
    }

    fn run(policy: &Policy, due: i64, now_ms: i64) -> Plan {
        plan(
            &hourly(),
            policy,
            due,
            None,
            now_ms,
            &mut Boundary::default(),
        )
        .unwrap()
    }

    /// Runs an `All` catch-up to its end the way the poller does: plan, apply, plan again while
    /// the next tick is due. Returns the ticks enqueued and the total missed.
    fn catch_up(policy: &Policy, timing: &Timing, due: i64, now_ms: i64) -> (Vec<i64>, u64) {
        let mut boundary = Boundary::default();
        let mut next = due;
        let mut fired = Vec::new();
        let mut missed = 0;
        while next * 1_000 <= now_ms {
            let plan = plan(timing, policy, next, None, now_ms, &mut boundary).unwrap();
            fired.extend(plan.fire);
            missed += plan.missed;
            assert!(plan.next > next, "{plan:?}");
            next = plan.next;
        }
        (fired, missed)
    }

    #[test]
    fn on_time_every_policy_enqueues_the_tick() {
        for catch_up in [CatchUp::Latest, CatchUp::Skip, CatchUp::All { max: 24 }] {
            let plan = run(&policy(catch_up, None), TEN, TEN * 1_000 + 3);
            assert_eq!(
                plan,
                Plan {
                    fire: Some(TEN),
                    missed: 0,
                    capped: false,
                    next: TEN + HOUR,
                    foreign: None
                },
                "{catch_up:?}"
            );
        }
    }

    #[test]
    fn after_downtime_each_policy_catches_up_its_way() {
        // Down from 10:00 to 13:20.
        let now_ms = (TEN + 3 * HOUR + 20 * 60) * 1_000;
        let latest = run(&policy(CatchUp::Latest, None), TEN, now_ms);
        assert_eq!(
            latest,
            Plan {
                fire: Some(TEN + 3 * HOUR),
                missed: 3,
                capped: false,
                next: TEN + 4 * HOUR,
                foreign: None
            }
        );
        // 13:00 is 20 minutes late: beyond Skip's 5 minutes.
        let skip = run(&policy(CatchUp::Skip, None), TEN, now_ms);
        assert_eq!(
            skip,
            Plan {
                fire: None,
                missed: 4,
                capped: false,
                next: TEN + 4 * HOUR,
                foreign: None
            }
        );
        // All: 10:00, 11:00, 12:00, 13:00 in successive fires.
        let all = policy(CatchUp::All { max: 24 }, None);
        assert_eq!(
            catch_up(&all, &hourly(), TEN, now_ms),
            (vec![TEN, TEN + HOUR, TEN + 2 * HOUR, TEN + 3 * HOUR], 0)
        );
        // All { max: 2 }: 12:00 and 13:00, 2 missed.
        let two = policy(CatchUp::All { max: 2 }, None);
        assert_eq!(
            catch_up(&two, &hourly(), TEN, now_ms),
            (vec![TEN + 2 * HOUR, TEN + 3 * HOUR], 2)
        );
    }

    #[test]
    fn deadlines_drop_late_ticks_and_include_their_boundary() {
        // 13:00 exactly 20 minutes late, deadline 20 minutes: enqueued.
        let now_ms = (TEN + 3 * HOUR + 20 * 60) * 1_000;
        let exact = run(&policy(CatchUp::Latest, Some(20 * 60)), TEN, now_ms);
        assert_eq!(exact.fire, Some(TEN + 3 * HOUR));
        // One millisecond later: dropped.
        let late = run(&policy(CatchUp::Latest, Some(20 * 60)), TEN, now_ms + 1);
        assert_eq!((late.fire, late.missed), (None, 4));
        // Skip with a longer deadline catches the latest tick.
        let skip = run(&policy(CatchUp::Skip, Some(HOUR as u64)), TEN, now_ms);
        assert_eq!((skip.fire, skip.missed), (Some(TEN + 3 * HOUR), 3));
        // All with a deadline: only ticks within it (12:00 is 80 minutes late).
        let all = policy(CatchUp::All { max: 24 }, Some(90 * 60));
        assert_eq!(
            catch_up(&all, &hourly(), TEN, now_ms),
            (vec![TEN + 2 * HOUR, TEN + 3 * HOUR], 2)
        );
        let none = policy(CatchUp::All { max: 24 }, Some(60));
        assert_eq!(catch_up(&none, &hourly(), TEN, now_ms), (vec![], 4));
    }

    #[test]
    fn all_with_a_backlog_smaller_and_larger_than_max() {
        let every = Timing::every(60, 0).unwrap();
        let start = 6_000_000;
        // 100 ticks missed, max 10: the 10 most recent.
        let now_ms = (start + 99 * 60) * 1_000 + 500;
        let (fired, missed) = catch_up(
            &policy(CatchUp::All { max: 10 }, None),
            &every,
            start,
            now_ms,
        );
        assert_eq!(fired, (90..100).map(|k| start + k * 60).collect::<Vec<_>>());
        assert_eq!(missed, 90);
        // max larger than the backlog: everything, oldest first.
        let (fired, missed) = catch_up(
            &policy(CatchUp::All { max: 1_000 }, None),
            &every,
            start,
            now_ms,
        );
        assert_eq!(fired.len(), 100);
        assert_eq!(fired.first(), Some(&start));
        assert_eq!(missed, 0);
    }

    #[test]
    fn a_long_backlog_of_a_fast_cron_is_planned_quickly_and_capped() {
        // Every second, down for a week, All { max: 10 000 }: the plans stay fast because the
        // poller's boundary remembers where the window starts.
        let timing = Timing::cron("* * * * * *", "UTC").unwrap();
        let started = std::time::Instant::now();
        let start = 1_800_000_000;
        let now_ms = (start + 7 * 86_400) * 1_000;
        let mut boundary = Boundary::default();
        let all = policy(CatchUp::All { max: MAX_CATCH_UP }, None);
        let first = plan(&timing, &all, start, None, now_ms, &mut boundary).unwrap();
        assert_eq!(first.fire, Some(start + 7 * 86_400 - 9_999));
        assert_eq!((first.missed, first.capped), (MISSED_CAP, true));
        let mut next = first.next;
        for _ in 0..1_000 {
            let plan = plan(&timing, &all, next, None, now_ms, &mut boundary).unwrap();
            assert_eq!(plan.fire, Some(next));
            assert_eq!(plan.missed, 0);
            next = plan.next;
        }
        // Latest after the same week reports the cap.
        let latest = plan(
            &timing,
            &policy(CatchUp::Latest, None),
            start,
            None,
            now_ms,
            &mut Boundary::default(),
        )
        .unwrap();
        assert_eq!(latest.fire, Some(start + 7 * 86_400));
        assert!(latest.capped);
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn a_next_that_is_not_a_tick_stands_for_the_nearest_tick() {
        // periodic-4: such a `next` used to be dropped without a trace. `next` set by hand to
        // 10:30 for an hourly timetable, now 10:45: it stands for 10:00 (a tie goes to the
        // earlier tick), which nobody ran: enqueued in its place.
        let edited = run(
            &policy(CatchUp::Latest, None),
            TEN + 1_800,
            (TEN + 2_700) * 1_000,
        );
        assert_eq!(
            edited,
            Plan {
                fire: Some(TEN + 1_800),
                missed: 0,
                capped: false,
                next: TEN + HOUR,
                foreign: Some(Foreign::InPlaceOf(TEN))
            }
        );
        // 10:00 was run already: 10:30 is not run again.
        let handled = plan_with(&hourly(), TEN + 1_800, Some(TEN), (TEN + 2_700) * 1_000);
        assert_eq!(
            (handled.fire, handled.missed, handled.next, handled.foreign),
            (None, 0, TEN + HOUR, Some(Foreign::Handled(TEN)))
        );
        // Beyond the policy's deadline: one tick missed.
        let late = plan(
            &hourly(),
            &policy(CatchUp::Latest, Some(60)),
            TEN + 1_800,
            None,
            (TEN + 2_700) * 1_000,
            &mut Boundary::default(),
        )
        .unwrap();
        assert_eq!((late.fire, late.missed), (None, 1));
    }

    /// The plan of `due` with `last_tick`, latest policy.
    fn plan_with(timing: &Timing, due: i64, last_tick: Option<i64>, now_ms: i64) -> Plan {
        plan(
            timing,
            &policy(CatchUp::Latest, None),
            due,
            last_tick,
            now_ms,
            &mut Boundary::default(),
        )
        .unwrap()
    }

    #[test]
    fn processes_with_other_zone_rules_run_each_day_once() {
        // periodic-4: `0 9 * * *` where another process's rules put the tick at 10:00 (its tzdata
        // is an hour off). Whichever process leases the schedule, each day runs once.
        let daily = Timing::cron("0 9 * * *", "UTC").unwrap();
        let day = 86_400;
        // 2026-11-03 09:00 UTC.
        let nine = 1_793_696_400;
        // The other process wrote 10:00; this one leases it at 10:00:01. Nobody ran today's
        // 09:00: enqueued in its place, then tomorrow's 09:00.
        let ours = plan_with(
            &daily,
            nine + 3_600,
            Some(nine - day),
            (nine + 3_601) * 1_000,
        );
        assert_eq!(
            (ours.fire, ours.next, ours.foreign),
            (
                Some(nine + 3_600),
                nine + day,
                Some(Foreign::InPlaceOf(nine))
            )
        );
        // The reverse: rules an hour earlier wrote 08:00 for today; it stands for today's 09:00,
        // which it replaces: tomorrow's 09:00 follows (not today's).
        let earlier = plan_with(
            &daily,
            nine - 3_600,
            Some(nine - day),
            (nine - 3_599) * 1_000,
        );
        assert_eq!(
            (earlier.fire, earlier.next, earlier.foreign),
            (
                Some(nine - 3_600),
                nine + day,
                Some(Foreign::InPlaceOf(nine))
            )
        );
        // This process ran 09:00, the other then wrote its own 10:00 for the same day: not run
        // twice.
        let twice = plan_with(&daily, nine + 3_600, Some(nine), (nine + 3_601) * 1_000);
        assert_eq!(
            (twice.fire, twice.next, twice.foreign),
            (None, nine + day, Some(Foreign::Handled(nine)))
        );
        // A `next` far from any tick (an edit by hand): enqueued as a tick of its own.
        let edited = plan_with(
            &daily,
            nine + 9 * 3_600,
            Some(nine),
            (nine + 9 * 3_600 + 1) * 1_000,
        );
        assert_eq!(
            (edited.fire, edited.next, edited.foreign),
            (Some(nine + 9 * 3_600), nine + day, Some(Foreign::Extra))
        );
        // A tick of the timetable is never foreign.
        assert_eq!(
            plan_with(&daily, nine, Some(nine - day), (nine + 1) * 1_000).foreign,
            None
        );
    }

    #[test]
    fn the_skip_deadline_defaults_to_five_minutes() {
        assert_eq!(
            policy(CatchUp::Skip, None).effective_deadline(),
            Some(SKIP_DEADLINE)
        );
        assert_eq!(policy(CatchUp::Latest, None).effective_deadline(), None);
        // Four minutes late: Skip enqueues it.
        let plan = run(&policy(CatchUp::Skip, None), TEN, (TEN + 240) * 1_000);
        assert_eq!(plan.fire, Some(TEN));
        let plan = run(&policy(CatchUp::Skip, None), TEN, (TEN + 301) * 1_000);
        assert_eq!(plan.fire, None);
    }
}
