//! Filtering a list (`?substr=`): the jobs whose JSON contains a piece of text, found by
//! reading the list in the order it is shown, a page at a time, with bounded work.
//!
//! The same rules hold for queues and the sorted sets: the filter is literal text, except that
//! `*` stands for any run of characters (`Mailer*welcome`); there is no other wildcard. The
//! matching happens in the dashboard, never as a Redis glob (`SCAN … MATCH`), whose `?` and
//! `[…]` patterns cost Redis O(length × pattern) for every entry.

use std::future::Future;

use hench::api::{Page, SortOrder};

use crate::error::WebResult;

/// Jobs read per round trip.
const PAGE_SIZE: usize = 1_000;

/// Jobs a filter looks through at most (a list can hold millions).
pub(crate) const SCAN_LIMIT: u64 = 50_000;

/// Matches a filter shows at most.
pub(crate) const MAX_MATCHES: usize = 500;

/// A filter: text a job's JSON must contain, `*` standing for any run of characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pattern {
    /// The literal pieces between the `*`s, which must appear in this order.
    parts: Vec<String>,
}

impl Pattern {
    /// The pattern of a filter.
    pub(crate) fn new(filter: &str) -> Self {
        Self {
            parts: filter
                .split('*')
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect(),
        }
    }

    /// `true` when `text` contains the pieces in order (each search starts where the previous
    /// piece ended, which finds a match whenever there is one).
    pub(crate) fn matches(&self, text: &str) -> bool {
        let mut rest = text;
        for part in &self.parts {
            match rest.find(part.as_str()) {
                Some(start) => rest = &rest[start + part.len()..],
                None => return false,
            }
        }
        true
    }
}

/// A job that matched.
pub(crate) struct Found<T> {
    pub(crate) item: T,
    /// Its index in the list as shown (0: the first row of the first page).
    pub(crate) index: u64,
    /// The length of the list when its page was read.
    pub(crate) total: u64,
}

/// How a search ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The whole list was searched.
    Complete,
    /// It stopped at [`MAX_MATCHES`] matches (more may follow).
    MatchLimit,
    /// It stopped after looking through `searched` of the `total` jobs.
    ScanLimit { searched: u64, total: u64 },
}

/// What a search found.
pub(crate) struct Results<T> {
    pub(crate) found: Vec<Found<T>>,
    pub(crate) outcome: Outcome,
    /// The length of the list when its last page was read.
    pub(crate) total: u64,
}

/// The place in line of the job at `index` of a queue of `total` jobs shown in `order`: 1 is
/// the next job to run, `total` the newest.
pub(crate) fn place_in_line(order: SortOrder, index: u64, total: u64) -> u64 {
    match order {
        SortOrder::Ascending => index.saturating_add(1),
        SortOrder::Descending => total.saturating_sub(index),
    }
}

/// Searches a list read page by page with `fetch(page_number, page_size)` (pages numbered
/// from 1, in the order the list is shown) for items whose `value` matches `pattern`: at most
/// [`SCAN_LIMIT`] items are read and [`MAX_MATCHES`] kept.
pub(crate) async fn search<T, F, Fut>(
    pattern: &Pattern,
    mut fetch: F,
    value: impl Fn(&T) -> &str,
) -> WebResult<Results<T>>
where
    F: FnMut(usize, usize) -> Fut,
    Fut: Future<Output = hench::Result<Page<T>>>,
{
    let mut found = Vec::new();
    let mut scanned: u64 = 0;
    let mut number = 1;
    loop {
        let page = fetch(number, PAGE_SIZE).await?;
        let total = page.total;
        let complete = page.items.len() < PAGE_SIZE;
        for item in page.items {
            let index = scanned;
            scanned += 1;
            if pattern.matches(value(&item)) {
                found.push(Found { item, index, total });
                if found.len() >= MAX_MATCHES {
                    let outcome = if scanned < total {
                        Outcome::MatchLimit
                    } else {
                        Outcome::Complete
                    };
                    return Ok(Results {
                        found,
                        outcome,
                        total,
                    });
                }
            }
        }
        if complete || scanned >= total {
            return Ok(Results {
                found,
                outcome: Outcome::Complete,
                total,
            });
        }
        if scanned >= SCAN_LIMIT {
            return Ok(Results {
                found,
                outcome: Outcome::ScanLimit {
                    searched: scanned,
                    total,
                },
                total,
            });
        }
        number += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(items: Vec<String>, number: usize, total: u64) -> Page<String> {
        Page::new(items, number, PAGE_SIZE, total)
    }

    #[test]
    fn patterns_are_literal_text_with_star_wildcards() {
        let pattern = Pattern::new("Mailer");
        assert!(pattern.matches("{\"class\":\"Mailer\"}"));
        assert!(!pattern.matches("{\"class\":\"mailer\"}"));
        for literal in ["a?c", "a[b]c", "a\\c", "a.c"] {
            assert!(!Pattern::new(literal).matches("abc"), "{literal}");
            assert!(
                Pattern::new(literal).matches(&format!("x{literal}y")),
                "{literal}"
            );
        }
        let pattern = Pattern::new("Mailer*welcome");
        assert!(pattern.matches("{\"class\":\"Mailer\",\"args\":[\"welcome\"]}"));
        assert!(!pattern.matches("{\"args\":[\"welcome\"],\"class\":\"Mailer\"}"));
        // Pieces may not overlap, and later pieces are searched after earlier ones.
        assert!(!Pattern::new("abab*abab").matches("ababab"));
        assert!(Pattern::new("aba*bab").matches("ababab"));
        assert!(Pattern::new("ab*ab*ab").matches("ababab"));
        assert!(Pattern::new("*").matches(""));
        assert!(Pattern::new("**a**").matches("xa"));
        assert!(Pattern::new("é*ü").matches("xéyü"));
    }

    #[tokio::test]
    async fn searches_stop_at_the_limits() {
        let pattern = Pattern::new("7");
        // Everything matches: the match limit stops the search.
        let all = |number: usize, size: usize| async move {
            let start = (number - 1) * size;
            Ok(page(
                (start..start + size).map(|_| "7".to_owned()).collect(),
                number,
                1_000_000,
            ))
        };
        let results = search(&pattern, all, String::as_str).await.unwrap();
        assert_eq!(results.found.len(), MAX_MATCHES);
        assert_eq!(results.outcome, Outcome::MatchLimit);
        assert_eq!(results.found[499].index, 499);
        // Nothing matches: the scan limit stops it.
        let none = |number: usize, size: usize| async move {
            Ok(page(vec!["x".to_owned(); size], number, 60_000))
        };
        let results = search(&pattern, none, String::as_str).await.unwrap();
        assert!(results.found.is_empty());
        assert_eq!(
            results.outcome,
            Outcome::ScanLimit {
                searched: SCAN_LIMIT,
                total: 60_000
            }
        );
        // A short list is searched completely, with indexes in list order.
        let short = |number: usize, _: usize| async move {
            let items = if number == 1 {
                (0..12).map(|i| i.to_string()).collect()
            } else {
                Vec::new()
            };
            Ok(page(items, number, 12))
        };
        let results = search(&pattern, short, String::as_str).await.unwrap();
        assert_eq!(results.outcome, Outcome::Complete);
        assert_eq!(
            results
                .found
                .iter()
                .map(|found| found.index)
                .collect::<Vec<_>>(),
            [7]
        );
        assert_eq!(results.found[0].total, 12);
    }
}
