//! Pagination (Sidekiq's `Paginator`): one page of a Redis list or sorted set, or of an
//! in-memory collection.

use crate::api::Api;
use crate::error::{Error, Result};
use crate::keys;

/// The largest page served by the API; larger requests are capped (a dashboard never needs
/// more, and huge `LRANGE`/`ZRANGE` replies would block Redis).
pub(crate) const MAX_PAGE_SIZE: usize = 1_000;

/// Which end of a collection a page starts from. Collections are ordered by time: queues by
/// when jobs were pushed, sorted sets by score (run-at, retry-at or death time).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortOrder {
    /// Oldest (or earliest) first. The usual order for the scheduled and retry sets.
    #[default]
    Ascending,
    /// Newest (or latest) first. The usual order for queues and the morgue.
    Descending,
}

/// One page of a collection.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Page<T> {
    /// The items of this page (fewer than `size` on the last page, none past the end).
    pub items: Vec<T>,
    /// The page number, starting at 1.
    pub number: usize,
    /// The requested page size (after capping).
    pub size: usize,
    /// The number of items in the whole collection.
    pub total: u64,
}

impl<T> Page<T> {
    /// Page `number` (from 1) of a collection of `total` items cut into pages of `size`
    /// items, holding `items`.
    pub fn new(items: Vec<T>, number: usize, size: usize, total: u64) -> Self {
        Self {
            items,
            number,
            size,
            total,
        }
    }

    /// The number of pages in the collection (at least 1).
    pub fn total_pages(&self) -> u64 {
        match u64::try_from(self.size) {
            Ok(size) if size > 0 => self.total.div_ceil(size).max(1),
            _ => 1,
        }
    }

    /// `true` if there is a page before this one.
    pub fn has_previous(&self) -> bool {
        self.number > 1
    }

    /// `true` if there is a page after this one.
    pub fn has_next(&self) -> bool {
        u64::try_from(self.number).is_ok_and(|number| number < self.total_pages())
    }

    /// The same page with each item converted by `convert`.
    pub fn map<U>(self, convert: impl FnMut(T) -> U) -> Page<U> {
        Page {
            items: self.items.into_iter().map(convert).collect(),
            number: self.number,
            size: self.size,
            total: self.total,
        }
    }
}

/// Normalizes a requested page number (numbers below 1 mean the first page) and size.
pub(crate) fn normalize(page: usize, page_size: usize) -> (usize, usize) {
    (page.max(1), page_size.min(MAX_PAGE_SIZE))
}

/// The first and last index (inclusive) of a page, or `None` if it is empty or out of range.
pub(crate) fn bounds(page: usize, page_size: usize) -> Option<(i64, i64)> {
    if page_size == 0 {
        return None;
    }
    let start = (page - 1).checked_mul(page_size)?;
    let end = start.checked_add(page_size - 1)?;
    Some((i64::try_from(start).ok()?, i64::try_from(end).ok()?))
}

/// One page of an in-memory collection (Sidekiq's `page_items`). A page number below 1 means
/// the first page; a page past the end falls back to the first page (Sidekiq only does so
/// for pages starting strictly past the end).
pub fn page_items<T>(items: impl IntoIterator<Item = T>, page: usize, page_size: usize) -> Page<T> {
    let items: Vec<T> = items.into_iter().collect();
    let total = u64::try_from(items.len()).unwrap_or(u64::MAX);
    let (mut number, size) = (page.max(1), page_size);
    let mut start = (number - 1).saturating_mul(size);
    if start > 0 && start >= items.len() {
        start = 0;
        number = 1;
    }
    let items = items.into_iter().skip(start).take(size).collect();
    Page {
        items,
        number,
        size,
        total,
    }
}

/// A raw element of a Redis list or sorted set, as returned by [`page_key`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RawEntry {
    /// The element (for hench's own collections, job JSON).
    pub value: String,
    /// The score, for sorted sets.
    pub score: Option<f64>,
}

/// The kinds of keys [`page_key`] can page through.
enum KeyKind {
    List,
    SortedSet,
    Missing,
}

async fn key_kind(api: &Api, key: &str) -> Result<KeyKind> {
    if key == keys::SCHEDULE || key == keys::RETRY || key == keys::DEAD {
        return Ok(KeyKind::SortedSet);
    }
    if key.starts_with(keys::QUEUE_PREFIX) {
        return Ok(KeyKind::List);
    }
    let kind: String = api
        .redis()
        .with_retry(
            |mut conn| async move { ::redis::cmd("TYPE").arg(key).query_async(&mut conn).await },
        )
        .await?;
    match kind.as_str() {
        "list" => Ok(KeyKind::List),
        "zset" => Ok(KeyKind::SortedSet),
        "none" => Ok(KeyKind::Missing),
        other => Err(Error::Redis(::redis::RedisError::from((
            ::redis::ErrorKind::Client,
            "cannot page through this key",
            format!("`{key}` is a {other}, not a list or sorted set"),
        )))),
    }
}

/// One page of any Redis list or sorted set (Sidekiq's `Paginator#page`), read atomically
/// with its size. A missing key is an empty first page. Lists are paged by index (for
/// queues, index 0 is the newest job), sorted sets by score.
///
/// [`Queue::page`](crate::api::Queue::page) and [`JobSet::page`](crate::api::JobSet::page)
/// return typed records; this is the building block for other keys.
pub async fn page_key(
    api: &Api,
    key: &str,
    page: usize,
    page_size: usize,
    order: SortOrder,
) -> Result<Page<RawEntry>> {
    let (number, size) = normalize(page, page_size);
    let (total, items) = match key_kind(api, key).await? {
        KeyKind::Missing => {
            return Ok(Page {
                items: Vec::new(),
                number: 1,
                size,
                total: 0,
            });
        }
        KeyKind::List => {
            let (total, values) = list_page(api, key, number, size, order).await?;
            let items = values
                .into_iter()
                .map(|value| RawEntry { value, score: None })
                .collect();
            (total, items)
        }
        KeyKind::SortedSet => {
            let (total, values) = sorted_set_page(api, key, number, size, order).await?;
            let items = values
                .into_iter()
                .map(|(value, score)| RawEntry {
                    value,
                    score: Some(score),
                })
                .collect();
            (total, items)
        }
    };
    Ok(Page {
        items,
        number,
        size,
        total,
    })
}

/// `(LLEN, page)` of a list, atomically. `Descending` starts at index 0 (for queues: newest
/// first); `Ascending` starts at the end.
pub(crate) async fn list_page(
    api: &Api,
    key: &str,
    number: usize,
    size: usize,
    order: SortOrder,
) -> Result<(u64, Vec<String>)> {
    let Some((start, end)) = bounds(number, size) else {
        let total: u64 = api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("LLEN").arg(key).query_async(&mut conn).await
            })
            .await?;
        return Ok((total, Vec::new()));
    };
    let (start, end) = match order {
        SortOrder::Descending => (start, end),
        SortOrder::Ascending => (-end - 1, -start - 1),
    };
    let mut pipe = ::redis::pipe();
    pipe.atomic()
        .cmd("LLEN")
        .arg(key)
        .cmd("LRANGE")
        .arg(key)
        .arg(start)
        .arg(end);
    let pipe = &pipe;
    let (total, mut values): (u64, Vec<String>) = api
        .redis()
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await?;
    if order == SortOrder::Ascending {
        values.reverse();
    }
    Ok((total, values))
}

/// `(ZCARD, page with scores)` of a sorted set, atomically.
pub(crate) async fn sorted_set_page(
    api: &Api,
    key: &str,
    number: usize,
    size: usize,
    order: SortOrder,
) -> Result<(u64, Vec<(String, f64)>)> {
    let Some((start, end)) = bounds(number, size) else {
        let total: u64 = api
            .redis()
            .with_retry(|mut conn| async move {
                ::redis::cmd("ZCARD").arg(key).query_async(&mut conn).await
            })
            .await?;
        return Ok((total, Vec::new()));
    };
    let mut pipe = ::redis::pipe();
    pipe.atomic().cmd("ZCARD").arg(key);
    let range = pipe.cmd("ZRANGE").arg(key).arg(start).arg(end);
    if order == SortOrder::Descending {
        range.arg("REV");
    }
    range.arg("WITHSCORES");
    let pipe = &pipe;
    api.redis()
        .with_retry(|mut conn| async move { pipe.query_async(&mut conn).await })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numbers(page: Page<i32>) -> (usize, u64, Vec<i32>) {
        (page.number, page.total, page.items)
    }

    #[test]
    fn pages_in_memory_items() {
        let all: Vec<i32> = (1..=100).collect();
        assert_eq!(
            numbers(page_items(all.clone(), 1, 25)),
            (1, 100, (1..=25).collect())
        );
        assert_eq!(
            numbers(page_items(all.clone(), 2, 25)),
            (2, 100, (26..=50).collect())
        );
        assert_eq!(page_items(1..=10, 0, 25).number, 1);
        assert_eq!(
            numbers(page_items(1..=10, 100, 25)),
            (1, 10, (1..=10).collect())
        );
        assert_eq!(numbers(page_items(1..=5, 1, 25)), (1, 5, (1..=5).collect()));
        assert_eq!(numbers(page_items(Vec::new(), 1, 25)), (1, 0, vec![]));
        assert_eq!(numbers(page_items(Vec::new(), 5, 25)), (1, 0, vec![]));
        assert_eq!(numbers(page_items(1..=3, 1, 0)), (1, 3, vec![]));
        assert_eq!(
            numbers(page_items(1..=3, usize::MAX, 2)),
            (1, 3, vec![1, 2])
        );
        // A page starting exactly at the end is out of range too.
        assert_eq!(
            numbers(page_items(1..=50, 3, 25)),
            (1, 50, (1..=25).collect())
        );
        assert_eq!(numbers(page_items(1..=51, 3, 25)), (3, 51, vec![51]));
    }

    #[test]
    fn page_navigation() {
        let page = page_items(1..=51, 2, 25);
        assert_eq!(page.total_pages(), 3);
        assert!(page.has_previous() && page.has_next());
        let last = page_items(1..=51, 3, 25);
        assert!(!last.has_next());
        assert_eq!(last.items, vec![51]);
        let empty = page_items(Vec::<i32>::new(), 1, 25);
        assert_eq!(empty.total_pages(), 1);
        assert!(!empty.has_previous() && !empty.has_next());
        assert_eq!(page_items(1..=3, 1, 0).total_pages(), 1);
        let mapped = page.map(|n| n.to_string());
        assert_eq!(mapped.items[0], "26");
        assert_eq!(mapped.total, 51);
    }

    #[test]
    fn bounds_and_normalization() {
        assert_eq!(normalize(0, 5_000), (1, MAX_PAGE_SIZE));
        assert_eq!(bounds(1, 25), Some((0, 24)));
        assert_eq!(bounds(3, 10), Some((20, 29)));
        assert_eq!(bounds(1, 0), None);
        assert_eq!(bounds(usize::MAX, 1_000), None);
    }
}
