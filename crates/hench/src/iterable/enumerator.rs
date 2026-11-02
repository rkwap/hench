//! The sequences iterable jobs walk through.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream, Stream, StreamExt};

use crate::error::JobError;

/// The items an [iterable job](super::IterableJob) processes, each paired with the cursor
/// reached once it is processed.
///
/// An enumerator built from a cursor must yield exactly the items that come after the item
/// whose cursor it was: for positions (vectors, iterators, ranges) the cursor is the position
/// of the next item, for keyset pagination the key of the last item handled. The job saves the
/// cursor of the last item it processed, and builds a new enumerator from it when it resumes.
///
/// Build one from any [`Stream`] or iterator of `(item, cursor)` pairs, or with the helpers for
/// the usual cases: [`Enumerator::vec`], [`Enumerator::iter`], [`Enumerator::range`] and the
/// keyset pagination helpers [`Enumerator::paginate`] and [`Enumerator::batches`].
///
/// ```
/// use futures::StreamExt;
/// use hench::iterable::Enumerator;
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// // Resuming after two items: the third one comes next, and its cursor is 3.
/// let mut items = Enumerator::vec(vec!["a", "b", "c", "d"], Some(2));
/// assert_eq!(items.next().await.unwrap().unwrap(), ("c", 3));
/// # });
/// ```
pub struct Enumerator<'a, T, C> {
    stream: BoxStream<'a, Result<(T, C), JobError>>,
}

impl<'a, T, C> Enumerator<'a, T, C>
where
    T: Send + 'a,
    C: Send + 'a,
{
    /// An enumerator over a stream of `(item, cursor)` pairs that may fail: an error stops the
    /// iteration and fails the job, which is retried from the last cursor saved.
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<(T, C), JobError>> + Send + 'a,
    {
        Self {
            stream: stream.boxed(),
        }
    }

    /// An enumerator over a stream of `(item, cursor)` pairs.
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: Stream<Item = (T, C)> + Send + 'a,
    {
        Self::new(stream.map(Ok))
    }

    /// An enumerator over `(item, cursor)` pairs.
    pub fn from_pairs<I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (T, C)>,
        I::IntoIter: Send + 'a,
    {
        Self::from_stream(stream::iter(pairs))
    }

    /// An enumerator with nothing to iterate: the job completes right away.
    pub fn empty() -> Self {
        Self::new(stream::empty())
    }

    /// Keyset pagination: `fetch` returns the page of `(item, cursor)` pairs that comes after
    /// a cursor (`None`: the first page); the items are yielded one by one, and the next page
    /// is fetched after the last cursor of the previous one. The iteration ends with the
    /// first empty page; a failing fetch fails the job.
    ///
    /// ```
    /// use hench::JobError;
    /// use hench::iterable::Enumerator;
    ///
    /// // Rows of a table ordered by id, 100 at a time, resuming after `cursor`.
    /// fn rows(cursor: Option<u64>) -> Enumerator<'static, (u64, String), u64> {
    ///     Enumerator::paginate(cursor, |after| async move {
    ///         let first = after.map_or(1, |id| id + 1);
    ///         let page: Vec<((u64, String), u64)> = (first..first + 100)
    ///             .take_while(|id| *id <= 250)
    ///             .map(|id| ((id, format!("row {id}")), id))
    ///             .collect();
    ///         Ok::<_, JobError>(page)
    ///     })
    /// }
    /// # let _ = rows(None);
    /// ```
    pub fn paginate<F, Fut>(cursor: Option<C>, fetch: F) -> Self
    where
        C: Clone,
        F: FnMut(Option<C>) -> Fut + Send + 'a,
        Fut: Future<Output = Result<Vec<(T, C)>, JobError>> + Send + 'a,
    {
        struct Pages<T, C, F> {
            cursor: Option<C>,
            fetch: F,
            page: VecDeque<(T, C)>,
            done: bool,
        }
        let pages = Pages {
            cursor,
            fetch,
            page: VecDeque::new(),
            done: false,
        };
        Self::new(stream::unfold(pages, |mut pages| async move {
            loop {
                if let Some((item, cursor)) = pages.page.pop_front() {
                    pages.cursor = Some(cursor.clone());
                    return Some((Ok((item, cursor)), pages));
                }
                if pages.done {
                    return None;
                }
                match (pages.fetch)(pages.cursor.clone()).await {
                    Ok(page) if page.is_empty() => return None,
                    Ok(page) => pages.page = page.into(),
                    Err(error) => {
                        pages.done = true;
                        return Some((Err(error), pages));
                    }
                }
            }
        }))
    }
}

impl<'a, T, C> Enumerator<'a, Vec<T>, C>
where
    T: Send + 'a,
    C: Clone + Send + 'a,
{
    /// Keyset pagination by batch: like [`Enumerator::paginate`], but each page is one item
    /// (a vector of the page's items), whose cursor is the cursor of the page's last item.
    pub fn batches<F, Fut>(cursor: Option<C>, fetch: F) -> Self
    where
        F: FnMut(Option<C>) -> Fut + Send + 'a,
        Fut: Future<Output = Result<Vec<(T, C)>, JobError>> + Send + 'a,
    {
        Self::new(stream::unfold(
            (cursor, fetch, false),
            |(cursor, mut fetch, done)| async move {
                if done {
                    return None;
                }
                match fetch(cursor).await {
                    Err(error) => Some((Err(error), (None, fetch, true))),
                    Ok(page) => {
                        let last = page.last().map(|(_, cursor)| cursor.clone())?;
                        let items = page.into_iter().map(|(item, _)| item).collect();
                        Some((Ok((items, last.clone())), (Some(last), fetch, false)))
                    }
                }
            },
        ))
    }
}

impl<'a, T> Enumerator<'a, T, usize>
where
    T: Send + 'a,
{
    /// The items of a vector, starting at position `cursor` (the number of items already
    /// processed); each item's cursor is its position plus one.
    pub fn vec(items: Vec<T>, cursor: Option<usize>) -> Self {
        Self::iter(items, cursor)
    }

    /// The items of an iterator, skipping the first `cursor` (the number of items already
    /// processed); each item's cursor is its position plus one. Resuming walks through the
    /// skipped items again, so prefer a source that can start at a position (a range, a
    /// paginated query) for long sequences.
    pub fn iter<I>(items: I, cursor: Option<usize>) -> Self
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: Send + 'a,
    {
        let start = cursor.unwrap_or(0);
        Self::from_stream(stream::iter(
            items.into_iter().skip(start).zip(start.saturating_add(1)..),
        ))
    }
}

impl<'a, N> Enumerator<'a, N, N>
where
    N: RangeCursor + 'a,
{
    /// The integers of `range`, starting at `cursor` (the next integer to process) when it is
    /// within the range; each integer's cursor is the integer that follows it.
    ///
    /// ```
    /// use futures::StreamExt;
    /// use hench::iterable::Enumerator;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let resumed: Vec<(i64, i64)> = Enumerator::range(0..10_i64, Some(8))
    ///     .map(|item| item.unwrap())
    ///     .collect()
    ///     .await;
    /// assert_eq!(resumed, [(8, 9), (9, 10)]);
    /// # });
    /// ```
    pub fn range(range: Range<N>, cursor: Option<N>) -> Self {
        let start = match cursor {
            Some(cursor) if cursor > range.start => cursor,
            _ => range.start,
        };
        let end = range.end;
        Self::from_stream(stream::unfold(Some(start), move |next| async move {
            let current = next.filter(|current| *current < end)?;
            let after = current.successor();
            // `after` exists: `current` is below `end`.
            after.map(|after| ((current, after), Some(after)))
        }))
    }
}

impl<T, C> Stream for Enumerator<'_, T, C> {
    type Item = Result<(T, C), JobError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<T, C> fmt::Debug for Enumerator<'_, T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Enumerator").finish_non_exhaustive()
    }
}

/// Integer types [`Enumerator::range`] works with. Sealed: implemented for the primitive
/// integers.
pub trait RangeCursor: sealed::Sealed + Copy + Ord + Send {
    /// The next integer, `None` on overflow.
    fn successor(self) -> Option<Self>;
}

mod sealed {
    pub trait Sealed {}
}

macro_rules! range_cursor {
    ($($ty:ty),*) => {$(
        impl sealed::Sealed for $ty {}

        impl RangeCursor for $ty {
            fn successor(self) -> Option<Self> {
                self.checked_add(1)
            }
        }
    )*};
}

range_cursor!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn collect<T: Send, C: Send>(enumerator: Enumerator<'_, T, C>) -> Vec<(T, C)> {
        enumerator
            .map(|item| item.unwrap())
            .collect::<Vec<_>>()
            .await
    }

    #[tokio::test]
    async fn vectors_resume_at_their_cursor() {
        assert_eq!(
            collect(Enumerator::vec(vec![10, 11, 12], None)).await,
            [(10, 1), (11, 2), (12, 3)]
        );
        assert_eq!(
            collect(Enumerator::vec(vec![10, 11, 12], Some(2))).await,
            [(12, 3)]
        );
        assert!(
            collect(Enumerator::vec(vec![10, 11, 12], Some(9)))
                .await
                .is_empty()
        );
        assert_eq!(
            collect(Enumerator::iter("abc".chars(), Some(1))).await,
            [('b', 2), ('c', 3)]
        );
    }

    #[tokio::test]
    async fn ranges_resume_at_their_cursor() {
        assert_eq!(
            collect(Enumerator::range(3..6_u32, None)).await,
            [(3, 4), (4, 5), (5, 6)]
        );
        assert_eq!(
            collect(Enumerator::range(3..6_u32, Some(5))).await,
            [(5, 6)]
        );
        // A cursor before the start is ignored, one past the end leaves nothing.
        assert_eq!(
            collect(Enumerator::range(3..5_i64, Some(-4))).await.len(),
            2
        );
        assert!(
            collect(Enumerator::range(3..5_i64, Some(5)))
                .await
                .is_empty()
        );
        let (start, end) = (5_i64, 3_i64);
        assert!(
            collect(Enumerator::range(start..end, None))
                .await
                .is_empty()
        );
        // The last value of the type has no successor to be its cursor.
        assert_eq!(
            collect(Enumerator::range(254..u8::MAX, None)).await,
            [(254, 255)]
        );
    }

    #[tokio::test]
    async fn pagination_fetches_after_the_last_cursor() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let counter = fetches.clone();
        let enumerator = Enumerator::paginate(Some(2_u32), move |after| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                let first = after.map_or(0, |after| after + 1);
                Ok((first..(first + 3).min(10))
                    .map(|id| (id * 10, id))
                    .collect())
            }
        });
        let items = collect(enumerator).await;
        assert_eq!(items.first(), Some(&(30, 3)));
        assert_eq!(items.last(), Some(&(90, 9)));
        assert_eq!(items.len(), 7);
        // Pages 3-5, 6-8, 9, then an empty page.
        assert_eq!(fetches.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn pagination_errors_end_the_stream() {
        let mut enumerator = Enumerator::<u32, u32>::paginate(None, |_| async {
            Err(JobError::msg("database down"))
        });
        let error = enumerator.next().await.unwrap().unwrap_err();
        assert_eq!(error.message(), "database down");
        assert!(enumerator.next().await.is_none());
    }

    #[tokio::test]
    async fn batches_yield_whole_pages() {
        let enumerator = Enumerator::batches(None, |after: Option<u32>| async move {
            let first = after.map_or(1, |after| after + 1);
            Ok((first..(first + 3).min(11)).map(|id| (id, id)).collect())
        });
        let batches = collect(enumerator).await;
        let sizes: Vec<usize> = batches.iter().map(|(batch, _)| batch.len()).collect();
        assert_eq!(sizes, [3, 3, 3, 1]);
        assert_eq!(batches[1], (vec![4, 5, 6], 6));
        let failing =
            Enumerator::<Vec<u32>, u32>::batches(None, |_| async { Err(JobError::msg("nope")) });
        let results: Vec<_> = failing.collect().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[tokio::test]
    async fn other_sources() {
        assert!(collect(Enumerator::<u8, u8>::empty()).await.is_empty());
        assert_eq!(
            collect(Enumerator::from_pairs([("a", 1), ("b", 2)])).await,
            [("a", 1), ("b", 2)]
        );
        let stream = stream::iter([Ok((1, 1)), Err(JobError::msg("x"))]);
        let results: Vec<_> = Enumerator::new(stream).collect().await;
        assert!(results[0].is_ok() && results[1].is_err());
        assert!(format!("{:?}", Enumerator::<u8, u8>::empty()).contains("Enumerator"));
    }
}
