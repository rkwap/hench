//! Query string parameters: parsed leniently where a default makes sense (page, count),
//! strictly where a wrong value would mislead (enumerations), and always bounded.

use hench::api::SortOrder;
use hench::metrics::{Period, SortBy};

use crate::error::WebError;
use crate::strings::errors;

/// Largest page number accepted; larger numbers are clamped (they show an empty or first page).
pub(crate) const MAX_PAGE: usize = 100_000;

/// Largest page size accepted (like Sidekiq: below 1000).
pub(crate) const MAX_COUNT: usize = 999;

/// Longest filter accepted, in characters.
pub(crate) const MAX_FILTER_CHARS: usize = 256;

/// Parameters kept when a link or redirect carries the current list position along.
pub(crate) const SAFE_PARAMS: [&str; 5] = ["page", "count", "direction", "substr", "only"];

/// A parsed query string (repeated keys are kept in order).
#[derive(Debug, Default, Clone)]
pub(crate) struct Query {
    pairs: Vec<(String, String)>,
}

impl Query {
    /// Parses a raw query string (without the `?`).
    pub(crate) fn parse(raw: &str) -> Self {
        Self {
            pairs: form_urlencoded::parse(raw.as_bytes())
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect(),
        }
    }

    /// The first value of `name`.
    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// The page number: 1 when missing, malformed or below 1; at most [`MAX_PAGE`].
    pub(crate) fn page(&self) -> usize {
        match self.get("page").map(str::trim).map(str::parse::<i64>) {
            Some(Ok(page)) if page > 1 => usize::try_from(page).unwrap_or(MAX_PAGE).min(MAX_PAGE),
            _ => 1,
        }
    }

    /// The page size: `default` unless `count` is a number between 1 and [`MAX_COUNT`].
    pub(crate) fn count(&self, default: usize) -> usize {
        match self.get("count").map(str::trim).map(str::parse::<i64>) {
            Some(Ok(count)) if (1..=MAX_COUNT as i64).contains(&count) => {
                usize::try_from(count).unwrap_or(default)
            }
            _ => default,
        }
    }

    /// The sort direction: `asc`, `desc` (or missing: `default`); anything else is refused.
    pub(crate) fn direction(&self, default: SortOrder) -> Result<SortOrder, WebError> {
        match self.get("direction") {
            None | Some("") => Ok(default),
            Some("asc") => Ok(SortOrder::Ascending),
            Some("desc") => Ok(SortOrder::Descending),
            Some(_) => Err(WebError::BadRequest(errors::BAD_DIRECTION.to_owned())),
        }
    }

    /// The filter (`substr`), trimmed; `None` when missing or blank. Refused when longer
    /// than [`MAX_FILTER_CHARS`].
    pub(crate) fn filter(&self) -> Result<Option<String>, WebError> {
        let Some(filter) = self.get("substr").map(str::trim) else {
            return Ok(None);
        };
        if filter.chars().count() > MAX_FILTER_CHARS {
            return Err(WebError::BadRequest(errors::LONG_FILTER.to_owned()));
        }
        Ok((!filter.is_empty()).then(|| filter.to_owned()))
    }

    /// The busy page's `only` parameter.
    pub(crate) fn only(&self) -> Result<Only, WebError> {
        match self.get("only") {
            None | Some("") => Ok(Only::Both),
            Some("jobs") => Ok(Only::Jobs),
            Some("processes") => Ok(Only::Processes),
            Some(_) => Err(WebError::BadRequest(errors::BAD_ONLY.to_owned())),
        }
    }

    /// The metrics period (`period`): one of [`Period::PRESETS`] by its label, `1h` when
    /// missing; anything else is refused. Returns the label and the period.
    pub(crate) fn period(&self) -> Result<(&'static str, Period), WebError> {
        match self.get("period").map(str::trim) {
            None | Some("") => Ok(Period::PRESETS[0]),
            Some(label) => Period::PRESETS
                .iter()
                .find(|(name, _)| *name == label)
                .copied()
                .ok_or_else(|| WebError::BadRequest(errors::BAD_PERIOD.to_owned())),
        }
    }

    /// The order of the metrics table (`sort`): total time when missing; anything but the
    /// known orders is refused.
    pub(crate) fn metrics_sort(&self) -> Result<MetricsSort, WebError> {
        match self.get("sort") {
            None | Some("") => Ok(MetricsSort::Time),
            Some(value) => MetricsSort::ALL
                .into_iter()
                .find(|sort| sort.param() == value)
                .ok_or_else(|| WebError::BadRequest(errors::BAD_SORT.to_owned())),
        }
    }

    /// The dashboard's history length in days: 1 to 180, 30 by default.
    pub(crate) fn days(&self) -> Result<u32, WebError> {
        match self.get("days").map(str::trim) {
            None | Some("") => Ok(30),
            Some(days) => match days.parse::<u32>() {
                Ok(days) if (1..=180).contains(&days) => Ok(days),
                _ => Err(WebError::BadRequest(errors::BAD_DAYS.to_owned())),
            },
        }
    }
}

/// Which tables the busy page shows (`?only=jobs|processes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Only {
    /// Both tables.
    Both,
    /// Only the running jobs.
    Jobs,
    /// Only the processes.
    Processes,
}

/// How the metrics table is ordered (`?sort=`), largest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricsSort {
    /// Total execution time (`time`, the default).
    Time,
    /// Number of executions (`count`).
    Count,
    /// Number of failures (`failures`).
    Failures,
    /// Average execution time (`average`).
    Average,
}

impl MetricsSort {
    /// Every order, in the table's column order.
    pub(crate) const ALL: [MetricsSort; 4] = [
        MetricsSort::Count,
        MetricsSort::Failures,
        MetricsSort::Time,
        MetricsSort::Average,
    ];

    /// The value of the `sort` parameter.
    pub(crate) fn param(self) -> &'static str {
        match self {
            MetricsSort::Time => "time",
            MetricsSort::Count => "count",
            MetricsSort::Failures => "failures",
            MetricsSort::Average => "average",
        }
    }

    /// The query result's order.
    pub(crate) fn by(self) -> SortBy {
        match self {
            MetricsSort::Time => SortBy::TotalTime,
            MetricsSort::Count => SortBy::Processed,
            MetricsSort::Failures => SortBy::Failed,
            MetricsSort::Average => SortBy::AverageTime,
        }
    }
}

/// Builds a query string from `pairs` (empty values are left out). Returns `""` or `?...`.
pub(crate) fn query_string(pairs: &[(&str, &str)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    let mut any = false;
    for (key, value) in pairs {
        if !value.is_empty() {
            serializer.append_pair(key, value);
            any = true;
        }
    }
    if any {
        format!("?{}", serializer.finish())
    } else {
        String::new()
    }
}

/// The [`SAFE_PARAMS`] of a raw query string, re-encoded, with `overrides` applied (an empty
/// override removes the parameter). Returns `""` or `?...`.
pub(crate) fn carry(raw: &str, overrides: &[(&str, &str)]) -> String {
    let query = Query::parse(raw);
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    for name in SAFE_PARAMS {
        let value = match overrides.iter().find(|(key, _)| *key == name) {
            Some((_, value)) => Some(*value),
            None => query.get(name),
        };
        if let Some(value) = value {
            pairs.push((name, value));
        }
    }
    query_string(&pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_are_lenient_and_bounded() {
        let query = |raw: &str| Query::parse(raw);
        assert_eq!(query("").page(), 1);
        assert_eq!(query("page=3").page(), 3);
        assert_eq!(query("page=0").page(), 1);
        assert_eq!(query("page=-4").page(), 1);
        assert_eq!(query("page=abc").page(), 1);
        assert_eq!(query("page=99999999999999999999").page(), 1);
        assert_eq!(query("page=9999999").page(), MAX_PAGE);
        assert_eq!(query("count=10").count(25), 10);
        // Sidekiq's "does not crash on a negative count parameter" scenario.
        assert_eq!(query("count=-1").count(25), 25);
        assert_eq!(query("count=1000").count(25), 25);
        assert_eq!(query("count=999").count(25), 999);
        assert_eq!(query("count=x").count(100), 100);
    }

    #[test]
    fn enumerations_are_strict() {
        let query = |raw: &str| Query::parse(raw);
        assert_eq!(
            query("").direction(SortOrder::Descending).unwrap(),
            SortOrder::Descending
        );
        assert_eq!(
            query("direction=asc")
                .direction(SortOrder::Descending)
                .unwrap(),
            SortOrder::Ascending
        );
        assert!(
            query("direction=up")
                .direction(SortOrder::Ascending)
                .is_err()
        );
        assert_eq!(query("only=jobs").only().unwrap(), Only::Jobs);
        assert_eq!(query("only=processes").only().unwrap(), Only::Processes);
        assert_eq!(query("").only().unwrap(), Only::Both);
        assert!(query("only=all").only().is_err());
        assert_eq!(query("").days().unwrap(), 30);
        assert_eq!(query("days=7").days().unwrap(), 7);
        assert!(query("days=0").days().is_err());
        assert!(query("days=181").days().is_err());
        assert!(query("days=1000000").days().is_err());
        assert!(query("days=x").days().is_err());
        assert_eq!(query("").period().unwrap(), ("1h", Period::Minutes(60)));
        assert_eq!(
            query("period=4h").period().unwrap(),
            ("4h", Period::Minutes(240))
        );
        assert_eq!(
            query("period=72h").period().unwrap(),
            ("72h", Period::Hours(72))
        );
        assert!(query("period=3h").period().is_err());
        assert!(query("period=1H").period().is_err());
        assert_eq!(query("").metrics_sort().unwrap(), MetricsSort::Time);
        for sort in MetricsSort::ALL {
            let raw = format!("sort={}", sort.param());
            assert_eq!(query(&raw).metrics_sort().unwrap(), sort);
        }
        assert_eq!(MetricsSort::Count.by(), SortBy::Processed);
        assert!(query("sort=name").metrics_sort().is_err());
    }

    #[test]
    fn filters_are_trimmed_and_bounded() {
        let query = |raw: &str| Query::parse(raw);
        assert_eq!(query("").filter().unwrap(), None);
        assert_eq!(query("substr=+").filter().unwrap(), None);
        assert_eq!(
            query("substr=+MIKE1234+").filter().unwrap().as_deref(),
            Some("MIKE1234")
        );
        assert_eq!(
            query("substr=%3Ca%3E").filter().unwrap().as_deref(),
            Some("<a>")
        );
        let long = format!("substr={}", "x".repeat(MAX_FILTER_CHARS + 1));
        assert!(query(&long).filter().is_err());
        let limit = format!("substr={}", "é".repeat(MAX_FILTER_CHARS));
        assert!(query(&limit).filter().is_ok());
    }

    #[test]
    fn query_strings_escape_values() {
        // Sidekiq's to_query_string / qparams scenarios.
        assert_eq!(query_string(&[("page", "B<H")]), "?page=B%3CH");
        assert_eq!(query_string(&[("page", "a b&c")]), "?page=a+b%26c");
        assert_eq!(query_string(&[("page", "")]), "");
        assert_eq!(
            carry("direction=H%3EB&jid=abc", &[("page", "B<H")]),
            "?page=B%3CH&direction=H%3EB"
        );
        assert_eq!(
            carry("page=2&jid=abc&direction=asc&only=jobs", &[]),
            "?page=2&direction=asc&only=jobs"
        );
        assert_eq!(carry("page=2&substr=x", &[("page", "")]), "?substr=x");
        assert_eq!(carry("", &[]), "");
    }
}
