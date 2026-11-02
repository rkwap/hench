//! `GET /metrics` and `GET /metrics/{class}`: what the workers recorded about each job class
//! ([`hench::metrics`]): the busiest classes of a period, and one class in detail (executions
//! per minute, the distribution of its execution times, deploys).

use axum::extract::Path;
use axum::response::Response;
use chrono::{DateTime, SecondsFormat, Utc};
use hench::metrics::{
    self, BUCKET_BOUNDS_MS, BUCKETS, Granularity, Histogram, JobMetrics, Period, QueryResult,
};
use http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::ctx::Ctx;
use crate::error::{WebError, WebResult};
use crate::format;
use crate::params::{MetricsSort, Query, query_string};
use crate::strings::{self, EN, errors};
use crate::templates::{JobMetricsPage, MetricsPage};
use crate::views::{
    FactView, MarkView, MetricsRow, RangeLink, SortColumn, TableRow, Totals, segment,
};

/// Rows of the metrics table: the first job classes in the selected order (a filter finds the
/// others).
const TOP_JOBS: usize = 25;

/// Job classes charted when the page opens (viewers can tick up to eight).
const CHARTED: usize = 5;

/// Longest job class name the pages accept.
const MAX_CLASS_BYTES: usize = 1_024;

/// The period one job class's page falls back to: execution-time histograms are kept per
/// minute for 8 hours, so its page never reads ten-minute buckets.
const LONGEST_JOB_PERIOD: (&str, Period) = ("8h", Period::Minutes(480));

/// The period selected when none is given (the only one that refreshes itself).
const DEFAULT_PERIOD: &str = "1h";

/// The `{class}` of the job metrics route (extracted by name, see `queues::QueueName`).
#[derive(Deserialize)]
pub(crate) struct JobClass {
    class: String,
}

/// `GET /metrics`.
pub(crate) async fn overview(ctx: Ctx) -> Response {
    let result = render_overview(&ctx).await;
    super::finish(&ctx, result)
}

/// `GET /metrics/{class}`.
pub(crate) async fn job(ctx: Ctx, Path(JobClass { class }): Path<JobClass>) -> Response {
    let result = render_job(&ctx, &class).await;
    super::finish(&ctx, result)
}

/// The metrics pages answer 404 when the dashboard was built without them.
fn ensure_enabled(ctx: &Ctx) -> WebResult<()> {
    if ctx.web.metrics {
        Ok(())
    } else {
        Err(WebError::NotFound(errors::NO_SUCH_PAGE.to_owned()))
    }
}

async fn render_overview(ctx: &Ctx) -> WebResult<Response> {
    ensure_enabled(ctx)?;
    let query = Query::parse(&ctx.query);
    let (label, period) = query.period()?;
    let sort = query.metrics_sort()?;
    let filter = query.filter()?.unwrap_or_default();
    let api = ctx.web.api();
    let stats = api.stats().await?;
    let result = metrics::Query::new(api.redis())
        .top_jobs(period, Some(filter.as_str()))
        .await?;

    let base = ctx.url("/metrics");
    let link = |period: &str, sort: MetricsSort, filter: &str| -> String {
        let period = if period == DEFAULT_PERIOD { "" } else { period };
        let sort = if sort == MetricsSort::Time {
            ""
        } else {
            sort.param()
        };
        format!(
            "{base}{}",
            query_string(&[("period", period), ("sort", sort), ("substr", filter)])
        )
    };
    let periods = Period::PRESETS
        .iter()
        .map(|&(name, _)| RangeLink {
            label: name,
            href: link(name, sort, &filter),
            active: name == label,
        })
        .collect();
    let columns = MetricsSort::ALL
        .into_iter()
        .map(|column| SortColumn {
            label: column_label(column),
            href: link(label, column, &filter),
            active: column == sort,
        })
        .collect();

    let sorted = result.sorted(sort.by());
    let total = sorted.len();
    let shown = &sorted[..total.min(TOP_JOBS)];
    let charted = &shown[..shown.len().min(CHARTED)];
    let job_query = query_string(&[("period", if label == DEFAULT_PERIOD { "" } else { label })]);
    // The features' columns that some shown class has a value in.
    let features: Vec<FeatureColumn> = FEATURE_COLUMNS
        .into_iter()
        .filter(|(_, pick)| shown.iter().any(|(_, metrics)| pick(&metrics.totals) > 0))
        .collect();
    let rows = shown
        .iter()
        .enumerate()
        .map(|(index, (class, metrics))| MetricsRow {
            class: (*class).to_owned(),
            href: format!("{base}/{}{job_query}", segment(class)),
            toggle_label: strings::chart_toggle(class),
            executions: format::delimit(metrics.totals.processed),
            failed: format::delimit(metrics.totals.failed),
            features: features
                .iter()
                .map(|(_, pick)| format::delimit(pick(&metrics.totals)))
                .collect(),
            total: format::decimal(metrics.totals.seconds(), 2),
            average: format::decimal(metrics.totals.average_ms(), 1),
            charted: index < CHARTED,
        })
        .collect();

    let buckets = result.buckets();
    let summary = if filter.is_empty() {
        strings::metrics_summary(total, label)
    } else {
        strings::metrics_matching(total, &filter, label)
    };
    let page = MetricsPage {
        layout: super::layout(ctx, Some(EN.metrics), Some(Totals::new(&stats)), false),
        s: &EN,
        periods,
        period: label,
        sort: sort.param(),
        default_sort: sort == MetricsSort::Time,
        filter_action: base.clone(),
        clear_href: link(label, sort, ""),
        filter_value: filter,
        summary,
        columns,
        feature_columns: features.iter().map(|(name, _)| *name).collect(),
        rows,
        cut_note: (total > shown.len()).then(|| strings::metrics_cut(shown.len(), total)),
        chart_title: match result.granularity {
            Granularity::Minute => EN.time_per_minute,
            Granularity::TenMinutes => EN.time_per_ten_minutes,
        },
        chart_json: overview_chart(&result, &buckets, shown),
        chart_head: charted
            .iter()
            .map(|(class, _)| (*class).to_owned())
            .collect(),
        chart_rows: overview_table(&buckets, charted),
        marks: marks(&result),
        window: window(&result),
        auto_refresh: label == DEFAULT_PERIOD,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// A column of the features' outcomes: its header and the counter it shows.
type FeatureColumn = (&'static str, fn(&metrics::Counts) -> u64);

/// The outcomes of the features, each a column of the metrics table when some class has a
/// non-zero value in the period (`docs/design/pro-features.md` §2.8): rescheduled over a
/// limiter's limit, parked by a throttle, skipped as a member of a cancelled batch.
const FEATURE_COLUMNS: [FeatureColumn; 3] = [
    (EN.over_limit, |counts| counts.over_limit),
    (EN.throttled, |counts| counts.throttled),
    (EN.skipped, |counts| counts.skipped),
];

/// The header of the metrics table column that sorts by `sort`.
fn column_label(sort: MetricsSort) -> &'static str {
    match sort {
        MetricsSort::Count => EN.executions,
        MetricsSort::Failures => EN.failed,
        MetricsSort::Time => EN.total_time,
        MetricsSort::Average => EN.average_time,
    }
}

/// The data of the metrics page's chart: the seconds each shown class ran per bucket.
fn overview_chart(
    result: &QueryResult,
    buckets: &[DateTime<Utc>],
    shown: &[(&str, &JobMetrics)],
) -> String {
    let series: Vec<Value> = shown
        .iter()
        .map(|(class, metrics)| {
            let values: Vec<f64> = buckets
                .iter()
                .map(|bucket| {
                    metrics
                        .series
                        .get(bucket)
                        .map_or(0.0, |counts| counts.seconds())
                })
                .collect();
            json!({ "name": class, "values": values })
        })
        .collect();
    json!({
        "buckets": buckets.iter().map(iso).collect::<Vec<_>>(),
        "granularity": granularity_name(result.granularity),
        "series": series,
        "visible": CHARTED,
        "decimals": 3,
        "unit": EN.seconds_suffix,
        "marks": marks_json(result),
    })
    .to_string()
}

/// The table view of the metrics chart: the seconds of the charted classes per bucket, newest
/// first, buckets where none of them took any time left out. (`metrics.js` rebuilds it the
/// same way from the chart's data when the viewer ticks other classes.)
fn overview_table(buckets: &[DateTime<Utc>], charted: &[(&str, &JobMetrics)]) -> Vec<TableRow> {
    buckets
        .iter()
        .rev()
        .filter_map(|bucket| {
            let values: Vec<f64> = charted
                .iter()
                .map(|(_, metrics)| {
                    metrics
                        .series
                        .get(bucket)
                        .map_or(0.0, |counts| counts.seconds())
                })
                .collect();
            values
                .iter()
                .any(|seconds| *seconds > 0.0)
                .then(|| TableRow {
                    label: bucket_label(*bucket),
                    values: values
                        .into_iter()
                        .map(|seconds| format::decimal(seconds, 3))
                        .collect(),
                })
        })
        .collect()
}

async fn render_job(ctx: &Ctx, class: &str) -> WebResult<Response> {
    ensure_enabled(ctx)?;
    if class.is_empty() || class.len() > MAX_CLASS_BYTES {
        return Err(WebError::NotFound(errors::NO_SUCH_PAGE.to_owned()));
    }
    let query = Query::parse(&ctx.query);
    let (requested, period) = query.period()?;
    // Histograms are only kept per minute: longer periods show the longest minute period.
    let ((label, period), period_note) = match period.granularity() {
        Granularity::Minute => ((requested, period), None),
        Granularity::TenMinutes => (LONGEST_JOB_PERIOD, Some(strings::JOB_PERIOD_NOTE)),
    };
    let api = ctx.web.api();
    let stats = api.stats().await?;
    let result = metrics::Query::new(api.redis())
        .for_job(class, period)
        .await?;
    let job = result.jobs.get(class).cloned().unwrap_or_default();

    let base = ctx.url("/metrics");
    let own = format!("{base}/{}", segment(class));
    let periods = Period::PRESETS
        .iter()
        .filter(|(_, preset)| preset.granularity() == Granularity::Minute)
        .map(|&(name, _)| RangeLink {
            label: name,
            href: format!(
                "{own}{}",
                query_string(&[("period", if name == DEFAULT_PERIOD { "" } else { name })])
            ),
            active: name == label,
        })
        .collect();
    let back_href = format!(
        "{base}{}",
        query_string(&[(
            "period",
            if requested == DEFAULT_PERIOD {
                ""
            } else {
                requested
            }
        )])
    );
    let buckets = result.buckets();
    let histogram = job.histogram();
    let heading = strings::job_metrics_heading(class);
    let page = JobMetricsPage {
        layout: super::layout(ctx, Some(&heading), Some(Totals::new(&stats)), false),
        s: &EN,
        class: class.to_owned(),
        back_href,
        periods,
        period_note,
        empty: job.totals.processed == 0,
        empty_text: strings::no_job_metrics(class, label),
        facts: facts(&job, &histogram),
        series_title: EN.executions_per_minute,
        series_json: job_series(&result, &buckets, &job),
        histogram_json: histogram_chart(&histogram),
        histogram_rows: histogram_table(&histogram),
        heatmap_json: heatmap_chart(&result, &buckets, &job, &histogram),
        minute_rows: minute_table(&buckets, &job),
        marks: marks(&result),
        window: window(&result),
        auto_refresh: label == DEFAULT_PERIOD,
    };
    Ok(super::render(ctx, StatusCode::OK, &page))
}

/// The figures summing up one job class over the period.
fn facts(job: &JobMetrics, histogram: &Histogram) -> Vec<FactView> {
    let totals = &job.totals;
    let quantile =
        |q: f64| quantile_bucket(histogram, q).map_or_else(|| "–".to_owned(), quantile_label);
    let mut facts = vec![
        FactView {
            label: EN.executions,
            value: format::delimit(totals.processed),
        },
        FactView {
            label: EN.failed,
            value: format::delimit(totals.failed),
        },
        FactView {
            label: EN.failure_rate,
            value: format!("{}%", format::percent(totals.failed, totals.processed)),
        },
        FactView {
            label: EN.total_time_fact,
            value: format!("{} s", format::decimal(totals.seconds(), 2)),
        },
        FactView {
            label: EN.average_fact,
            value: format::duration_ms(totals.average_ms()),
        },
        FactView {
            label: EN.median,
            value: quantile(0.5),
        },
        FactView {
            label: EN.p95,
            value: quantile(0.95),
        },
    ];
    // The features' outcomes, when the class has some in the period.
    for (label, pick) in FEATURE_COLUMNS {
        let count = pick(totals);
        if count > 0 {
            facts.push(FactView {
                label,
                value: format::delimit(count),
            });
        }
    }
    facts
}

/// Executions and failures of one class per bucket.
fn job_series(result: &QueryResult, buckets: &[DateTime<Utc>], job: &JobMetrics) -> String {
    let counts = |pick: fn(&metrics::Counts) -> u64| -> Vec<u64> {
        buckets
            .iter()
            .map(|bucket| job.series.get(bucket).map_or(0, pick))
            .collect()
    };
    json!({
        "buckets": buckets.iter().map(iso).collect::<Vec<_>>(),
        "granularity": granularity_name(result.granularity),
        "series": [
            { "name": EN.executions, "values": counts(|counts| counts.processed) },
            { "name": EN.failed, "values": counts(|counts| counts.failed) },
        ],
        "visible": 2,
        "decimals": 0,
        "unit": "",
        "marks": marks_json(result),
    })
    .to_string()
}

/// The duration buckets worth showing: from the fastest to the slowest one with executions,
/// with one empty bucket on each side when there is one (so the shape reads as a curve).
fn shown_buckets(histogram: &Histogram) -> std::ops::RangeInclusive<usize> {
    let counts = histogram.counts();
    let first = counts.iter().position(|count| *count > 0);
    let last = counts.iter().rposition(|count| *count > 0);
    match (first, last) {
        (Some(first), Some(last)) => first.saturating_sub(1)..=(last + 1).min(BUCKETS - 1),
        _ => 0..=0,
    }
}

/// Executions per duration bucket over the period.
fn histogram_chart(histogram: &Histogram) -> String {
    let range = shown_buckets(histogram);
    let indexes: Vec<usize> = range.collect();
    json!({
        "labels": indexes.iter().map(|index| short_label(*index)).collect::<Vec<_>>(),
        "ranges": indexes.iter().map(|index| bucket_range(*index)).collect::<Vec<_>>(),
        "counts": indexes.iter().map(|index| histogram.count(*index)).collect::<Vec<_>>(),
    })
    .to_string()
}

/// The table view of the histogram: every shown duration bucket and its executions.
fn histogram_table(histogram: &Histogram) -> Vec<TableRow> {
    shown_buckets(histogram)
        .map(|index| TableRow {
            label: bucket_range(index),
            values: vec![format::delimit(histogram.count(index))],
        })
        .collect()
}

/// Executions per minute and duration bucket: the heat map's rows (fastest first: the buckets
/// `histogram`, the period's, shows) and columns (the period's minutes, oldest first).
fn heatmap_chart(
    result: &QueryResult,
    buckets: &[DateTime<Utc>],
    job: &JobMetrics,
    histogram: &Histogram,
) -> String {
    let range = shown_buckets(histogram);
    let rows: Vec<Value> = range
        .map(|index| {
            let counts: Vec<u64> = buckets
                .iter()
                .map(|bucket| {
                    job.histograms
                        .get(bucket)
                        .map_or(0, |minute| minute.count(index))
                })
                .collect();
            json!({
                "label": short_label(index),
                "range": bucket_range(index),
                "counts": counts,
            })
        })
        .collect();
    json!({
        "buckets": buckets.iter().map(iso).collect::<Vec<_>>(),
        "granularity": granularity_name(result.granularity),
        "rows": rows,
        "marks": marks_json(result),
    })
    .to_string()
}

/// The table view of the series and the heat map: each minute with executions, newest first,
/// with its median and 95th percentile.
fn minute_table(buckets: &[DateTime<Utc>], job: &JobMetrics) -> Vec<TableRow> {
    buckets
        .iter()
        .rev()
        .filter_map(|bucket| {
            let counts = job.series.get(bucket)?;
            let histogram = job.histograms.get(bucket).copied().unwrap_or_default();
            let quantile = |q: f64| {
                quantile_bucket(&histogram, q).map_or_else(|| "–".to_owned(), quantile_label)
            };
            Some(TableRow {
                label: bucket_label(*bucket),
                values: vec![
                    format::delimit(counts.processed),
                    format::delimit(counts.failed),
                    format::decimal(counts.seconds(), 3),
                    quantile(0.5),
                    quantile(0.95),
                ],
            })
        })
        .collect()
}

/// The bucket holding the `q` quantile (`0 < q <= 1`) of the executions counted in
/// `histogram`, or `None` when it is empty: the bucket of the execution ranked `ceil(q × n)`.
fn quantile_bucket(histogram: &Histogram, q: f64) -> Option<usize> {
    let total = histogram.total();
    if total == 0 {
        return None;
    }
    let rank = ((q.clamp(0.0, 1.0) * total as f64).ceil() as u64).clamp(1, total);
    let mut seen = 0_u64;
    for (index, count) in histogram.counts().iter().enumerate() {
        seen = seen.saturating_add(*count);
        if seen >= rank {
            return Some(index);
        }
    }
    Some(BUCKETS - 1)
}

/// A quantile as the bound of its bucket: `"< 45 ms"`, or `"≥ 335 s"` for the last bucket.
fn quantile_label(index: usize) -> String {
    match BUCKET_BOUNDS_MS.get(index) {
        Some(bound) => format!("< {}", format::millis(*bound)),
        None => format!("≥ {}", format::millis(BUCKET_BOUNDS_MS[BUCKETS - 2])),
    }
}

/// A duration bucket on a chart axis: its bound, `"<20ms"` … `"<335s"`, then `">335s"`.
fn short_label(index: usize) -> String {
    if index + 1 < BUCKETS {
        format!("<{}", Histogram::label(index))
    } else {
        Histogram::label(index)
    }
}

/// The durations a bucket counts: `"under 20 ms"`, `"20 ms – 30 ms"`, `"335 s or more"`.
fn bucket_range(index: usize) -> String {
    match (
        index
            .checked_sub(1)
            .map(|lower| BUCKET_BOUNDS_MS.get(lower)),
        BUCKET_BOUNDS_MS.get(index),
    ) {
        (None, Some(upper)) => format!("under {}", format::millis(*upper)),
        (Some(Some(lower)), Some(upper)) => {
            format!("{} – {}", format::millis(*lower), format::millis(*upper))
        }
        (Some(Some(lower)), None) => format!("{} or more", format::millis(*lower)),
        _ => String::new(),
    }
}

/// A bucket's start for tables: `2026-11-01 14:03`.
fn bucket_label(bucket: DateTime<Utc>) -> String {
    bucket.format("%Y-%m-%d %H:%M").to_string()
}

/// RFC 3339, to the second, UTC.
fn iso(time: &DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn granularity_name(granularity: Granularity) -> &'static str {
    match granularity {
        Granularity::Minute => "minute",
        Granularity::TenMinutes => "ten_minutes",
    }
}

/// The deploy marks of a result, for the charts.
fn marks_json(result: &QueryResult) -> Vec<Value> {
    result
        .marks
        .iter()
        .map(|mark| {
            json!({
                "at": iso(&mark.at),
                "bucket": iso(&mark.bucket),
                "label": mark.label,
            })
        })
        .collect()
}

/// The deploy marks of a result, for the page.
fn marks(result: &QueryResult) -> Vec<MarkView> {
    result
        .marks
        .iter()
        .map(|mark| MarkView {
            iso: iso(&mark.at),
            utc: mark.at.format("%Y-%m-%d %H:%M UTC").to_string(),
            label: mark.label.clone(),
        })
        .collect()
}

/// `Data from … to ….`
fn window(result: &QueryResult) -> String {
    strings::metrics_window(
        &result.starts_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        &result.ends_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram(counts: &[(usize, u64)]) -> Histogram {
        let mut histogram = Histogram::new();
        for (index, count) in counts {
            histogram.add_to_bucket(*index, *count);
        }
        histogram
    }

    #[test]
    fn quantiles_come_from_bucket_bounds() {
        let empty = Histogram::new();
        assert_eq!(quantile_bucket(&empty, 0.5), None);
        // 10 executions: 6 under 20 ms, 3 in 20-30 ms, 1 in 1.1-1.7 s.
        let h = histogram(&[(0, 6), (1, 3), (11, 1)]);
        assert_eq!(quantile_bucket(&h, 0.5), Some(0));
        assert_eq!(quantile_bucket(&h, 0.6), Some(0));
        assert_eq!(quantile_bucket(&h, 0.61), Some(1));
        assert_eq!(quantile_bucket(&h, 0.9), Some(1));
        assert_eq!(quantile_bucket(&h, 0.95), Some(11));
        assert_eq!(quantile_bucket(&h, 1.0), Some(11));
        assert_eq!(quantile_bucket(&h, 0.0), Some(0));
        assert_eq!(quantile_label(0), "< 20 ms");
        assert_eq!(quantile_label(11), "< 1.7 s");
        assert_eq!(quantile_label(BUCKETS - 1), "≥ 335 s");
    }

    #[test]
    fn buckets_are_described() {
        assert_eq!(short_label(0), "<20ms");
        assert_eq!(short_label(10), "<1.1s");
        assert_eq!(short_label(BUCKETS - 1), ">335s");
        assert_eq!(bucket_range(0), "under 20 ms");
        assert_eq!(bucket_range(1), "20 ms – 30 ms");
        assert_eq!(bucket_range(10), "750 ms – 1.1 s");
        assert_eq!(bucket_range(BUCKETS - 1), "335 s or more");
        assert_eq!(bucket_range(BUCKETS), "");
    }

    #[test]
    fn shown_buckets_pad_the_used_range() {
        assert_eq!(shown_buckets(&Histogram::new()), 0..=0);
        assert_eq!(shown_buckets(&histogram(&[(0, 1)])), 0..=1);
        assert_eq!(shown_buckets(&histogram(&[(3, 1), (5, 2)])), 2..=6);
        assert_eq!(
            shown_buckets(&histogram(&[(BUCKETS - 1, 1)])),
            BUCKETS - 2..=BUCKETS - 1
        );
    }
}
