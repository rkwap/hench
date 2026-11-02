//! Parsing the options of `#[hench::job(...)]`.

use proc_macro2::{Span, TokenStream};
use syn::meta::ParseNestedMeta;
use syn::parse::Parser;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, ExprUnary, Lit, LitBool, LitInt, LitStr, Path, UnOp};

/// The options accepted by the attribute, for error messages.
const OPTIONS: &str = "name, queue, retry, backtrace, dead, tags, retry_for, retry_queue, \
                       log_level, unique_for, unique_until, on_conflict, unique_across_queues, \
                       unique_by, throttle, throttle_by, crate";

/// The values `log_level` accepts.
const LOG_LEVELS: [&str; 5] = ["trace", "debug", "info", "warn", "error"];

/// Largest accepted `retry_for`, in seconds (the client rejects larger values, which look
/// like timestamps).
const MAX_RETRY_FOR_SECS: u64 = 1_000_000_000;

/// `true`/`false` or a count (`retry`, `backtrace`).
#[derive(Debug, Clone)]
pub(crate) enum BoolOrCount {
    Bool(LitBool),
    Count(u32, Span),
}

/// The parsed options of one `#[hench::job(...)]` attribute.
#[derive(Default)]
pub(crate) struct JobAttrs {
    pub(crate) name: Option<LitStr>,
    pub(crate) queue: Option<LitStr>,
    pub(crate) retry: Option<BoolOrCount>,
    pub(crate) backtrace: Option<BoolOrCount>,
    pub(crate) dead: Option<LitBool>,
    pub(crate) tags: Option<Vec<LitStr>>,
    pub(crate) retry_for: Option<(u64, Span)>,
    pub(crate) retry_queue: Option<LitStr>,
    pub(crate) log_level: Option<LitStr>,
    pub(crate) krate: Option<Path>,
    /// The unique-job options (`unique_for`, `unique_until`, `on_conflict`,
    /// `unique_across_queues`, `unique_by`).
    pub(crate) unique: crate::unique::UniqueAttrs,
    /// The throttle options (`throttle = "name"`, `throttle(..)`, `throttle_by`).
    pub(crate) throttle: crate::throttle::ThrottleAttrs,
}

impl JobAttrs {
    /// Parses the tokens between the attribute's parentheses.
    pub(crate) fn parse(tokens: TokenStream) -> syn::Result<Self> {
        let mut attrs = Self::default();
        syn::meta::parser(|meta| attrs.parse_one(&meta)).parse2(tokens)?;
        Ok(attrs)
    }

    /// `true` if any option that ends up in `Job::options` was given.
    pub(crate) fn has_job_options(&self) -> bool {
        self.queue.is_some()
            || self.retry.is_some()
            || self.backtrace.is_some()
            || self.dead.is_some()
            || self.tags.is_some()
            || self.retry_for.is_some()
            || self.retry_queue.is_some()
            || self.log_level.is_some()
            || !self.unique.is_empty()
            || self.throttle.is_set()
    }

    fn parse_one(&mut self, meta: &ParseNestedMeta<'_>) -> syn::Result<()> {
        let Some(ident) = meta.path.get_ident() else {
            return Err(meta.error(format!("unknown option; expected one of: {OPTIONS}")));
        };
        let option = ident.to_string();
        // The options of the features (unique jobs, throttles), in their own modules.
        if let Some(result) = self.throttle.parse_option(meta, &option) {
            return result;
        }
        if let Some(result) = self.unique.parse(meta, &option) {
            return result;
        }
        if !meta.input.peek(syn::Token![=]) {
            return Err(meta.error(format!("expected `{option} = ...`")));
        }
        match option.as_str() {
            "name" => set(meta, &option, &mut self.name, |meta| {
                non_empty_string(meta, &option)
            }),
            "queue" => set(meta, &option, &mut self.queue, |meta| {
                non_empty_string(meta, &option)
            }),
            "retry_queue" => set(meta, &option, &mut self.retry_queue, |meta| {
                non_empty_string(meta, &option)
            }),
            "log_level" => set(meta, &option, &mut self.log_level, |meta| {
                let level = non_empty_string(meta, &option)?;
                if LOG_LEVELS.contains(&level.value().as_str()) {
                    Ok(level)
                } else {
                    Err(syn::Error::new(
                        level.span(),
                        format!("expected one of: {}", LOG_LEVELS.join(", ")),
                    ))
                }
            }),
            "retry" => set(meta, &option, &mut self.retry, |meta| {
                bool_or_count(meta, "`true`, `false` or a maximum number of retries")
            }),
            "backtrace" => set(meta, &option, &mut self.backtrace, |meta| {
                bool_or_count(meta, "`true`, `false` or a number of lines")
            }),
            "dead" => set(meta, &option, &mut self.dead, |meta| match expr(meta)? {
                Expr::Lit(ExprLit {
                    lit: Lit::Bool(value),
                    ..
                }) => Ok(value),
                other => Err(syn::Error::new(other.span(), "expected `true` or `false`")),
            }),
            "tags" => set(meta, &option, &mut self.tags, tags),
            "retry_for" => set(meta, &option, &mut self.retry_for, retry_for),
            "crate" => set(meta, &option, &mut self.krate, |meta| {
                let path: LitStr = meta.value()?.parse().map_err(|error| {
                    syn::Error::new(
                        error.span(),
                        "expected the path of the hench crate as a string, e.g. `crate = \"::hench\"`",
                    )
                })?;
                path.parse::<Path>()
            }),
            _ => Err(syn::Error::new(
                ident.span(),
                format!("unknown option `{option}`; expected one of: {OPTIONS}"),
            )),
        }
    }
}

/// Parses the value of an option into `slot`, rejecting duplicates.
fn set<T>(
    meta: &ParseNestedMeta<'_>,
    option: &str,
    slot: &mut Option<T>,
    parse: impl FnOnce(&ParseNestedMeta<'_>) -> syn::Result<T>,
) -> syn::Result<()> {
    if slot.is_some() {
        return Err(meta.error(format!("duplicate option `{option}`")));
    }
    *slot = Some(parse(meta)?);
    Ok(())
}

fn expr(meta: &ParseNestedMeta<'_>) -> syn::Result<Expr> {
    meta.value()?.parse()
}

fn non_empty_string(meta: &ParseNestedMeta<'_>, option: &str) -> syn::Result<LitStr> {
    let value = match expr(meta)? {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => value,
        other => {
            return Err(syn::Error::new(
                other.span(),
                format!("expected a string literal, e.g. `{option} = \"...\"`"),
            ));
        }
    };
    if value.value().is_empty() {
        return Err(syn::Error::new(
            value.span(),
            format!("`{option}` must not be empty"),
        ));
    }
    Ok(value)
}

fn bool_or_count(meta: &ParseNestedMeta<'_>, expected: &str) -> syn::Result<BoolOrCount> {
    match expr(meta)? {
        Expr::Lit(ExprLit {
            lit: Lit::Bool(value),
            ..
        }) => Ok(BoolOrCount::Bool(value)),
        Expr::Lit(ExprLit {
            lit: Lit::Int(value),
            ..
        }) => Ok(BoolOrCount::Count(count(&value)?, value.span())),
        Expr::Unary(ExprUnary {
            op: UnOp::Neg(minus),
            ..
        }) => Err(syn::Error::new(
            minus.span,
            format!("expected {expected}, not a negative number"),
        )),
        other => Err(syn::Error::new(
            other.span(),
            format!("expected {expected}"),
        )),
    }
}

fn count(value: &LitInt) -> syn::Result<u32> {
    if !value.suffix().is_empty() {
        return Err(syn::Error::new(
            value.span(),
            "expected a number without a type suffix",
        ));
    }
    value.base10_parse::<u32>().map_err(|_| {
        syn::Error::new(
            value.span(),
            format!("expected a number between 0 and {}", u32::MAX),
        )
    })
}

fn tags(meta: &ParseNestedMeta<'_>) -> syn::Result<Vec<LitStr>> {
    const EXPECTED: &str = "expected an array of string literals, e.g. `tags = [\"billing\"]`";
    let array = match expr(meta)? {
        Expr::Array(array) => array,
        other => return Err(syn::Error::new(other.span(), EXPECTED)),
    };
    array
        .elems
        .into_iter()
        .map(|element| match element {
            Expr::Lit(ExprLit {
                lit: Lit::Str(tag), ..
            }) => Ok(tag),
            other => Err(syn::Error::new(other.span(), EXPECTED)),
        })
        .collect()
}

fn retry_for(meta: &ParseNestedMeta<'_>) -> syn::Result<(u64, Span)> {
    const EXPECTED: &str = "expected a number of seconds, e.g. `retry_for = 3600`";
    let value = match expr(meta)? {
        Expr::Lit(ExprLit {
            lit: Lit::Int(value),
            ..
        }) => value,
        other => return Err(syn::Error::new(other.span(), EXPECTED)),
    };
    if !value.suffix().is_empty() {
        return Err(syn::Error::new(value.span(), EXPECTED));
    }
    let secs = value
        .base10_parse::<u64>()
        .map_err(|_| syn::Error::new(value.span(), EXPECTED))?;
    if secs >= MAX_RETRY_FOR_SECS {
        return Err(syn::Error::new(
            value.span(),
            "`retry_for` must be a relative amount of time below 1000000000 seconds",
        ));
    }
    Ok((secs, value.span()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn error(tokens: TokenStream) -> String {
        match JobAttrs::parse(tokens) {
            Ok(_) => panic!("expected an error"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn feature_options_are_parsed_and_checked() {
        let attrs = JobAttrs::parse(quote!(
            queue = "billing",
            unique_for = 600,
            unique_until = "start",
            on_conflict = "replace",
            unique_across_queues = true,
            unique_by = [user_id],
            throttle = "stripe",
            throttle_by = [account]
        ))
        .unwrap();
        assert!(attrs.has_job_options());
        assert!(!attrs.unique.is_empty());
        assert!(attrs.throttle.is_set());
        // The feature options alone make `options()`.
        assert!(
            JobAttrs::parse(quote!(unique_for = 60))
                .unwrap()
                .has_job_options()
        );
        assert!(
            JobAttrs::parse(quote!(throttle(concurrency = 3)))
                .unwrap()
                .has_job_options()
        );
        for (tokens, expected) in [
            (
                quote!(unique_for = 0),
                "expected a number of seconds from 1 to 999999999",
            ),
            (
                quote!(unique_until = "later"),
                "expected one of: \"success\", \"start\"",
            ),
            (
                quote!(on_conflict = "keep"),
                "expected one of: \"drop\", \"error\"",
            ),
            (
                quote!(unique_across_queues = 1),
                "expected `true` or `false`",
            ),
            (quote!(unique_by = []), "expected at least one parameter"),
            (
                quote!(unique_for = 60, unique_for = 70),
                "duplicate option `unique_for`",
            ),
            (quote!(throttle = ""), "expected the name of a throttle"),
            (quote!(throttle(window = "5")), "invalid limit `window=5`"),
            (quote!(throttle(rate = 3)), "unknown limit `rate`"),
            (quote!(throttle()), "expected at least one limit"),
            (
                quote!(throttle = "a", throttle(concurrency = 1)),
                "duplicate option `throttle`",
            ),
        ] {
            let message = error(tokens.clone());
            assert!(message.contains(expected), "{tokens}: {message}");
        }
        // Unknown options list every supported one.
        let message = error(quote!(uniq = 1));
        assert!(
            message.contains(&format!("expected one of: {OPTIONS}")),
            "{message}"
        );
        assert!(message.contains("unique_for") && message.contains("throttle_by"));
    }

    #[test]
    fn parses_every_option() {
        let attrs = JobAttrs::parse(quote! {
            name = "Billing::Charge", queue = "critical", retry = 5, backtrace = true,
            dead = false, tags = ["billing", "money"], retry_for = 3600,
            retry_queue = "slow", crate = "::my_hench"
        })
        .unwrap();
        assert_eq!(attrs.name.unwrap().value(), "Billing::Charge");
        assert_eq!(attrs.queue.unwrap().value(), "critical");
        assert!(matches!(attrs.retry, Some(BoolOrCount::Count(5, _))));
        assert!(matches!(attrs.backtrace, Some(BoolOrCount::Bool(ref b)) if b.value));
        assert!(!attrs.dead.unwrap().value);
        let tags: Vec<String> = attrs.tags.unwrap().iter().map(LitStr::value).collect();
        assert_eq!(tags, ["billing", "money"]);
        assert_eq!(attrs.retry_for.unwrap().0, 3600);
        assert_eq!(attrs.retry_queue.unwrap().value(), "slow");
        let krate = attrs.krate.unwrap();
        assert_eq!(quote!(#krate).to_string(), ":: my_hench");
    }

    #[test]
    fn empty_attributes_are_fine() {
        let attrs = JobAttrs::parse(TokenStream::new()).unwrap();
        assert!(!attrs.has_job_options());
        assert!(attrs.name.is_none() && attrs.krate.is_none());
        assert!(
            JobAttrs::parse(quote!(retry = false))
                .unwrap()
                .has_job_options()
        );
    }

    #[test]
    fn rejects_bad_options() {
        assert!(error(quote!(queu = "x")).contains("unknown option `queu`"));
        assert!(error(quote!(a::b = "x")).contains("unknown option"));
        assert!(error(quote!(queue = "a", queue = "b")).contains("duplicate option `queue`"));
        assert!(error(quote!(dead)).contains("expected `dead = ...`"));
        assert!(error(quote!(queue = "")).contains("must not be empty"));
        assert!(error(quote!(name = "")).contains("must not be empty"));
        assert!(error(quote!(queue = 5)).contains("expected a string literal"));
        assert!(error(quote!(retry = "yes")).contains("maximum number of retries"));
        assert!(error(quote!(retry = -1)).contains("negative"));
        assert!(error(quote!(retry = 5u8)).contains("type suffix"));
        assert!(error(quote!(retry = 99999999999)).contains("between 0 and"));
        assert!(error(quote!(backtrace = 1.5)).contains("number of lines"));
        assert!(error(quote!(dead = 1)).contains("`true` or `false`"));
        assert!(error(quote!(tags = "a")).contains("array of string literals"));
        assert!(error(quote!(tags = ["a", 1])).contains("array of string literals"));
        assert!(error(quote!(retry_for = 1000000000)).contains("below 1000000000"));
        assert!(error(quote!(retry_for = "1h")).contains("number of seconds"));
        assert!(error(quote!(crate = hench)).contains("as a string"));
        assert!(error(quote!(crate = "not a path")).contains("unexpected"));
    }
}
