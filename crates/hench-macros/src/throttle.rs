//! The throttle options of `#[hench::job(...)]`: `throttle = "name"`, `throttle_by =
//! [params]` and the inline definition `throttle(concurrency = n, window = "N/period", ..)`
//! (`docs/design/ratelimit.md` §2.4.3).
//!
//! [`ThrottleAttrs`] parses them (specs checked at compile time with the grammar of
//! [`crate::spec`]), checks them against the function's parameters and generates what they
//! mean: the option `.throttle(name)` of `Job::options`, `Job::throttle_key` from the
//! parameters of `throttle_by` (compact JSON of their values, a lone string unquoted) and, for
//! an inline definition, `Job::throttle()`: a throttle named after the job.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::meta::ParseNestedMeta;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, Ident, Lit, LitStr, Path, Token};

use crate::spec::{Kind, Period, Spec};

/// Longest throttle name (`hench::limit`'s rule).
const MAX_NAME: usize = 100;

/// The throttle options of one `#[hench::job(...)]` attribute.
#[derive(Default)]
pub(crate) struct ThrottleAttrs {
    /// `throttle = "name"`.
    name: Option<LitStr>,
    /// `throttle_by = [params]`, with the option's span.
    by: Option<(Span, Vec<Ident>)>,
    /// `throttle(..)`: its limits.
    inline: Option<Vec<Spec>>,
    /// The span of `throttle(..)`.
    inline_span: Option<Span>,
}

impl ThrottleAttrs {
    /// Parses `option` into these options when it is a throttle option: `None` for any other
    /// option.
    pub(crate) fn parse_option(
        &mut self,
        meta: &ParseNestedMeta<'_>,
        option: &str,
    ) -> Option<syn::Result<()>> {
        let parsed = match option {
            "throttle" if self.name.is_some() || self.inline.is_some() => Err(meta.error(
                "duplicate option `throttle`: name a throttle (`throttle = \"name\"`) or define \
                 one (`throttle(..)`), once",
            )),
            "throttle" if meta.input.peek(syn::token::Paren) => inline(meta).map(|limits| {
                self.inline = Some(limits);
                self.inline_span = Some(meta.path.span());
            }),
            "throttle" => name(meta).map(|name| self.name = Some(name)),
            "throttle_by" if self.by.is_some() => Err(meta.error("duplicate option `throttle_by`")),
            "throttle_by" => {
                parameters(meta).map(|names| self.by = Some((meta.path.span(), names)))
            }
            _ => return None,
        };
        Some(parsed)
    }

    /// Whether any throttle option was given.
    pub(crate) fn is_set(&self) -> bool {
        self.name.is_some() || self.by.is_some() || self.inline.is_some()
    }

    /// Checks `throttle_by` against the function's parameters (`params`: the job's arguments,
    /// in order; `context`: the context parameter, when the function takes one): every name
    /// must be an argument, once, and there must be a throttle to key; and an inline throttle
    /// with per-key limits needs `throttle_by` (else its per-key limits would never apply).
    pub(crate) fn check(&self, params: &[Ident], context: Option<&Ident>) -> syn::Result<()> {
        let arguments = || {
            if params.is_empty() {
                "the job takes no arguments".to_owned()
            } else {
                let names: Vec<String> = params.iter().map(|param| format!("`{param}`")).collect();
                format!("its arguments are {}", names.join(", "))
            }
        };
        let Some((span, names)) = &self.by else {
            let per_key: Vec<String> = self
                .inline
                .iter()
                .flatten()
                .filter(|spec| spec.per_key)
                .map(|spec| format!("`{}_per_key`", spec.kind.name()))
                .collect();
            if let (Some(span), false) = (self.inline_span, per_key.is_empty()) {
                return Err(syn::Error::new(
                    span,
                    format!(
                        "the per-key limits of this throttle ({}) need a key, and the job has \
                         none: add `throttle_by = [..]` with the arguments that make the key \
                         ({}), or define the throttle with `HenchBuilder::throttle` and name it \
                         (`throttle = \"name\"`) to give each push its key (`throttle_key`)",
                        per_key.join(", "),
                        arguments(),
                    ),
                ));
            }
            return Ok(());
        };
        if self.name.is_none() && self.inline.is_none() {
            return Err(syn::Error::new(
                *span,
                "`throttle_by` keys a throttle: name one too (`throttle = \"name\"` or \
                 `throttle(..)`)",
            ));
        }
        let mut seen: Vec<&Ident> = Vec::new();
        for name in names {
            if seen.contains(&name) {
                return Err(syn::Error::new(
                    name.span(),
                    format!("`{name}` appears twice in `throttle_by`"),
                ));
            }
            seen.push(name);
            if context.is_some_and(|context| context == name) {
                return Err(syn::Error::new(
                    name.span(),
                    "the job context is not an argument: `throttle_by` takes the job's arguments",
                ));
            }
            if !params.contains(name) {
                return Err(syn::Error::new(
                    name.span(),
                    format!("`{name}` is not an argument of the job: {}", arguments()),
                ));
            }
        }
        Ok(())
    }

    /// The option call of `Job::options`: `.throttle("name")`, named after the job (`job`: its
    /// `NAME`) for an inline definition.
    pub(crate) fn option_call(&self, job: &str) -> Option<TokenStream> {
        if let Some(name) = &self.name {
            return Some(quote!(.throttle(#name)));
        }
        self.inline.as_ref().map(|_| {
            let name = inline_name(job);
            quote!(.throttle(#name))
        })
    }

    /// The items of `impl Job` the options generate: `throttle_key` (from `throttle_by`; the
    /// arguments are the tuple `Self::Args`, in the order of `params`) and `throttle` (from an
    /// inline definition, named after the job: `job` is its `NAME`).
    pub(crate) fn job_items(&self, krate: &Path, job: &str, params: &[Ident]) -> TokenStream {
        let key = self.by.as_ref().map(|(_, names)| {
            let values = names.iter().filter_map(|name| {
                let index = params.iter().position(|param| param == name)?;
                let index = syn::Index::from(index);
                Some(quote! {
                    #krate::__private::serde_json::to_value(&__hench_args.#index).ok()?
                })
            });
            quote! {
                fn throttle_key(
                    __hench_args: &<Self as #krate::Job>::Args,
                ) -> ::core::option::Option<::std::string::String> {
                    let __hench_values = [#(#values),*];
                    ::core::option::Option::Some(match __hench_values.as_slice() {
                        [#krate::__private::serde_json::Value::String(__hench_text)] => {
                            ::core::clone::Clone::clone(__hench_text)
                        }
                        [__hench_value] => ::std::string::ToString::to_string(__hench_value),
                        __hench_all => ::std::string::ToString::to_string(
                            &#krate::__private::serde_json::Value::Array(
                                <[_]>::to_vec(__hench_all),
                            ),
                        ),
                    })
                }
            }
        });
        let throttle = self.inline.as_ref().map(|limits| {
            let name = inline_name(job);
            let calls = limits.iter().map(|spec| {
                let limit = limit_tokens(krate, spec);
                if spec.per_key {
                    quote!(.limit_per_key(#limit))
                } else {
                    quote!(.limit(#limit))
                }
            });
            quote! {
                fn throttle() -> ::core::option::Option<#krate::Throttle> {
                    ::core::option::Option::Some(#krate::Throttle::new(#name) #(#calls)*)
                }
            }
        });
        quote!(#key #throttle)
    }
}

/// The name of the inline throttle of the job named `job`: the job's name, with each run of
/// characters a throttle name cannot hold (`::` in `Billing::Charge`) spelled `.`, at most 100
/// characters; `_` comes first when that leaves `.` or `..`, which are not throttle names.
fn inline_name(job: &str) -> String {
    let mut name = String::with_capacity(job.len());
    for c in job.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
            name.push(c);
        } else if !name.ends_with('.') {
            name.push('.');
        }
    }
    if name.is_empty() {
        name.push('.');
    }
    let name: String = name.chars().take(MAX_NAME).collect();
    if matches!(name.as_str(), "." | "..") {
        format!("_{name}")
    } else {
        name
    }
}

/// The expression building the limit of `spec`.
fn limit_tokens(krate: &Path, spec: &Spec) -> TokenStream {
    let count = spec.count;
    let duration = |millis: u64| quote!(::core::time::Duration::from_millis(#millis));
    let period = match spec.period {
        Some(Period::Millis(millis)) => duration(millis),
        Some(Period::Named(_)) | None => duration(1_000),
    };
    match spec.kind {
        Kind::Concurrency => quote!(#krate::limit::Limit::Concurrency { limit: #count }),
        Kind::Window => quote!(#krate::limit::Limit::Window { limit: #count, period: #period }),
        Kind::Bucket => {
            let interval = match spec.period {
                Some(Period::Named("second")) => quote!(#krate::limit::Interval::Second),
                Some(Period::Named("minute")) => quote!(#krate::limit::Interval::Minute),
                Some(Period::Named("hour")) => quote!(#krate::limit::Interval::Hour),
                Some(Period::Named(_)) => quote!(#krate::limit::Interval::Day),
                Some(Period::Millis(_)) | None => quote!(#krate::limit::Interval::Every(#period)),
            };
            quote!(#krate::limit::Limit::Bucket { limit: #count, interval: #interval })
        }
        Kind::Leaky => quote!(#krate::limit::Limit::Leaky { size: #count, drain: #period }),
        Kind::Points => {
            quote!(#krate::limit::Limit::Points { capacity: #count, refill: #period })
        }
    }
}

/// `throttle = "name"`.
fn name(meta: &ParseNestedMeta<'_>) -> syn::Result<LitStr> {
    let value: Expr = meta.value()?.parse()?;
    match &value {
        Expr::Lit(ExprLit {
            lit: Lit::Str(text),
            ..
        }) if valid_name(&text.value()) => Ok(text.clone()),
        Expr::Lit(ExprLit {
            lit: Lit::Str(text),
            ..
        }) if !text.value().is_empty() => Err(syn::Error::new(
            text.span(),
            "a throttle name is 1 to 100 characters among letters, digits, `_`, `.` and `-`, \
             other than `.` and `..`",
        )),
        other => Err(syn::Error::new(
            other.span(),
            "expected the name of a throttle, such as `throttle = \"stripe\"`",
        )),
    }
}

/// Whether `name` is a valid throttle name (`hench::limit`'s rule: not `.` or `..`, which are
/// dot segments in the URLs that name limits).
fn valid_name(name: &str) -> bool {
    !matches!(name, "." | "..")
        && (1..=MAX_NAME).contains(&name.chars().count())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

/// `[param, ...]`: the job parameters a key is made of (`unique_by`, `throttle_by`).
pub(crate) fn parameters(meta: &ParseNestedMeta<'_>) -> syn::Result<Vec<Ident>> {
    let value = meta.value()?;
    let content;
    syn::bracketed!(content in value);
    let names: Punctuated<Ident, Token![,]> = Punctuated::parse_terminated(&content)?;
    if names.is_empty() {
        return Err(meta.error("expected at least one parameter, such as `[account]`"));
    }
    Ok(names.into_iter().collect())
}

/// `throttle(concurrency = 3, window_per_key = "5/1s", ...)`: every limit checked with the
/// spec grammar.
fn inline(meta: &ParseNestedMeta<'_>) -> syn::Result<Vec<Spec>> {
    let content;
    syn::parenthesized!(content in meta.input);
    if content.is_empty() {
        return Err(meta.error("expected at least one limit, such as `throttle(concurrency = 3)`"));
    }
    let tokens: TokenStream = content.parse()?;
    let mut limits = Vec::new();
    syn::parse::Parser::parse2(
        syn::meta::parser(|limit| {
            limits.push(one_limit(&limit)?);
            Ok(())
        }),
        tokens,
    )?;
    Ok(limits)
}

/// One limit of an inline throttle (`window = "100/1s"`), checked with the spec grammar.
fn one_limit(limit: &ParseNestedMeta<'_>) -> syn::Result<Spec> {
    let Some(ident) = limit.path.get_ident() else {
        return Err(limit.error("expected a limit such as `concurrency = 3`"));
    };
    let name = ident.to_string();
    let (kind, per_key) = match name.strip_suffix("_per_key") {
        Some(kind) => (kind, true),
        None => (name.as_str(), false),
    };
    if Kind::parse(kind).is_none() || kind.len() == 1 {
        return Err(syn::Error::new(
            ident.span(),
            format!(
                "unknown limit `{name}`; expected concurrency, window, bucket, leaky or \
                 points (optionally `_per_key`)"
            ),
        ));
    }
    let value: Expr = limit.value()?.parse()?;
    let text = match &value {
        Expr::Lit(ExprLit {
            lit: Lit::Int(int), ..
        }) => int.base10_digits().to_owned(),
        Expr::Lit(ExprLit {
            lit: Lit::Str(text),
            ..
        }) => text.value(),
        other => {
            return Err(syn::Error::new(
                other.span(),
                "expected a count (`3`) or a count and a period (`\"100/1s\"`)",
            ));
        }
    };
    let suffix = if per_key { " per key" } else { "" };
    crate::spec::parse(&format!("{kind}={text}{suffix}"))
        .map_err(|reason| syn::Error::new(value.span(), reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::format_ident;
    use syn::parse::Parser;

    /// The throttle options of an attribute's tokens.
    fn attrs(tokens: TokenStream) -> syn::Result<ThrottleAttrs> {
        let mut attrs = ThrottleAttrs::default();
        syn::meta::parser(|meta| {
            let option = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            attrs
                .parse_option(&meta, &option)
                .unwrap_or_else(|| Err(meta.error("not a throttle option")))
        })
        .parse2(tokens)?;
        Ok(attrs)
    }

    /// Tokens as one line with single spaces.
    fn flat(tokens: &TokenStream) -> String {
        tokens
            .to_string()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn named_throttles_set_the_option_and_the_key() {
        let attrs = attrs(quote!(throttle = "stripe", throttle_by = [account])).unwrap();
        assert!(attrs.is_set());
        let params = [format_ident!("account"), format_ident!("cents")];
        attrs.check(&params, Some(&format_ident!("ctx"))).unwrap();
        assert_eq!(
            flat(&attrs.option_call("charge").unwrap()),
            ". throttle (\"stripe\")"
        );
        let items = flat(&attrs.job_items(&syn::parse_quote!(::hench), "charge", &params));
        assert!(items.contains("fn throttle_key"), "{items}");
        assert!(items.contains("__hench_args . 0"), "{items}");
        assert!(!items.contains("__hench_args . 1"), "{items}");
        assert!(!items.contains("fn throttle ()"), "{items}");
        // Keys follow the order of `throttle_by`.
        let attrs = attrs_of(quote!(throttle = "x", throttle_by = [cents, account]));
        let items = flat(&attrs.job_items(&syn::parse_quote!(::hench), "charge", &params));
        let (first, second) = (
            items.find("__hench_args . 1").unwrap(),
            items.find("__hench_args . 0").unwrap(),
        );
        assert!(first < second, "{items}");
    }

    fn attrs_of(tokens: TokenStream) -> ThrottleAttrs {
        attrs(tokens).unwrap()
    }

    #[test]
    fn inline_throttles_are_named_after_the_job() {
        let attrs = attrs_of(quote!(throttle(
            concurrency = 3,
            window_per_key = "5/1s",
            bucket = "1000/day",
            leaky = "60/1m",
            points_per_key = "100/20s"
        )));
        assert_eq!(
            flat(&attrs.option_call("export").unwrap()),
            ". throttle (\"export\")"
        );
        // Its per-key limits are keyed by `throttle_by`; without it, they could never apply.
        let attrs_keyed = attrs_of(quote!(
            throttle(concurrency = 3, window_per_key = "5/1s"),
            throttle_by = [org]
        ));
        attrs_keyed.check(&[format_ident!("org")], None).unwrap();
        attrs_of(quote!(throttle(concurrency = 3)))
            .check(&[], None)
            .unwrap();
        let items = flat(&attrs.job_items(&syn::parse_quote!(::hench), "export", &[]));
        for expected in [
            "fn throttle ()",
            ":: hench :: Throttle :: new (\"export\")",
            ". limit (:: hench :: limit :: Limit :: Concurrency { limit : 3u32 })",
            ". limit_per_key (:: hench :: limit :: Limit :: Window { limit : 5u32 , period : :: core :: time :: Duration :: from_millis (1000u64) })",
            ":: hench :: limit :: Interval :: Day",
            "Limit :: Leaky { size : 60u32 , drain : :: core :: time :: Duration :: from_millis (60000u64) }",
            ". limit_per_key (:: hench :: limit :: Limit :: Points { capacity : 100u32",
        ] {
            assert!(items.contains(expected), "{expected}\n{items}");
        }
        assert!(!items.contains("fn throttle_key"));
        // Job names a throttle name cannot hold.
        assert_eq!(inline_name("Billing::Charge"), "Billing.Charge");
        assert_eq!(inline_name("a b/c"), "a.b.c");
        assert_eq!(inline_name(&"x".repeat(150)).len(), 100);
        assert!(valid_name(&inline_name("::Odd  name::")));
        // Never a dot segment (round 3, web-api-security-7).
        assert_eq!(inline_name("日本"), "_.");
        assert_eq!(inline_name(".."), "_..");
        assert_eq!(inline_name(". ."), "_..");
        assert_eq!(inline_name("..."), "...");
        for job in ["日本", "..", ". .", "...", "::"] {
            assert!(valid_name(&inline_name(job)), "{job}");
        }
    }

    #[test]
    fn mistakes_are_reported() {
        for (tokens, expected) in [
            (quote!(throttle = ""), "expected the name of a throttle"),
            (quote!(throttle = "a b"), "letters, digits"),
            (quote!(throttle = ".."), "other than `.` and `..`"),
            (quote!(throttle = "."), "other than `.` and `..`"),
            (quote!(throttle = 3), "expected the name of a throttle"),
            (quote!(throttle(window = "5")), "invalid limit `window=5`"),
            (quote!(throttle(rate = 3)), "unknown limit `rate`"),
            (quote!(throttle(c = 3)), "unknown limit `c`"),
            (quote!(throttle()), "expected at least one limit"),
            (quote!(throttle(concurrency = x)), "expected a count"),
            (
                quote!(throttle = "a", throttle(concurrency = 1)),
                "duplicate option `throttle`",
            ),
            (
                quote!(throttle = "a", throttle = "b"),
                "duplicate option `throttle`",
            ),
            (quote!(throttle_by = []), "expected at least one parameter"),
            (
                quote!(throttle_by = [a], throttle_by = [b]),
                "duplicate option `throttle_by`",
            ),
        ] {
            let Err(error) = attrs(tokens.clone()) else {
                panic!("{tokens}: accepted");
            };
            assert!(error.to_string().contains(expected), "{tokens}: {error}");
        }
        let params = [format_ident!("account")];
        let context = format_ident!("ctx");
        for (tokens, expected) in [
            (quote!(throttle_by = [account]), "name one too"),
            (
                quote!(throttle = "x", throttle_by = [ctx]),
                "the job context is not an argument",
            ),
            (
                quote!(throttle = "x", throttle_by = [other]),
                "`other` is not an argument of the job: its arguments are `account`",
            ),
            (
                quote!(throttle(concurrency_per_key = 1)),
                "the per-key limits of this throttle (`concurrency_per_key`) need a key",
            ),
            (
                quote!(throttle(concurrency = 5, window_per_key = "5/1s")),
                "(`window_per_key`) need a key, and the job has none: add `throttle_by = [..]` \
                 with the arguments that make the key (its arguments are `account`)",
            ),
            (
                quote!(throttle = "x", throttle_by = [account, account]),
                "appears twice",
            ),
        ] {
            let error = attrs_of(tokens.clone())
                .check(&params, Some(&context))
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{tokens}: {error}");
        }
    }
}
