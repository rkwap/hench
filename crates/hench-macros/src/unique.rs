//! The unique-job options of `#[hench::job(...)]`: `unique_for = <seconds>`,
//! `unique_until = "success" | "start" | "expired"`, `on_conflict = "drop" | "error" |
//! "replace"`, `unique_across_queues = <bool>` and `unique_by = [param, ...]`
//! (`docs/design/unique.md` §2.6.2).
//!
//! [`UniqueAttrs`] parses them (values checked at compile time, duplicates refused) and
//! generates their code: the `JobOptions` calls of the job type's `options()`, and, for
//! `unique_by`, the job's `Job::unique_key`, which returns the named arguments, in the order
//! given, as a JSON array (unknown, duplicated and context parameters are compile errors at the
//! name).
//!
//! The attribute parser (`attrs.rs`) keeps a `UniqueAttrs` and feeds it every option
//! ([`UniqueAttrs::parse`]) and counts it in `has_job_options` ([`UniqueAttrs::is_empty`]); the
//! expansion adds [`UniqueAttrs::option_calls`] to `options()` and
//! [`UniqueAttrs::unique_key_method`] to the `impl Job`.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, Ident, Lit, LitBool, LitInt, LitStr, Pat, PatType, Path};

/// Longest `unique_for`, in seconds.
const MAX_UNIQUE_FOR_SECS: u64 = 999_999_999;

/// The values `unique_until` accepts, and the `UniqueUntil` variant each names.
const POLICIES: [(&str, &str); 3] = [
    ("success", "Success"),
    ("start", "Start"),
    ("expired", "Expired"),
];

/// The values `on_conflict` accepts, and the `OnConflict` variant each names.
const STRATEGIES: [(&str, &str); 3] =
    [("drop", "Drop"), ("error", "Error"), ("replace", "Replace")];

/// The unique-job options of one attribute.
#[derive(Default)]
pub(crate) struct UniqueAttrs {
    unique_for: Option<(u64, Span)>,
    unique_until: Option<LitStr>,
    on_conflict: Option<LitStr>,
    across_queues: Option<LitBool>,
    unique_by: Option<Vec<Ident>>,
}

impl UniqueAttrs {
    /// Parses `option` into these attributes when it is a unique-job option (`None` for any
    /// other option): its value is checked, and a second occurrence refused.
    pub(crate) fn parse(
        &mut self,
        meta: &ParseNestedMeta<'_>,
        option: &str,
    ) -> Option<syn::Result<()>> {
        let result = match option {
            "unique_for" => set(meta, option, &mut self.unique_for, unique_for),
            "unique_until" => set(meta, option, &mut self.unique_until, |meta| {
                one_of(meta, &POLICIES)
            }),
            "on_conflict" => set(meta, option, &mut self.on_conflict, |meta| {
                one_of(meta, &STRATEGIES)
            }),
            "unique_across_queues" => set(meta, option, &mut self.across_queues, boolean),
            "unique_by" => set(
                meta,
                option,
                &mut self.unique_by,
                crate::throttle::parameters,
            ),
            _ => return None,
        };
        Some(result)
    }
}

/// The code generation of [`UniqueAttrs`].
impl UniqueAttrs {
    /// Refuses the other unique-job options without `unique_for`: a job is unique only for a
    /// time, so they would make every push fail (or, for `unique_by`, make nothing unique). The
    /// error is at the first of them.
    pub(crate) fn check(&self) -> syn::Result<()> {
        if self.unique_for.is_some() {
            return Ok(());
        }
        let given = [
            self.unique_until
                .as_ref()
                .map(|until| ("unique_until", until.span())),
            self.on_conflict
                .as_ref()
                .map(|strategy| ("on_conflict", strategy.span())),
            self.across_queues
                .as_ref()
                .map(|across| ("unique_across_queues", across.span())),
            self.unique_by
                .as_ref()
                .and_then(|names| names.first())
                .map(|name| ("unique_by", name.span())),
        ];
        match given.into_iter().flatten().next() {
            Some((option, span)) => Err(syn::Error::new(
                span,
                format!(
                    "`{option}` needs `unique_for = <seconds>`: a job is unique only for a time \
                     (a push that should not be unique says `.not_unique()`)"
                ),
            )),
            None => Ok(()),
        }
    }

    /// `true` when no unique-job option was given.
    pub(crate) fn is_empty(&self) -> bool {
        self.unique_for.is_none()
            && self.unique_until.is_none()
            && self.on_conflict.is_none()
            && self.across_queues.is_none()
            && self.unique_by.is_none()
    }

    /// The calls of `JobOptions` the options become (after `JobOptions::new()`).
    pub(crate) fn option_calls(&self, krate: &Path) -> Vec<TokenStream> {
        let mut calls = Vec::new();
        if let Some((secs, span)) = self.unique_for {
            let secs = LitInt::new(&format!("{secs}u64"), span);
            calls.push(quote!(.unique_for(::core::time::Duration::from_secs(#secs))));
        }
        if let Some(until) = &self.unique_until {
            let variant = variant(until, &POLICIES);
            calls.push(quote!(.unique_until(#krate::UniqueUntil::#variant)));
        }
        if let Some(strategy) = &self.on_conflict {
            let variant = variant(strategy, &STRATEGIES);
            calls.push(quote!(.on_conflict(#krate::OnConflict::#variant)));
        }
        if let Some(across) = &self.across_queues {
            calls.push(quote!(.unique_across_queues(#across)));
        }
        calls
    }

    /// `Job::unique_key` for `unique_by = [..]`: the named arguments, in the order given, as
    /// a JSON array; `None` without `unique_by`. `args` are the job's argument parameters (the
    /// context excluded), `context` the context parameter if the job takes one. An unknown or
    /// duplicated name, or the context, is an error at the name.
    pub(crate) fn unique_key_method(
        &self,
        krate: &Path,
        args: &[PatType],
        context: Option<&PatType>,
    ) -> syn::Result<Option<TokenStream>> {
        let Some(names) = &self.unique_by else {
            return Ok(None);
        };
        let params: Vec<Option<&Ident>> = args.iter().map(param_name).collect();
        let context_name = context.and_then(param_name);
        let mut errors: Option<syn::Error> = None;
        let mut fail = |error: syn::Error| match errors.as_mut() {
            Some(errors) => errors.combine(error),
            None => errors = Some(error),
        };
        let mut selected: Vec<usize> = Vec::with_capacity(names.len());
        for name in names {
            if context_name.is_some_and(|context| context == name) {
                fail(syn::Error::new(
                    name.span(),
                    format!("`{name}` is the job context: `unique_by` names the job's arguments"),
                ));
                continue;
            }
            match params.iter().position(|param| *param == Some(name)) {
                Some(index) if selected.contains(&index) => fail(syn::Error::new(
                    name.span(),
                    format!("`{name}` is listed twice in `unique_by`"),
                )),
                Some(index) => selected.push(index),
                None => {
                    let known: Vec<String> = params
                        .iter()
                        .flatten()
                        .map(|param| format!("`{param}`"))
                        .collect();
                    let known = if known.is_empty() {
                        "the job takes no arguments".to_owned()
                    } else {
                        format!("its arguments are {}", known.join(", "))
                    };
                    fail(syn::Error::new(
                        name.span(),
                        format!("`{name}` is not an argument of the job: {known}"),
                    ));
                }
            }
        }
        if let Some(errors) = errors {
            return Err(errors);
        }
        let bindings: Vec<TokenStream> = (0..args.len())
            .map(|index| {
                if selected.contains(&index) {
                    let binding = format_ident!("__hench_key{}", index);
                    quote!(#binding)
                } else {
                    quote!(_)
                }
            })
            .collect();
        let values: Vec<TokenStream> = selected
            .iter()
            .map(|index| {
                let binding = format_ident!("__hench_key{}", index);
                quote!(#krate::__private::serde_json::to_value(#binding).ok()?)
            })
            .collect();
        Ok(Some(quote! {
            fn unique_key(
                __hench_args: &<Self as #krate::Job>::Args,
            ) -> ::core::option::Option<#krate::__private::serde_json::Value> {
                let (#(#bindings,)*) = __hench_args;
                ::core::option::Option::Some(#krate::__private::serde_json::Value::Array(
                    ::std::vec![#(#values),*],
                ))
            }
        }))
    }
}

/// The name of a parameter declared as a plain identifier.
pub(crate) fn param_name(param: &PatType) -> Option<&Ident> {
    match &*param.pat {
        Pat::Ident(ident) => Some(&ident.ident),
        _ => None,
    }
}

/// The enum variant a checked value names.
fn variant(value: &LitStr, values: &[(&str, &str)]) -> Ident {
    let text = value.value();
    let name = values
        .iter()
        .find(|(option, _)| *option == text)
        .map_or("Success", |(_, variant)| *variant);
    Ident::new(name, value.span())
}

/// Parses the value of an option into `slot`, refusing a second occurrence.
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

fn unique_for(meta: &ParseNestedMeta<'_>) -> syn::Result<(u64, Span)> {
    let value: Expr = meta.value()?.parse()?;
    let expected = format!("expected a number of seconds from 1 to {MAX_UNIQUE_FOR_SECS}");
    match &value {
        Expr::Lit(ExprLit {
            lit: Lit::Int(int), ..
        }) => match int.base10_parse::<u64>() {
            Ok(secs) if (1..=MAX_UNIQUE_FOR_SECS).contains(&secs) => Ok((secs, int.span())),
            _ => Err(syn::Error::new(int.span(), expected)),
        },
        other => Err(syn::Error::new(other.span(), expected)),
    }
}

fn one_of(meta: &ParseNestedMeta<'_>, values: &[(&str, &str)]) -> syn::Result<LitStr> {
    let value: Expr = meta.value()?.parse()?;
    let expected = || {
        let quoted: Vec<String> = values
            .iter()
            .map(|(value, _)| format!("\"{value}\""))
            .collect();
        format!("expected one of: {}", quoted.join(", "))
    };
    match &value {
        Expr::Lit(ExprLit {
            lit: Lit::Str(text),
            ..
        }) if values.iter().any(|(value, _)| *value == text.value()) => Ok(text.clone()),
        other => Err(syn::Error::new(other.span(), expected())),
    }
}

fn boolean(meta: &ParseNestedMeta<'_>) -> syn::Result<LitBool> {
    let value: Expr = meta.value()?.parse()?;
    match &value {
        Expr::Lit(ExprLit {
            lit: Lit::Bool(value),
            ..
        }) => Ok(value.clone()),
        other => Err(syn::Error::new(other.span(), "expected `true` or `false`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse::Parser;
    use syn::parse_quote;

    /// Parses the tokens of an attribute's unique options.
    fn parse(tokens: TokenStream) -> syn::Result<UniqueAttrs> {
        let mut attrs = UniqueAttrs::default();
        syn::meta::parser(|meta| {
            let option = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            attrs
                .parse(&meta, &option)
                .unwrap_or_else(|| Err(meta.error("not a unique option")))
        })
        .parse2(tokens)?;
        Ok(attrs)
    }

    fn error(tokens: TokenStream) -> String {
        match parse(tokens) {
            Ok(_) => panic!("expected an error"),
            Err(error) => error.to_string(),
        }
    }

    /// The argument parameters of `function` and its context parameter.
    fn params(function: syn::ItemFn) -> (Vec<PatType>, Option<PatType>) {
        let mut params: Vec<PatType> = function
            .sig
            .inputs
            .into_iter()
            .filter_map(|input| match input {
                syn::FnArg::Typed(typed) => Some(typed),
                syn::FnArg::Receiver(_) => None,
            })
            .collect();
        let takes_context = params.first().is_some_and(|first| {
            let ty = &first.ty;
            quote!(#ty).to_string().contains("JobContext")
        });
        let context = takes_context.then(|| params.remove(0));
        (params, context)
    }

    #[test]
    fn every_option_becomes_a_builder_call() {
        let attrs = parse(quote! {
            unique_for = 600, unique_until = "start", on_conflict = "replace",
            unique_across_queues = true
        })
        .unwrap();
        assert!(!attrs.is_empty());
        let krate: Path = parse_quote!(::hench);
        let calls: Vec<String> = attrs
            .option_calls(&krate)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            calls,
            [
                ". unique_for (:: core :: time :: Duration :: from_secs (600u64))",
                ". unique_until (:: hench :: UniqueUntil :: Start)",
                ". on_conflict (:: hench :: OnConflict :: Replace)",
                ". unique_across_queues (true)",
            ]
        );
        assert!(UniqueAttrs::default().is_empty());
        let expired = parse(quote!(unique_until = "expired", on_conflict = "drop")).unwrap();
        let calls: Vec<String> = expired
            .option_calls(&krate)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            calls,
            [
                ". unique_until (:: hench :: UniqueUntil :: Expired)",
                ". on_conflict (:: hench :: OnConflict :: Drop)",
            ]
        );
    }

    #[test]
    fn values_are_checked() {
        for (tokens, expected) in [
            (
                quote!(unique_for = 0),
                "expected a number of seconds from 1",
            ),
            (
                quote!(unique_for = 1000000000),
                "expected a number of seconds",
            ),
            (quote!(unique_for = "10"), "expected a number of seconds"),
            (
                quote!(unique_until = "done"),
                "expected one of: \"success\"",
            ),
            (quote!(on_conflict = "skip"), "expected one of: \"drop\""),
            (
                quote!(unique_across_queues = 1),
                "expected `true` or `false`",
            ),
            (quote!(unique_by = []), "expected at least one parameter"),
            (
                quote!(unique_for = 60, unique_for = 70),
                "duplicate option `unique_for`",
            ),
        ] {
            let message = error(tokens.clone());
            assert!(message.contains(expected), "{tokens}: {message}");
        }
    }

    #[test]
    fn unique_by_selects_arguments_in_order() {
        let attrs = parse(quote!(unique_by = [account, user])).unwrap();
        let (args, context) = params(parse_quote! {
            async fn sync(ctx: &JobContext, user: i64, reason: String, account: String) {}
        });
        let krate: Path = parse_quote!(::hench);
        let method = attrs
            .unique_key_method(&krate, &args, context.as_ref())
            .unwrap()
            .unwrap()
            .to_string();
        assert!(
            method.contains("let (__hench_key0 , _ , __hench_key2 ,) = __hench_args"),
            "{method}"
        );
        let account = method.find("to_value (__hench_key2)").unwrap();
        let user = method.find("to_value (__hench_key0)").unwrap();
        assert!(account < user, "in the order given: {method}");
        assert!(method.contains("fn unique_key"), "{method}");
        // Without `unique_by`, no method.
        assert!(
            parse(quote!(unique_for = 5))
                .unwrap()
                .unique_key_method(&krate, &args, context.as_ref())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unique_by_names_are_checked() {
        let (args, context) = params(parse_quote! {
            async fn sync(ctx: &JobContext, user: i64) {}
        });
        let krate: Path = parse_quote!(::hench);
        for (tokens, expected) in [
            (
                quote!(unique_by = [nope]),
                "`nope` is not an argument of the job: its arguments are `user`",
            ),
            (quote!(unique_by = [user, user]), "`user` is listed twice"),
            (quote!(unique_by = [ctx]), "`ctx` is the job context"),
        ] {
            let attrs = parse(tokens.clone()).unwrap();
            let Err(error) = attrs.unique_key_method(&krate, &args, context.as_ref()) else {
                panic!("expected an error for {tokens}");
            };
            assert!(error.to_string().contains(expected), "{tokens}: {error}");
        }
        let (none, _) = params(parse_quote!(
            async fn nothing() {}
        ));
        let attrs = parse(quote!(unique_by = [x])).unwrap();
        let Err(error) = attrs.unique_key_method(&krate, &none, None) else {
            panic!("expected an error");
        };
        assert!(error.to_string().contains("the job takes no arguments"));
    }
}
