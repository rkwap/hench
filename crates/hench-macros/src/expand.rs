//! Turning an `async fn` into a job: signature analysis and code generation.

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote};
use syn::ext::IdentExt as _;
use syn::spanned::Spanned;
use syn::{
    Attribute, FnArg, GenericArgument, Ident, Item, ItemFn, LitInt, LitStr, PatType, Path,
    PathArguments, ReturnType, Type, parse_quote,
};

use crate::attrs::{BoolOrCount, JobAttrs};

/// Expands `#[hench::job(<attr>)] <item>`.
///
/// Problems with the options and with the function are reported together, as
/// `compile_error!`s next to a fallback that reduces follow-up errors elsewhere in the crate:
/// when only the options are wrong, the expansion without them (keeping a valid `crate`
/// path), so that uses of the job still compile; else the item as written.
pub(crate) fn job(attr: TokenStream, item: TokenStream) -> TokenStream {
    if let Ok(Item::Impl(block)) = syn::parse2::<Item>(item.clone()) {
        return registered_impl(attr, block);
    }
    let attr_tokens = attr.clone();
    let attrs = JobAttrs::parse(attr);
    let function = parse_function(item.clone()).and_then(JobFn::analyze);
    match (attrs, function) {
        (Ok(attrs), Ok(function)) => match function.features(&attrs) {
            Ok(features) => function.generate(&attrs, &features),
            // A feature option naming a parameter the job does not have: the expansion
            // without the feature options, so that uses of the job still compile.
            Err(error) => {
                let expansion = function.generate(&attrs, &Features::default());
                let error = error.into_compile_error();
                quote!(#error #expansion)
            }
        },
        (Err(error), Ok(function)) => {
            let fallback = JobAttrs {
                krate: crate_path(&attr_tokens),
                ..JobAttrs::default()
            };
            let expansion = function.generate(&fallback, &Features::default());
            let error = error.into_compile_error();
            quote!(#error #expansion)
        }
        (attrs, Err(error)) => {
            let error = match attrs {
                Err(mut options) => {
                    options.combine(error);
                    options
                }
                Ok(_) => error,
            };
            let error = error.into_compile_error();
            quote!(#error #item)
        }
    }
}

/// The `crate = "..."` option of attribute tokens that failed to parse as a whole, if it is
/// valid on its own, found token by token so that other malformed options do not hide it.
fn crate_path(attr: &TokenStream) -> Option<Path> {
    let tokens: Vec<proc_macro2::TokenTree> = attr.clone().into_iter().collect();
    tokens.windows(3).find_map(|window| match window {
        [
            proc_macro2::TokenTree::Ident(name),
            proc_macro2::TokenTree::Punct(equals),
            proc_macro2::TokenTree::Literal(literal),
        ] if name == "crate" && equals.as_char() == '=' => {
            syn::parse2::<LitStr>(literal.to_token_stream())
                .ok()?
                .parse()
                .ok()
        }
        _ => None,
    })
}

fn parse_function(item: TokenStream) -> syn::Result<ItemFn> {
    match syn::parse2::<Item>(item)? {
        Item::Fn(function) => Ok(function),
        other => Err(syn::Error::new(
            other.span(),
            "#[hench::job] can only be applied to an `async fn`, or to an `impl Job` or \
             `impl IterableJob` block",
        )),
    }
}

/// `#[hench::job]` on `impl Job for T` or `impl IterableJob for T`: the block as written, and
/// the registration of `T::default()` with the jobs found automatically (`T` must implement
/// `Default`). The job's name and options are the trait implementation's, so the attribute
/// only accepts `crate`.
fn registered_impl(attr: TokenStream, block: syn::ItemImpl) -> TokenStream {
    let attrs = match JobAttrs::parse(attr) {
        Ok(attrs) => attrs,
        Err(error) => {
            let error = error.into_compile_error();
            return quote!(#error #block);
        }
    };
    let problem = if attrs.name.is_some() || attrs.has_job_options() {
        Some((
            block.impl_token.span,
            "the name and options of a job implemented by hand are set in its trait \
             implementation (`NAME`, `fn options()`); on an impl block #[hench::job] only \
             accepts `crate`",
        ))
    } else {
        match &block.trait_ {
            None => Some((
                block.impl_token.span,
                "#[hench::job] applies to `impl Job for ...` or `impl IterableJob for ...` \
                 blocks, not to inherent impls",
            )),
            Some((_, path, _))
                if !path.segments.last().is_some_and(|segment| {
                    segment.ident == "Job" || segment.ident == "IterableJob"
                }) =>
            {
                Some((
                    path.span(),
                    "#[hench::job] applies to `impl Job for ...` or `impl IterableJob for ...` \
                     blocks",
                ))
            }
            Some(_) if !block.generics.params.is_empty() => Some((
                block.generics.span(),
                "generic jobs cannot be registered automatically: register each instance with \
                 `HenchBuilder::register`",
            )),
            Some(_) => None,
        }
    };
    if let Some((span, message)) = problem {
        let error = syn::Error::new(span, message).into_compile_error();
        return quote!(#error #block);
    }
    let krate: Path = attrs.krate.unwrap_or_else(|| parse_quote!(::hench));
    let self_ty = &block.self_ty;
    let cfgs = block
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("cfg"));
    quote! {
        #block

        #(#cfgs)*
        #krate::__private::inventory::submit! {
            #krate::__private::JobRegistration::of::<#self_ty>()
        }
    }
}

/// What a parameter type says about the job context.
#[derive(Debug, PartialEq, Eq)]
enum ContextParam {
    /// `&JobContext`.
    Shared,
    /// `&mut JobContext`.
    Mutable,
    /// `JobContext`.
    ByValue,
    /// Anything else.
    No,
}

fn context_param(ty: &Type) -> ContextParam {
    match ty {
        Type::Reference(reference) if names_job_context(&reference.elem) => {
            if reference.mutability.is_some() {
                ContextParam::Mutable
            } else {
                ContextParam::Shared
            }
        }
        Type::Group(group) => context_param(&group.elem),
        Type::Paren(paren) => context_param(&paren.elem),
        ty if names_job_context(ty) => ContextParam::ByValue,
        _ => ContextParam::No,
    }
}

/// Whether a parameter that is not `&JobContext` looks meant as the job context: a reference to
/// a type, where the type or the parameter mentions a context (`ctx: &Ctx` with `type Ctx =
/// JobContext`, `context: &AppContext`).
fn looks_like_context(param: &PatType) -> bool {
    let mut ty = &*param.ty;
    while let Type::Group(syn::TypeGroup { elem, .. }) | Type::Paren(syn::TypeParen { elem, .. }) =
        ty
    {
        ty = elem;
    }
    let Type::Reference(reference) = ty else {
        return false;
    };
    let Type::Path(path) = &*reference.elem else {
        return false;
    };
    let type_name = path
        .path
        .segments
        .last()
        .map(|segment| segment.ident.to_string());
    let param_name = match &*param.pat {
        syn::Pat::Ident(pattern) => Some(pattern.ident.to_string()),
        _ => None,
    };
    [type_name, param_name].into_iter().flatten().any(|name| {
        let name = name.to_lowercase();
        name.contains("ctx") || name.contains("context")
    })
}

/// The lifetime written on a reference type (`'static` in `&'static JobContext`), unless it is
/// the anonymous `'_`.
fn reference_lifetime(ty: &Type) -> Option<&syn::Lifetime> {
    match ty {
        Type::Reference(reference) => reference
            .lifetime
            .as_ref()
            .filter(|lifetime| lifetime.ident != "_"),
        Type::Group(group) => reference_lifetime(&group.elem),
        Type::Paren(paren) => reference_lifetime(&paren.elem),
        _ => None,
    }
}

/// `true` for `JobContext`, `hench::JobContext`, ...
fn names_job_context(ty: &Type) -> bool {
    match ty {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "JobContext"),
        Type::Group(group) => names_job_context(&group.elem),
        Type::Paren(paren) => names_job_context(&paren.elem),
        _ => false,
    }
}

/// Job arguments are deserialized from JSON into values the job owns.
const REFERENCE: &str = "job arguments are deserialized from JSON, so they must be owned \
                         types (for example `String` instead of `&str`)";

/// [`REFERENCE`] for a first parameter, which may be meant as the job context.
const REFERENCE_OR_CONTEXT: &str = "job arguments are deserialized from JSON, so they must be \
                                    owned types (for example `String` instead of `&str`); if \
                                    this is the job context, write its type as `&JobContext` \
                                    (the context is recognized by that name, not through a \
                                    type alias or a renamed import)";

/// A lifetime other than `'static` in an argument type.
const BORROWED: &str = "job arguments are deserialized from JSON, so they cannot borrow: use \
                        `'static` (as in `Cow<'static, str>`) or an owned type";

/// `Cow` without its lifetime, which is elided and borrows.
const COW: &str = "job arguments are deserialized from JSON, so they cannot borrow: write the \
                   lifetime of `Cow` as `'static` (`Cow<'static, str>`), or use an owned type";

/// The span of the first part of `ty` that cannot be a job argument: a reference or a lifetime
/// other than `'static` (job arguments are deserialized, so they must be owned), `impl Trait`
/// or `_`. Elided lifetimes are invisible in the syntax, except that `Cow` always has one.
fn invalid_argument_type(ty: &Type) -> Option<(Span, &'static str)> {
    const IMPL: &str = "job arguments must have concrete types, not `impl Trait`";
    const INFER: &str = "job arguments must have explicit types";
    match ty {
        Type::Reference(reference) => Some((reference.and_token.span, REFERENCE)),
        Type::ImplTrait(implementation) => Some((implementation.impl_token.span, IMPL)),
        Type::Infer(infer) => Some((infer.underscore_token.span, INFER)),
        Type::Group(group) => invalid_argument_type(&group.elem),
        Type::Paren(paren) => invalid_argument_type(&paren.elem),
        Type::Array(array) => invalid_argument_type(&array.elem),
        Type::Slice(slice) => invalid_argument_type(&slice.elem),
        Type::Tuple(tuple) => tuple.elems.iter().find_map(invalid_argument_type),
        Type::Path(path) => {
            let qualified = path
                .qself
                .as_ref()
                .and_then(|qself| invalid_argument_type(&qself.ty));
            qualified.or_else(|| {
                path.path.segments.iter().find_map(|segment| {
                    let arguments = match &segment.arguments {
                        PathArguments::AngleBracketed(arguments) => Some(arguments),
                        _ => None,
                    };
                    let has_lifetime = arguments.is_some_and(|arguments| {
                        arguments
                            .args
                            .iter()
                            .any(|argument| matches!(argument, GenericArgument::Lifetime(_)))
                    });
                    if segment.ident == "Cow" && !has_lifetime {
                        return Some((segment.ident.span(), COW));
                    }
                    arguments?.args.iter().find_map(|argument| match argument {
                        GenericArgument::Type(ty) => invalid_argument_type(ty),
                        GenericArgument::Lifetime(lifetime) if lifetime.ident != "static" => {
                            Some((lifetime.span(), BORROWED))
                        }
                        _ => None,
                    })
                })
            })
        }
        _ => None,
    }
}

/// The span of an `impl Trait` in a return type: a job returns `()` or `Result<(), E>`.
fn opaque_return_type(ty: &Type) -> Option<Span> {
    match ty {
        Type::ImplTrait(implementation) => Some(implementation.impl_token.span),
        Type::Group(group) => opaque_return_type(&group.elem),
        Type::Paren(paren) => opaque_return_type(&paren.elem),
        Type::Tuple(tuple) => tuple.elems.iter().find_map(opaque_return_type),
        Type::Path(path) => path.path.segments.iter().find_map(|segment| {
            let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
                return None;
            };
            arguments.args.iter().find_map(|argument| match argument {
                GenericArgument::Type(ty) => opaque_return_type(ty),
                _ => None,
            })
        }),
        _ => None,
    }
}

/// What the options of the features (unique jobs, throttles) add to a job: calls of
/// `JobOptions` in `options()`, and items of the `impl Job` (`unique_key`, `throttle_key`,
/// `throttle`).
#[derive(Default)]
struct Features {
    option_calls: Vec<TokenStream>,
    job_items: TokenStream,
}

/// An `async fn` that passed the checks, split into the parts the expansion needs.
struct JobFn {
    function: ItemFn,
    /// The parameters after the optional context: `(pattern, type)`.
    args: Vec<PatType>,
    /// Whether the first parameter is `&JobContext`.
    takes_context: bool,
}

impl JobFn {
    fn analyze(function: ItemFn) -> syn::Result<Self> {
        let mut errors: Option<syn::Error> = None;
        let mut fail = |span: Span, message: &str| {
            let error = syn::Error::new(span, message);
            match errors.as_mut() {
                Some(errors) => errors.combine(error),
                None => errors = Some(error),
            }
        };
        let sig = &function.sig;
        if sig.asyncness.is_none() {
            fail(
                sig.ident.span(),
                "#[hench::job] must be applied to an `async fn`",
            );
        }
        if let Some(constness) = &sig.constness {
            fail(constness.span, "job functions cannot be `const`");
        }
        if let Some(unsafety) = &sig.unsafety {
            fail(unsafety.span, "job functions cannot be `unsafe`");
        }
        if let Some(abi) = &sig.abi {
            fail(abi.span(), "job functions cannot have an ABI");
        }
        const GENERIC: &str =
            "job functions cannot be generic: a job has one concrete set of arguments";
        if !sig.generics.params.is_empty() {
            fail(sig.generics.span(), GENERIC);
        } else if let Some(where_clause) = &sig.generics.where_clause {
            fail(where_clause.where_token.span, GENERIC);
        }
        if let Some(variadic) = &sig.variadic {
            fail(variadic.span(), "job functions cannot be variadic");
        }

        let mut args = Vec::new();
        let mut takes_context = false;
        for (index, input) in sig.inputs.iter().enumerate() {
            let typed = match input {
                FnArg::Receiver(receiver) => {
                    fail(
                        receiver.span(),
                        "job functions cannot take `self`: put #[hench::job] on a free function",
                    );
                    continue;
                }
                FnArg::Typed(typed) => typed,
            };
            if let Some(attribute) = typed.attrs.iter().find(|attribute| {
                attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
            }) {
                fail(
                    attribute.span(),
                    "conditional job parameters are not supported: the arguments are stored with \
                     the job and must be the same in every build",
                );
                continue;
            }
            match context_param(&typed.ty) {
                ContextParam::Shared if index == 0 => {
                    takes_context = true;
                    if let Some(lifetime) = reference_lifetime(&typed.ty) {
                        fail(
                            lifetime.span(),
                            "the job context is lent to one execution: write \
                             `ctx: &JobContext`, without a lifetime",
                        );
                    }
                }
                ContextParam::Shared => fail(
                    typed.ty.span(),
                    "the job context must be the first parameter",
                ),
                ContextParam::Mutable => fail(
                    typed.ty.span(),
                    "take the job context by shared reference: `ctx: &JobContext`",
                ),
                ContextParam::ByValue => fail(
                    typed.ty.span(),
                    "take the job context by reference: `ctx: &JobContext`",
                ),
                ContextParam::No => {
                    if let Some((span, message)) = invalid_argument_type(&typed.ty) {
                        // The context can only come first.
                        let message =
                            if index == 0 && message == REFERENCE && looks_like_context(typed) {
                                REFERENCE_OR_CONTEXT
                            } else {
                                message
                            };
                        fail(span, message);
                    }
                    args.push(typed.clone());
                }
            }
        }
        if let ReturnType::Type(_, ty) = &sig.output
            && let Some(span) = opaque_return_type(ty)
        {
            fail(
                span,
                "job functions return `()` or `Result<(), E>` with a concrete error type, not \
                 `impl Trait`",
            );
        }

        match errors {
            Some(errors) => Err(errors),
            None => Ok(Self {
                function,
                args,
                takes_context,
            }),
        }
    }

    /// The job's name: `name = ".."`, else the function's.
    fn name(&self, attrs: &JobAttrs) -> LitStr {
        let ident = &self.function.sig.ident;
        attrs
            .name
            .clone()
            .unwrap_or_else(|| LitStr::new(&ident.unraw().to_string(), ident.span()))
    }

    /// The code of the feature options, checked against the function's parameters
    /// (`unique_by` and `throttle_by` name arguments, in order).
    fn features(&self, attrs: &JobAttrs) -> syn::Result<Features> {
        let krate: Path = attrs.krate.clone().unwrap_or_else(|| parse_quote!(::hench));
        let name = self.name(attrs).value();
        // Every argument's name, by position (an argument declared with a pattern gets a name
        // no option can spell, so that the positions stay right).
        let params: Vec<Ident> = self
            .args
            .iter()
            .enumerate()
            .map(|(index, arg)| {
                crate::unique::param_name(arg)
                    .cloned()
                    .unwrap_or_else(|| format_ident!("__hench_pattern{}", index))
            })
            .collect();
        let context = self.context();
        let context_name = context.and_then(crate::unique::param_name);
        let mut errors: Option<syn::Error> = None;
        let mut fail = |error: syn::Error| match errors.as_mut() {
            Some(errors) => errors.combine(error),
            None => errors = Some(error),
        };
        if let Err(error) = attrs.throttle.check(&params, context_name) {
            fail(error);
        }
        if let Err(error) = attrs.unique.check() {
            fail(error);
        }
        let unique_key = match attrs.unique.unique_key_method(&krate, &self.args, context) {
            Ok(method) => method,
            Err(error) => {
                fail(error);
                None
            }
        };
        if let Some(errors) = errors {
            return Err(errors);
        }
        let mut option_calls = attrs.unique.option_calls(&krate);
        option_calls.extend(attrs.throttle.option_call(&name));
        let throttle_items = attrs.throttle.job_items(&krate, &name, &params);
        Ok(Features {
            option_calls,
            job_items: quote!(#unique_key #throttle_items),
        })
    }

    /// The context parameter, when the function takes one.
    fn context(&self) -> Option<&PatType> {
        if !self.takes_context {
            return None;
        }
        match self.function.sig.inputs.first()? {
            FnArg::Typed(typed) => Some(typed),
            FnArg::Receiver(_) => None,
        }
    }

    fn generate(&self, attrs: &JobAttrs, features: &Features) -> TokenStream {
        let krate: Path = attrs.krate.clone().unwrap_or_else(|| parse_quote!(::hench));
        let function = &self.function;
        let vis = &function.vis;
        let ident = &function.sig.ident;
        let name = self.name(attrs);

        // Documentation and deprecation go on the struct, `cfg` on every generated item,
        // `allow` on the struct and the body, everything else (`tracing::instrument`, lints,
        // ...) on the function holding the body.
        let mut docs: Vec<&Attribute> = Vec::new();
        let mut cfgs: Vec<&Attribute> = Vec::new();
        let mut allows: Vec<Attribute> = Vec::new();
        let mut body_attrs: Vec<Attribute> = Vec::new();
        for attribute in &function.attrs {
            let path = attribute.path();
            if path.is_ident("doc") || path.is_ident("deprecated") {
                docs.push(attribute);
            } else if path.is_ident("cfg") {
                cfgs.push(attribute);
            } else if path.is_ident("expect") {
                // The lint may fire on the struct or on the body, and an expectation on the
                // other item would be unfulfilled: allow it on both instead.
                if let syn::Meta::List(list) = &attribute.meta {
                    let lints = &list.tokens;
                    let allow: Attribute = parse_quote!(#[allow(#lints)]);
                    allows.push(allow.clone());
                    body_attrs.push(allow);
                }
            } else {
                if path.is_ident("allow") {
                    allows.push(attribute.clone());
                }
                body_attrs.push(attribute.clone());
            }
        }

        let arg_types: Vec<&Type> = self.args.iter().map(|arg| &*arg.ty).collect();
        let arg_idents: Vec<Ident> = (0..self.args.len())
            .map(|index| format_ident!("__hench_arg{}", index))
            .collect();
        let context = format_ident!("__hench_ctx");
        // The body keeps the function's name, so that what it reports (for example the span
        // of `#[tracing::instrument]`) is named after the job.
        let inner = ident.clone();
        let call_args: Vec<TokenStream> = self
            .takes_context
            .then(|| quote!(#context))
            .into_iter()
            .chain(arg_idents.iter().map(|ident| quote!(#ident)))
            .collect();

        let mut inner_sig = function.sig.clone();
        inner_sig.ident = inner.clone();
        let block = &function.block;
        // The explicit type argument makes an unsupported return type an error at the
        // function's return type.
        let output = match &function.sig.output {
            ReturnType::Default => quote!(()),
            ReturnType::Type(_, ty) => quote!(#ty),
        };
        let result = quote! {
            #krate::__private::into_job_result::<#output>(#inner(#(#call_args),*).await)
        };

        let options = attrs.has_job_options().then(|| {
            let calls = option_calls(attrs);
            let feature_calls = &features.option_calls;
            quote! {
                fn options() -> #krate::JobOptions {
                    #krate::JobOptions::new() #(#calls)* #(#feature_calls)*
                }
            }
        });
        let feature_items = &features.job_items;

        // Lints: the generated code allows nothing but what the struct's name needs
        // (`non_camel_case_types`), so that the job's body, nested in `perform`, keeps every
        // lint of the crate (an `allow(deprecated)` around it silenced the deprecation warnings
        // of whatever the job uses) and crates that forbid a lint (`dead_code`, `deprecated`)
        // can define jobs: a forbidden lint cannot be allowed. Naming a deprecated job warns
        // even in its own generated code, so such a job is named through an alias defined
        // under the expansion's only `allow(deprecated)`; the helpers count as used through the
        // references below, instead of an `allow(dead_code)`.
        let deprecated = function
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("deprecated"));
        let (alias, job_type) = if deprecated {
            let alias = format_ident!("__HenchJob");
            (
                Some(quote! {
                    #[allow(deprecated)]
                    type #alias = #ident;
                }),
                alias,
            )
        } else {
            (None, ident.clone())
        };

        quote! {
            #(#cfgs)*
            #(#docs)*
            #(#allows)*
            #[allow(non_camel_case_types)]
            #[derive(
                ::core::clone::Clone,
                ::core::marker::Copy,
                ::core::fmt::Debug,
                ::core::default::Default,
            )]
            #vis struct #ident;

            #(#cfgs)*
            const _: () = {
            #alias

            impl #krate::Job for #job_type {
                const NAME: &'static str = #name;
                type Args = (#(#arg_types,)*);

                #options

                #feature_items

                async fn perform(
                    &self,
                    #context: &#krate::JobContext,
                    __hench_args: <Self as #krate::Job>::Args,
                ) -> ::core::result::Result<(), #krate::JobError> {
                    // The macro needs an `async fn`, so a body without `.await` cannot follow
                    // clippy's advice to drop the `async`.
                    #[allow(clippy::unused_async)]
                    #(#body_attrs)*
                    #inner_sig #block

                    let (#(#arg_idents,)*) = __hench_args;
                    #result
                }
            }

            // Bindings in generated code are `__hench_`-prefixed: a job (a unit struct) named
            // like a binding would turn it into a pattern.
            impl #job_type {
                /// Pushes the job for immediate execution. Returns its jid, or `None` if
                /// client middleware stopped the push. An error after a timeout may come with
                /// the job pushed all the same (see `hench::Client`, "When a push fails").
                #vis async fn perform_async(
                    __hench_client: &#krate::Client,
                    __hench_args: <Self as #krate::Job>::Args,
                ) -> #krate::Result<::core::option::Option<::std::string::String>> {
                    <Self as #krate::JobExt>::perform_async(__hench_client, __hench_args).await
                }

                /// Schedules the job to run after the given delay. A delay of 1e9 seconds or
                /// more is taken as an absolute Unix timestamp; a zero delay pushes
                /// immediately.
                #vis async fn perform_in(
                    __hench_client: &#krate::Client,
                    __hench_delay: ::core::time::Duration,
                    __hench_args: <Self as #krate::Job>::Args,
                ) -> #krate::Result<::core::option::Option<::std::string::String>> {
                    <Self as #krate::JobExt>::perform_in(__hench_client, __hench_delay, __hench_args).await
                }

                /// Schedules the job to run at the given time; a time that is not in the future
                /// pushes immediately.
                #vis async fn perform_at(
                    __hench_client: &#krate::Client,
                    __hench_at: ::std::time::SystemTime,
                    __hench_args: <Self as #krate::Job>::Args,
                ) -> #krate::Result<::core::option::Option<::std::string::String>> {
                    <Self as #krate::JobExt>::perform_at(__hench_client, __hench_at, __hench_args).await
                }

                /// Pushes one job per element of the arguments, in as few round trips as possible.
                /// Returns one entry per job: its jid, or `None` if client middleware stopped
                /// it.
                #vis async fn perform_bulk(
                    __hench_client: &#krate::Client,
                    __hench_args: ::std::vec::Vec<<Self as #krate::Job>::Args>,
                ) -> #krate::Result<
                    ::std::vec::Vec<::core::option::Option<::std::string::String>>,
                > {
                    <Self as #krate::JobExt>::perform_bulk(__hench_client, __hench_args).await
                }

                /// Runs the job right now, in this process, without Redis: the arguments make
                /// a JSON round trip, middleware does not run and panics are returned as
                /// errors.
                #vis async fn perform_inline(
                    __hench_args: <Self as #krate::Job>::Args,
                ) -> ::core::result::Result<(), #krate::JobError> {
                    <Self as #krate::JobExt>::perform_inline(&Self, __hench_args).await
                }

                /// Options for a single push, e.g. `set().queue("low").perform_async(&client,
                /// args)`.
                #vis fn set() -> #krate::JobSetter<Self> {
                    <Self as #krate::JobExt>::set()
                }
            }

            // Used, for the jobs nobody enqueues in their own crate.
            let _ = (
                #job_type::perform_async,
                #job_type::perform_in,
                #job_type::perform_at,
                #job_type::perform_bulk,
                #job_type::perform_inline,
                #job_type::set,
            );

            #krate::__private::inventory::submit! {
                #krate::__private::JobRegistration::of::<#job_type>()
            }
            };
        }
    }
}

/// The builder calls producing the job's options.
fn option_calls(attrs: &JobAttrs) -> Vec<TokenStream> {
    let mut calls = Vec::new();
    if let Some(queue) = &attrs.queue {
        calls.push(quote!(.queue(#queue)));
    }
    if let Some(retry) = &attrs.retry {
        let value = bool_or_count(retry);
        calls.push(quote!(.retry(#value)));
    }
    if let Some(backtrace) = &attrs.backtrace {
        let value = bool_or_count(backtrace);
        calls.push(quote!(.backtrace(#value)));
    }
    if let Some(dead) = &attrs.dead {
        calls.push(quote!(.dead(#dead)));
    }
    if let Some(tags) = &attrs.tags {
        // The explicit types make `tags = []` compile too.
        let count = tags.len();
        calls.push(quote!(.tags::<[&'static str; #count], &'static str>([#(#tags),*])));
    }
    if let Some((secs, span)) = attrs.retry_for {
        let secs = LitInt::new(&format!("{secs}u64"), span);
        calls.push(quote!(.retry_for(::core::time::Duration::from_secs(#secs))));
    }
    if let Some(queue) = &attrs.retry_queue {
        calls.push(quote!(.retry_queue(#queue)));
    }
    if let Some(level) = &attrs.log_level {
        // The payload key `JobOptions::log_level` writes.
        calls.push(quote!(.custom("log_level", #level)));
    }
    calls
}

fn bool_or_count(value: &BoolOrCount) -> TokenStream {
    match value {
        BoolOrCount::Bool(value) => quote!(#value),
        BoolOrCount::Count(count, span) => {
            let count = LitInt::new(&format!("{count}u32"), *span);
            quote!(#count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand(attr: TokenStream, item: TokenStream) -> String {
        let out = job(attr, item).to_string();
        assert!(!out.contains("compile_error"), "{out}");
        out
    }

    fn error(attr: TokenStream, item: TokenStream) -> String {
        let out = job(attr, item).to_string();
        assert!(
            out.contains("compile_error"),
            "expected an error, got {out}"
        );
        out
    }

    #[test]
    fn generates_a_struct_a_job_and_a_registration() {
        let out = expand(
            quote!(queue = "mailers", retry = 5),
            quote! {
                /// Sends an email.
                pub async fn send_email(ctx: &JobContext, user_id: i64, template: String) -> Result<(), MyError> {
                    Ok(())
                }
            },
        );
        assert!(out.contains("pub struct send_email ;"), "{out}");
        assert!(out.contains("# [doc = r\" Sends an email.\"]"), "{out}");
        assert!(out.contains("impl :: hench :: Job for send_email"), "{out}");
        assert!(
            out.contains("const NAME : & 'static str = \"send_email\""),
            "{out}"
        );
        assert!(out.contains("type Args = (i64 , String ,) ;"), "{out}");
        assert!(
            out.contains(". queue (\"mailers\") . retry (5u32)"),
            "{out}"
        );
        assert!(
            out.contains(
                "async fn send_email (ctx : & JobContext , user_id : i64 , template : String)"
            ),
            "{out}"
        );
        assert!(
            out.contains("send_email (__hench_ctx , __hench_arg0 , __hench_arg1) . await"),
            "{out}"
        );
        assert!(
            out.contains("JobRegistration :: of :: < send_email >"),
            "{out}"
        );
        assert!(out.contains("pub async fn perform_async"), "{out}");
    }

    #[test]
    fn argument_tuples_follow_the_parameter_count() {
        let none = expand(
            quote!(),
            quote!(
                async fn none() {}
            ),
        );
        assert!(none.contains("type Args = () ;"), "{none}");
        assert!(none.contains("let () = __hench_args ;"), "{none}");
        assert!(!none.contains("fn options"), "{none}");
        assert!(none.contains("into_job_result :: < () >"), "{none}");
        let one = expand(
            quote!(),
            quote!(
                async fn one(x: u8) -> () {}
            ),
        );
        assert!(one.contains("type Args = (u8 ,) ;"), "{one}");
        assert!(
            one.contains("let (__hench_arg0 ,) = __hench_args ;"),
            "{one}"
        );
        assert!(one.contains("one (__hench_arg0) . await"), "{one}");
        let context_only = expand(
            quote!(),
            quote!(
                async fn c(ctx: &hench::JobContext) {}
            ),
        );
        assert!(context_only.contains("type Args = () ;"), "{context_only}");
        assert!(
            context_only.contains("c (__hench_ctx) . await"),
            "{context_only}"
        );
    }

    #[test]
    fn options_and_names() {
        let out = expand(
            quote!(
                name = "Custom",
                backtrace = 10,
                dead = false,
                tags = ["a", "b"],
                retry_for = 60,
                retry_queue = "slow",
                retry = false,
                crate = "my_hench"
            ),
            quote!(
                async fn r#type() {}
            ),
        );
        assert!(out.contains("\"Custom\""), "{out}");
        assert!(out.contains(". backtrace (10u32)"), "{out}");
        assert!(out.contains(". dead (false)"), "{out}");
        assert!(
            out.contains(
                ". tags :: < [& 'static str ; 2usize] , & 'static str > ([\"a\" , \"b\"])"
            ),
            "{out}"
        );
        assert!(out.contains("from_secs (60u64)"), "{out}");
        assert!(out.contains(". retry_queue (\"slow\")"), "{out}");
        assert!(out.contains(". retry (false)"), "{out}");
        assert!(out.contains("impl my_hench :: Job for r#type"), "{out}");
        let raw = expand(
            quote!(),
            quote!(
                async fn r#type() {}
            ),
        );
        assert!(raw.contains("\"type\""), "{raw}");
    }

    #[test]
    fn attributes_are_distributed() {
        let out = expand(
            quote!(),
            quote! {
                #[cfg(feature = "jobs")]
                #[allow(clippy::too_many_arguments)]
                #[tracing::instrument]
                /// Docs.
                pub(crate) async fn job() {}
            },
        );
        // `cfg` on the struct and on the block holding the impls and the registration.
        assert_eq!(out.matches("cfg (feature = \"jobs\")").count(), 2, "{out}");
        // `allow` on the struct and the body.
        assert_eq!(
            out.matches("clippy :: too_many_arguments").count(),
            2,
            "{out}"
        );
        assert_eq!(out.matches("tracing :: instrument").count(), 1, "{out}");
        let deprecated = expand(
            quote!(),
            quote!(
                #[deprecated(note = "old")]
                async fn old() {}
            ),
        );
        let struct_at = deprecated.find("struct old").unwrap();
        let attribute_at = deprecated.find("# [deprecated (note = \"old\")]").unwrap();
        assert!(attribute_at < struct_at, "{deprecated}");
        assert_eq!(
            deprecated.matches("note = \"old\"").count(),
            1,
            "{deprecated}"
        );
        assert!(out.contains("pub (crate) struct job"), "{out}");
        assert!(out.contains("pub (crate) async fn perform_async"), "{out}");
    }

    #[test]
    fn the_generated_code_allows_as_few_lints_as_possible() {
        let out = expand(
            quote!(),
            quote!(
                async fn job(id: u64) {}
            ),
        );
        // The struct's name; clippy's advice to drop `async`, which the macro needs.
        assert_eq!(out.matches("# [allow (").count(), 2, "{out}");
        assert!(out.contains("# [allow (non_camel_case_types)]"), "{out}");
        assert!(out.contains("# [allow (clippy :: unused_async)]"), "{out}");
        // The helpers are used instead of `allow(dead_code)`.
        assert!(out.contains("job :: perform_bulk"), "{out}");

        // A deprecated job is named through an alias, the only item allowing `deprecated`:
        // the body keeps the crate's deprecation warnings.
        let deprecated = expand(
            quote!(),
            quote!(
                #[deprecated]
                async fn old() {}
            ),
        );
        assert_eq!(
            deprecated.matches("allow (deprecated)").count(),
            1,
            "{deprecated}"
        );
        assert!(
            deprecated.contains("# [allow (deprecated)] type __HenchJob = old ;"),
            "{deprecated}"
        );
        assert!(
            deprecated.contains("impl :: hench :: Job for __HenchJob"),
            "{deprecated}"
        );
        assert!(
            deprecated.contains("JobRegistration :: of :: < __HenchJob >"),
            "{deprecated}"
        );
    }

    #[test]
    fn borrowed_arguments_and_contexts_are_explained() {
        for (item, message) in [
            (
                quote!(
                    async fn anonymous(s: Cow<'_, str>) {}
                ),
                "cannot borrow: use `'static`",
            ),
            (
                quote!(
                    async fn named(s: Cow<'a, str>) {}
                ),
                "cannot borrow: use `'static`",
            ),
            (
                quote!(
                    async fn elided(s: std::borrow::Cow<str>) {}
                ),
                "write the lifetime of `Cow` as `'static`",
            ),
            (
                quote!(
                    async fn nested(s: Vec<Option<Cow<str>>>) {}
                ),
                "write the lifetime of `Cow` as `'static`",
            ),
            (
                quote!(
                    async fn context(ctx: &'static JobContext) {}
                ),
                "without a lifetime",
            ),
            (
                quote!(
                    async fn aliased(ctx: &Ctx, n: u32) {}
                ),
                "write its type as `&JobContext`",
            ),
            (
                quote!(
                    async fn opaque() -> impl Sized {}
                ),
                "not `impl Trait`",
            ),
            (
                quote!(
                    async fn opaque_error() -> Result<(), impl std::error::Error> {}
                ),
                "not `impl Trait`",
            ),
        ] {
            let out = error(quote!(), item);
            assert!(out.contains(message), "{message}: {out}");
        }
        // Owned and `'static` forms are fine, and a borrowed argument after the context gets
        // the plain message.
        expand(
            quote!(),
            quote!(
                async fn fine(ctx: &'_ JobContext, a: Cow<'static, str>, b: Box<str>) {}
            ),
        );
        for item in [
            quote!(
                async fn second(ctx: &JobContext, s: &str) {}
            ),
            quote!(
                async fn first(name: &str) {}
            ),
        ] {
            let out = error(quote!(), item);
            assert!(out.contains("must be owned types"), "{out}");
            assert!(!out.contains("job context"), "{out}");
        }
        let out = error(
            quote!(),
            quote!(
                async fn renamed(context: &AppState) {}
            ),
        );
        assert!(out.contains("write its type as `&JobContext`"), "{out}");
    }

    #[test]
    fn rejects_unsupported_functions() {
        assert!(
            error(
                quote!(),
                quote!(
                    fn sync() {}
                )
            )
            .contains("`async fn`")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    struct S;
                )
            )
            .contains("only be applied to an `async fn`")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn g<T>(x: T) {}
                )
            )
            .contains("generic")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn w()
                    where
                        u8: Copy,
                    {
                    }
                )
            )
            .contains("generic")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn s(&self) {}
                )
            )
            .contains("`self`")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async unsafe fn u() {}
                )
            )
            .contains("`unsafe`")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async extern "C" fn e() {}
                )
            )
            .contains("ABI")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn r(s: &str) {}
                )
            )
            .contains("owned")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn r(s: Vec<&str>) {}
                )
            )
            .contains("owned")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn r(s: (u8, &str)) {}
                )
            )
            .contains("owned")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn r(s: [&str; 2]) {}
                )
            )
            .contains("owned")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn i(s: impl Copy) {}
                )
            )
            .contains("impl Trait")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn c(x: u8, ctx: &JobContext) {}
                )
            )
            .contains("first")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn c(ctx: &mut JobContext) {}
                )
            )
            .contains("shared")
        );
        assert!(
            error(
                quote!(),
                quote!(
                    async fn c(ctx: JobContext) {}
                )
            )
            .contains("reference")
        );
    }

    #[test]
    fn errors_come_with_a_fallback() {
        // Invalid options: the job is still generated (without options) next to the error.
        let out = error(
            quote!(queu = "x"),
            quote!(
                async fn job(id: u64) {}
            ),
        );
        assert!(out.contains("unknown option"), "{out}");
        assert!(out.contains("impl :: hench :: Job for job"), "{out}");
        assert!(!out.contains("fn options"), "{out}");
        // An invalid function is kept as written.
        let out = error(
            quote!(),
            quote!(
                fn sync(id: u64) {}
            ),
        );
        assert!(out.contains("fn sync (id : u64) { }"), "{out}");
        assert!(!out.contains("impl"), "{out}");
        let out = error(
            quote!(),
            quote!(
                struct S;
            ),
        );
        assert!(out.contains("struct S ;"), "{out}");
    }

    #[test]
    fn invalid_options_keep_a_valid_crate_path() {
        let out = error(
            quote!(crate = "::renamed", queue = 5),
            quote!(
                async fn job() {}
            ),
        );
        assert!(out.contains("impl :: renamed :: Job for job"), "{out}");
        assert!(!out.contains(":: hench ::"), "{out}");
        assert_eq!(crate_path(&quote!(queue = 5)), None);
        assert_eq!(crate_path(&quote!(crate = 5)), None);
        assert_eq!(crate_path(&quote!(+++)), None);
        // Found even next to an option that is not valid syntax.
        let path = crate_path(&quote!(queue = "a" "b", crate = "::renamed")).unwrap();
        assert_eq!(quote!(#path).to_string(), ":: renamed");
    }

    #[test]
    fn expectations_become_allowances_on_both_items() {
        let out = expand(
            quote!(),
            quote! {
                #[expect(missing_docs, reason = "internal")]
                pub async fn job() {}
            },
        );
        assert_eq!(
            out.matches("# [allow (missing_docs , reason = \"internal\")]")
                .count(),
            2,
            "{out}"
        );
        assert!(!out.contains("expect"), "{out}");
    }

    #[test]
    fn a_job_may_be_called_args() {
        let out = expand(
            quote!(),
            quote!(
                async fn args(x: u32) {}
            ),
        );
        assert!(
            out.contains("let (__hench_arg0 ,) = __hench_args ;"),
            "{out}"
        );
    }

    #[test]
    fn conditional_parameters_are_rejected() {
        let out = error(
            quote!(),
            quote!(
                async fn c(#[cfg(test)] x: u8, y: u8) {}
            ),
        );
        assert!(out.contains("conditional job parameters"), "{out}");
        let out = error(
            quote!(),
            quote!(
                async fn c(#[cfg_attr(test, allow(unused))] x: u8) {}
            ),
        );
        assert!(out.contains("conditional job parameters"), "{out}");
        // Other parameter attributes are kept on the body.
        let out = expand(
            quote!(),
            quote!(
                async fn c(#[allow(unused)] x: u8) {}
            ),
        );
        assert!(out.contains("# [allow (unused)] x : u8"), "{out}");
    }

    #[test]
    fn reports_every_problem_at_once() {
        let message = error(
            quote!(queue = ""),
            quote!(
                fn both<T>(name: &str) {}
            ),
        );
        assert!(message.contains("must not be empty"), "{message}");
        assert!(message.contains("`async fn`"), "{message}");
        assert!(message.contains("generic"), "{message}");
        assert!(message.contains("owned"), "{message}");
    }
}
