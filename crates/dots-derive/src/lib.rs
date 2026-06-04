//! Procedural macros for dots-rust.
//!
//! `#[derive(DotsStruct)]` generates the runtime metadata, accessors,
//! builder methods, and `StructValue` impl for a DOTS struct.
//!
//! Each field must be declared as `Option<T>` and tagged with
//! `#[dots(tag = N)]` (and optionally `#[dots(tag = N, key)]`).
//! The struct itself may carry `#[dots(name = "WireName", cached, ...)]`
//! to override the wire name and set struct-level flags.
//!
//! # What's emitted
//!
//! For each derived struct, the macro produces:
//!
//! 1. A `&'static StructDescriptor` constant exposed as `T::DESCRIPTOR`,
//!    carrying the type's `(size, align)`, per-property `(offset, kind, vtable)`,
//!    and struct-level flags.
//! 2. Per-property [`PropertyVtable`] statics whose function pointers point at
//!    monomorphizations of the generic `dots_core::layout::opt_*` helpers.
//!    These are how the descriptor-driven codec encodes/decodes values
//!    without knowing the concrete `T` at the call site.
//! 3. Accessor methods (`fn field() -> Option<&T>`, `fn has_field()`,
//!    `fn with_field(value)`, `fn clear_field()`).
//! 4. A `StructValue` impl exposing the descriptor, valid set, type
//!    erasure, and a layout-compatible `data_ptr` for the codec.
//!
//! No `minicbor::Encode`/`Decode` impls are produced for the struct
//! itself — encoding goes through the descriptor's vtable thunks so
//! the same code path serves typed structs and dynamic `AnyStruct`.
//!
//! [`PropertyVtable`]: dots_core::PropertyVtable

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Data, DataEnum, DataStruct, DeriveInput, Field, Fields, GenericArgument, Ident, LitInt,
    LitStr, PathArguments, Type, Variant, parse_macro_input, spanned::Spanned,
};

#[proc_macro_derive(DotsStruct, attributes(dots))]
pub fn derive_dots_struct(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive a DOTS enum.
///
/// Each variant must be unit-style (no payload) and tagged with
/// `#[dots(tag = N)]`. The wire `int32` value defaults to `tag` but
/// can be overridden with `#[dots(tag = N, value = M)]`.
///
/// # Emitted code
///
/// 1. `T::DESCRIPTOR` static `&'static EnumDescriptor`.
/// 2. `impl DotsTypeKind for T` exposing `FieldKind::Enum(Self::DESCRIPTOR)`.
/// 3. `impl DotsField for T` that encodes/decodes the wire `int32`,
///    matching variants by value.
#[proc_macro_derive(DotsEnum, attributes(dots))]
pub fn derive_dots_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_enum(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Function-like macro `dots!` — terse constructor for DOTS structs.
///
/// Doc lives on the re-export in `dots-core::dots`.
#[proc_macro]
pub fn dots(input: TokenStream) -> TokenStream {
    let expr = parse_macro_input!(input as syn::ExprStruct);
    expand_dots_struct(&expr).into()
}

fn expand_dots_struct(expr: &syn::ExprStruct) -> TokenStream2 {
    if let Some(qself) = &expr.qself {
        return syn::Error::new_spanned(
            &qself.ty,
            "dots! does not support qualified self paths (<T as Trait>::...)",
        )
        .to_compile_error();
    }

    let path = &expr.path;

    // With an explicit `..rest` base, keep the original struct-literal
    // expansion: the base supplies the remaining fields, so there's no
    // key contract to enforce and no companion needed. (Bare-`T` keys
    // combined with `..rest` are an unsupported edge for now.)
    if let Some(rest) = &expr.rest {
        let field_assignments = expr.fields.iter().map(|fv| {
            let member = &fv.member;
            let value_tokens = expand_dots_field_value(&fv.expr);
            quote! { #member: #value_tokens }
        });
        return quote! {
            {
                #[allow(clippy::needless_update)]
                #path {
                    #(#field_assignments,)*
                    ..#rest
                }
            }
        };
    }

    // No base: delegate to the type's companion macro (Escape B). It
    // knows which fields are keys, so it can default absent optionals to
    // `None`, coerce bare-`T` keys, and `compile_error!` on a missing
    // key — none of which this schema-blind proc-macro can do itself.
    let Some(last) = path.segments.last() else {
        return syn::Error::new_spanned(path, "dots!: type path must not be empty").to_compile_error();
    };
    let macro_ident = ctor_macro_ident(&last.ident);
    let field_pairs = expr.fields.iter().map(|fv| {
        let member = &fv.member;
        let value = dots_value_for_companion(&fv.expr);
        quote! { #member : #value }
    });

    quote! {
        #macro_ident! { @ty #path ; #( #field_pairs ),* }
    }
}

/// Value-expression preprocessing for the companion path: a nested
/// struct literal is rewritten recursively (so it too routes through its
/// type's companion), everything else is passed through verbatim. The
/// per-field coercion (`Some`-wrap / bare-key) happens inside the
/// companion macro, not here.
fn dots_value_for_companion(expr: &syn::Expr) -> TokenStream2 {
    match expr {
        syn::Expr::Struct(inner) => expand_dots_struct(inner),
        other => quote! { #other },
    }
}

fn expand_dots_field_value(expr: &syn::Expr) -> TokenStream2 {
    // A nested struct literal is rewritten recursively; the result is
    // then wrapped in `Some(_)` by the same `DotsAssign` path the
    // top-level fields use.
    let inner = match expr {
        syn::Expr::Struct(inner) => expand_dots_struct(inner),
        other => quote! { #other },
    };
    quote! {
        {
            #[allow(unused_imports)]
            use ::dots_core::DotsAssignGeneric as _;
            ::dots_core::DotsAssign(#inner).into_dots_field()
        }
    }
}

#[derive(Default)]
struct ContainerAttrs {
    name: Option<String>,
    cached: bool,
    internal: bool,
    persistent: bool,
    cleanup: bool,
    local: bool,
    substruct_only: bool,
}

#[derive(Default)]
struct FieldAttrs {
    tag: Option<u32>,
    is_key: bool,
}

struct DotsField<'a> {
    ident: &'a Ident,
    /// The value type `T`. For ordinary `Option<T>` fields this is the
    /// `Option`'s inner type; for a bare-`T` key (`bare_key == true`) it
    /// is the field type itself.
    inner_ty: &'a Type,
    tag: u32,
    is_key: bool,
    /// True when this is a `#[dots(key)]` field written as bare `T`
    /// (not `Option<T>`): stored unwrapped, always-set, accessed as `&T`.
    bare_key: bool,
    kind: TokenStream2,
    /// `///` doc-comment lines on the field, with the leading `=`
    /// stripped. Used to populate the generated `new`-constructor's
    /// `# Arguments` section so IDE hover on `Foo::new(...)` shows
    /// what each parameter means.
    doc_lines: Vec<String>,
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let struct_ident = &input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "DotsStruct does not support generic structs",
        ));
    }

    let container = parse_container_attrs(&input.attrs)?;
    let wire_name = container
        .name
        .clone()
        .unwrap_or_else(|| struct_ident.to_string());

    let data_struct = match input.data {
        Data::Struct(ds) => ds,
        Data::Enum(e) => {
            return Err(syn::Error::new(
                e.enum_token.span(),
                "DotsStruct can only be derived for structs",
            ));
        }
        Data::Union(u) => {
            return Err(syn::Error::new(
                u.union_token.span(),
                "DotsStruct can only be derived for structs",
            ));
        }
    };

    let fields = collect_fields(&data_struct)?;

    // Detect duplicate tags
    for (i, a) in fields.iter().enumerate() {
        for b in &fields[i + 1..] {
            if a.tag == b.tag {
                return Err(syn::Error::new(
                    b.ident.span(),
                    format!("duplicate tag {} (also used by `{}`)", b.tag, a.ident),
                ));
            }
        }
    }

    // Per-property vtable statics + descriptor entries.
    let property_decls = fields.iter().map(|f| property_decl(struct_ident, f));

    let property_descriptors = fields.iter().map(|f| {
        let name = unraw(f.ident);
        let tag = f.tag;
        let is_key = f.is_key;
        let kind = &f.kind;
        let vtable_ident = vtable_ident(f.ident);
        let field_ident = f.ident;
        quote! {
            ::dots_core::PropertyDescriptor {
                name: #name,
                tag: #tag,
                is_key: #is_key,
                offset: ::core::mem::offset_of!(#struct_ident, #field_ident),
                kind: #kind,
                vtable: &#vtable_ident,
            }
        }
    });

    let flags_expr = build_flags_expr(&container);

    // Per-type companion macro that `dots!` delegates to (Escape B).
    let companion = companion_macro(struct_ident, &fields);

    // Per-field constants for the filter DSL (one PropertySet
    // projection-bit, plus a typed `Attr<Self, V>` handle for leaf
    // value types that the wire predicate value-slots support).
    // Emitted on the struct directly so users can write
    // `Pinger::PROP_SEQUENCE` and `Pinger::SEQUENCE.eq(value)`.
    let filter_consts = fields.iter().map(|f| {
        let upper = Ident::new(
            &unraw(f.ident).to_uppercase(),
            f.ident.span(),
        );
        let prop_ident = Ident::new(&format!("PROP_{upper}"), f.ident.span());
        let tag = f.tag;
        let prop_const = quote! {
            #[doc = concat!("Single-bit `PropertySet` selecting the `", stringify!(#upper), "` property — useful for `FilterBuilder::project` masks.")]
            pub const #prop_ident: ::dots_core::PropertySet =
                ::dots_core::PropertySet::EMPTY.with_tag(#tag);
        };
        if is_dsl_leaf_type(f.inner_ty) {
            let inner_ty = f.inner_ty;
            quote! {
                #prop_const

                #[doc = concat!("Filter DSL handle for the `", stringify!(#upper), "` property; compose with `.eq(v)`, `.lt(v)`, etc. to build a `Predicate<Self>`.")]
                pub const #upper: ::dots_model::filter::Attr<Self, #inner_ty> =
                    ::dots_model::filter::Attr::new(#tag);
            }
        } else {
            prop_const
        }
    });

    let accessors = fields.iter().map(|f| {
        let ident = f.ident;
        let inner_ty = f.inner_ty;
        let bare = unraw(ident);
        let has_ident = Ident::new(&format!("has_{bare}"), ident.span());
        let with_ident = Ident::new(&format!("with_{bare}"), ident.span());
        let clear_ident = Ident::new(&format!("clear_{bare}"), ident.span());

        if f.bare_key {
            // Bare-`T` key: always present, so the getter is infallible
            // (`&T`, no `Option`) and there is no `clear_*` — a key can't
            // be unset. The contract (key always set) is enforced at
            // construction and at decode.
            return quote! {
                #[doc = concat!("Borrow the `", stringify!(#ident), "` key property (always set).")]
                #[inline]
                pub fn #ident(&self) -> &#inner_ty {
                    &self.#ident
                }

                #[doc = concat!("Always `true`: `", stringify!(#ident), "` is a key property and is always set.")]
                #[inline]
                pub fn #has_ident(&self) -> bool {
                    true
                }

                #[doc = concat!("Builder: set the `", stringify!(#ident), "` key property.")]
                #[inline]
                pub fn #with_ident<__V>(mut self, value: __V) -> Self
                where
                    __V: ::core::convert::Into<#inner_ty>,
                {
                    self.#ident = value.into();
                    self
                }
            };
        }

        quote! {
            #[doc = concat!("Borrow the `", stringify!(#ident), "` property if set.")]
            #[inline]
            pub fn #ident(&self) -> ::core::option::Option<&#inner_ty> {
                self.#ident.as_ref()
            }

            #[doc = concat!("True if the `", stringify!(#ident), "` property is set.")]
            #[inline]
            pub fn #has_ident(&self) -> bool {
                self.#ident.is_some()
            }

            #[doc = concat!("Builder: set the `", stringify!(#ident), "` property.")]
            #[inline]
            pub fn #with_ident<__V>(mut self, value: __V) -> Self
            where
                __V: ::core::convert::Into<#inner_ty>,
            {
                self.#ident = ::core::option::Option::Some(value.into());
                self
            }

            #[doc = concat!("Builder: clear the `", stringify!(#ident), "` property.")]
            #[inline]
            pub fn #clear_ident(mut self) -> Self {
                self.#ident = ::core::option::Option::None;
                self
            }
        }
    });

    // Key-only `new` constructor — emitted iff the struct has at
    // least one `#[dots(key)]` field. Takes one parameter per key in
    // declaration order, each `impl Into<inner_ty>`; everything else
    // is left `None`. Types with no keys don't get a `new()` since
    // it'd just be `Self::default()` and would risk colliding with a
    // hand-written `new` on the same type.
    let key_fields: Vec<&DotsField<'_>> = fields.iter().filter(|f| f.is_key).collect();
    let new_constructor = if key_fields.is_empty() {
        quote! {}
    } else {
        let params = key_fields.iter().map(|f| {
            let ident = f.ident;
            let inner = f.inner_ty;
            quote! { #ident: impl ::core::convert::Into<#inner> }
        });
        let new_inits = key_fields.iter().map(|f| {
            let ident = f.ident;
            if f.bare_key {
                quote! { #ident: #ident.into() }
            } else {
                quote! { #ident: ::core::option::Option::Some(#ident.into()) }
            }
        });
        // Every non-key property is `Option<T>` and starts `None`. We set
        // these explicitly rather than via `..Default::default()` so the
        // type doesn't have to derive `Default` (which, for a struct with
        // bare-`T` keys, would only ever produce a bogus placeholder key).
        let new_rest_inits = fields.iter().filter(|f| !f.is_key).map(|f| {
            let ident = f.ident;
            quote! { #ident: ::core::option::Option::None }
        });

        // Build a per-key argument doc list. Each `///` line on a key
        // field flows into the `# Arguments` section so IDE hover on
        // `Foo::new(...)` shows what each parameter represents.
        let arg_doc_lines = key_fields.iter().map(|f| {
            let name = f.ident.to_string();
            let summary = if f.doc_lines.is_empty() {
                String::new()
            } else {
                let joined = f
                    .doc_lines
                    .iter()
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ");
                if joined.is_empty() {
                    String::new()
                } else {
                    format!(" — {joined}")
                }
            };
            format!("* `{name}`{summary}")
        });
        let arg_doc_attrs = arg_doc_lines.map(|line| quote! { #[doc = #line] });

        quote! {
            #[doc = "Build an instance with the `#[dots(key)]` properties set and every other property `None`."]
            #[doc = ""]
            #[doc = "Convenient for container lookups (where only the keys matter for `get`) and key-only publishes (`remove`-shaped messages)."]
            #[doc = ""]
            #[doc = "# Arguments"]
            #[doc = ""]
            #( #arg_doc_attrs )*
            #[doc = ""]
            #[doc = "All other properties are left `None`."]
            #[inline]
            pub fn new(#( #params ),*) -> Self {
                Self {
                    #( #new_inits, )*
                    #( #new_rest_inits, )*
                }
            }
        }
    };

    let valid_set_arms = fields.iter().map(|f| {
        let ident = f.ident;
        let tag = f.tag;
        if f.bare_key {
            // Bare-`T` key is always present.
            quote! {
                set = set.with_tag(#tag);
            }
        } else {
            quote! {
                if self.#ident.is_some() {
                    set = set.with_tag(#tag);
                }
            }
        }
    });

    let descriptor_const_ident = Ident::new(
        &format!(
            "_DOTS_DESCRIPTOR_{}",
            struct_ident.to_string().to_uppercase()
        ),
        struct_ident.span(),
    );

    // Marker impl that gates `publish` / `remove`. Substruct-only
    // structs are nested-only by definition, so they don't get one
    // and the compile error fires at the publish call site.
    let publishable_impl = if container.substruct_only {
        quote! {}
    } else {
        quote! {
            impl ::dots_core::Publishable for #struct_ident {
                fn static_descriptor(&self) -> ::core::option::Option<&'static ::dots_core::StructDescriptor> {
                    ::core::option::Option::Some(
                        <Self as ::dots_core::StructValue>::type_descriptor(),
                    )
                }
            }
        }
    };

    // Link-time registration hooks. Each fn body contains a static
    // tagged with the linkme distributed-slice attribute. The transport's
    // generic `publish::<T>` / `subscribe::<T>` entry points call these
    // methods, so monomorphization for a given `T` causes the static
    // to be emitted and linked into the slice. Types the binary never
    // publishes or subscribes to don't appear in the slice (with LTO).
    let global_registration_impl = quote! {
        impl ::dots_core::GlobalRegistration for #struct_ident {
            fn register_as_published() {
                #[::dots_core::linkme::distributed_slice(::dots_core::PUBLISHED_TYPES)]
                #[linkme(crate = ::dots_core::linkme)]
                static REG: &'static ::dots_core::StructDescriptor = #struct_ident::DESCRIPTOR;
                let _ = &REG;
            }
            fn register_as_subscribed() {
                #[::dots_core::linkme::distributed_slice(::dots_core::SUBSCRIBED_TYPES)]
                #[linkme(crate = ::dots_core::linkme)]
                static REG: &'static ::dots_core::StructDescriptor = #struct_ident::DESCRIPTOR;
                let _ = &REG;
            }
        }
    };

    let output = quote! {
        // Hidden module-level block so per-property vtables and the
        // descriptor live at 'static lifetime even when nothing else
        // references the type. Each `#property_decls` introduces a
        // `static __DOTS_VTABLE_<field>: PropertyVtable = ...;`.
        #[doc(hidden)]
        const _: () = {
            #( #property_decls )*

            static #descriptor_const_ident: ::dots_core::StructDescriptor =
                ::dots_core::StructDescriptor {
                    name: #wire_name,
                    flags: #flags_expr,
                    size: ::core::mem::size_of::<#struct_ident>(),
                    align: ::core::mem::align_of::<#struct_ident>(),
                    properties: &[
                        #( #property_descriptors ),*
                    ],
                };

            impl #struct_ident {
                #[doc = "Static descriptor for this DOTS struct."]
                pub const DESCRIPTOR: &'static ::dots_core::StructDescriptor =
                    &#descriptor_const_ident;
            }
        };

        impl #struct_ident {
            #new_constructor
            #( #filter_consts )*
            #( #accessors )*
        }

        impl ::dots_core::StructValue for #struct_ident {
            #[inline]
            fn descriptor(&self) -> &'static ::dots_core::StructDescriptor {
                Self::DESCRIPTOR
            }

            #[inline]
            fn type_descriptor() -> &'static ::dots_core::StructDescriptor {
                Self::DESCRIPTOR
            }

            #[inline]
            fn valid_set(&self) -> ::dots_core::PropertySet {
                #[allow(unused_mut)]
                let mut set = ::dots_core::PropertySet::EMPTY;
                #( #valid_set_arms )*
                set
            }

            #[inline]
            fn as_any(&self) -> &dyn ::core::any::Any {
                self
            }

            #[inline]
            fn data_ptr(&self) -> *const u8 {
                (self as *const Self).cast::<u8>()
            }
        }

        #publishable_impl

        #global_registration_impl

        // `DotsTypeKind` lets the parent struct's macro look up this
        // type's `FieldKind` without needing to know whether it's a
        // struct or an enum.
        impl ::dots_core::DotsTypeKind for #struct_ident {
            const KIND: ::dots_core::FieldKind =
                ::dots_core::FieldKind::Struct(Self::DESCRIPTOR);
        }

        // `DotsField` lets this struct appear as a nested field inside
        // another DOTS struct. Encoding/decoding go through the same
        // descriptor-driven path used at the top level. The seed is built
        // from the descriptor's `init` thunks, so no `Default` impl is
        // required on the nested type.
        impl ::dots_core::DotsField for #struct_ident {
            #[inline]
            fn dots_encode(
                &self,
                e: &mut ::dots_core::layout::CborEncoder<'_>,
            ) -> ::core::result::Result<(), ::dots_core::EncodeError> {
                ::dots_core::layout::encode_struct_value(self, e)
            }

            #[inline]
            fn dots_decode(
                d: &mut ::dots_core::layout::CborDecoder<'_>,
            ) -> ::core::result::Result<Self, ::dots_core::DecodeError> {
                ::dots_core::layout::decode_typed_from_decoder::<Self>(d)
            }
        }

        #companion
    };

    Ok(output)
}

/// Emit a `static` `PropertyVtable` for a single property, with
/// fn-pointer fields pointing at monomorphizations of the generic
/// `opt_*` helpers from `dots_core::layout`.
///
/// `Option<Vec<X>>` fields route through the `opt_*_vec<X>` helpers
/// (CBOR array). All other field types stay on the regular `opt_*<T>`
/// helpers, which dispatch through `T: DotsField`.
fn property_decl(_struct_ident: &Ident, f: &DotsField<'_>) -> TokenStream2 {
    let inner_ty = f.inner_ty;
    let vtable_ident = vtable_ident(f.ident);

    if f.bare_key {
        // Bare-`T` key: stored unwrapped. Slot starts at `T::default()`
        // (via `key_init`) so every thunk can assume a valid `T`.
        return quote! {
            static #vtable_ident: ::dots_core::PropertyVtable = ::dots_core::PropertyVtable {
                layout: ::core::alloc::Layout::new::<#inner_ty>(),
                init: ::dots_core::layout::key_init::<#inner_ty>,
                is_set: ::dots_core::layout::key_is_set,
                encode_value: ::dots_core::layout::key_encode::<#inner_ty>,
                decode_value: ::dots_core::layout::key_decode::<#inner_ty>,
                drop_in_place: ::dots_core::layout::key_drop::<#inner_ty>,
                clone_in_place: ::dots_core::layout::key_clone::<#inner_ty>,
            };
        };
    }

    if let Some(elem_ty) = vec_element_type(inner_ty) {
        // `inner_ty` is `Vec<elem_ty>` (and elem_ty is not `u8`).
        return quote! {
            static #vtable_ident: ::dots_core::PropertyVtable = ::dots_core::PropertyVtable {
                layout: ::core::alloc::Layout::new::<::core::option::Option<#inner_ty>>(),
                init: ::dots_core::layout::opt_init::<#inner_ty>,
                is_set: ::dots_core::layout::opt_is_set::<#inner_ty>,
                encode_value: ::dots_core::layout::opt_encode_vec::<#elem_ty>,
                decode_value: ::dots_core::layout::opt_decode_vec::<#elem_ty>,
                drop_in_place: ::dots_core::layout::opt_drop::<#inner_ty>,
                clone_in_place: ::dots_core::layout::opt_clone_vec::<#elem_ty>,
            };
        };
    }

    quote! {
        static #vtable_ident: ::dots_core::PropertyVtable = ::dots_core::PropertyVtable {
            layout: ::core::alloc::Layout::new::<::core::option::Option<#inner_ty>>(),
            init: ::dots_core::layout::opt_init::<#inner_ty>,
            is_set: ::dots_core::layout::opt_is_set::<#inner_ty>,
            encode_value: ::dots_core::layout::opt_encode::<#inner_ty>,
            decode_value: ::dots_core::layout::opt_decode::<#inner_ty>,
            drop_in_place: ::dots_core::layout::opt_drop::<#inner_ty>,
            clone_in_place: ::dots_core::layout::opt_clone::<#inner_ty>,
        };
    }
}

/// True if `ty`'s last path segment is `AnyObject` — the DOTS open
/// `any` field type. Matches both the bare `AnyObject` and the
/// fully-qualified `dots_core::AnyObject` form.
fn type_is_any(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if let Some(last) = tp.path.segments.last() {
            return last.ident == "AnyObject";
        }
    }
    false
}

/// If `ty` is `Vec<X>` for any `X`, return `&X`. Otherwise `None`.
fn vec_element_type(ty: &Type) -> Option<&Type> {
    let Type::Path(tp) = ty else {
        return None;
    };
    let last = tp.path.segments.last()?;
    if last.ident != "Vec" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

/// True if `ty` is a leaf type the filter DSL supports as the RHS
/// of a predicate comparison — scalars, String, Timepoint /
/// Duration, and the `uuid` array `[u8; 16]`. Vec / nested struct /
/// enum fields are intentionally excluded so emitted constants only
/// exist where they're actually usable.
fn is_dsl_leaf_type(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if let Some(last) = tp.path.segments.last() {
            let name = last.ident.to_string();
            return matches!(
                name.as_str(),
                "bool"
                    | "u8" | "u16" | "u32" | "u64"
                    | "i8" | "i16" | "i32" | "i64"
                    | "f32" | "f64"
                    | "String"
                    | "Timepoint"
                    | "Duration"
            );
        }
    }
    if let Type::Array(arr) = ty {
        // Match `[u8; 16]` exactly — the only fixed-byte type in
        // DOTS (`uuid`).
        if let Type::Path(tp) = &*arr.elem {
            if tp.path.segments.last().is_some_and(|s| s.ident == "u8") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(n),
                    ..
                }) = &arr.len
                {
                    if n.base10_parse::<u32>().ok() == Some(16) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn vtable_ident(field_ident: &Ident) -> Ident {
    Ident::new(
        &format!("__DOTS_VTABLE_{}", unraw(field_ident).to_uppercase()),
        field_ident.span(),
    )
}

/// Name of the per-type companion `macro_rules!` the `dots!` macro
/// delegates to. Derived deterministically from the type's identifier so
/// the `dots!` proc-macro can construct the same name from the call-site
/// path's last segment. Both `expand` (which defines it) and `dots`
/// (which calls it) must agree on this formula.
fn ctor_macro_ident(type_ident: &Ident) -> Ident {
    Ident::new(&format!("__dots_ctor_{type_ident}"), type_ident.span())
}

/// Generate the per-type companion macro for `dots!`.
///
/// This is the "Escape B" mechanism: a schema-aware `macro_rules!` the
/// derive emits because the `dots!` proc-macro itself is schema-blind
/// (it sees only the call-site fields, not which are keys). For each
/// provided `field: value`, the macro routes the value through the
/// right coercion — bare `T` for `#[dots(key)]` fields, `Option<T>` for
/// the rest — and emits a *complete* struct literal so that an omitted
/// non-key field defaults to `None` while an omitted **bare key** lands
/// on a `compile_error!`. That's the compile-time key enforcement.
fn companion_macro(struct_ident: &Ident, fields: &[DotsField<'_>]) -> TokenStream2 {
    let macro_name = ctor_macro_ident(struct_ident);

    let entry_inits = fields.iter().map(|f| {
        let fident = f.ident;
        quote! {
            #fident: #macro_name!(@pick #fident ; $($fname : $fval),*)
        }
    });

    let pick_arms = fields.iter().map(|f| {
        let fident = f.ident;
        let found_body = if f.bare_key {
            // Bare key: convert into the field's `T` (target inferred).
            quote! { ::dots_core::__dots_into_bare($v) }
        } else {
            // Optional field: same coercion `dots!` always used —
            // bare value → `Some`, `Option` passes through, inner `Into`.
            quote! {{
                #[allow(unused_imports)]
                use ::dots_core::DotsAssignGeneric as _;
                ::dots_core::DotsAssign($v).into_dots_field()
            }}
        };
        let missing_body = if f.bare_key {
            let msg = format!(
                "dots!: missing required `#[dots(key)]` field `{}` for `{}`",
                unraw(fident),
                struct_ident
            );
            quote! { ::core::compile_error!(#msg) }
        } else {
            quote! { ::core::option::Option::None }
        };
        quote! {
            (@pick #fident ; #fident : $v:expr $(, $rn:ident : $rv:expr)*) => { #found_body };
            (@pick #fident ; $on:ident : $ov:expr $(, $rn:ident : $rv:expr)*) => {
                #macro_name!(@pick #fident ; $($rn : $rv),*)
            };
            (@pick #fident ;) => { #missing_body };
        }
    });

    quote! {
        // Defined with a macro-2.0 `pub use` re-export rather than
        // `#[macro_export]`. `#[macro_export]` places the macro at the
        // crate root but a *derive-generated* one is then unreachable by
        // path from a sibling module in its own crate (you may only
        // refer to it by bare name, which doesn't cross module
        // boundaries) — fatal for generated `mod model { .. }` types.
        // A `pub use` re-export is path-addressable both same-crate
        // (`crate::path::__dots_ctor_T`) and cross-crate, and is brought
        // into bare scope by a glob import of the type's module.
        // `#[allow(non_local_definitions)]`: a `#[macro_export]` macro
        // emitted by a derive on a *function-local* struct is a non-local
        // definition; harmless here (the macro is name-mangled per type).
        #[doc(hidden)]
        #[macro_export]
        #[allow(non_local_definitions)]
        macro_rules! #macro_name {
            // Entry: `dots!` expands to `__dots_ctor_T! { @ty <path> ; f: v, ... }`.
            // Emit a complete struct literal; each field picks its value
            // from the provided list (or defaults / errors).
            (@ty $ty:path ; $($fname:ident : $fval:expr),* $(,)?) => {{
                #[allow(clippy::needless_update)]
                $ty {
                    #( #entry_inits ),*
                }
            }};
            #( #pick_arms )*
        }
        // Two bindings, because stable Rust offers no single one that
        // covers every consumer of a *derive-generated* macro:
        //   * `#[macro_export]` (above) → crate-root home, reachable
        //     cross-crate and from binaries via `use thatcrate::*`;
        //   * `pub(crate) use` (below) → binds the macro at the type's
        //     own module, the only way a *same-crate sibling module*
        //     (e.g. a reactor using a generated `mod model`) can reach
        //     it — bare names don't cross modules and absolute paths to
        //     `#[macro_export]` macros are forbidden.
        // Consumers glob-import the type's module (`use crate::model::*`)
        // or crate (`use dots_model::*`). The two bindings collide only
        // for a type declared at its crate's *root* module, so DOTS
        // structs must live in a module (generated code always does).
        #[doc(hidden)]
        #[allow(unused_imports)]
        pub(crate) use #macro_name;
    }
}

/// `Ident::to_string` keeps the `r#` raw-ident prefix. Strip it so
/// raw-keyword fields like `r#type` flow cleanly into wire names and
/// derived identifiers (e.g. `PROP_TYPE` rather than the invalid
/// `PROP_R#TYPE`).
fn unraw(ident: &Ident) -> String {
    let s = ident.to_string();
    s.strip_prefix("r#").map(str::to_owned).unwrap_or(s)
}

fn collect_fields(data: &DataStruct) -> syn::Result<Vec<DotsField<'_>>> {
    let named = match &data.fields {
        Fields::Named(n) => &n.named,
        Fields::Unnamed(u) => {
            return Err(syn::Error::new(
                u.paren_token.span.open(),
                "DotsStruct requires named fields",
            ));
        }
        Fields::Unit => {
            return Err(syn::Error::new(
                data.struct_token.span,
                "DotsStruct does not support unit structs",
            ));
        }
    };

    let mut out = Vec::with_capacity(named.len());
    for field in named {
        out.push(parse_field(field)?);
    }
    Ok(out)
}

fn parse_field(field: &Field) -> syn::Result<DotsField<'_>> {
    let ident = field
        .ident
        .as_ref()
        .ok_or_else(|| syn::Error::new(field.span(), "field must be named"))?;

    let attrs = parse_field_attrs(&field.attrs)?;

    // Field-type rule:
    //   * ordinary properties must be `Option<T>` (partial-object
    //     semantics are explicit);
    //   * a `#[dots(key)]` property may instead be a bare `T` — a key is
    //     always present by contract, so the `Option` wrapper is dropped.
    //     It's still allowed to be `Option<T>` for back-compat.
    let (inner_ty, bare_key) = match option_inner_type(&field.ty) {
        Some(inner) => (inner, false),
        None if attrs.is_key => (&field.ty, true),
        None => {
            return Err(syn::Error::new(
                field.ty.span(),
                "DotsStruct fields must be `Option<T>` so partial-object semantics are explicit \
                 (only `#[dots(key)]` fields may be a bare `T`)",
            ));
        }
    };

    let tag = attrs.tag.ok_or_else(|| {
        syn::Error::new(field.span(), "field is missing `#[dots(tag = N)]` attribute")
    })?;

    if tag == 0 {
        return Err(syn::Error::new(
            field.span(),
            "DOTS tags are 1-based; tag must be > 0",
        ));
    }
    if tag > 31 {
        return Err(syn::Error::new(
            field.span(),
            "this iteration supports tags 1..=31 (PropertySet is u32 with bit n = tag n)",
        ));
    }

    // `any` (and, later, `variant`) properties as keys are disallowed:
    // comparing opaque heterogeneous blobs as keys is a footgun. Caught
    // here syntactically by the `AnyObject` type name.
    if attrs.is_key && type_is_any(inner_ty) {
        return Err(syn::Error::new(
            field.span(),
            "`any` (AnyObject) properties cannot be `#[dots(key)]`",
        ));
    }

    let kind = field_kind_for(inner_ty);
    let doc_lines = extract_doc_lines(&field.attrs);

    Ok(DotsField {
        ident,
        inner_ty,
        tag,
        is_key: attrs.is_key,
        bare_key,
        kind,
        doc_lines,
    })
}

/// Extract `#[doc = "..."]` attribute text from a field. Rust's
/// `///` doc comments are surface syntax for `#[doc = "..."]`, so
/// reading these recovers the comment text the user wrote (with the
/// leading space the compiler inserts).
fn extract_doc_lines(attrs: &[syn::Attribute]) -> Vec<String> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }
        if let syn::Meta::NameValue(nv) = &attr.meta {
            if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) = &nv.value
            {
                // Compiler inserts a single leading space — strip it
                // so the rendered doc looks like the user's source.
                let raw = s.value();
                let trimmed = raw.strip_prefix(' ').unwrap_or(&raw);
                out.push(trimmed.to_string());
            }
        }
    }
    out
}

fn parse_container_attrs(attrs: &[syn::Attribute]) -> syn::Result<ContainerAttrs> {
    let mut out = ContainerAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("dots") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let lit: LitStr = meta.value()?.parse()?;
                out.name = Some(lit.value());
            } else if meta.path.is_ident("cached") {
                out.cached = true;
            } else if meta.path.is_ident("internal") {
                out.internal = true;
            } else if meta.path.is_ident("persistent") {
                out.persistent = true;
            } else if meta.path.is_ident("cleanup") {
                out.cleanup = true;
            } else if meta.path.is_ident("local") {
                out.local = true;
            } else if meta.path.is_ident("substruct_only") {
                out.substruct_only = true;
            } else {
                return Err(meta.error("unknown #[dots(...)] container attribute"));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

fn parse_field_attrs(attrs: &[syn::Attribute]) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("dots") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("tag") {
                let lit: LitInt = meta.value()?.parse()?;
                out.tag = Some(lit.base10_parse::<u32>()?);
            } else if meta.path.is_ident("key") {
                out.is_key = true;
            } else {
                return Err(meta.error("unknown #[dots(...)] field attribute"));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// Extract `T` from `Option<T>` syntactically. Accepts both the bare
/// `Option<...>` and fully-qualified `::core::option::Option<...>` /
/// `std::option::Option<...>` forms.
fn option_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(tp) = ty else {
        return None;
    };
    let last = tp.path.segments.last()?;
    if last.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    let mut iter = args.args.iter().filter_map(|a| match a {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    });
    let inner = iter.next()?;
    if iter.next().is_some() {
        return None;
    }
    Some(inner)
}

/// Map the syntactic field type to a `FieldKind` expression.
///
/// Recognized primitives produce the matching `FieldKind` variant.
/// `Vec<X>` for any `X` (including `u8`) becomes
/// `FieldKind::Vec(&<inner kind>)` — the wire format is a CBOR array,
/// matching dots-cpp's `CborSerializer::visitVectorBeginDerived`.
/// Anything else is treated as a nested DOTS struct: we emit
/// `<T as DotsTypeKind>::KIND`, which fails to compile if the type is
/// not in fact `#[derive(DotsStruct)]` / `#[derive(DotsEnum)]`.
fn field_kind_for(ty: &Type) -> TokenStream2 {
    if let Type::Path(tp) = ty {
        if let Some(last) = tp.path.segments.last() {
            let name = last.ident.to_string();
            let primitive = match name.as_str() {
                "bool" => Some("Bool"),
                "u8" => Some("U8"),
                "u16" => Some("U16"),
                "u32" => Some("U32"),
                "u64" => Some("U64"),
                "i8" => Some("I8"),
                "i16" => Some("I16"),
                "i32" => Some("I32"),
                "i64" => Some("I64"),
                "f32" => Some("F32"),
                "f64" => Some("F64"),
                "String" => Some("String"),
                _ => None,
            };
            if let Some(p) = primitive {
                let kind_ident = Ident::new(p, last.ident.span());
                return quote! { ::dots_core::FieldKind::#kind_ident };
            }
            if name == "Vec" {
                // `Vec<X>` — recurse on inner type and wrap in
                // `FieldKind::Vec(&inner)`. Rvalue static promotion
                // lifts the inner literal to `'static`.
                if let Some(inner) = vec_element_type(ty) {
                    let inner_kind = field_kind_for(inner);
                    return quote! { ::dots_core::FieldKind::Vec(&#inner_kind) };
                }
            }
        }
    }
    // Treat as a user-defined DOTS type: defer to the type's
    // `DotsTypeKind::KIND`. That trait is implemented by both
    // `#[derive(DotsStruct)]` and `#[derive(DotsEnum)]`, so this single
    // fallback covers both nested structs and enums. Compile error
    // points at the type if it isn't a derived DOTS type.
    quote! { <#ty as ::dots_core::DotsTypeKind>::KIND }
}

fn build_flags_expr(c: &ContainerAttrs) -> TokenStream2 {
    let cached = c.cached;
    let internal = c.internal;
    let persistent = c.persistent;
    let cleanup = c.cleanup;
    let local = c.local;
    let substruct_only = c.substruct_only;
    quote! {
        ::dots_core::StructFlags::NONE
            .cached(#cached)
            .internal(#internal)
            .persistent(#persistent)
            .cleanup(#cleanup)
            .local(#local)
            .substruct_only(#substruct_only)
    }
}

// ===== DotsEnum =====

#[derive(Default)]
struct EnumContainerAttrs {
    name: Option<String>,
}

#[derive(Default)]
struct EnumVariantAttrs {
    tag: Option<u32>,
    value: Option<i32>,
}

struct DotsEnumVariant<'a> {
    ident: &'a Ident,
    tag: u32,
    value: i32,
}

fn expand_enum(input: DeriveInput) -> syn::Result<TokenStream2> {
    let enum_ident = &input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "DotsEnum does not support generic enums",
        ));
    }

    let container = parse_enum_container_attrs(&input.attrs)?;
    let wire_name = container.name.unwrap_or_else(|| enum_ident.to_string());

    let data_enum = match input.data {
        Data::Enum(de) => de,
        Data::Struct(s) => {
            return Err(syn::Error::new(
                s.struct_token.span,
                "DotsEnum can only be derived for enums",
            ));
        }
        Data::Union(u) => {
            return Err(syn::Error::new(
                u.union_token.span(),
                "DotsEnum can only be derived for enums",
            ));
        }
    };

    let variants = collect_enum_variants(&data_enum)?;

    // Detect duplicate tags / values.
    for (i, a) in variants.iter().enumerate() {
        for b in &variants[i + 1..] {
            if a.tag == b.tag {
                return Err(syn::Error::new(
                    b.ident.span(),
                    format!("duplicate tag {} (also used by `{}`)", b.tag, a.ident),
                ));
            }
            if a.value == b.value {
                return Err(syn::Error::new(
                    b.ident.span(),
                    format!(
                        "duplicate enum value {} (also used by `{}`)",
                        b.value, a.ident
                    ),
                ));
            }
        }
    }

    let element_inits = variants.iter().map(|v| {
        let name = v.ident.to_string();
        let tag = v.tag;
        let value = v.value;
        quote! {
            ::dots_core::EnumElement {
                name: #name,
                tag: #tag,
                value: #value,
            }
        }
    });

    let encode_arms = variants.iter().map(|v| {
        let ident = v.ident;
        let value = v.value;
        quote! { Self::#ident => #value }
    });

    let decode_arms = variants.iter().map(|v| {
        let ident = v.ident;
        let value = v.value;
        quote! { #value => ::core::result::Result::Ok(Self::#ident) }
    });

    let descriptor_const_ident = Ident::new(
        &format!(
            "_DOTS_ENUM_DESCRIPTOR_{}",
            enum_ident.to_string().to_uppercase()
        ),
        enum_ident.span(),
    );

    let output = quote! {
        #[doc(hidden)]
        const _: () = {
            static #descriptor_const_ident: ::dots_core::EnumDescriptor =
                ::dots_core::EnumDescriptor {
                    name: #wire_name,
                    elements: &[
                        #( #element_inits ),*
                    ],
                };

            impl #enum_ident {
                #[doc = "Static descriptor for this DOTS enum."]
                pub const DESCRIPTOR: &'static ::dots_core::EnumDescriptor =
                    &#descriptor_const_ident;
            }
        };

        impl ::dots_core::DotsTypeKind for #enum_ident {
            const KIND: ::dots_core::FieldKind =
                ::dots_core::FieldKind::Enum(Self::DESCRIPTOR);
        }

        impl ::dots_core::DotsField for #enum_ident {
            #[inline]
            fn dots_encode(
                &self,
                e: &mut ::dots_core::layout::CborEncoder<'_>,
            ) -> ::core::result::Result<(), ::dots_core::EncodeError> {
                let v: i32 = match self {
                    #( #encode_arms ),*
                };
                e.i32(v)?;
                ::core::result::Result::Ok(())
            }

            #[inline]
            fn dots_decode(
                d: &mut ::dots_core::layout::CborDecoder<'_>,
            ) -> ::core::result::Result<Self, ::dots_core::DecodeError> {
                let v: i32 = d.i32()?;
                match v {
                    #( #decode_arms ),*,
                    _ => ::core::result::Result::Err(
                        ::dots_core::DecodeError::message(
                            "unknown DOTS enum value"
                        ),
                    ),
                }
            }
        }
    };

    Ok(output)
}

fn collect_enum_variants(de: &DataEnum) -> syn::Result<Vec<DotsEnumVariant<'_>>> {
    let mut out = Vec::with_capacity(de.variants.len());
    for v in &de.variants {
        out.push(parse_enum_variant(v)?);
    }
    Ok(out)
}

fn parse_enum_variant(v: &Variant) -> syn::Result<DotsEnumVariant<'_>> {
    if !matches!(v.fields, Fields::Unit) {
        return Err(syn::Error::new(
            v.span(),
            "DotsEnum variants must be unit-style (no payload)",
        ));
    }
    let attrs = parse_enum_variant_attrs(&v.attrs)?;
    let tag = attrs.tag.ok_or_else(|| {
        syn::Error::new(v.span(), "variant is missing `#[dots(tag = N)]` attribute")
    })?;
    if tag == 0 {
        return Err(syn::Error::new(
            v.span(),
            "DOTS enum tags are 1-based; tag must be > 0",
        ));
    }
    // Default the wire `int32` value to the tag — matches the .dots
    // convention for `1: variant_name` declarations.
    let value = attrs.value.unwrap_or(tag as i32);
    Ok(DotsEnumVariant {
        ident: &v.ident,
        tag,
        value,
    })
}

fn parse_enum_container_attrs(attrs: &[syn::Attribute]) -> syn::Result<EnumContainerAttrs> {
    let mut out = EnumContainerAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("dots") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let lit: LitStr = meta.value()?.parse()?;
                out.name = Some(lit.value());
            } else {
                return Err(meta.error("unknown #[dots(...)] container attribute on enum"));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

fn parse_enum_variant_attrs(attrs: &[syn::Attribute]) -> syn::Result<EnumVariantAttrs> {
    let mut out = EnumVariantAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("dots") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("tag") {
                let lit: LitInt = meta.value()?.parse()?;
                out.tag = Some(lit.base10_parse::<u32>()?);
            } else if meta.path.is_ident("value") {
                let lit: LitInt = meta.value()?.parse()?;
                out.value = Some(lit.base10_parse::<i32>()?);
            } else {
                return Err(meta.error("unknown #[dots(...)] attribute on enum variant"));
            }
            Ok(())
        })?;
    }
    Ok(out)
}
