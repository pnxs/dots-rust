//! Procedural macros for dots-rust.
//!
//! `#[derive(DotsStruct)]` generates the runtime metadata, accessors,
//! builder methods, and `StructValue` impl for a DOTS struct.
//!
//! Each field must be declared as `Option<T>` and tagged with
//! `#[dots(tag = N)]` (and optionally `#[dots(tag = N, key)]`).
//! The struct itself may carry `#[dots(name = "WireName", cached, ...)]`
//! to override the wire name and set struct-level flags.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Data, DataStruct, DeriveInput, Field, Fields, GenericArgument, Ident, LitInt, LitStr,
    PathArguments, Type, parse_macro_input, spanned::Spanned,
};

#[proc_macro_derive(DotsStruct, attributes(dots))]
pub fn derive_dots_struct(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
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
    inner_ty: &'a Type,
    tag: u32,
    is_key: bool,
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let struct_ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

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

    let property_descriptors = fields.iter().map(|f| {
        let name = f.ident.to_string();
        let tag = f.tag;
        let is_key = f.is_key;
        let type_name = type_to_display_string(f.inner_ty);
        quote! {
            ::dots_core::PropertyDescriptor {
                name: #name,
                tag: #tag,
                is_key: #is_key,
                type_name: #type_name,
            }
        }
    });

    let flags_expr = build_flags_expr(&container);

    let accessors = fields.iter().map(|f| {
        let ident = f.ident;
        let inner_ty = f.inner_ty;
        let has_ident = Ident::new(&format!("has_{}", ident), ident.span());
        let with_ident = Ident::new(&format!("with_{}", ident), ident.span());
        let clear_ident = Ident::new(&format!("clear_{}", ident), ident.span());
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

    let valid_set_arms = fields.iter().map(|f| {
        let ident = f.ident;
        let tag = f.tag;
        quote! {
            if self.#ident.is_some() {
                set = set.with_tag(#tag);
            }
        }
    });

    let encode_arms = fields.iter().map(|f| {
        let ident = f.ident;
        let tag = f.tag;
        quote! {
            if let ::core::option::Option::Some(__v) = &self.#ident {
                __e.u32(#tag)?;
                <_ as ::dots_core::minicbor::Encode<()>>::encode(__v, __e, __ctx)?;
            }
        }
    });

    let decode_arms = fields.iter().map(|f| {
        let ident = f.ident;
        let tag = f.tag;
        let inner_ty = f.inner_ty;
        quote! {
            #tag => {
                __out.#ident = ::core::option::Option::Some(
                    <#inner_ty as ::dots_core::minicbor::Decode<'_b, ()>>::decode(__d, __ctx)?,
                );
            }
        }
    });

    let descriptor_const_ident = Ident::new(
        &format!("_DOTS_DESCRIPTOR_{}", struct_ident.to_string().to_uppercase()),
        struct_ident.span(),
    );

    let output = quote! {
        // Hidden module-level constant so the descriptor lives at 'static lifetime
        // even when nothing else references it.
        #[doc(hidden)]
        const _: () = {
            static #descriptor_const_ident: ::dots_core::StructDescriptor =
                ::dots_core::StructDescriptor {
                    name: #wire_name,
                    flags: #flags_expr,
                    properties: &[
                        #( #property_descriptors ),*
                    ],
                };

            impl #impl_generics #struct_ident #ty_generics #where_clause {
                #[doc = "Static descriptor for this DOTS struct."]
                pub const DESCRIPTOR: &'static ::dots_core::StructDescriptor =
                    &#descriptor_const_ident;
            }
        };

        impl #impl_generics #struct_ident #ty_generics #where_clause {
            #( #accessors )*
        }

        impl #impl_generics ::dots_core::StructValue for #struct_ident #ty_generics
            #where_clause
        {
            #[inline]
            fn descriptor(&self) -> &'static ::dots_core::StructDescriptor {
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
        }

        impl #impl_generics ::dots_core::minicbor::Encode<()> for #struct_ident #ty_generics
            #where_clause
        {
            fn encode<__W>(
                &self,
                __e: &mut ::dots_core::minicbor::Encoder<__W>,
                __ctx: &mut (),
            ) -> ::core::result::Result<
                (),
                ::dots_core::minicbor::encode::Error<__W::Error>,
            >
            where
                __W: ::dots_core::minicbor::encode::Write,
            {
                let __valid = <Self as ::dots_core::StructValue>::valid_set(self);
                __e.map(__valid.len() as u64)?;
                #( #encode_arms )*
                ::core::result::Result::Ok(())
            }
        }

        impl<'_b> #impl_generics ::dots_core::minicbor::Decode<'_b, ()> for #struct_ident #ty_generics
            #where_clause
        {
            fn decode(
                __d: &mut ::dots_core::minicbor::Decoder<'_b>,
                __ctx: &mut (),
            ) -> ::core::result::Result<Self, ::dots_core::minicbor::decode::Error> {
                #[allow(unused_mut)]
                let mut __out = <Self as ::core::default::Default>::default();
                let __len = __d.map()?.ok_or_else(|| {
                    ::dots_core::minicbor::decode::Error::message(
                        "indefinite-length maps are not supported in DOTS structs",
                    )
                })?;
                for _ in 0..__len {
                    let __tag = __d.u32()?;
                    match __tag {
                        #( #decode_arms )*
                        _ => __d.skip()?,
                    }
                }
                ::core::result::Result::Ok(__out)
            }
        }
    };

    Ok(output)
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

    let inner_ty = option_inner_type(&field.ty).ok_or_else(|| {
        syn::Error::new(
            field.ty.span(),
            "DotsStruct fields must be `Option<T>` so partial-object semantics are explicit",
        )
    })?;

    let attrs = parse_field_attrs(&field.attrs)?;
    let tag = attrs.tag.ok_or_else(|| {
        syn::Error::new(
            field.span(),
            "field is missing `#[dots(tag = N)]` attribute",
        )
    })?;

    if tag == 0 {
        return Err(syn::Error::new(
            field.span(),
            "DOTS tags are 1-based; tag must be > 0",
        ));
    }
    if tag > 64 {
        return Err(syn::Error::new(
            field.span(),
            "this iteration supports tags 1..=64 (PropertySet is u64)",
        ));
    }

    Ok(DotsField {
        ident,
        inner_ty,
        tag,
        is_key: attrs.is_key,
    })
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

/// Extract `T` from `Option<T>`, syntactically. Accepts both the bare
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

fn type_to_display_string(ty: &Type) -> String {
    quote!(#ty).to_string().split_whitespace().collect()
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
