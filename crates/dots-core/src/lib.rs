//! Core runtime types for dots-rust.
//!
//! Re-exports the types used by `dots-derive`-generated code and consumers
//! of derived DOTS structs.
//!
//! The codec is *descriptor-driven*: encoding and decoding walk the
//! `StructDescriptor`'s property list and dispatch through per-property
//! [`PropertyVtable`] thunks. The same code path serves typed structs
//! (produced by `#[derive(DotsStruct)]`) and dynamic [`AnyStruct`]
//! instances allocated from a descriptor alone — wire bytes are
//! identical because the descriptor *is* the format.
//!
//! [`PropertyVtable`]: descriptor::PropertyVtable
//! [`AnyStruct`]: layout::AnyStruct

#![cfg_attr(not(test), no_std)]

extern crate alloc;

mod descriptor;
pub mod dynamic;
pub mod layout;
mod property_set;
mod temporal;
mod value;

pub use descriptor::{
    DotsTypeKind, EnumDescriptor, EnumElement, FieldKind, PropertyDescriptor, PropertyVtable,
    StructDescriptor, StructFlags,
};
pub use dynamic::{
    DynamicEnumDescriptor, DynamicEnumElement, DynamicFieldKind, DynamicPropertyDescriptor,
    DynamicStruct, DynamicStructDescriptor, DynamicValue,
};
pub use layout::{
    AnyStruct, DecodeError, DotsField, EncodeError, decode_typed_from_decoder,
    decode_typed_from_slice, encode_into_encoder, encode_into_encoder_with_mask, encode_into_vec,
    encode_into_vec_with_mask, encode_key_bytes, encode_key_into, encode_to_vec, key_set,
};
pub use property_set::PropertySet;
pub use temporal::{Duration, Timepoint};
pub use value::StructValue;

/// Re-export of the `minicbor` crate so derived code and downstream users
/// reference a single, version-aligned copy. Prefer importing
/// `dots_core::minicbor` over adding minicbor directly to your `Cargo.toml`.
pub use minicbor;

/// Construct a DOTS struct literal with terse syntax.
///
/// Every named field is wrapped in `Some(_)` and converted via `Into`,
/// so `&str` literals become `String` and most coercions Just Work.
/// Unspecified fields fall back to `Default::default()` (which for
/// DOTS structs is all-`None`).
///
/// **Integer literal note:** integer literals default to `i32` before
/// `Into` resolution. For non-`i32` integer fields, use a type suffix:
/// `dots!(Foo { id: 42_u32 })`. The generated `with_<field>(...)` builder
/// methods accept `impl Into<T>` so you can pass an unsuffixed literal
/// when the target type is more constrained.
///
/// See the `dots-example` crate for a runnable demonstration.
#[macro_export]
macro_rules! dots {
    ($($ty:ident)::+ { $($field:ident : $value:expr),* $(,)? }) => {
        {
            // The trailing `..Default::default()` is always emitted so
            // the macro works whether the caller listed every field or
            // not. When all fields are listed clippy flags that as
            // `needless_update`; suppress here because the macro can't
            // know in advance whether the caller covered the struct.
            #[allow(clippy::needless_update)]
            let __dots_value = $($ty)::+ {
                $(
                    $field: ::core::option::Option::Some(
                        ::core::convert::Into::into($value)
                    ),
                )*
                ..::core::default::Default::default()
            };
            __dots_value
        }
    };
}

#[cfg(test)]
mod macro_tests {
    use alloc::string::String;

    #[derive(Default, Debug, PartialEq)]
    struct Foo {
        id: Option<i32>,
        big_id: Option<u64>,
        name: Option<String>,
        flag: Option<bool>,
    }

    #[test]
    fn dots_macro_constructs_partial_struct() {
        let foo = dots!(Foo { id: 42, name: "hi" });
        assert_eq!(foo.id, Some(42));
        assert_eq!(foo.name.as_deref(), Some("hi"));
        assert_eq!(foo.flag, None);
        assert_eq!(foo.big_id, None);
    }

    #[test]
    fn dots_macro_handles_typed_integer_literals() {
        let foo = dots!(Foo { big_id: 9000_u64, flag: true });
        assert_eq!(foo.big_id, Some(9000));
        assert_eq!(foo.flag, Some(true));
    }

    #[test]
    fn dots_macro_supports_trailing_comma() {
        let foo = dots!(Foo { id: 1, });
        assert_eq!(foo.id, Some(1));
    }

    #[test]
    fn dots_macro_with_no_fields_yields_default() {
        let foo = dots!(Foo {});
        assert_eq!(foo, Foo::default());
    }
}
