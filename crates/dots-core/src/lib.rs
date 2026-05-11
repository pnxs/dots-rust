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
mod registration;
mod temporal;
mod value;

/// Re-export of the `linkme` crate so the derive macro can reference
/// it as `::dots_core::linkme` without users adding it as a direct
/// dep.
pub use linkme;

pub use descriptor::{
    DotsTypeKind, EnumDescriptor, EnumElement, FieldKind, PropertyDescriptor, PropertyVtable,
    StructDescriptor, StructFlags,
};
pub use dynamic::{
    DynamicEnumDescriptor, DynamicEnumElement, DynamicFieldKind, DynamicPropertyDescriptor,
    DynamicPublishable, DynamicStruct, DynamicStructDescriptor, DynamicValue, NotPublishable,
};
pub use layout::{
    AnyStruct, DecodeError, DotsField, EncodeError, decode_typed_from_decoder,
    decode_typed_from_slice, encode_into_encoder, encode_into_encoder_with_mask, encode_into_vec,
    encode_into_vec_with_mask, encode_key_bytes, encode_key_into, encode_to_vec, key_set,
};
pub use property_set::PropertySet;
pub use registration::{GlobalRegistration, PUBLISHED_TYPES, SUBSCRIBED_TYPES};
pub use temporal::{Duration, Timepoint};
pub use value::{Publishable, StructValue, Transmittable};

/// Re-export of the `minicbor` crate so derived code and downstream users
/// reference a single, version-aligned copy. Prefer importing
/// `dots_core::minicbor` over adding minicbor directly to your `Cargo.toml`.
pub use minicbor;

/// Helper used by the [`dots!`] macro to coerce each field value
/// into `Option<T>`. Wraps the user's expression so method dispatch
/// can pick:
///
/// - **Inherent method** on `DotsAssign<Option<U>>` — passes the
///   `Option` through, applying `Into::into` to the inner value if
///   present.
/// - **Trait method** ([`DotsAssignGeneric`]) on `DotsAssign<U>` for
///   any other `U: Into<T>` — wraps a bare value in `Some(_)`.
///
/// The "auto-ref specialization" trick (inherent methods take
/// priority over trait methods during dispatch) is what lets a
/// single call site handle both shapes without overlapping impls.
#[doc(hidden)]
pub struct DotsAssign<V>(pub V);

impl<U> DotsAssign<Option<U>> {
    /// Inherent path for `Option<U>` values — wins method-dispatch
    /// priority over the trait impl below.
    #[doc(hidden)]
    #[inline]
    pub fn into_dots_field<T>(self) -> Option<T>
    where
        U: Into<T>,
    {
        self.0.map(Into::into)
    }
}

/// Generic fallback path for non-`Option` values. Wraps `value` in
/// `Some(_)` after an `Into::into` conversion. Hidden because it
/// exists only to support the [`dots!`] macro.
#[doc(hidden)]
pub trait DotsAssignGeneric<T> {
    fn into_dots_field(self) -> Option<T>;
}

impl<T, U: Into<T>> DotsAssignGeneric<T> for DotsAssign<U> {
    #[inline]
    fn into_dots_field(self) -> Option<T> {
        Some(self.0.into())
    }
}

/// Construct a DOTS struct literal with terse syntax.
///
/// Each field's value is coerced into the field's `Option<T>` type:
///
/// - A bare value is wrapped in `Some(_)` and `Into`-converted, so
///   `dots!(Foo { name: "hi" })` produces `name: Some("hi".to_string())`.
/// - An `Option<U>` is passed through verbatim (with `Into` on the
///   inner type if needed), so `dots!(Foo { brightness: other.brightness })`
///   forwards `None` as `None` and `Some(x)` as `Some(x.into())`.
///
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
                    $field: {
                        // Pull `DotsAssignGeneric` into scope so the
                        // trait method is callable; the inherent
                        // method on `DotsAssign<Option<_>>` wins for
                        // `Option` values, the trait method handles
                        // everything else.
                        #[allow(unused_imports)]
                        use $crate::DotsAssignGeneric as _;
                        $crate::DotsAssign($value).into_dots_field()
                    },
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

    #[test]
    fn dots_macro_passes_option_through() {
        // `Option<u64>` flows verbatim — `Some(_)` stays `Some(_)`,
        // `None` stays `None`. No second `Some(_)` wrap.
        let upstream: Option<u64> = Some(7);
        let foo = dots!(Foo { big_id: upstream });
        assert_eq!(foo.big_id, Some(7));

        let cleared: Option<u64> = None;
        let foo = dots!(Foo { big_id: cleared });
        assert_eq!(foo.big_id, None);
    }

    #[test]
    fn dots_macro_inner_into_on_option() {
        // Inner-type `Into` runs even when wrapped in `Option` —
        // `Option<&str>` lands in an `Option<String>` field.
        let upstream: Option<&str> = Some("hi");
        let foo = dots!(Foo { name: upstream });
        assert_eq!(foo.name.as_deref(), Some("hi"));
    }
}
