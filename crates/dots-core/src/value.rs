use core::any::Any;

use crate::{
    PropertySet, StructDescriptor,
    layout::{CborEncoder, EncodeError},
};

/// Runtime view of a DOTS struct value backed by a compiled `&'static
/// StructDescriptor` and a layout-compatible memory buffer.
///
/// Implemented for every type that derives `DotsStruct`, plus the
/// dynamic [`AnyStruct`] type. The descriptor-driven codec walks any
/// `&dyn StructValue` regardless of whether the underlying storage is
/// a typed `Foo` or an `AnyStruct` — both expose a layout-compatible
/// data pointer.
///
/// Runtime-described values without a static descriptor
/// (`DynamicStruct`) do **not** implement this trait. The wire-level
/// surface common to both is [`Transmittable`].
///
/// [`AnyStruct`]: crate::layout::AnyStruct
pub trait StructValue: Any {
    /// Static metadata for this struct's type.
    fn descriptor(&self) -> &'static StructDescriptor;

    /// Static metadata for this struct's type, accessible from generic
    /// code that doesn't have an instance. Equivalent to
    /// `Self::DESCRIPTOR` for `#[derive(DotsStruct)]`-derived types.
    fn type_descriptor() -> &'static StructDescriptor
    where
        Self: Sized;

    /// Bitmask of properties currently set on this instance.
    fn valid_set(&self) -> PropertySet;

    /// Erase to `&dyn Any` for downcasting in typed handlers.
    fn as_any(&self) -> &dyn Any;

    /// Pointer to the start of the value's memory, laid out per
    /// `descriptor().layout()`. The pointer is valid for at least the
    /// lifetime of `&self`. The field offsets recorded in
    /// `descriptor().properties` are relative to this base.
    fn data_ptr(&self) -> *const u8;
}

/// Wire-level surface common to every DOTS struct value, whether the
/// descriptor was known at compile time or learned at runtime.
///
/// Both [`StructValue`] (compiled-in types and [`AnyStruct`]) and
/// [`DynamicStruct`] implement this trait; the framing layer and the
/// transport's publish paths operate on `&dyn Transmittable` so they
/// don't need to care which path produced the value.
///
/// A `Transmittable` value can appear *somewhere* in a transmission —
/// either as a top-level publication or as a nested substruct. Whether
/// it may be published *standalone* is the stricter question answered
/// by [`Publishable`].
///
/// [`AnyStruct`]: crate::layout::AnyStruct
/// [`DynamicStruct`]: crate::DynamicStruct
pub trait Transmittable {
    /// The DOTS type name for this value's struct.
    fn type_name(&self) -> &str;

    /// Bitmask of properties currently set on this instance.
    fn valid_set(&self) -> PropertySet;

    /// Bitmask of properties declared as `#[dots(key)]` on the
    /// underlying type. Independent of which properties are set on
    /// this particular instance.
    fn key_set(&self) -> PropertySet;

    /// Encode this value to a CBOR map, emitting only the properties
    /// whose tag appears in `mask`. Pass `self.valid_set()` to emit
    /// everything that's set.
    fn encode_into(
        &self,
        mask: PropertySet,
        encoder: &mut CborEncoder<'_>,
    ) -> Result<(), EncodeError>;
}

/// Blanket: every `StructValue` (typed Rust structs and `AnyStruct`)
/// is automatically `Transmittable`. The encode path is the existing
/// descriptor-driven walk over the value's layout-compatible buffer.
impl<T: StructValue> Transmittable for T {
    fn type_name(&self) -> &str {
        StructValue::descriptor(self).name
    }

    fn valid_set(&self) -> PropertySet {
        StructValue::valid_set(self)
    }

    fn key_set(&self) -> PropertySet {
        crate::layout::key_set(self)
    }

    fn encode_into(
        &self,
        mask: PropertySet,
        encoder: &mut CborEncoder<'_>,
    ) -> Result<(), EncodeError> {
        crate::layout::encode_into_encoder_with_mask(self, mask, encoder)
    }
}

/// Marker for DOTS structs that are allowed to be published as
/// top-level instances.
///
/// `#[derive(DotsStruct)]` emits this impl automatically *unless* the
/// struct carries `#[dots(substruct_only)]` (or `[substruct_only]` in a
/// `.dots` file). Callers of `publish` / `remove` are therefore
/// statically prevented from sending a substruct-only typed value —
/// the compiler reports a missing `Publishable` bound at the call
/// site.
///
/// Runtime-described values ([`DynamicStruct`]) don't implement this
/// trait directly. Use
/// [`DynamicStruct::try_as_publishable`](crate::DynamicStruct::try_as_publishable)
/// to obtain a [`Publishable`] view after a runtime
/// `substruct_only`-flag check.
///
/// [`DynamicStruct`]: crate::DynamicStruct
pub trait Publishable: Transmittable {
    /// Static descriptor for this type, if one exists. Used by the
    /// transport to register the type with the broker before
    /// publication.
    ///
    /// The derive emits `Some(Self::DESCRIPTOR)` for compiled-in
    /// types. Runtime-described values return `None` — the caller is
    /// responsible for ensuring the broker already knows the
    /// descriptor (typically because it learned it from the broker in
    /// the first place).
    fn static_descriptor(&self) -> Option<&'static StructDescriptor> {
        None
    }
}
