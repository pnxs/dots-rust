use core::any::Any;

use crate::{PropertySet, StructDescriptor};

/// Runtime view of a DOTS struct value.
///
/// Implemented for every type that derives `DotsStruct`, plus the
/// dynamic [`AnyStruct`] type. The descriptor-driven codec walks any
/// `&dyn StructValue` regardless of whether the underlying storage is
/// a typed `Foo` or an `AnyStruct` — both expose a layout-compatible
/// data pointer.
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

/// Marker for DOTS structs that are allowed to be published as
/// top-level instances.
///
/// `#[derive(DotsStruct)]` emits this impl automatically *unless* the
/// struct carries `#[dots(substruct_only)]` (or `[substruct_only]` in a
/// `.dots` file). Callers of [`publish`] / [`remove`] are therefore
/// statically prevented from sending a substruct-only type — the
/// compiler reports a missing `Publishable` bound at the call site.
///
/// Dynamic values (`AnyStruct`) deliberately do not implement this
/// trait; if dynamic publishing is needed later it should go through a
/// runtime-checked helper that consults `descriptor().flags`.
///
/// [`publish`]: https://docs.rs/dots-transport
/// [`remove`]: https://docs.rs/dots-transport
pub trait Publishable: StructValue {}
