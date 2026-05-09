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
