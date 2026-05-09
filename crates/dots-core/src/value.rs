use core::any::Any;

use crate::{PropertySet, StructDescriptor};

/// Runtime view of a DOTS struct value.
///
/// Implemented for every type that derives `DotsStruct`. The dispatcher
/// uses this trait to handle decoded values without knowing the concrete
/// type at the dispatch site; typed handlers downcast via [`Self::as_any`].
///
/// Object-safe — `dyn StructValue` is the canonical type-erased value.
pub trait StructValue: Any {
    /// Static metadata for this struct's type.
    fn descriptor(&self) -> &'static StructDescriptor;

    /// Bitmask of properties currently set on this instance.
    ///
    /// Computed from the underlying `Option<T>` fields, not stored —
    /// so it cannot disagree with the actual field state.
    fn valid_set(&self) -> PropertySet;

    /// Erase to `&dyn Any` for downcasting in typed handlers.
    fn as_any(&self) -> &dyn Any;
}
