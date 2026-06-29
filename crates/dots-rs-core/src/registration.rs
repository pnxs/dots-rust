//! Link-time type registration via `linkme` distributed slices.
//!
//! Each DOTS struct gets two registration methods (emitted by the
//! `#[derive(DotsStruct)]` macro): [`GlobalRegistration::register_as_published`]
//! and [`GlobalRegistration::register_as_subscribed`]. Each body
//! contains a static that the linker collects into [`PUBLISHED_TYPES`]
//! or [`SUBSCRIBED_TYPES`] respectively.
//!
//! The transport's `publish::<T>` / `subscribe::<T>` entry points call
//! the matching registration method; monomorphization for any `T` that
//! appears at one of those entry points causes the static to be
//! emitted. With LTO, types the binary never publishes or subscribes
//! to don't appear in the corresponding slice, so the runtime view
//! exactly matches the binary's link-time intent.
//!
//! This mirrors C++ DOTS's `io::register_global_subscribe_type<T>` /
//! `register_global_publish_type<T>` mechanism — the linker-collected
//! slice is the per-binary list the broker handshake feeds during
//! preload.

use crate::StructDescriptor;

/// Every type the binary publishes anywhere. Populated at link time
/// from `register_as_published` invocations reachable from the binary.
#[linkme::distributed_slice]
pub static PUBLISHED_TYPES: [&'static StructDescriptor];

/// Every type the binary subscribes to anywhere. Populated at link
/// time from `register_as_subscribed` invocations reachable from the
/// binary. Consumed by the transport during the preload handshake to
/// request cached state for these types.
#[linkme::distributed_slice]
pub static SUBSCRIBED_TYPES: [&'static StructDescriptor];

/// Per-type link-time registration hooks. Implemented by
/// `#[derive(DotsStruct)]`; not intended for hand-written impls.
///
/// The two methods exist solely so that calling them from
/// `publish::<T>` / `subscribe::<T>` pulls the static-with-link-section
/// inside each fn body into the linker's view of the binary. Their
/// bodies do nothing at runtime — the work happens at link time.
pub trait GlobalRegistration {
    /// Mark this type as published by the current binary. Idempotent
    /// at runtime; the underlying link-time slot is set once.
    ///
    /// Default impl is a no-op — runtime-described values (e.g.
    /// `DynamicPublishable`) have no compile-time descriptor and
    /// register nothing.
    fn register_as_published() {}
    /// Mark this type as subscribed by the current binary.
    ///
    /// Default impl is a no-op — same rationale as
    /// [`register_as_published`](Self::register_as_published).
    fn register_as_subscribed() {}
}
