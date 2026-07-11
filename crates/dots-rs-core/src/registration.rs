//! Link-time type registration via `linkme` distributed slices.
//!
//! Each DOTS struct gets two registration methods (emitted by the
//! `#[derive(DotsStruct)]` macro): [`GlobalRegistration::register_as_published`]
//! and [`GlobalRegistration::register_as_subscribed`]. Each body
//! contains a static that the linker collects into [`PUBLISHED_TYPES`]
//! or [`SUBSCRIBED_TYPES`] respectively.
//!
//! The transport's `publish::<T>` / `subscribe::<T>` / `container::<T>`
//! entry points call the matching registration method. The methods are
//! non-generic, so rustc emits their statics for every derived type; on
//! ELF targets the derive therefore emits the entry *without* `#[used]`,
//! kept alive solely by a `black_box` reference from the method body. An
//! entry then survives dead-code elimination exactly when its method is
//! reachable from one of those entry points: with LTO (and, more
//! coarsely, plain `--gc-sections`) types the binary never publishes or
//! subscribes to are culled from the slice, so the runtime view matches
//! the binary's actual use. Builds without LTO retain more entries —
//! merely conservative, never wrong. On non-ELF targets the derive falls
//! back to linkme's `#[used]` entries (every derived type, no culling).
//!
//! Note the coupling: the derive's ELF path hard-codes the section names
//! `linkme_PUBLISHED_TYPES` / `linkme_SUBSCRIBED_TYPES` — linkme's ELF
//! naming for the two slice idents below. Renaming a slice requires
//! updating the derive.
//!
//! This mirrors C++ DOTS's `io::register_global_subscribe_type<T>` /
//! `register_global_publish_type<T>` mechanism, where the registration
//! statics are template members that only exist for instantiated types —
//! the linker-collected slice is the per-binary list the broker
//! handshake feeds during preload.

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
/// `publish::<T>` / `subscribe::<T>` keeps the slice entry inside each
/// fn body alive through dead-code elimination (the entry is anchored by
/// a `black_box` reference, not `#[used]`, so it is culled together with
/// the method when the binary never uses the type). Their bodies do no
/// meaningful work at runtime — membership is decided at link time.
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
