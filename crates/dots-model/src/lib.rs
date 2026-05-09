//! DOTS-internal types — the system structs that travel on the wire to
//! describe user-defined types, connection state, events, and so on.
//!
//! These mirror the `.dots` model files from `dots-cpp`
//! (`external/dots/model/*.dots`), defined here as Rust structs with
//! `#[derive(DotsStruct)]`. Each tag matches the `.dots` source so wire
//! bytes are cross-language compatible with C++ DOTS peers.
//!
//! Modules:
//!
//! - [`descriptors`] — wire form of DOTS struct and enum metadata
//!   ([`StructDescriptorData`], [`EnumDescriptorData`], etc.).
//! - [`connection`] — per-transmission [`DotsHeader`] and the handshake
//!   messages ([`DotsMsgHello`], [`DotsMsgConnect`],
//!   [`DotsMsgConnectResponse`]) plus the [`DotsConnectionState`] enum.
//! - [`registry`] — name-keyed [`Registry`] for resolving wire-form
//!   descriptors back into owned `DynamicStructDescriptor` /
//!   `DynamicEnumDescriptor` instances.

pub mod connection;
pub mod daemon;
pub mod descriptors;
pub mod framing;
pub mod registry;

pub use connection::{
    DotsCacheInfo, DotsClearCache, DotsCloneInformation, DotsConnectionState, DotsDescriptorRequest,
    DotsEcho, DotsHeader, DotsMember, DotsMemberEvent, DotsMsgConnect, DotsMsgConnectResponse,
    DotsMsgError, DotsMsgHello, DotsMt,
};
pub use daemon::{
    DotsCacheStatus, DotsClient, DotsDaemonStatus, DotsResourceUsage, DotsStatistics,
};
pub use descriptors::{
    DotsStructFlags, DotsStructScope, EnumDescriptorData, EnumElementDescriptor,
    StructDescriptorData, StructDocumentation, StructPropertyData,
};
pub use framing::{
    FramingError, MAX_BODY_SIZE, SIZE_PREFIX_LEN, SIZE_PREFIX_MARKER, Transmission,
    decode_typed_transmission, encode_typed_transmission, encode_typed_transmission_into,
    parse_size_prefix,
};
pub use registry::{DescriptorEntry, Registry, RegistryError};

/// Register the DOTS-internal types — the handshake messages, the
/// per-transmission [`DotsHeader`], descriptor-data types, and
/// connection-state enum — into a [`Registry`].
///
/// Any client that wants the codec to decode handshake traffic must
/// have these registered. Order doesn't matter for static registration
/// since each `&'static StructDescriptor` already references its
/// nested types directly; the registry just needs name → descriptor
/// entries.
pub fn register_dots_internal_types(reg: &mut Registry) {
    // Connection / handshake.
    reg.register_struct_static(DotsHeader::DESCRIPTOR);
    reg.register_struct_static(DotsMsgHello::DESCRIPTOR);
    reg.register_struct_static(DotsMsgConnect::DESCRIPTOR);
    reg.register_struct_static(DotsMsgConnectResponse::DESCRIPTOR);
    reg.register_struct_static(DotsMsgError::DESCRIPTOR);
    reg.register_enum_static(DotsConnectionState::DESCRIPTOR);

    // Group membership / events / cache metadata.
    reg.register_enum_static(DotsMemberEvent::DESCRIPTOR);
    reg.register_struct_static(DotsMember::DESCRIPTOR);
    reg.register_enum_static(DotsMt::DESCRIPTOR);
    reg.register_struct_static(DotsCloneInformation::DESCRIPTOR);

    // System events the broker pushes (user.dots).
    reg.register_struct_static(DotsCacheInfo::DESCRIPTOR);
    reg.register_struct_static(DotsClearCache::DESCRIPTOR);
    reg.register_struct_static(DotsDescriptorRequest::DESCRIPTOR);
    reg.register_struct_static(DotsEcho::DESCRIPTOR);

    // Descriptor exchange.
    reg.register_struct_static(StructPropertyData::DESCRIPTOR);
    reg.register_struct_static(StructDocumentation::DESCRIPTOR);
    reg.register_struct_static(DotsStructFlags::DESCRIPTOR);
    reg.register_enum_static(DotsStructScope::DESCRIPTOR);
    reg.register_struct_static(StructDescriptorData::DESCRIPTOR);
    reg.register_struct_static(EnumElementDescriptor::DESCRIPTOR);
    reg.register_struct_static(EnumDescriptorData::DESCRIPTOR);

    // Daemon-side records (broker introspection).
    reg.register_struct_static(daemon::DotsClient::DESCRIPTOR);
    reg.register_struct_static(daemon::DotsStatistics::DESCRIPTOR);
    reg.register_struct_static(daemon::DotsCacheStatus::DESCRIPTOR);
    reg.register_struct_static(daemon::DotsResourceUsage::DESCRIPTOR);
    reg.register_struct_static(daemon::DotsDaemonStatus::DESCRIPTOR);
}

/// One-line constructor: a [`Registry`] pre-populated with the DOTS-
/// internal types. Equivalent to `Registry::new()` followed by
/// [`register_dots_internal_types`].
pub fn registry_with_internal_types() -> Registry {
    let mut reg = Registry::new();
    register_dots_internal_types(&mut reg);
    reg
}
