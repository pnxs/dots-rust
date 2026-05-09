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
pub mod descriptors;
pub mod framing;
pub mod registry;

pub use connection::{
    DotsConnectionState, DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgError,
    DotsMsgHello,
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
