//! DOTS-internal types — the system structs that travel on the wire to
//! describe user-defined types, connection state, events, and so on.
//!
//! These mirror the `.dots` model files from `dots-cpp`
//! (`external/dots/model/*.dots`), defined here as Rust structs with
//! `#[derive(DotsStruct)]`. Each tag matches the `.dots` source so wire
//! bytes are cross-language compatible with C++ DOTS peers.
//!
//! Types implemented in this iteration:
//!
//! - [`StructDescriptorData`] / [`StructPropertyData`] / [`StructDocumentation`]
//!   / [`DotsStructFlags`] — describe a DOTS struct type.
//! - [`EnumDescriptorData`] / [`EnumElementDescriptor`] — describe a
//!   DOTS enum type.
//!
//! Future iterations will add `DotsStructScope` (needs enum support),
//! `DotsHeader` and the `DotsMsgHello`/`DotsMsgConnect` handshake
//! triplet, etc.

pub mod descriptors;

pub use descriptors::{
    DotsStructFlags, DotsStructScope, EnumDescriptorData, EnumElementDescriptor,
    StructDescriptorData, StructDocumentation, StructPropertyData,
};
