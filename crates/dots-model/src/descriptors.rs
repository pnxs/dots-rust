//! Descriptor data — wire form of DOTS struct and enum metadata.
//!
//! Mirrors `external/dots/model/descriptors.dots` from dots-cpp.

use dots_core::{FieldKind, PropertyDescriptor, StructDescriptor, StructFlags};
use dots_derive::DotsStruct;

/// Per-flag bool view of `StructFlags`. Wire form for transmitting
/// struct-level flags between peers.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsStructFlags [internal,cached=false] {
///     1: bool cached;
///     2: bool internal;
///     3: bool persistent;
///     4: bool cleanup;
///     5: bool local;
///     6: bool substructOnly;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsStructFlags", internal)]
pub struct DotsStructFlags {
    #[dots(tag = 1)]
    pub cached: Option<bool>,
    #[dots(tag = 2)]
    pub internal: Option<bool>,
    #[dots(tag = 3)]
    pub persistent: Option<bool>,
    #[dots(tag = 4)]
    pub cleanup: Option<bool>,
    #[dots(tag = 5)]
    pub local: Option<bool>,
    #[dots(tag = 6)]
    pub substruct_only: Option<bool>,
}

impl DotsStructFlags {
    /// Project a packed `StructFlags` into the wire-form struct of bools.
    pub fn from_static(f: StructFlags) -> Self {
        Self {
            cached: Some(f.is_cached()),
            internal: Some(f.is_internal()),
            persistent: Some(f.is_persistent()),
            cleanup: Some(f.is_cleanup()),
            local: Some(f.is_local()),
            substruct_only: Some(f.is_substruct_only()),
        }
    }
}

/// One property within a `StructDescriptorData`.
///
/// Mirrors `.dots`:
/// ```text
/// struct StructPropertyData [internal,cached=false] {
///     1: string name;
///     2: uint32 tag;
///     3: bool isKey;
///     4: string type;     // type name; the field is keyword `type` in C++
///     5: uint32 typeId;
/// }
/// ```
///
/// The `type_name` field on the Rust side keeps tag 4 (`type` would be
/// a Rust keyword); the wire bytes are unchanged.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "StructPropertyData", internal)]
pub struct StructPropertyData {
    #[dots(tag = 1)]
    pub name: Option<String>,
    #[dots(tag = 2)]
    pub tag: Option<u32>,
    #[dots(tag = 3)]
    pub is_key: Option<bool>,
    #[dots(tag = 4)]
    pub type_name: Option<String>,
    #[dots(tag = 5)]
    pub type_id: Option<u32>,
}

impl StructPropertyData {
    /// Build from a static `PropertyDescriptor`.
    pub fn from_static(p: &'static PropertyDescriptor) -> Self {
        Self {
            name: Some(p.name.into()),
            tag: Some(p.tag),
            is_key: Some(p.is_key),
            type_name: Some(field_kind_type_name(&p.kind)),
            type_id: None,
        }
    }
}

/// Free-form documentation attached to a struct descriptor.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "StructDocumentation", internal)]
pub struct StructDocumentation {
    #[dots(tag = 1)]
    pub description: Option<String>,
    #[dots(tag = 2)]
    pub comment: Option<String>,
}

/// Wire form of a complete struct type descriptor.
///
/// Mirrors `.dots`:
/// ```text
/// struct StructDescriptorData [internal,cached=false] {
///     1: [key] string name;
///     2: vector<StructPropertyData> properties;
///     3: StructDocumentation documentation;
///     4: DotsStructScope scope;          // enum — deferred until enum support
///     5: DotsStructFlags flags;
///     6: uint32 publisherId;
/// }
/// ```
///
/// Tag 4 (`scope`) is deliberately omitted in this iteration — DOTS
/// enums aren't supported yet on the Rust side. CBOR maps are sparse,
/// so the field is simply absent on the wire; peers that send a scope
/// have it skipped on decode (forward-compat). Add it back when enum
/// support lands.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "StructDescriptorData", internal)]
pub struct StructDescriptorData {
    #[dots(tag = 1, key)]
    pub name: Option<String>,
    #[dots(tag = 2)]
    pub properties: Option<Vec<StructPropertyData>>,
    #[dots(tag = 3)]
    pub documentation: Option<StructDocumentation>,
    // tag 4: scope — TODO when DOTS enum support lands
    #[dots(tag = 5)]
    pub flags: Option<DotsStructFlags>,
    #[dots(tag = 6)]
    pub publisher_id: Option<u32>,
}

impl StructDescriptorData {
    /// Build a descriptor data record from a static descriptor.
    ///
    /// Nested struct types are referenced by name only (the receiver
    /// resolves them through their own registry); this is therefore a
    /// shallow, single-struct conversion — caller is responsible for
    /// transmitting any referenced types separately.
    pub fn from_static(d: &'static StructDescriptor) -> Self {
        Self {
            name: Some(d.name.into()),
            properties: Some(d.properties.iter().map(StructPropertyData::from_static).collect()),
            documentation: None,
            flags: Some(DotsStructFlags::from_static(d.flags)),
            publisher_id: None,
        }
    }
}

/// One element of an enum descriptor.
///
/// Mirrors `.dots`:
/// ```text
/// struct EnumElementDescriptor [internal,cached=false] {
///     1: int32 enum_value;
///     2: string name;
///     3: uint32 tag;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "EnumElementDescriptor", internal)]
pub struct EnumElementDescriptor {
    #[dots(tag = 1)]
    pub enum_value: Option<i32>,
    #[dots(tag = 2)]
    pub name: Option<String>,
    #[dots(tag = 3)]
    pub tag: Option<u32>,
}

/// Wire form of a complete enum type descriptor.
///
/// Mirrors `.dots`:
/// ```text
/// struct EnumDescriptorData [internal,cached=false] {
///     1: [key] string name;
///     2: vector<EnumElementDescriptor> elements;
///     // tag 3 deprecated/skipped in C++
///     4: uint32 publisherId;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "EnumDescriptorData", internal)]
pub struct EnumDescriptorData {
    #[dots(tag = 1, key)]
    pub name: Option<String>,
    #[dots(tag = 2)]
    pub elements: Option<Vec<EnumElementDescriptor>>,
    // tag 3 deprecated in the .dots source — skipped here too.
    #[dots(tag = 4)]
    pub publisher_id: Option<u32>,
}

/// Map a [`FieldKind`] to its DOTS wire-level type-name string.
///
/// DOTS uses names like `uint32`, `int32`, `string`, `vector<uint32>`,
/// etc. — not Rust's spelling. Nested struct fields produce just the
/// struct's name (the receiver resolves it through their registry).
pub fn field_kind_type_name(kind: &FieldKind) -> String {
    match kind {
        FieldKind::Bool => "bool".into(),
        FieldKind::U8 => "uint8".into(),
        FieldKind::U16 => "uint16".into(),
        FieldKind::U32 => "uint32".into(),
        FieldKind::U64 => "uint64".into(),
        FieldKind::I8 => "int8".into(),
        FieldKind::I16 => "int16".into(),
        FieldKind::I32 => "int32".into(),
        FieldKind::I64 => "int64".into(),
        FieldKind::F32 => "float32".into(),
        FieldKind::F64 => "float64".into(),
        FieldKind::String => "string".into(),
        FieldKind::Bytes => "vector<uint8>".into(),
        FieldKind::Vec(inner) => format!("vector<{}>", field_kind_type_name(inner)),
        FieldKind::Struct(d) => d.name.into(),
    }
}
