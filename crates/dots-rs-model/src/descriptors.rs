//! Descriptor data — wire form of DOTS struct and enum metadata.
//!
//! Mirrors `external/dots/model/descriptors.dots` from dots-cpp.

use dots_rs_core::{EnumDescriptor, FieldKind, PropertyDescriptor, StructDescriptor, StructFlags};
use dots_rs_derive::{DotsEnum, DotsStruct};

/// Scope at which a DOTS struct is valid.
///
/// Mirrors `.dots`:
/// ```text
/// enum DotsStructScope {
///     1: program,   // client-internal only
///     2: server,    // routed only to clients on the same server
///     3: site,      // routed within the same site
///     4: global     // no limitation
/// }
/// ```
#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(rt_internal)]
#[dots(name = "DotsStructScope")]
pub enum DotsStructScope {
    #[default]
    #[dots(tag = 1)]
    Program,
    #[dots(tag = 2)]
    Server,
    #[dots(tag = 3)]
    Site,
    #[dots(tag = 4)]
    Global,
}

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
#[dots(rt_internal)]
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
#[dots(rt_internal)]
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

    /// Build from a runtime-owned `DynamicPropertyDescriptor`.
    pub fn from_dynamic(p: &dots_rs_core::DynamicPropertyDescriptor) -> Self {
        Self {
            name: Some(p.name.clone()),
            tag: Some(p.tag),
            is_key: Some(p.is_key),
            type_name: Some(dyn_field_kind_type_name(&p.kind)),
            type_id: None,
        }
    }
}

/// Free-form documentation attached to a struct descriptor.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(rt_internal)]
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
///     4: DotsStructScope scope;
///     5: DotsStructFlags flags;
///     6: uint32 publisherId;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(rt_internal)]
#[dots(name = "StructDescriptorData", internal)]
pub struct StructDescriptorData {
    #[dots(tag = 1, key)]
    pub name: Option<String>,
    #[dots(tag = 2)]
    pub properties: Option<Vec<StructPropertyData>>,
    #[dots(tag = 3)]
    pub documentation: Option<StructDocumentation>,
    #[dots(tag = 4)]
    pub scope: Option<DotsStructScope>,
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
            // We don't track scope on the Rust side yet — leave it
            // unset on the wire; peers default to their own policy.
            scope: None,
            flags: Some(DotsStructFlags::from_static(d.flags)),
            publisher_id: None,
        }
    }

    /// Build from a runtime-owned `DynamicStructDescriptor`. Used by
    /// the broker's `DotsDescriptorRequest` handler to re-emit a
    /// previously registered descriptor without needing the original
    /// `&'static` reference.
    pub fn from_dynamic(d: &dots_rs_core::DynamicStructDescriptor) -> Self {
        Self {
            name: Some(d.name.clone()),
            properties: Some(
                d.properties
                    .iter()
                    .map(StructPropertyData::from_dynamic)
                    .collect(),
            ),
            documentation: None,
            scope: None,
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
#[dots(rt_internal)]
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
#[dots(rt_internal)]
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

impl EnumDescriptorData {
    /// Build from a static `EnumDescriptor`. Symmetric with
    /// `StructDescriptorData::from_static`.
    pub fn from_static(d: &'static EnumDescriptor) -> Self {
        Self {
            name: Some(d.name.into()),
            elements: Some(
                d.elements
                    .iter()
                    .map(|e| EnumElementDescriptor {
                        enum_value: Some(e.value),
                        name: Some(e.name.into()),
                        tag: Some(e.tag),
                    })
                    .collect(),
            ),
            publisher_id: None,
        }
    }
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
        FieldKind::PropertySet => "property_set".into(),
        FieldKind::String => "string".into(),
        FieldKind::Uuid => "uuid".into(),
        FieldKind::Timepoint => "timepoint".into(),
        FieldKind::Duration => "duration".into(),
        FieldKind::Vec(inner) => format!("vector<{}>", field_kind_type_name(inner)),
        FieldKind::Struct(d) => d.name.into(),
        FieldKind::Enum(d) => d.name.into(),
        FieldKind::Any => "any".into(),
    }
}

/// Same as [`field_kind_type_name`] but for the runtime-owned
/// [`DynamicFieldKind`] (descriptors received over the wire). Used
/// when the broker re-emits a known struct's descriptor in response
/// to `DotsDescriptorRequest`.
pub fn dyn_field_kind_type_name(kind: &dots_rs_core::DynamicFieldKind) -> String {
    use dots_rs_core::DynamicFieldKind;
    match kind {
        DynamicFieldKind::Bool => "bool".into(),
        DynamicFieldKind::U8 => "uint8".into(),
        DynamicFieldKind::U16 => "uint16".into(),
        DynamicFieldKind::U32 => "uint32".into(),
        DynamicFieldKind::U64 => "uint64".into(),
        DynamicFieldKind::I8 => "int8".into(),
        DynamicFieldKind::I16 => "int16".into(),
        DynamicFieldKind::I32 => "int32".into(),
        DynamicFieldKind::I64 => "int64".into(),
        DynamicFieldKind::F32 => "float32".into(),
        DynamicFieldKind::F64 => "float64".into(),
        DynamicFieldKind::String => "string".into(),
        DynamicFieldKind::PropertySet => "property_set".into(),
        DynamicFieldKind::Uuid => "uuid".into(),
        DynamicFieldKind::Timepoint => "timepoint".into(),
        DynamicFieldKind::Duration => "duration".into(),
        DynamicFieldKind::Vec(inner) => format!("vector<{}>", dyn_field_kind_type_name(inner)),
        DynamicFieldKind::Struct(d) => d.name.clone(),
        DynamicFieldKind::Enum(d) => d.name.clone(),
        DynamicFieldKind::Any => "any".into(),
    }
}
