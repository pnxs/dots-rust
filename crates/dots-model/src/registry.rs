//! Type registry for descriptor exchange.
//!
//! A [`Registry`] maps type names to [`DescriptorEntry`]s. It serves
//! two roles:
//!
//! 1. **Resolve nested type references** when reverse-converting
//!    [`StructDescriptorData`] / [`EnumDescriptorData`] into the owned
//!    `DynamicStructDescriptor` / `DynamicEnumDescriptor` shape — the
//!    wire form references nested types by name (`"MyType"`,
//!    `"vector<MyType>"`), and the registry is the lookup table.
//!
//! 2. **Hold the union of known types** in a process — both
//!    compile-time-known types (registered from a `&'static
//!    StructDescriptor` / `&'static EnumDescriptor`) and types learned
//!    at runtime through descriptor exchange (registered from owned
//!    `Arc<DynamicStructDescriptor>` / `Arc<DynamicEnumDescriptor>`).
//!
//! Once a type has been registered, the codec can decode any instance
//! of that type given just its name. This is the foundation of the
//! `dotsd` use case: a process learns about user-defined types it has
//! never compiled against, then routes their instances.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use dots_core::{
    AnyObject, AnyStruct, DynamicEnumDescriptor, DynamicEnumElement, DynamicFieldKind,
    DynamicPropertyDescriptor, DynamicStruct, DynamicStructDescriptor, DynamicValue,
    EnumDescriptor, StructDescriptor, StructFlags,
};

use crate::{EnumDescriptorData, StructDescriptorData};

/// One named entry in a [`Registry`] — either a struct or an enum.
#[derive(Debug, Clone)]
pub enum DescriptorEntry {
    Struct(Arc<DynamicStructDescriptor>),
    Enum(Arc<DynamicEnumDescriptor>),
}

/// Errors produced when reverse-converting wire-form descriptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// A property's `type` field referenced a name not in the registry.
    UnknownType(String),
    /// A `vector<...>` type-name was malformed (unclosed `<`, empty body, etc.).
    MalformedTypeName(String),
    /// `StructDescriptorData` or `EnumDescriptorData` was missing a required field.
    MissingField(&'static str),
    /// Property in `StructDescriptorData` lacked a name, tag, or type.
    MalformedProperty(&'static str),
}

impl core::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownType(name) => write!(f, "type `{name}` not in registry"),
            Self::MalformedTypeName(name) => write!(f, "malformed type name `{name}`"),
            Self::MissingField(name) => write!(f, "descriptor missing required field `{name}`"),
            Self::MalformedProperty(reason) => write!(f, "malformed property: {reason}"),
        }
    }
}

impl std::error::Error for RegistryError {}

/// The object recovered from an [`AnyObject`] by [`Registry::from_any`].
///
/// Which variant you get depends on what the registry knows about the
/// contained type:
///
/// - [`Typed`](DecodedAny::Typed) — the type was registered from a
///   compile-time `&'static StructDescriptor`, so the payload decodes
///   into a layout-compatible [`AnyStruct`] that can be downcast to a
///   typed `&T` via [`AnyStruct::as_typed`].
/// - [`Dynamic`](DecodedAny::Dynamic) — the type is only known through
///   runtime descriptor exchange (no compiled `T`), so the payload
///   decodes into a wire-only [`DynamicStruct`].
#[derive(Debug)]
pub enum DecodedAny {
    Typed(AnyStruct),
    Dynamic(DynamicStruct),
}

/// Errors from [`Registry::from_any`].
#[derive(Debug)]
pub enum FromAnyError {
    /// The contained type name is not registered. By the DOTS
    /// descriptor contract a publisher exports a type's descriptor
    /// before any instance referencing it, so this normally signals a
    /// contract violation (descriptor not yet learned).
    UnknownType(String),
    /// The contained type name resolved to an enum, not a struct.
    NotAStruct(String),
    /// The opaque payload failed to decode against the resolved
    /// descriptor.
    Decode(dots_core::DecodeError),
}

impl core::fmt::Display for FromAnyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownType(name) => write!(f, "contained type `{name}` not in registry"),
            Self::NotAStruct(name) => write!(f, "contained type `{name}` is an enum, not a struct"),
            Self::Decode(e) => write!(f, "failed to decode `any` payload: {e}"),
        }
    }
}

impl std::error::Error for FromAnyError {}

/// A name-keyed registry of [`DescriptorEntry`]s.
///
/// Uses interior `RwLock` mutability so registrations can happen via
/// `&self` — including from the [`crate::App`]-equivalent layer in
/// `dots-transport`, which auto-registers user types on `subscribe<T>`
/// while the codec holds a long-lived `Arc<Registry>` for decoding.
///
/// Tracks the compile-time `&'static StructDescriptor` separately from
/// the dynamic projection so the framing layer can pick the
/// layout-compatible decode path when one is available.
#[derive(Debug, Default)]
pub struct Registry {
    entries: RwLock<BTreeMap<String, DescriptorEntry>>,
    static_structs: RwLock<BTreeMap<String, &'static StructDescriptor>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a compile-time struct descriptor by name. The static
    /// descriptor is projected into its dynamic form once and stored
    /// behind an `Arc` — subsequent lookups are cheap.
    ///
    /// Existing entries with the same name are silently overwritten.
    pub fn register_struct_static(&self, d: &'static StructDescriptor) {
        let arc = Arc::new(DynamicStructDescriptor::from_static(d));
        self.entries
            .write()
            .expect("registry poisoned")
            .insert(d.name.into(), DescriptorEntry::Struct(arc));
        self.static_structs
            .write()
            .expect("registry poisoned")
            .insert(d.name.into(), d);
    }

    /// Look up the static descriptor for a registered type, if one
    /// exists. Returns `None` for types learned via
    /// [`register_struct_dynamic`](Self::register_struct_dynamic) (no
    /// compile-time `T` available).
    pub fn lookup_static_struct(&self, name: &str) -> Option<&'static StructDescriptor> {
        self.static_structs
            .read()
            .expect("registry poisoned")
            .get(name)
            .copied()
    }

    /// Register a compile-time enum descriptor.
    pub fn register_enum_static(&self, d: &'static EnumDescriptor) {
        let arc = Arc::new(DynamicEnumDescriptor::from_static(d));
        self.entries
            .write()
            .expect("registry poisoned")
            .insert(d.name.into(), DescriptorEntry::Enum(arc));
    }

    /// Register a runtime-received struct descriptor.
    pub fn register_struct_dynamic(&self, d: Arc<DynamicStructDescriptor>) {
        self.entries
            .write()
            .expect("registry poisoned")
            .insert(d.name.clone(), DescriptorEntry::Struct(d));
    }

    /// Register a runtime-received enum descriptor.
    pub fn register_enum_dynamic(&self, d: Arc<DynamicEnumDescriptor>) {
        self.entries
            .write()
            .expect("registry poisoned")
            .insert(d.name.clone(), DescriptorEntry::Enum(d));
    }

    /// Look up an entry by name. Returns an owned `DescriptorEntry`
    /// (which holds an `Arc` internally, so cloning is cheap) — this
    /// avoids holding the registry's read lock across the caller's
    /// usage.
    pub fn lookup(&self, name: &str) -> Option<DescriptorEntry> {
        self.entries
            .read()
            .expect("registry poisoned")
            .get(name)
            .cloned()
    }

    /// Recover the DOTS object stored in an [`AnyObject`], resolving the
    /// contained type by name against this registry.
    ///
    /// Prefers the compile-time descriptor when one is registered
    /// (returning [`DecodedAny::Typed`], which is downcastable to a
    /// typed `&T`); otherwise falls back to the runtime-learned
    /// descriptor (returning [`DecodedAny::Dynamic`]). Mirrors dots-cpp
    /// `from_any`, which resolves the type via the registry and throws
    /// on an unknown type — here that is [`FromAnyError::UnknownType`].
    pub fn from_any(&self, any: &AnyObject) -> Result<DecodedAny, FromAnyError> {
        let name = any.type_name();
        if let Some(descriptor) = self.lookup_static_struct(name) {
            return AnyStruct::decode_from_slice(descriptor, any.payload())
                .map(DecodedAny::Typed)
                .map_err(FromAnyError::Decode);
        }
        match self.lookup(name) {
            Some(DescriptorEntry::Struct(d)) => DynamicStruct::decode(d, any.payload())
                .map(DecodedAny::Dynamic)
                .map_err(FromAnyError::Decode),
            Some(DescriptorEntry::Enum(_)) => Err(FromAnyError::NotAStruct(name.into())),
            None => Err(FromAnyError::UnknownType(name.into())),
        }
    }

    /// Wrap a [`DynamicStruct`] for `Display` so that `any` fields are
    /// **expanded inline**: each contained object is decoded via
    /// [`from_any`](Self::from_any) and printed recursively, instead of
    /// the opaque `any<Type>[N bytes]` the bare `Display` produces. Any
    /// `any` field whose contained type the registry can't resolve
    /// falls back to that opaque form.
    ///
    /// Intended for trace/inspection tools (e.g. `dots-trace`) that hold
    /// the registry and want to see *through* the envelope.
    pub fn display_struct<'a>(&'a self, value: &'a DynamicStruct) -> StructDisplay<'a> {
        StructDisplay {
            value,
            registry: self,
        }
    }

    /// Number of registered types.
    pub fn len(&self) -> usize {
        self.entries.read().expect("registry poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().expect("registry poisoned").is_empty()
    }

    /// Snapshot of all registered struct descriptors. Returns an
    /// owned `Vec` so callers don't have to hold the registry lock
    /// while iterating.
    pub fn iter_structs(&self) -> Vec<Arc<dots_core::DynamicStructDescriptor>> {
        self.entries
            .read()
            .expect("registry poisoned")
            .values()
            .filter_map(|e| match e {
                DescriptorEntry::Struct(d) => Some(d.clone()),
                _ => None,
            })
            .collect()
    }

    /// Snapshot of all registered enum descriptors.
    pub fn iter_enums(&self) -> Vec<Arc<dots_core::DynamicEnumDescriptor>> {
        self.entries
            .read()
            .expect("registry poisoned")
            .values()
            .filter_map(|e| match e {
                DescriptorEntry::Enum(d) => Some(d.clone()),
                _ => None,
            })
            .collect()
    }

    // ----- Reverse conversion -----

    /// Build a `DynamicStructDescriptor` from its wire-form
    /// `StructDescriptorData`, resolving nested type references through
    /// the registry.
    ///
    /// Nested struct and enum references must already be registered;
    /// otherwise [`RegistryError::UnknownType`] is returned. (This
    /// implies a caller-managed dependency order — register types
    /// before any types that reference them. Cycles aren't supported
    /// in this iteration.)
    pub fn build_dynamic_struct(
        &self,
        data: &StructDescriptorData,
    ) -> Result<DynamicStructDescriptor, RegistryError> {
        let name = data
            .name
            .as_ref()
            .ok_or(RegistryError::MissingField("name"))?
            .clone();
        let flags = data
            .flags
            .as_ref()
            .map(|f| {
                StructFlags::NONE
                    .cached(f.cached.unwrap_or(false))
                    .internal(f.internal.unwrap_or(false))
                    .persistent(f.persistent.unwrap_or(false))
                    .cleanup(f.cleanup.unwrap_or(false))
                    .local(f.local.unwrap_or(false))
                    .substruct_only(f.substruct_only.unwrap_or(false))
            })
            .unwrap_or(StructFlags::NONE);

        let wire_props = data
            .properties
            .as_ref()
            .ok_or(RegistryError::MissingField("properties"))?;
        let mut properties = Vec::with_capacity(wire_props.len());
        for p in wire_props {
            let prop_name = p
                .name
                .as_ref()
                .ok_or(RegistryError::MalformedProperty("missing name"))?
                .clone();
            let tag = p
                .tag
                .ok_or(RegistryError::MalformedProperty("missing tag"))?;
            let is_key = p.is_key.unwrap_or(false);
            let type_name = p
                .type_name
                .as_ref()
                .ok_or(RegistryError::MalformedProperty("missing type"))?;
            let kind = self.parse_type_name(type_name)?;
            properties.push(DynamicPropertyDescriptor {
                name: prop_name,
                tag,
                is_key,
                kind,
            });
        }

        Ok(DynamicStructDescriptor {
            name,
            flags,
            properties,
        })
    }

    /// Build a `DynamicEnumDescriptor` from its wire-form
    /// `EnumDescriptorData`. Enums have no nested type references, so
    /// this is registry-independent.
    pub fn build_dynamic_enum(
        &self,
        data: &EnumDescriptorData,
    ) -> Result<DynamicEnumDescriptor, RegistryError> {
        let name = data
            .name
            .as_ref()
            .ok_or(RegistryError::MissingField("name"))?
            .clone();
        let wire_elements = data
            .elements
            .as_ref()
            .ok_or(RegistryError::MissingField("elements"))?;
        let mut elements = Vec::with_capacity(wire_elements.len());
        for el in wire_elements {
            let el_name = el
                .name
                .as_ref()
                .ok_or(RegistryError::MalformedProperty("enum element missing name"))?
                .clone();
            let tag = el
                .tag
                .ok_or(RegistryError::MalformedProperty("enum element missing tag"))?;
            let value = el
                .enum_value
                .ok_or(RegistryError::MalformedProperty("enum element missing value"))?;
            elements.push(DynamicEnumElement {
                name: el_name,
                tag,
                value,
            });
        }
        Ok(DynamicEnumDescriptor { name, elements })
    }

    /// Parse a DOTS wire-level type-name string into a [`DynamicFieldKind`].
    ///
    /// Recognized:
    /// - Primitives: `bool`, `uint8..64`, `int8..64`, `float32`, `float64`, `string`
    /// - Arrays: `vector<X>` for any inner type X (including `vector<uint8>`,
    ///   which is a CBOR array of u8 — *not* a byte string. dots-cpp's
    ///   `CborSerializer::visitVectorBeginDerived` always emits major-type-4
    ///   for `vector_t<T>`, with no special case for byte vectors.)
    /// - `.dots`-source aliases used by dots-cpp peers: `property_set`
    ///   (u32 on the wire — `PropertySet::value_t` is `uint32_t` in
    ///   dots-cpp) and `uuid` (16-byte CBOR byte string —
    ///   `dots::serialization::CborWriter::write(std::array<uint8_t,N>)`
    ///   emits major-type-2).
    /// - Named types: looked up in the registry; struct or enum entries match.
    pub fn parse_type_name(&self, raw: &str) -> Result<DynamicFieldKind, RegistryError> {
        let s = raw.trim();
        match s {
            "bool" => return Ok(DynamicFieldKind::Bool),
            "uint8" => return Ok(DynamicFieldKind::U8),
            "uint16" => return Ok(DynamicFieldKind::U16),
            "uint32" => return Ok(DynamicFieldKind::U32),
            "uint64" => return Ok(DynamicFieldKind::U64),
            "int8" => return Ok(DynamicFieldKind::I8),
            "int16" => return Ok(DynamicFieldKind::I16),
            "int32" => return Ok(DynamicFieldKind::I32),
            "int64" => return Ok(DynamicFieldKind::I64),
            "float32" => return Ok(DynamicFieldKind::F32),
            "float64" => return Ok(DynamicFieldKind::F64),
            "string" => return Ok(DynamicFieldKind::String),
            "timepoint" => return Ok(DynamicFieldKind::Timepoint),
            "duration" => return Ok(DynamicFieldKind::Duration),
            "steady_timepoint" => return Ok(DynamicFieldKind::Timepoint),
            "property_set" => return Ok(DynamicFieldKind::PropertySet),
            "uuid" => return Ok(DynamicFieldKind::Uuid),
            "any" => return Ok(DynamicFieldKind::Any),
            _ => {}
        }

        if let Some(inner) = strip_vector(s) {
            let inner_kind = self.parse_type_name(inner)?;
            return Ok(DynamicFieldKind::Vec(Box::new(inner_kind)));
        }

        match self.lookup(s) {
            Some(DescriptorEntry::Struct(d)) => Ok(DynamicFieldKind::Struct(d)),
            Some(DescriptorEntry::Enum(d)) => Ok(DynamicFieldKind::Enum(d)),
            None => Err(RegistryError::UnknownType(s.into())),
        }
    }
}

/// A [`DynamicStruct`] paired with the [`Registry`] that can resolve
/// its `any` fields, rendered with those fields expanded inline.
///
/// Returned by [`Registry::display_struct`]. The rendering mirrors the
/// bare [`DynamicStruct`] `Display` (descriptor-ordered `Type{ field:
/// value, … }`, enum variant names resolved) but recurses through
/// `any` fields — decoding the contained object and printing it in
/// place. Nested `any` (an expanded object that itself holds an `any`
/// field) expands too. An unresolved contained type stays opaque.
pub struct StructDisplay<'a> {
    value: &'a DynamicStruct,
    registry: &'a Registry,
}

impl core::fmt::Display for StructDisplay<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt_struct(self.value, self.registry, f)
    }
}

fn fmt_struct(
    s: &DynamicStruct,
    reg: &Registry,
    f: &mut core::fmt::Formatter<'_>,
) -> core::fmt::Result {
    f.write_str(&s.descriptor.name)?;
    f.write_str("{")?;
    let mut first = true;
    for prop in &s.descriptor.properties {
        if !s.valid.has(prop.tag) {
            continue;
        }
        f.write_str(if first { " " } else { ", " })?;
        first = false;
        f.write_str(&prop.name)?;
        f.write_str(": ")?;
        match s.properties.iter().find(|(t, _)| *t == prop.tag).map(|(_, v)| v) {
            Some(v) => fmt_value(v, &prop.kind, reg, f)?,
            None => f.write_str("?")?,
        }
    }
    if !first {
        f.write_str(" ")?;
    }
    f.write_str("}")
}

fn fmt_value(
    v: &DynamicValue,
    kind: &DynamicFieldKind,
    reg: &Registry,
    f: &mut core::fmt::Formatter<'_>,
) -> core::fmt::Result {
    match (v, kind) {
        // Resolve enum variant names from the property's descriptor,
        // matching the bare `DynamicStruct` Display.
        (DynamicValue::Enum(int_val), DynamicFieldKind::Enum(enum_desc)) => {
            match enum_desc.element_by_value(*int_val) {
                Some(elem) => f.write_str(&elem.name),
                None => write!(f, "{int_val}"),
            }
        }
        // Recurse so nested `any` inside a sub-struct also expands.
        (DynamicValue::Struct(inner), _) => fmt_struct(inner, reg, f),
        (DynamicValue::Vec(items), DynamicFieldKind::Vec(inner_kind)) => {
            f.write_str("[")?;
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                fmt_value(item, inner_kind, reg, f)?;
            }
            f.write_str("]")
        }
        // The point of this renderer: expand the opaque envelope.
        (DynamicValue::Any(a), _) => fmt_any(a, reg, f),
        // Leaves (and any value/kind mismatch): the value's own Display.
        (other, _) => core::fmt::Display::fmt(other, f),
    }
}

fn fmt_any(a: &AnyObject, reg: &Registry, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match reg.from_any(a) {
        Ok(DecodedAny::Dynamic(s)) => fmt_struct(&s, reg, f),
        Ok(DecodedAny::Typed(any_struct)) => {
            // Project the layout-compatible value into the wire-shaped
            // form so the same registry-aware walk applies.
            fmt_struct(&DynamicStruct::from_struct_value(&any_struct), reg, f)
        }
        // Unknown or undecodable contained type: keep it opaque, same
        // as the bare `Display`.
        Err(_) => write!(f, "any<{}>[{} bytes]", a.type_name(), a.payload().len()),
    }
}

/// Strip the outer `vector<...>` wrapper. Returns `Some(inner)` if `s`
/// is exactly of the form `vector<X>` (with matching brackets), else
/// `None`. Whitespace inside the angle brackets is preserved for
/// recursive parsing.
fn strip_vector(s: &str) -> Option<&str> {
    let s = s.trim();
    let prefix = "vector<";
    if !s.starts_with(prefix) || !s.ends_with('>') {
        return None;
    }
    Some(&s[prefix.len()..s.len() - 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_primitives() {
        let r = Registry::new();
        assert!(matches!(r.parse_type_name("bool"), Ok(DynamicFieldKind::Bool)));
        assert!(matches!(r.parse_type_name("uint32"), Ok(DynamicFieldKind::U32)));
        assert!(matches!(r.parse_type_name("int64"), Ok(DynamicFieldKind::I64)));
        assert!(matches!(r.parse_type_name("float64"), Ok(DynamicFieldKind::F64)));
        assert!(matches!(
            r.parse_type_name("string"),
            Ok(DynamicFieldKind::String)
        ));
    }

    #[test]
    fn vector_of_uint8_is_array_not_bytestring() {
        // dots-cpp's CborSerializer encodes every `vector_t<T>` as a
        // CBOR array — including `vector<uint8>`. Mapping to `Vec(U8)`
        // matches that wire format. (Byte-string is reserved for
        // `uuid`, which dots-cpp emits as `std::array<uint8_t, 16>`.)
        let r = Registry::new();
        match r.parse_type_name("vector<uint8>").unwrap() {
            DynamicFieldKind::Vec(inner) => assert!(matches!(*inner, DynamicFieldKind::U8)),
            other => panic!("expected Vec(U8), got {other:?}"),
        }
    }

    #[test]
    fn vector_of_other_primitives_is_vec() {
        let r = Registry::new();
        match r.parse_type_name("vector<uint32>").unwrap() {
            DynamicFieldKind::Vec(inner) => assert!(matches!(*inner, DynamicFieldKind::U32)),
            other => panic!("expected Vec(U32), got {other:?}"),
        }
    }

    #[test]
    fn unknown_type_errors() {
        let r = Registry::new();
        let err = r.parse_type_name("MyType").unwrap_err();
        assert_eq!(err, RegistryError::UnknownType("MyType".into()));
    }

    #[test]
    fn nested_vector_parses_recursively() {
        let r = Registry::new();
        match r.parse_type_name("vector<vector<int32>>").unwrap() {
            DynamicFieldKind::Vec(outer) => match *outer {
                DynamicFieldKind::Vec(inner) => assert!(matches!(*inner, DynamicFieldKind::I32)),
                other => panic!("expected inner Vec, got {other:?}"),
            },
            other => panic!("expected outer Vec, got {other:?}"),
        }
    }

    #[test]
    fn parses_any() {
        let r = Registry::new();
        assert!(matches!(r.parse_type_name("any"), Ok(DynamicFieldKind::Any)));
    }
}

#[cfg(test)]
mod from_any_tests {
    use super::*;
    use dots_core::{Transmittable, to_any};
    use dots_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Ping")]
    struct Ping {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        note: Option<String>,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Envelope")]
    struct Envelope {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        payload: Option<dots_core::AnyObject>,
    }

    fn envelope_wrapping(ping: &Ping) -> dots_core::DynamicStruct {
        let envelope = Envelope {
            id: Some(1),
            payload: Some(to_any(ping)),
        };
        dots_core::DynamicStruct::from_struct_value(&envelope)
    }

    #[test]
    fn from_any_typed_when_static_registered() {
        let registry = Registry::new();
        registry.register_struct_static(Ping::DESCRIPTOR);

        let ping = Ping {
            id: Some(42),
            note: Some("hi".into()),
        };
        let any = to_any(&ping);

        match registry.from_any(&any).expect("from_any succeeds") {
            DecodedAny::Typed(s) => {
                let recovered = s.as_typed::<Ping>().expect("downcast to Ping");
                assert_eq!(recovered, &ping);
            }
            DecodedAny::Dynamic(_) => panic!("expected Typed, got Dynamic"),
        }
    }

    #[test]
    fn from_any_dynamic_when_only_dynamic_registered() {
        let registry = Registry::new();
        // Register only the runtime-shaped descriptor (no static type).
        let dyn_desc = std::sync::Arc::new(dots_core::DynamicStructDescriptor::from_static(
            Ping::DESCRIPTOR,
        ));
        registry.register_struct_dynamic(dyn_desc);

        let ping = Ping {
            id: Some(7),
            note: None,
        };
        let any = to_any(&ping);

        match registry.from_any(&any).expect("from_any succeeds") {
            DecodedAny::Dynamic(s) => {
                assert_eq!(s.type_name(), "Ping");
                assert!(s.valid.has(1));
                assert!(!s.valid.has(2));
            }
            DecodedAny::Typed(_) => panic!("expected Dynamic, got Typed"),
        }
    }

    #[test]
    fn from_any_unknown_type_errors() {
        let registry = Registry::new();
        let any = dots_core::AnyObject::new("NotRegistered", vec![0xA0]);
        match registry.from_any(&any) {
            Err(FromAnyError::UnknownType(name)) => assert_eq!(name, "NotRegistered"),
            other => panic!("expected UnknownType, got {other:?}"),
        }
    }

    #[test]
    fn display_struct_expands_any_inline() {
        let registry = Registry::new();
        registry.register_struct_static(Ping::DESCRIPTOR);

        let ping = Ping {
            id: Some(42),
            note: Some("hi".into()),
        };
        let dyn_env = envelope_wrapping(&ping);

        let rendered = format!("{}", registry.display_struct(&dyn_env));
        assert_eq!(
            rendered,
            r#"Envelope{ id: 1, payload: Ping{ id: 42, note: "hi" } }"#
        );
    }

    #[test]
    fn display_struct_keeps_unresolved_any_opaque() {
        // Ping is not registered, so the contained object can't be
        // decoded — the renderer falls back to the opaque form.
        let registry = Registry::new();
        let ping = Ping {
            id: Some(42),
            note: Some("hi".into()),
        };
        let dyn_env = envelope_wrapping(&ping);

        let rendered = format!("{}", registry.display_struct(&dyn_env));
        assert!(
            rendered.starts_with("Envelope{ id: 1, payload: any<Ping>["),
            "got: {rendered}"
        );
    }
}
