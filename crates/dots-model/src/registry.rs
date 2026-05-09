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
    DynamicEnumDescriptor, DynamicEnumElement, DynamicFieldKind, DynamicPropertyDescriptor,
    DynamicStructDescriptor, EnumDescriptor, StructDescriptor, StructFlags,
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

/// A name-keyed registry of [`DescriptorEntry`]s.
///
/// Uses interior `RwLock` mutability so registrations can happen via
/// `&self` — including from the [`crate::App`]-equivalent layer in
/// `dots-transport`, which auto-registers user types on `subscribe<T>`
/// while the codec holds a long-lived `Arc<Registry>` for decoding.
#[derive(Debug, Default)]
pub struct Registry {
    entries: RwLock<BTreeMap<String, DescriptorEntry>>,
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

    /// Number of registered types.
    pub fn len(&self) -> usize {
        self.entries.read().expect("registry poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().expect("registry poisoned").is_empty()
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
    /// - Bytes: `vector<uint8>` (treated specially to preserve byte-string wire format)
    /// - Arrays: `vector<X>` for any inner type X
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
            _ => {}
        }

        if let Some(inner) = strip_vector(s) {
            if inner.trim() == "uint8" {
                return Ok(DynamicFieldKind::Bytes);
            }
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
    fn vector_of_uint8_is_bytes() {
        let r = Registry::new();
        assert!(matches!(
            r.parse_type_name("vector<uint8>"),
            Ok(DynamicFieldKind::Bytes)
        ));
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
}
