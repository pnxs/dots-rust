//! Instance references — [`InstanceRef`] (wire-format spec §6.5).
//!
//! An instance reference names an instance of a cached type by its
//! canonical instance key (spec §6.2) without carrying the instance's
//! state — the identity-only counterpart of [`AnyObject`]'s
//! full-payload envelope.
//!
//! [`InstanceRef<T>`] covers both model forms through its type
//! parameter:
//!
//! - `InstanceRef` (= `InstanceRef<AnyInstance>`) is the **generic
//!   reference** — model type `instance_ref` — and encodes as
//!   `[ typeName (text), canonical key (array) ]`.
//! - `InstanceRef<MyType>` is the **typed reference** — model type
//!   `instance_ref<MyType>` — whose target type is fixed by the
//!   schema, so it encodes as the bare canonical key array.
//!
//! Byte equality of encoded references is instance identity. The
//! contained canonical key of a reference must denote a *complete*
//! identity: it must not contain `null` (only the standalone canonical
//! key of a locally constructed partial value may).
//!
//! The elements of a typed reference's key correspond 1:1 to the
//! target type's key properties in ascending tag order;
//! [`materialize_key`] turns such a key back into a partial
//! [`DynamicStruct`] carrying exactly the key properties.
//!
//! [`AnyObject`]: crate::AnyObject

use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::marker::PhantomData;

use crate::{
    DotsField, DotsTypeKind, DynamicStruct, DynamicStructDescriptor, FieldKind, PropertySet,
    StructValue,
    layout::{CborDecoder, CborEncoder, DecodeError, EncodeError},
};

/// Marker for the generic (any-type) reference target — the type
/// parameter of a bare [`InstanceRef`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnyInstance {}

/// A type usable as the target of a typed [`InstanceRef<T>`].
///
/// Implemented by `#[derive(DotsStruct)]` for every derived struct
/// (with its DOTS type name), and by [`AnyInstance`] (with `None`) for
/// the generic reference. Not meant to be implemented by hand.
pub trait RefTarget {
    /// The DOTS type name a typed reference resolves to, or `None`
    /// for the generic reference.
    const TARGET_NAME: Option<&'static str>;
}

impl RefTarget for AnyInstance {
    const TARGET_NAME: Option<&'static str> = None;
}

/// A reference to a DOTS instance: type identity plus canonical
/// instance key, without the instance's state. See the [module
/// docs](self) for the two forms and their wire encodings.
///
/// Equality and hashing are over the encoded identity (type name and
/// key bytes), so a reference works directly as a map key.
pub struct InstanceRef<T: RefTarget = AnyInstance> {
    /// `Some` for generic references; `None` for typed references,
    /// whose name is `T::TARGET_NAME`.
    type_name: Option<String>,
    /// The complete canonical-key CBOR array (spec §6.2).
    key: Vec<u8>,
    _marker: PhantomData<T>,
}

// Manual impls: derived ones would bound `T`, but the marker type
// carries no data — a reference is Clone/Eq/Hash regardless of whether
// its *target* type is.
impl<T: RefTarget> Clone for InstanceRef<T> {
    fn clone(&self) -> Self {
        Self {
            type_name: self.type_name.clone(),
            key: self.key.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: RefTarget> PartialEq for InstanceRef<T> {
    fn eq(&self, other: &Self) -> bool {
        self.type_name == other.type_name && self.key == other.key
    }
}

impl<T: RefTarget> Eq for InstanceRef<T> {}

impl<T: RefTarget> core::hash::Hash for InstanceRef<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.type_name.hash(state);
        self.key.hash(state);
    }
}

impl<T: RefTarget> core::fmt::Debug for InstanceRef<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("InstanceRef")
            .field("type_name", &self.type_name())
            .field("key", &self.key)
            .finish()
    }
}

impl<T: RefTarget> Default for InstanceRef<T> {
    /// The empty reference: a canonical key of `[]` (and, for the
    /// generic form, an empty type name). It denotes nothing — it
    /// exists so bare `#[dots(key)]` reference fields have a valid
    /// pre-assignment slot value, like `String`'s `""`.
    fn default() -> Self {
        Self {
            type_name: T::TARGET_NAME.is_none().then(String::new),
            key: alloc::vec![0x80],
            _marker: PhantomData,
        }
    }
}

impl<T: RefTarget> crate::DotsFieldOps for InstanceRef<T> {
    fn dots_eq(&self, other: &Self) -> bool {
        self == other
    }

    /// Total order over the encoded identity — type name, then key
    /// bytes. Byte order of the deterministic encoding is coherent
    /// with identity (spec §6.2).
    fn dots_cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.type_name
            .cmp(&other.type_name)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl InstanceRef<AnyInstance> {
    /// Generic reference from raw parts. `key` must be a complete
    /// canonical-key CBOR array.
    pub fn new(type_name: impl Into<String>, key: Vec<u8>) -> Self {
        debug_assert!(
            key_is_complete(&key),
            "an instance reference must carry a complete canonical key \
             (a CBOR array without null elements)"
        );
        Self {
            type_name: Some(type_name.into()),
            key,
            _marker: PhantomData,
        }
    }
}

impl<T: RefTarget + StructValue> InstanceRef<T> {
    /// Typed reference to `value` — its canonical key, with the type
    /// fixed by `T`. All of `value`'s key properties must be set.
    pub fn of(value: &T) -> Self {
        let key = crate::encode_key_bytes(value);
        debug_assert!(
            key_is_complete(&key),
            "an instance reference must carry a complete canonical key; \
             the referenced value has unset key properties"
        );
        Self {
            type_name: None,
            key,
            _marker: PhantomData,
        }
    }
}

impl<T: RefTarget> InstanceRef<T> {
    /// The referenced instance's DOTS type name.
    pub fn type_name(&self) -> &str {
        match &self.type_name {
            Some(name) => name,
            None => T::TARGET_NAME.unwrap_or(""),
        }
    }

    /// The canonical instance key — a complete CBOR array
    /// (wire-format spec §6.2).
    pub fn key_bytes(&self) -> &[u8] {
        &self.key
    }

    /// Encode this reference's wire form (spec §6.5): generic refs as
    /// `[type-name, key]`, typed refs as the bare canonical key.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf);
        buf
    }

    /// Append the wire form to an existing buffer.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        let mut encoder = minicbor::Encoder::new(buf);
        self.dots_encode(&mut encoder)
            .expect("Vec<u8> writes are infallible");
    }

    /// Decode one reference from `bytes`, rejecting trailing input.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut d = minicbor::Decoder::new(bytes);
        let decoded = Self::dots_decode(&mut d)?;
        if d.position() != bytes.len() {
            return Err(DecodeError::message(
                "trailing bytes after instance reference",
            ));
        }
        Ok(decoded)
    }

    /// Widen to the generic form, materializing the type name.
    pub fn into_any(self) -> InstanceRef<AnyInstance> {
        InstanceRef {
            type_name: Some(match self.type_name {
                Some(name) => name,
                None => T::TARGET_NAME.unwrap_or("").to_string(),
            }),
            key: self.key,
            _marker: PhantomData,
        }
    }
}

/// Generic reference to any DOTS value — model type `instance_ref`.
/// The type name comes from the value's descriptor, the key is its
/// canonical instance key. All key properties must be set.
pub fn instance_ref(value: &dyn StructValue) -> InstanceRef {
    InstanceRef::new(value.descriptor().name, crate::encode_key_bytes(value))
}

impl DynamicStruct {
    /// Generic reference to this instance — dynamic counterpart of
    /// [`instance_ref`].
    pub fn instance_ref(&self) -> InstanceRef {
        InstanceRef::new(self.descriptor.name.clone(), self.key_bytes())
    }
}

// ===== Codec =====

impl<T: RefTarget> DotsField for InstanceRef<T> {
    fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        let generic_name = T::TARGET_NAME.is_none().then(|| self.type_name());
        encode_ref(generic_name, &self.key, e)
    }

    fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
        let (type_name, key) = decode_ref(T::TARGET_NAME.is_none(), d)?;
        Ok(Self {
            type_name,
            key,
            _marker: PhantomData,
        })
    }
}

/// Encode a reference's wire form (spec §6.5). `generic_name` is
/// `Some(type name)` for the generic form — `[type-name, key]` — and
/// `None` for the typed form, which is the bare canonical key. The key
/// is already a complete CBOR item and is spliced in raw.
pub(crate) fn encode_ref(
    generic_name: Option<&str>,
    key: &[u8],
    e: &mut CborEncoder<'_>,
) -> Result<(), EncodeError> {
    if let Some(name) = generic_name {
        e.array(2)?;
        e.str(name)?;
    }
    e.writer_mut().extend_from_slice(key);
    Ok(())
}

/// Decode a reference's wire form, the shape selected by the *schema*
/// (`generic`), never by the bytes: the generic envelope carries its
/// type name, the typed form is the bare key. Returns
/// `(type name if generic, key bytes)`.
pub(crate) fn decode_ref(
    generic: bool,
    d: &mut CborDecoder<'_>,
) -> Result<(Option<String>, Vec<u8>), DecodeError> {
    let type_name = if generic {
        let len = d.array()?.ok_or_else(|| {
            DecodeError::message("indefinite-length array not supported in `instance_ref`")
        })?;
        if len != 2 {
            return Err(DecodeError::message(
                "invalid `instance_ref`: expected a 2-element array",
            ));
        }
        Some(d.str()?.to_string())
    } else {
        None
    };
    let key = read_key_array(d)?;
    Ok((type_name, key))
}

impl<T: RefTarget> DotsTypeKind for InstanceRef<T> {
    const KIND: FieldKind = match T::TARGET_NAME {
        Some(name) => FieldKind::InstanceRefTo(name),
        None => FieldKind::InstanceRef,
    };
}

/// Read one canonical-key array from the decoder, returning its raw
/// bytes (the complete CBOR item). The key is kept opaque — comparison
/// never needs its elements — but it must at least *be* an array.
pub(crate) fn read_key_array(d: &mut CborDecoder<'_>) -> Result<Vec<u8>, DecodeError> {
    if d.datatype()? != minicbor::data::Type::Array {
        return Err(DecodeError::message(
            "invalid instance reference: canonical key must be a CBOR array",
        ));
    }
    let start = d.position();
    d.skip()?;
    Ok(d.input()[start..d.position()].to_vec())
}

/// Whether `key` is a definite-length CBOR array without top-level
/// `null` elements — the completeness requirement on reference values
/// (spec §6.5). Used in debug assertions; malformed input yields
/// `false`.
fn key_is_complete(key: &[u8]) -> bool {
    let mut d = minicbor::Decoder::new(key);
    let Ok(Some(len)) = d.array() else {
        return false;
    };
    for _ in 0..len {
        match d.datatype() {
            Ok(minicbor::data::Type::Null) => return false,
            Ok(_) => {
                if d.skip().is_err() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
    d.position() == key.len()
}

// ===== Materialization =====

/// Decode a canonical key back into a partial [`DynamicStruct`] of
/// `descriptor`'s type, containing exactly the key properties
/// (wire-format spec §6.5: element *i* of the key corresponds to the
/// *i*-th key property in ascending tag order). A `null` element
/// leaves its property unset.
pub fn materialize_key(
    descriptor: &Arc<DynamicStructDescriptor>,
    key: &[u8],
) -> Result<DynamicStruct, DecodeError> {
    let mut d = minicbor::Decoder::new(key);
    let len = d.array()?.ok_or_else(|| {
        DecodeError::message("indefinite-length array not supported in a canonical key")
    })?;
    let key_mask = descriptor.key_mask();
    if len != key_mask.len() as u64 {
        return Err(DecodeError::message(
            "canonical key length does not match the type's key property count",
        ));
    }

    let mut valid = PropertySet::EMPTY;
    let mut properties = Vec::with_capacity(len as usize);
    for tag in key_mask.iter() {
        if d.datatype()? == minicbor::data::Type::Null {
            d.skip()?;
            continue;
        }
        let prop = descriptor
            .property(tag)
            .expect("tag comes from this descriptor's key mask");
        properties.push((tag, crate::dynamic::decode_value(&prop.kind, &mut d)?));
        valid = valid.with_tag(tag);
    }
    if d.position() != key.len() {
        return Err(DecodeError::message("trailing bytes after canonical key"));
    }

    Ok(DynamicStruct {
        descriptor: descriptor.clone(),
        valid,
        properties,
    })
}

// ===== Diagnostic rendering =====

impl<T: RefTarget> core::fmt::Display for InstanceRef<T> {
    /// Non-normative diagnostic form: the type name followed by the
    /// decoded key, e.g. `ExampleType["eth0",100]`. Not a parseable
    /// contract; malformed key bytes fall back to hex.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.type_name())?;
        fmt_key(&self.key, f)
    }
}

/// Render a canonical key in the non-normative diagnostic form —
/// `["eth0",100]` — or, on malformed bytes, as hex. Shared by
/// [`InstanceRef`]'s `Display`, the dynamic value's, and renderers
/// that have the target type name from the field kind.
pub fn fmt_key(key: &[u8], f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut d = minicbor::Decoder::new(key);
    match fmt_key_item(&mut d, f, 0) {
        Ok(()) if d.position() == key.len() => Ok(()),
        // The formatter may already hold partial output; hex after it
        // is still unambiguous for a diagnostic form.
        _ => {
            f.write_str("0x")?;
            for b in key {
                write!(f, "{b:02x}")?;
            }
            Ok(())
        }
    }
}

/// Deepest array nesting the diagnostic renderer follows. Canonical
/// keys nest only as deep as reference-typed key properties chain
/// (spec §6.3), so real keys are shallow; the cap keeps a hostile or
/// corrupt key from recursing the stack away — it falls back to hex.
const MAX_KEY_DISPLAY_DEPTH: u32 = 16;

fn fmt_key_item(
    d: &mut CborDecoder<'_>,
    f: &mut core::fmt::Formatter<'_>,
    depth: u32,
) -> Result<(), core::fmt::Error> {
    use minicbor::data::Type;
    // Any decode error is mapped to fmt::Error and handled by the
    // caller's hex fallback.
    let err = |_| core::fmt::Error;
    match d.datatype().map_err(err)? {
        Type::Array => {
            if depth >= MAX_KEY_DISPLAY_DEPTH {
                return Err(core::fmt::Error);
            }
            let len = d.array().map_err(err)?.ok_or(core::fmt::Error)?;
            f.write_str("[")?;
            for i in 0..len {
                if i > 0 {
                    f.write_str(",")?;
                }
                fmt_key_item(d, f, depth + 1)?;
            }
            f.write_str("]")
        }
        Type::Bool => write!(f, "{}", d.bool().map_err(err)?),
        Type::Null => {
            d.skip().map_err(err)?;
            f.write_str("null")
        }
        Type::String => write!(f, "{:?}", d.str().map_err(err)?),
        Type::Bytes => {
            f.write_str("0x")?;
            for b in d.bytes().map_err(err)? {
                write!(f, "{b:02x}")?;
            }
            Ok(())
        }
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => {
            write!(f, "{}", d.u64().map_err(err)?)
        }
        Type::I8 | Type::I16 | Type::I32 | Type::I64 => {
            write!(f, "{}", d.i64().map_err(err)?)
        }
        _ => Err(core::fmt::Error),
    }
}

#[cfg(test)]
mod tests {
    use alloc::{format, vec};

    use super::*;
    use crate::{DynamicFieldKind, DynamicPropertyDescriptor, DynamicValue, StructFlags};

    fn example_type() -> Arc<DynamicStructDescriptor> {
        // The spec §6.5/§6.6 `ExampleType`.
        Arc::new(DynamicStructDescriptor {
            name: "ExampleType".into(),
            flags: StructFlags::NONE,
            properties: vec![
                DynamicPropertyDescriptor {
                    name: "name".into(),
                    tag: 1,
                    is_key: true,
                    kind: DynamicFieldKind::String,
                },
                DynamicPropertyDescriptor {
                    name: "enabled".into(),
                    tag: 2,
                    is_key: false,
                    kind: DynamicFieldKind::Bool,
                },
            ],
        })
    }

    fn eth0() -> DynamicStruct {
        DynamicStruct {
            descriptor: example_type(),
            valid: PropertySet::EMPTY.with_tag(1).with_tag(2),
            properties: vec![
                (1, DynamicValue::String("eth0".into())),
                (2, DynamicValue::Bool(true)),
            ],
        }
    }

    /// Spec §6.6 golden vectors.
    #[test]
    fn golden_vectors() {
        let value = eth0();
        assert_eq!(value.key_bytes(), [0x81, 0x64, b'e', b't', b'h', b'0']);

        let generic = value.instance_ref();
        assert_eq!(generic.key_bytes(), [0x81, 0x64, b'e', b't', b'h', b'0']);

        let mut buf = Vec::new();
        let mut e: CborEncoder<'_> = minicbor::Encoder::new(&mut buf);
        generic.dots_encode(&mut e).unwrap();
        let expected: &[u8] = &[
            0x82, 0x6B, b'E', b'x', b'a', b'm', b'p', b'l', b'e', b'T', b'y', b'p', b'e', 0x81,
            0x64, b'e', b't', b'h', b'0',
        ];
        assert_eq!(buf, expected);
    }

    #[test]
    fn keyless_type_has_empty_key() {
        let desc = Arc::new(DynamicStructDescriptor {
            name: "TimeType".into(),
            flags: StructFlags::NONE,
            properties: vec![DynamicPropertyDescriptor {
                name: "tz".into(),
                tag: 1,
                is_key: false,
                kind: DynamicFieldKind::String,
            }],
        });
        let value = DynamicStruct {
            descriptor: desc,
            valid: PropertySet::EMPTY.with_tag(1),
            properties: vec![(1, DynamicValue::String("UTC".into()))],
        };
        assert_eq!(value.key_bytes(), [0x80]);
    }

    /// Composite key of the spec's `RouteType`, with the dynamic
    /// descriptor deliberately listing the key properties out of tag
    /// order — the canonical key must still come out ascending.
    #[test]
    fn composite_key_is_tag_ordered_even_with_out_of_order_descriptor() {
        let desc = Arc::new(DynamicStructDescriptor {
            name: "RouteType".into(),
            flags: StructFlags::NONE,
            properties: vec![
                DynamicPropertyDescriptor {
                    name: "name".into(),
                    tag: 2,
                    is_key: true,
                    kind: DynamicFieldKind::String,
                },
                DynamicPropertyDescriptor {
                    name: "parent".into(),
                    tag: 1,
                    is_key: true,
                    kind: DynamicFieldKind::String,
                },
            ],
        });
        let value = DynamicStruct {
            descriptor: desc,
            valid: PropertySet::EMPTY.with_tag(1).with_tag(2),
            properties: vec![
                (2, DynamicValue::String("r1".into())),
                (1, DynamicValue::String("eth0".into())),
            ],
        };
        assert_eq!(
            value.key_bytes(),
            [0x82, 0x64, b'e', b't', b'h', b'0', 0x62, b'r', b'1']
        );
    }

    #[test]
    fn generic_ref_round_trips() {
        let generic = eth0().instance_ref();
        let mut buf = Vec::new();
        let mut e: CborEncoder<'_> = minicbor::Encoder::new(&mut buf);
        generic.dots_encode(&mut e).unwrap();

        let mut d: CborDecoder<'_> = minicbor::Decoder::new(&buf);
        let decoded = InstanceRef::<AnyInstance>::dots_decode(&mut d).unwrap();
        assert_eq!(d.position(), buf.len());
        assert_eq!(decoded, generic);
        assert_eq!(decoded.type_name(), "ExampleType");
    }

    #[test]
    fn decode_rejects_malformed_refs() {
        // Not an array.
        assert!(decode_any(&[0x64, b'e', b't', b'h', b'0']).is_err());
        // Wrong arity.
        assert!(decode_any(&[0x81, 0x6B]).is_err());
        // Key element is not an array (a text string instead).
        assert!(decode_any(&[0x82, 0x61, b'T', 0x64, b'e', b't', b'h', b'0']).is_err());
    }

    fn decode_any(bytes: &[u8]) -> Result<InstanceRef, DecodeError> {
        let mut d: CborDecoder<'_> = minicbor::Decoder::new(bytes);
        InstanceRef::<AnyInstance>::dots_decode(&mut d)
    }

    #[test]
    fn materialize_key_recovers_key_properties() {
        let value = eth0();
        let materialized = materialize_key(&value.descriptor, &value.key_bytes()).unwrap();
        assert_eq!(
            materialized.get(1),
            Some(&DynamicValue::String("eth0".into()))
        );
        // Non-key properties are absent.
        assert_eq!(materialized.get(2), None);
        // Identity is preserved.
        assert!(materialized.same(&value));
    }

    #[test]
    fn materialize_key_rejects_wrong_arity_and_trailing_bytes() {
        let desc = example_type();
        assert!(materialize_key(&desc, &[0x80]).is_err()); // 0 of 1 keys
        assert!(materialize_key(&desc, &[0x81, 0x64, b'e', b't', b'h', b'0', 0x00]).is_err());
    }

    #[test]
    fn display_is_diagnostic() {
        assert_eq!(
            format!("{}", eth0().instance_ref()),
            r#"ExampleType["eth0"]"#
        );
        // Malformed key falls back to hex.
        let broken = InstanceRef {
            type_name: Some("X".into()),
            key: vec![0xFF, 0xFF],
            _marker: PhantomData::<AnyInstance>,
        };
        assert_eq!(format!("{broken}"), "X0xffff");
    }

    #[test]
    fn key_completeness_check() {
        assert!(key_is_complete(&[0x80]));
        assert!(key_is_complete(&[0x81, 0x64, b'e', b't', b'h', b'0']));
        assert!(!key_is_complete(&[0x81, 0xF6])); // null element
        assert!(!key_is_complete(&[0x64, b'e', b't', b'h', b'0'])); // not an array
        assert!(!key_is_complete(&[0x81])); // truncated
    }
}
