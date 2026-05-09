//! Wire-only descriptors and values.
//!
//! This module supports the case where a process needs to encode and
//! decode DOTS struct values whose Rust types it has *not* compiled
//! against — for example, `dotsd` routing user-defined types it has
//! only learned about over the wire via descriptor exchange.
//!
//! There are no compiled thunks here, no `&'static StructDescriptor`,
//! no layout-compatible memory. Just owned metadata
//! ([`DynamicStructDescriptor`]) and a tagged-union value
//! representation ([`DynamicValue`]). The wire format is identical to
//! the static path's: walk the descriptor's properties, dispatch by
//! [`DynamicFieldKind`] to a primitive CBOR encoder/decoder.
//!
//! Static descriptors can be projected into the dynamic shape via
//! [`DynamicStructDescriptor::from_static`] so the same descriptor
//! tree drives both paths during testing and migration.

use alloc::{
    boxed::Box,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};

use crate::{
    EnumDescriptor, FieldKind, PropertySet, StructDescriptor, StructFlags,
    layout::{CborDecoder, CborEncoder, DecodeError, EncodeError},
};

/// Owned, runtime-shaped variant of [`FieldKind`].
///
/// Whereas the static `FieldKind` references nested struct descriptors
/// via `&'static StructDescriptor`, this owns its nested descriptors
/// through `Arc<DynamicStructDescriptor>` so the whole tree is heap-
/// resident and detachable from any compiled-in static data.
#[derive(Debug, Clone)]
pub enum DynamicFieldKind {
    Bool,
    U8, U16, U32, U64,
    I8, I16, I32, I64,
    F32, F64,
    String,
    Bytes,
    Timepoint,
    Duration,
    Vec(Box<DynamicFieldKind>),
    Struct(Arc<DynamicStructDescriptor>),
    Enum(Arc<DynamicEnumDescriptor>),
}

/// Owned property metadata.
#[derive(Debug, Clone)]
pub struct DynamicPropertyDescriptor {
    pub name: String,
    pub tag: u32,
    pub is_key: bool,
    pub kind: DynamicFieldKind,
}

/// Owned struct metadata. Self-contained: nested struct fields hold
/// `Arc`s to their child descriptors, so a `DynamicStructDescriptor`
/// can travel between threads and outlive its source (a wire payload
/// or a static `StructDescriptor`).
#[derive(Debug, Clone)]
pub struct DynamicStructDescriptor {
    pub name: String,
    pub flags: StructFlags,
    pub properties: Vec<DynamicPropertyDescriptor>,
}

/// Owned enum element metadata.
#[derive(Debug, Clone)]
pub struct DynamicEnumElement {
    pub name: String,
    pub tag: u32,
    pub value: i32,
}

/// Owned enum metadata.
#[derive(Debug, Clone)]
pub struct DynamicEnumDescriptor {
    pub name: String,
    pub elements: Vec<DynamicEnumElement>,
}

impl DynamicEnumDescriptor {
    /// Look up a variant by its wire `int32` value.
    pub fn element_by_value(&self, value: i32) -> Option<&DynamicEnumElement> {
        self.elements.iter().find(|e| e.value == value)
    }

    /// Project a static enum descriptor into the dynamic shape.
    pub fn from_static(d: &'static EnumDescriptor) -> Self {
        Self {
            name: d.name.to_string(),
            elements: d
                .elements
                .iter()
                .map(|e| DynamicEnumElement {
                    name: e.name.to_string(),
                    tag: e.tag,
                    value: e.value,
                })
                .collect(),
        }
    }
}

impl DynamicStructDescriptor {
    /// Look up a property by tag. Linear scan; same trade-off as the
    /// static path.
    pub fn property(&self, tag: u32) -> Option<&DynamicPropertyDescriptor> {
        self.properties.iter().find(|p| p.tag == tag)
    }

    /// Project a static descriptor into the dynamic shape, recursively
    /// walking nested struct fields.
    ///
    /// Useful for tests (cross-roundtrip the same logical descriptor
    /// through both paths) and as a foundation for descriptor exchange:
    /// future `from_wire` constructors will produce the same shape from
    /// `StructDescriptorData` payloads.
    pub fn from_static(s: &'static StructDescriptor) -> Self {
        Self {
            name: s.name.to_string(),
            flags: s.flags,
            properties: s
                .properties
                .iter()
                .map(DynamicPropertyDescriptor::from_static)
                .collect(),
        }
    }
}

impl DynamicPropertyDescriptor {
    fn from_static(p: &'static crate::PropertyDescriptor) -> Self {
        Self {
            name: p.name.to_string(),
            tag: p.tag,
            is_key: p.is_key,
            kind: DynamicFieldKind::from_static(&p.kind),
        }
    }
}

impl DynamicFieldKind {
    fn from_static(k: &FieldKind) -> Self {
        match k {
            FieldKind::Bool => Self::Bool,
            FieldKind::U8 => Self::U8,
            FieldKind::U16 => Self::U16,
            FieldKind::U32 => Self::U32,
            FieldKind::U64 => Self::U64,
            FieldKind::I8 => Self::I8,
            FieldKind::I16 => Self::I16,
            FieldKind::I32 => Self::I32,
            FieldKind::I64 => Self::I64,
            FieldKind::F32 => Self::F32,
            FieldKind::F64 => Self::F64,
            FieldKind::String => Self::String,
            FieldKind::Bytes => Self::Bytes,
            FieldKind::Timepoint => Self::Timepoint,
            FieldKind::Duration => Self::Duration,
            FieldKind::Vec(inner) => Self::Vec(Box::new(Self::from_static(inner))),
            FieldKind::Struct(inner) => {
                Self::Struct(Arc::new(DynamicStructDescriptor::from_static(inner)))
            }
            FieldKind::Enum(inner) => {
                Self::Enum(Arc::new(DynamicEnumDescriptor::from_static(inner)))
            }
        }
    }
}

/// A wire-only value. Tagged union covering every primitive plus
/// arrays and nested structs.
#[derive(Debug, Clone, PartialEq)]
pub enum DynamicValue {
    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Vec(Vec<DynamicValue>),
    /// Nested struct value. Boxed to keep `DynamicValue`'s size bounded
    /// independently of `DynamicStruct`'s growth.
    Struct(Box<DynamicStruct>),
    /// DOTS enum value — the wire `int32`. The descriptor lives in the
    /// containing property's `DynamicFieldKind::Enum`, so consumers
    /// look up the element name by walking back to the descriptor.
    Enum(i32),
    /// Wall-clock timestamp (fractional seconds since Unix epoch).
    Timepoint(f64),
    /// Fractional-second duration.
    Duration(f64),
}

/// A wire-only struct value: descriptor + sparse property map.
///
/// `properties` is kept in *descriptor order* (matching
/// `descriptor.properties`) so encoding produces a deterministic CBOR
/// map keyed by ascending tag. The `valid` `PropertySet` is the
/// authoritative "which fields are present" view.
#[derive(Debug, Clone)]
pub struct DynamicStruct {
    pub descriptor: Arc<DynamicStructDescriptor>,
    pub valid: PropertySet,
    pub properties: Vec<(u32, DynamicValue)>,
}

impl PartialEq for DynamicStruct {
    /// Compares descriptors by `Arc` pointer identity, then valid set,
    /// then properties. Same-descriptor struct values compare on their
    /// data alone; values built from different descriptor instances
    /// (even if structurally identical) compare unequal.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.descriptor, &other.descriptor)
            && self.valid == other.valid
            && self.properties == other.properties
    }
}

impl DynamicStruct {
    /// Decode a wire-only struct from CBOR bytes.
    pub fn decode(
        descriptor: Arc<DynamicStructDescriptor>,
        bytes: &[u8],
    ) -> Result<Self, DecodeError> {
        let mut decoder = minicbor::Decoder::new(bytes);
        Self::decode_from_decoder(descriptor, &mut decoder)
    }

    /// Decode a wire-only struct from an active decoder. Useful when
    /// reading a stream of CBOR items and needing to track consumed
    /// bytes via `decoder.position()`.
    pub fn decode_from_decoder(
        descriptor: Arc<DynamicStructDescriptor>,
        decoder: &mut CborDecoder<'_>,
    ) -> Result<Self, DecodeError> {
        decode_struct(&descriptor, decoder).map(|(valid, properties)| Self {
            descriptor,
            valid,
            properties,
        })
    }

    /// Encode this value to a freshly allocated `Vec<u8>`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        self.encode_into(&mut buf);
        buf
    }

    /// Append the encoded form of this value to an existing `Vec<u8>`.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        let mut encoder = minicbor::Encoder::new(buf);
        encode_struct(self, &mut encoder).expect("Vec<u8> writes are infallible");
    }

    /// Encode directly into an active CBOR encoder. Used by the framing
    /// layer to assemble header + payload into a single buffer.
    pub fn encode_into_encoder(&self, encoder: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        encode_struct(self, encoder)
    }
}

// ===== Encoding =====

fn encode_struct(s: &DynamicStruct, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
    e.map(u64::from(s.valid.len()))?;
    // Walk the descriptor's properties (ascending declaration order)
    // and emit only the ones that are present in `valid`. This matches
    // the static encoder's ordering so wire bytes are identical.
    for prop in &s.descriptor.properties {
        if !s.valid.has(prop.tag) {
            continue;
        }
        let value = s
            .properties
            .iter()
            .find(|(t, _)| *t == prop.tag)
            .map(|(_, v)| v)
            .expect("valid_set claims tag is set but no value present");
        e.u32(prop.tag)?;
        encode_value(value, e)?;
    }
    Ok(())
}

fn encode_value(value: &DynamicValue, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
    match value {
        DynamicValue::Bool(v) => e.bool(*v).map(|_| ()),
        DynamicValue::U8(v) => e.u8(*v).map(|_| ()),
        DynamicValue::U16(v) => e.u16(*v).map(|_| ()),
        DynamicValue::U32(v) => e.u32(*v).map(|_| ()),
        DynamicValue::U64(v) => e.u64(*v).map(|_| ()),
        DynamicValue::I8(v) => e.i8(*v).map(|_| ()),
        DynamicValue::I16(v) => e.i16(*v).map(|_| ()),
        DynamicValue::I32(v) => e.i32(*v).map(|_| ()),
        DynamicValue::I64(v) => e.i64(*v).map(|_| ()),
        DynamicValue::F32(v) => e.f32(*v).map(|_| ()),
        DynamicValue::F64(v) => e.f64(*v).map(|_| ()),
        DynamicValue::String(s) => e.str(s).map(|_| ()),
        DynamicValue::Bytes(b) => e.bytes(b).map(|_| ()),
        DynamicValue::Vec(items) => {
            e.array(items.len() as u64)?;
            for item in items {
                encode_value(item, e)?;
            }
            Ok(())
        }
        DynamicValue::Struct(inner) => encode_struct(inner, e),
        DynamicValue::Enum(v) => e.i32(*v).map(|_| ()),
        DynamicValue::Timepoint(v) | DynamicValue::Duration(v) => e.f64(*v).map(|_| ()),
    }
}

// ===== Decoding =====

fn decode_struct(
    descriptor: &Arc<DynamicStructDescriptor>,
    d: &mut CborDecoder<'_>,
) -> Result<(PropertySet, Vec<(u32, DynamicValue)>), DecodeError> {
    let len = d.map()?.ok_or_else(|| {
        DecodeError::message("indefinite-length maps are not supported in DOTS structs")
    })?;
    let mut valid = PropertySet::EMPTY;
    let mut properties = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let tag = d.u32()?;
        match descriptor.property(tag) {
            Some(prop) => {
                let value = decode_value(&prop.kind, d)?;
                properties.push((tag, value));
                valid = valid.with_tag(tag);
            }
            None => {
                // Forward-compat: descriptor doesn't list this tag.
                d.skip()?;
            }
        }
    }
    Ok((valid, properties))
}

fn decode_value(
    kind: &DynamicFieldKind,
    d: &mut CborDecoder<'_>,
) -> Result<DynamicValue, DecodeError> {
    match kind {
        DynamicFieldKind::Bool => Ok(DynamicValue::Bool(d.bool()?)),
        DynamicFieldKind::U8 => Ok(DynamicValue::U8(d.u8()?)),
        DynamicFieldKind::U16 => Ok(DynamicValue::U16(d.u16()?)),
        DynamicFieldKind::U32 => Ok(DynamicValue::U32(d.u32()?)),
        DynamicFieldKind::U64 => Ok(DynamicValue::U64(d.u64()?)),
        DynamicFieldKind::I8 => Ok(DynamicValue::I8(d.i8()?)),
        DynamicFieldKind::I16 => Ok(DynamicValue::I16(d.i16()?)),
        DynamicFieldKind::I32 => Ok(DynamicValue::I32(d.i32()?)),
        DynamicFieldKind::I64 => Ok(DynamicValue::I64(d.i64()?)),
        DynamicFieldKind::F32 => Ok(DynamicValue::F32(d.f32()?)),
        DynamicFieldKind::F64 => Ok(DynamicValue::F64(d.f64()?)),
        DynamicFieldKind::String => Ok(DynamicValue::String(d.str()?.to_string())),
        DynamicFieldKind::Bytes => Ok(DynamicValue::Bytes(d.bytes()?.to_vec())),
        DynamicFieldKind::Vec(inner) => {
            let len = d.array()?.ok_or_else(|| {
                DecodeError::message("indefinite-length arrays are not supported")
            })?;
            let mut items = Vec::with_capacity(len as usize);
            for _ in 0..len {
                items.push(decode_value(inner, d)?);
            }
            Ok(DynamicValue::Vec(items))
        }
        DynamicFieldKind::Struct(child_desc) => {
            let (valid, properties) = decode_struct(child_desc, d)?;
            Ok(DynamicValue::Struct(Box::new(DynamicStruct {
                descriptor: child_desc.clone(),
                valid,
                properties,
            })))
        }
        DynamicFieldKind::Enum(_) => Ok(DynamicValue::Enum(d.i32()?)),
        DynamicFieldKind::Timepoint => Ok(DynamicValue::Timepoint(d.f64()?)),
        DynamicFieldKind::Duration => Ok(DynamicValue::Duration(d.f64()?)),
    }
}
