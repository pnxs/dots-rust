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
    DotsField, EnumDescriptor, FieldKind, Publishable, PropertySet, StructDescriptor, StructFlags,
    Transmittable,
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
    /// DOTS `property_set` — wire payload is the bitmask encoded by
    /// [`PropertySet`]'s `DotsField` impl. Kept distinct from the
    /// raw-integer kinds so the dynamic side doesn't bind to
    /// `PropertySet`'s current storage width.
    PropertySet,
    String,
    /// DOTS `uuid` — wire payload is a CBOR ByteString of exactly 16
    /// bytes. For arbitrary binary blobs use `Vec(Box::new(U8))`
    /// (DOTS `vector<uint8>`).
    Uuid,
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
            FieldKind::PropertySet => Self::PropertySet,
            FieldKind::String => Self::String,
            FieldKind::Uuid => Self::Uuid,
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
    /// DOTS `property_set` — wrapped [`PropertySet`] rather than its
    /// raw integer so consumers don't depend on the storage width.
    PropertySet(PropertySet),
    /// DOTS `uuid` — exactly 16 raw bytes. For arbitrary binary blobs
    /// use `Vec(Vec<DynamicValue::U8(_)>)` (DOTS `vector<uint8>`).
    Uuid([u8; 16]),
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
    ///
    /// Equivalent to building a `CborEncoder` over a fresh `Vec<u8>`
    /// and calling [`Transmittable::encode_into`] with the full
    /// `valid_set()`; offered as an inherent method since the
    /// majority of callers want the simple "give me the bytes" shape.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        let mut encoder = minicbor::Encoder::new(&mut buf);
        encode_struct(self, self.valid, &mut encoder).expect("Vec<u8> writes are infallible");
        buf
    }

    /// Encode the key-properties of this struct as a deterministic
    /// CBOR array — same shape as [`crate::encode_key_bytes`] for
    /// typed values, suitable as a map key for in-memory caches that
    /// need to dedupe instances by their declared key fields.
    ///
    /// Properties marked `#[dots(key)]` are emitted in declaration
    /// order; missing key fields are encoded as `null` so partial-key
    /// values stay distinguishable from values where the key is set
    /// to a default-encoded zero.
    pub fn key_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut encoder = minicbor::Encoder::new(&mut buf);
        let key_count = self
            .descriptor
            .properties
            .iter()
            .filter(|p| p.is_key)
            .count() as u64;
        encoder
            .array(key_count)
            .expect("Vec<u8> writes are infallible");
        for prop in &self.descriptor.properties {
            if !prop.is_key {
                continue;
            }
            let value = if self.valid.has(prop.tag) {
                self.properties
                    .iter()
                    .find(|(t, _)| *t == prop.tag)
                    .map(|(_, v)| v)
            } else {
                None
            };
            match value {
                Some(v) => encode_value(v, &mut encoder).expect("Vec<u8> writes are infallible"),
                None => {
                    encoder.null().expect("Vec<u8> writes are infallible");
                }
            }
        }
        buf
    }

    /// Project any `&dyn StructValue` (typed or `AnyStruct`) into a
    /// `DynamicStruct` — useful for the dynamic-subscriber path where
    /// the receiver wants the runtime-shaped view even though the
    /// wire-decode may have produced a layout-compatible
    /// [`crate::AnyStruct`].
    ///
    /// Goes through the descriptor-driven CBOR encode/decode pipeline
    /// because the wire-only representation has no shared bits with
    /// the typed layout — the tagged union must be rebuilt
    /// field-by-field. The cost is one CBOR encode + decode, which is
    /// the same overhead the receiver was already paying before the
    /// `Payload::Typed` optimization; this path is intentionally only
    /// used by the dynamic-subscriber shim, not by typed containers.
    pub fn from_struct_value(value: &dyn crate::StructValue) -> Self {
        let descriptor = Arc::new(DynamicStructDescriptor::from_static(value.descriptor()));
        let bytes = crate::layout::encode_to_vec(value);
        Self::decode(descriptor, &bytes).expect(
            "descriptor-driven encode + decode roundtrip must succeed: \
             same descriptor on both sides",
        )
    }

    /// Borrow this value as a [`Publishable`].
    ///
    /// Runtime-checks the descriptor's `substruct_only` flag — this is
    /// the dynamic counterpart of the compile-time guarantee that
    /// `#[derive(DotsStruct)]` provides for typed structs (the derive
    /// suppresses the `Publishable` impl when `substruct_only` is set,
    /// so the call site fails to compile).
    pub fn try_as_publishable(&self) -> Result<DynamicPublishable<'_>, NotPublishable> {
        if self.descriptor.flags.is_substruct_only() {
            Err(NotPublishable::SubstructOnly)
        } else {
            Ok(DynamicPublishable(self))
        }
    }
}

impl Transmittable for DynamicStruct {
    fn type_name(&self) -> &str {
        &self.descriptor.name
    }

    fn valid_set(&self) -> PropertySet {
        self.valid
    }

    fn key_set(&self) -> PropertySet {
        let mut set = PropertySet::EMPTY;
        for prop in &self.descriptor.properties {
            if prop.is_key {
                set = set.with_tag(prop.tag);
            }
        }
        set
    }

    fn encode_into(
        &self,
        mask: PropertySet,
        encoder: &mut CborEncoder<'_>,
    ) -> Result<(), EncodeError> {
        encode_struct(self, mask, encoder)
    }
}

/// Publishable view of a [`DynamicStruct`] obtained via
/// [`DynamicStruct::try_as_publishable`].
///
/// Holds a borrow of the underlying value and forwards every
/// [`Transmittable`] method to it. Implements [`Publishable`] so it
/// can be passed to the transport's `publish` / `publish_with_mask` /
/// `remove` methods.
///
/// `static_descriptor()` returns `None` — runtime-described values
/// have no compile-time descriptor, so the transport will not
/// auto-register the type with the broker. The caller is responsible
/// for ensuring the broker already knows about the descriptor (e.g.
/// because the descriptor was learned from the broker, or because the
/// caller has published a `StructDescriptorData` for it).
pub struct DynamicPublishable<'a>(&'a DynamicStruct);

impl<'a> DynamicPublishable<'a> {
    /// Borrow the underlying [`DynamicStruct`].
    pub fn as_struct(&self) -> &'a DynamicStruct {
        self.0
    }
}

impl<'a> Transmittable for DynamicPublishable<'a> {
    fn type_name(&self) -> &str {
        self.0.type_name()
    }

    fn valid_set(&self) -> PropertySet {
        self.0.valid_set()
    }

    fn key_set(&self) -> PropertySet {
        Transmittable::key_set(self.0)
    }

    fn encode_into(
        &self,
        mask: PropertySet,
        encoder: &mut CborEncoder<'_>,
    ) -> Result<(), EncodeError> {
        self.0.encode_into(mask, encoder)
    }
}

impl<'a> Publishable for DynamicPublishable<'a> {}

/// Runtime-described values have no static descriptor to link, so
/// they fall through to the default no-op `GlobalRegistration` impl.
impl<'a> crate::GlobalRegistration for DynamicPublishable<'a> {}

/// Reasons a [`DynamicStruct`] cannot be published as a top-level
/// instance. Returned by [`DynamicStruct::try_as_publishable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotPublishable {
    /// The struct's descriptor carries `substruct_only`, so values of
    /// this type may only appear nested inside another struct.
    SubstructOnly,
}

impl core::fmt::Display for NotPublishable {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::SubstructOnly => {
                f.write_str("struct is marked `substruct_only` and cannot be published")
            }
        }
    }
}

// ===== Encoding =====

fn encode_struct(
    s: &DynamicStruct,
    mask: PropertySet,
    e: &mut CborEncoder<'_>,
) -> Result<(), EncodeError> {
    let emit = s.valid & mask;
    e.map(u64::from(emit.len()))?;
    // Walk the descriptor's properties (ascending declaration order)
    // and emit only the ones present in both `valid` and `mask`. The
    // descriptor-order walk matches the static encoder so wire bytes
    // are identical.
    for prop in &s.descriptor.properties {
        if !emit.has(prop.tag) {
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
        DynamicValue::PropertySet(p) => p.dots_encode(e),
        DynamicValue::Uuid(b) => e.bytes(b).map(|_| ()),
        DynamicValue::Vec(items) => {
            e.array(items.len() as u64)?;
            for item in items {
                encode_value(item, e)?;
            }
            Ok(())
        }
        DynamicValue::Struct(inner) => encode_struct(inner, inner.valid, e),
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
        DynamicFieldKind::PropertySet => Ok(DynamicValue::PropertySet(PropertySet::dots_decode(d)?)),
        DynamicFieldKind::Uuid => {
            let bytes = d.bytes()?;
            let arr: [u8; 16] = bytes.try_into().map_err(|_| {
                DecodeError::message("uuid byte-string length mismatch")
            })?;
            Ok(DynamicValue::Uuid(arr))
        }
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

// ===== Display formatting =====
//
// Human-readable output for trace/inspection tools (mirrors the role
// of dots-cpp's StringSerializer for *printing*, but is NOT
// byte-compatible with it — that's a separate, larger piece of work).
// Format intent: `TypeName{ field: value, field: value }`, with
// strings debug-quoted, byte arrays as hex, and enum values resolved
// to variant names where the descriptor is reachable.

impl core::fmt::Display for DynamicValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DynamicValue::Bool(v) => write!(f, "{v}"),
            DynamicValue::U8(v) => write!(f, "{v}"),
            DynamicValue::U16(v) => write!(f, "{v}"),
            DynamicValue::U32(v) => write!(f, "{v}"),
            DynamicValue::U64(v) => write!(f, "{v}"),
            DynamicValue::I8(v) => write!(f, "{v}"),
            DynamicValue::I16(v) => write!(f, "{v}"),
            DynamicValue::I32(v) => write!(f, "{v}"),
            DynamicValue::I64(v) => write!(f, "{v}"),
            DynamicValue::F32(v) => write!(f, "{v}"),
            DynamicValue::F64(v) => write!(f, "{v}"),
            DynamicValue::String(s) => write!(f, "{s:?}"),
            DynamicValue::PropertySet(p) => write!(f, "{p:?}"),
            DynamicValue::Uuid(b) => {
                f.write_str("0x")?;
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            DynamicValue::Vec(items) => {
                f.write_str("[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    core::fmt::Display::fmt(item, f)?;
                }
                f.write_str("]")
            }
            DynamicValue::Struct(s) => core::fmt::Display::fmt(s.as_ref(), f),
            // Bare Display can't see the property's enum descriptor —
            // [`DynamicStruct`]'s impl resolves variant names where it
            // can. For standalone values we fall back to the int.
            DynamicValue::Enum(v) => write!(f, "{v}"),
            DynamicValue::Timepoint(s) => write!(f, "{s}"),
            DynamicValue::Duration(s) => write!(f, "{s}"),
        }
    }
}

impl core::fmt::Display for DynamicStruct {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.descriptor.name)?;
        f.write_str("{")?;
        let mut first = true;
        for prop in &self.descriptor.properties {
            if !self.valid.has(prop.tag) {
                continue;
            }
            if first {
                f.write_str(" ")?;
            } else {
                f.write_str(", ")?;
            }
            first = false;
            f.write_str(&prop.name)?;
            f.write_str(": ")?;

            let value = self
                .properties
                .iter()
                .find(|(t, _)| *t == prop.tag)
                .map(|(_, v)| v);

            match (value, &prop.kind) {
                (Some(DynamicValue::Enum(int_val)), DynamicFieldKind::Enum(enum_desc)) => {
                    match enum_desc.element_by_value(*int_val) {
                        Some(elem) => f.write_str(&elem.name)?,
                        None => write!(f, "{int_val}")?,
                    }
                }
                (Some(v), _) => core::fmt::Display::fmt(v, f)?,
                (None, _) => f.write_str("?")?,
            }
        }
        if !first {
            f.write_str(" ")?;
        }
        f.write_str("}")
    }
}

#[cfg(test)]
mod display_tests {
    use super::*;
    use alloc::format;

    fn pinger_descriptor() -> Arc<DynamicStructDescriptor> {
        Arc::new(DynamicStructDescriptor {
            name: "Pinger".into(),
            flags: StructFlags::NONE,
            properties: alloc::vec![
                DynamicPropertyDescriptor {
                    name: "id".into(),
                    tag: 1,
                    is_key: true,
                    kind: DynamicFieldKind::U32,
                },
                DynamicPropertyDescriptor {
                    name: "message".into(),
                    tag: 2,
                    is_key: false,
                    kind: DynamicFieldKind::String,
                },
                DynamicPropertyDescriptor {
                    name: "sequence".into(),
                    tag: 3,
                    is_key: false,
                    kind: DynamicFieldKind::U64,
                },
            ],
        })
    }

    #[test]
    fn display_shows_set_properties_in_descriptor_order() {
        let desc = pinger_descriptor();
        let value = DynamicStruct {
            descriptor: desc,
            valid: PropertySet::EMPTY.with_tag(1).with_tag(2).with_tag(3),
            properties: alloc::vec![
                (1, DynamicValue::U32(7)),
                (2, DynamicValue::String("hello".into())),
                (3, DynamicValue::U64(42)),
            ],
        };
        assert_eq!(
            format!("{value}"),
            r#"Pinger{ id: 7, message: "hello", sequence: 42 }"#
        );
    }

    #[test]
    fn display_omits_unset_properties() {
        let desc = pinger_descriptor();
        let value = DynamicStruct {
            descriptor: desc,
            valid: PropertySet::EMPTY.with_tag(1).with_tag(3),
            properties: alloc::vec![
                (1, DynamicValue::U32(99)),
                (3, DynamicValue::U64(1)),
            ],
        };
        assert_eq!(format!("{value}"), "Pinger{ id: 99, sequence: 1 }");
    }

    #[test]
    fn display_resolves_enum_variant_names() {
        let enum_desc = Arc::new(DynamicEnumDescriptor {
            name: "Mood".into(),
            elements: alloc::vec![
                DynamicEnumElement { name: "Happy".into(), tag: 1, value: 1 },
                DynamicEnumElement { name: "Grumpy".into(), tag: 2, value: 7 },
            ],
        });
        let desc = Arc::new(DynamicStructDescriptor {
            name: "Person".into(),
            flags: StructFlags::NONE,
            properties: alloc::vec![DynamicPropertyDescriptor {
                name: "mood".into(),
                tag: 1,
                is_key: false,
                kind: DynamicFieldKind::Enum(enum_desc),
            }],
        });
        let value = DynamicStruct {
            descriptor: desc,
            valid: PropertySet::EMPTY.with_tag(1),
            properties: alloc::vec![(1, DynamicValue::Enum(7))],
        };
        assert_eq!(format!("{value}"), "Person{ mood: Grumpy }");
    }

    #[test]
    fn display_handles_empty_struct() {
        let desc = pinger_descriptor();
        let value = DynamicStruct {
            descriptor: desc,
            valid: PropertySet::EMPTY,
            properties: alloc::vec![],
        };
        assert_eq!(format!("{value}"), "Pinger{}");
    }
}
