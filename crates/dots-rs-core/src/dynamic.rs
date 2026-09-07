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
    /// Open `any` — an opaque, self-describing object envelope. The
    /// contained type isn't known from the schema; the value carries
    /// its own type identity. See [`crate::AnyObject`].
    Any,
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
    /// Look up a variant by its `int32` metadata value.
    pub fn element_by_value(&self, value: i32) -> Option<&DynamicEnumElement> {
        self.elements.iter().find(|e| e.value == value)
    }

    /// Look up a variant by metadata tag (the wire encoding).
    pub fn element_by_tag(&self, tag: u32) -> Option<&DynamicEnumElement> {
        self.elements.iter().find(|e| e.tag == tag)
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

    /// Bitmask of all `is_key` property tags — dynamic counterpart of
    /// [`StructDescriptor::key_mask`].
    pub fn key_mask(&self) -> PropertySet {
        let mut set = PropertySet::EMPTY;
        for prop in &self.properties {
            if prop.is_key {
                set = set.with_tag(prop.tag);
            }
        }
        set
    }

    /// Bitmask of *all* property tags — dynamic counterpart of
    /// [`StructDescriptor::property_mask`].
    pub fn property_mask(&self) -> PropertySet {
        let mut set = PropertySet::EMPTY;
        for prop in &self.properties {
            set = set.with_tag(prop.tag);
        }
        set
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
            FieldKind::Any => Self::Any,
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
    /// DOTS enum value — the wire `u32` enumerator *tag* (dots-cpp
    /// convention; the `int32` `value` is descriptor metadata and is
    /// never transmitted). The descriptor lives in the containing
    /// property's `DynamicFieldKind::Enum`, so consumers look up the
    /// element name/value by walking back to the descriptor.
    Enum(u32),
    /// Wall-clock timestamp (fractional seconds since Unix epoch).
    Timepoint(f64),
    /// Fractional-second duration.
    Duration(f64),
    /// Open `any` value — the decoded `[type-name, payload]` envelope,
    /// kept opaque. The inner object is *not* decoded here; recovering
    /// it is a separate, registry-resolved step.
    Any(crate::AnyObject),
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
    /// Compares descriptors by `Arc` pointer identity, then property-
    /// wise validity and values. Same-descriptor struct values compare
    /// on their data alone; values built from different descriptor
    /// instances (even if structurally identical) compare unequal.
    ///
    /// Value comparison uses the same total-order semantics as the
    /// static path ([`crate::layout::equal`]): floats compare via
    /// `total_cmp`, so a NaN equals a bit-identical NaN.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.descriptor, &other.descriptor)
            && self.equal(other, PropertySet::ALL)
    }
}

/// Total-order comparison of two same-kind [`DynamicValue`]s.
///
/// `kind` supplies the schema context the value representation lacks —
/// most importantly the enum descriptor, so enums order by their
/// `int32` `value` (matching the static path and dots-cpp) rather than
/// by wire tag. Floats compare via `total_cmp`, structs property-wise
/// in descriptor order, vectors lexicographically.
///
/// Panics when the two values are of different variants — same-kind
/// values decoded through the same descriptor can't disagree, so a
/// mismatch is a caller bug.
pub fn value_cmp(
    kind: &DynamicFieldKind,
    a: &DynamicValue,
    b: &DynamicValue,
) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    use crate::DotsFieldOps;
    match (a, b) {
        (DynamicValue::Bool(x), DynamicValue::Bool(y)) => x.cmp(y),
        (DynamicValue::U8(x), DynamicValue::U8(y)) => x.cmp(y),
        (DynamicValue::U16(x), DynamicValue::U16(y)) => x.cmp(y),
        (DynamicValue::U32(x), DynamicValue::U32(y)) => x.cmp(y),
        (DynamicValue::U64(x), DynamicValue::U64(y)) => x.cmp(y),
        (DynamicValue::I8(x), DynamicValue::I8(y)) => x.cmp(y),
        (DynamicValue::I16(x), DynamicValue::I16(y)) => x.cmp(y),
        (DynamicValue::I32(x), DynamicValue::I32(y)) => x.cmp(y),
        (DynamicValue::I64(x), DynamicValue::I64(y)) => x.cmp(y),
        (DynamicValue::F32(x), DynamicValue::F32(y)) => x.total_cmp(y),
        (DynamicValue::F64(x), DynamicValue::F64(y)) => x.total_cmp(y),
        (DynamicValue::String(x), DynamicValue::String(y)) => x.cmp(y),
        (DynamicValue::PropertySet(x), DynamicValue::PropertySet(y)) => x.dots_cmp(y),
        (DynamicValue::Uuid(x), DynamicValue::Uuid(y)) => x.cmp(y),
        (DynamicValue::Timepoint(x), DynamicValue::Timepoint(y)) => x.total_cmp(y),
        (DynamicValue::Duration(x), DynamicValue::Duration(y)) => x.total_cmp(y),
        (DynamicValue::Enum(x), DynamicValue::Enum(y)) => {
            // Order by the enumerator's `int32` value, like the static
            // path. Tags the descriptor doesn't know fall back to tag
            // order so the comparison stays total.
            if let DynamicFieldKind::Enum(descriptor) = kind {
                let vx = descriptor.element_by_tag(*x).map(|e| e.value);
                let vy = descriptor.element_by_tag(*y).map(|e| e.value);
                if let (Some(vx), Some(vy)) = (vx, vy) {
                    return vx.cmp(&vy);
                }
            }
            x.cmp(y)
        }
        (DynamicValue::Vec(xs), DynamicValue::Vec(ys)) => {
            let elem_kind = match kind {
                DynamicFieldKind::Vec(inner) => inner,
                _ => panic!("vector value with non-vector field kind"),
            };
            for (x, y) in xs.iter().zip(ys) {
                match value_cmp(elem_kind, x, y) {
                    Ordering::Equal => continue,
                    non_eq => return non_eq,
                }
            }
            xs.len().cmp(&ys.len())
        }
        (DynamicValue::Struct(x), DynamicValue::Struct(y)) => x.cmp_masked(y, PropertySet::ALL),
        (DynamicValue::Any(x), DynamicValue::Any(y)) => x.dots_cmp(y),
        _ => panic!("DynamicValue kind mismatch in value_cmp"),
    }
}

/// Equality companion of [`value_cmp`] — `Ordering::Equal` ⇔ equal,
/// except that nested struct values built from *different* descriptor
/// `Arc`s compare unequal here (where [`value_cmp`] would panic), so
/// equality never panics on structurally-odd but well-typed data.
pub fn value_eq(kind: &DynamicFieldKind, a: &DynamicValue, b: &DynamicValue) -> bool {
    if let (DynamicValue::Struct(x), DynamicValue::Struct(y)) = (a, b) {
        return Arc::ptr_eq(&x.descriptor, &y.descriptor) && x.equal(y, PropertySet::ALL);
    }
    value_cmp(kind, a, b) == core::cmp::Ordering::Equal
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

    /// Look up the value stored for `tag`, if that property is set.
    pub fn get(&self, tag: u32) -> Option<&DynamicValue> {
        self.properties
            .iter()
            .find(|(t, _)| *t == tag)
            .map(|(_, v)| v)
    }

    /// Flat masked mirror of `src` onto `self` — dots-cpp `_copy`
    /// semantics restricted to non-key properties, mirroring
    /// [`AnyStruct::copy_from`](crate::AnyStruct::copy_from) — the
    /// partial-update operation the cache performs on every non-create
    /// transmission.
    ///
    /// For each **non-key** property whose tag is in `mask`: present in
    /// `src` → its value is cloned into `self`; absent in `src` →
    /// cleared in `self` (this is how a publish invalidates a single
    /// field). Properties whose tag is **outside** `mask`, and all key
    /// properties, are left as-is. The overlay is flat — a
    /// nested-struct property is replaced wholesale, matching dots-cpp's
    /// `Container::updateWithoutKeys`. (The recursive, non-destructive
    /// overlay is [`merge_from`](Self::merge_from).)
    pub fn copy_from(&mut self, src: &DynamicStruct, mask: PropertySet) {
        self.copy_impl(mask, |tag| {
            src.valid
                .has(tag)
                .then(|| src.properties.iter().find(|(t, _)| *t == tag).map(|(_, v)| v.clone()))
                .flatten()
        });
    }

    /// Like [`copy_from`](Self::copy_from), but **moves** each set
    /// property out of `src` instead of cloning it — `src`'s owned data
    /// is handed over. After the call the moved-out slots of `src` are
    /// gone and its `valid` set is updated, so `src` stays consistent.
    /// Use this on the cache update path when the incoming value is
    /// owned and about to be dropped.
    pub fn copy_take(&mut self, src: &mut DynamicStruct, mask: PropertySet) {
        self.copy_impl(mask, |tag| {
            if src.valid.has(tag) {
                src.valid = src.valid.without_tag(tag);
                src.properties
                    .iter()
                    .position(|(t, _)| *t == tag)
                    .map(|pos| src.properties.swap_remove(pos).1)
            } else {
                None
            }
        });
    }

    /// Shared core of [`copy_from`]/[`copy_take`]. `take` yields the
    /// source value for a masked tag (cloned or moved); returning
    /// `None` for a tag in the mask means "clear it". Rebuilds
    /// `self.properties` in descriptor order so the encode stays
    /// deterministic.
    fn copy_impl(&mut self, mask: PropertySet, mut take: impl FnMut(u32) -> Option<DynamicValue>) {
        // Clone the `Arc` so iterating the descriptor doesn't borrow
        // `self` while we mutate `self.properties`.
        let descriptor = self.descriptor.clone();
        let mut merged: Vec<(u32, DynamicValue)> = Vec::with_capacity(self.properties.len());
        let mut valid = PropertySet::EMPTY;
        for prop in &descriptor.properties {
            let tag = prop.tag;
            if !prop.is_key && mask.has(tag) {
                // In the mask: take from the source, or clear (skip).
                if let Some(v) = take(tag) {
                    merged.push((tag, v));
                    valid = valid.with_tag(tag);
                }
            } else if self.valid.has(tag) {
                // Out of mask (or a key): keep self's current value.
                if let Some(pos) = self.properties.iter().position(|(t, _)| *t == tag) {
                    merged.push((tag, self.properties.swap_remove(pos).1));
                    valid = valid.with_tag(tag);
                }
            }
        }
        self.properties = merged;
        self.valid = valid;
    }

    /// Masked property-wise equality — dots-cpp `Struct::_equal`,
    /// mirroring [`crate::layout::equal`] on the static path. A
    /// property in `mask` is equal when unset on both sides, or set on
    /// both with equal values (floats via `total_cmp`). Panics when
    /// the descriptors aren't the same `Arc`.
    pub fn equal(&self, other: &DynamicStruct, mask: PropertySet) -> bool {
        self.assert_shared_descriptor(other);
        for prop in &self.descriptor.properties {
            if !mask.has(prop.tag) {
                continue;
            }
            match (self.get(prop.tag), other.get(prop.tag)) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    if !value_eq(&prop.kind, a, b) {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }

    /// Key-property equality — dots-cpp `Struct::_same`: do the two
    /// values identify the same instance? Panics when the descriptors
    /// aren't the same `Arc`.
    pub fn same(&self, other: &DynamicStruct) -> bool {
        self.equal(other, self.descriptor.key_mask())
    }

    /// Masked total order — lexicographic over the properties in
    /// `mask` in descriptor order, unset before set, mirroring
    /// [`crate::layout::cmp_masked`]. Panics when the descriptors
    /// aren't the same `Arc`.
    pub fn cmp_masked(&self, other: &DynamicStruct, mask: PropertySet) -> core::cmp::Ordering {
        use core::cmp::Ordering;
        self.assert_shared_descriptor(other);
        for prop in &self.descriptor.properties {
            if !mask.has(prop.tag) {
                continue;
            }
            let ord = match (self.get(prop.tag), other.get(prop.tag)) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(a), Some(b)) => value_cmp(&prop.kind, a, b),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    /// The set of properties on which the two values differ — dots-cpp
    /// `Struct::_diffProperties`, mirroring
    /// [`crate::layout::diff_properties`]. A tag is in the result when
    /// it is set on exactly one side or set on both with unequal
    /// values. Panics when the descriptors aren't the same `Arc`.
    pub fn diff_properties(&self, other: &DynamicStruct, mask: PropertySet) -> PropertySet {
        self.assert_shared_descriptor(other);
        let mut diff = (self.valid ^ other.valid) & mask;
        for prop in &self.descriptor.properties {
            if !((self.valid & other.valid) & mask).has(prop.tag) {
                continue;
            }
            if let (Some(a), Some(b)) = (self.get(prop.tag), other.get(prop.tag)) {
                if !value_eq(&prop.kind, a, b) {
                    diff = diff.with_tag(prop.tag);
                }
            }
        }
        diff
    }

    /// True when every property named in `mask` is set — dots-cpp
    /// `Struct::_hasProperties` (subset flavor).
    pub fn has_properties(&self, mask: PropertySet) -> bool {
        mask.is_subset_of(self.valid)
    }

    /// Validate that every property named in `mask` is set, returning
    /// a [`MissingProperties`](crate::layout::MissingProperties) error
    /// naming the absent ones — mirrors
    /// [`crate::layout::expect_properties`].
    pub fn expect_properties(
        &self,
        mask: PropertySet,
    ) -> Result<(), crate::layout::MissingProperties> {
        let missing = mask - self.valid;
        if missing.is_empty() {
            return Ok(());
        }
        Err(crate::layout::MissingProperties {
            type_name: self.descriptor.name.clone(),
            missing: self
                .descriptor
                .properties
                .iter()
                .filter(|p| missing.has(p.tag))
                .map(|p| p.name.clone())
                .collect(),
        })
    }

    /// Make `self` an exact masked copy of `src` — dots-cpp `_assign`
    /// semantics, minus keys: every **non-key** property in `mask`
    /// that is set on `src` is cloned in; every other non-key property
    /// is cleared, including those outside `mask`. Key properties are
    /// never modified. Panics when the descriptors aren't the same
    /// `Arc`.
    pub fn assign_from(&mut self, src: &DynamicStruct, mask: PropertySet) {
        self.assert_shared_descriptor(src);
        let effective = mask & src.valid;
        self.copy_impl(PropertySet::ALL, |tag| {
            effective.has(tag).then(|| src.get(tag).cloned()).flatten()
        });
    }

    /// Like [`assign_from`](Self::assign_from), but **moves** each
    /// assigned property out of `src`. Panics when the descriptors
    /// aren't the same `Arc`.
    pub fn assign_take(&mut self, src: &mut DynamicStruct, mask: PropertySet) {
        self.assert_shared_descriptor(src);
        self.copy_impl(PropertySet::ALL, |tag| {
            if mask.has(tag) && src.valid.has(tag) {
                src.valid = src.valid.without_tag(tag);
                src.properties
                    .iter()
                    .position(|(t, _)| *t == tag)
                    .map(|pos| src.properties.swap_remove(pos).1)
            } else {
                None
            }
        });
    }

    /// Non-destructive recursive overlay of `src` onto `self` —
    /// dots-cpp `Struct::_merge` semantics, minus keys, mirroring
    /// [`AnyStruct::merge_from`](crate::AnyStruct::merge_from).
    ///
    /// Only **non-key** properties in `mask` that are set on `src` are
    /// touched; nothing is ever cleared. A nested-struct property
    /// present on both sides is merged recursively (fields the source
    /// sub-struct doesn't carry survive); everything else is replaced
    /// wholesale. Panics when the descriptors aren't the same `Arc`.
    pub fn merge_from(&mut self, src: &DynamicStruct, mask: PropertySet) {
        self.assert_shared_descriptor(src);
        let descriptor = self.descriptor.clone();
        for prop in &descriptor.properties {
            if prop.is_key || !mask.has(prop.tag) {
                continue;
            }
            let Some(src_val) = src.get(prop.tag) else {
                continue;
            };
            let merged = match (self.take_value(prop.tag), src_val) {
                (Some(DynamicValue::Struct(mut dst_sub)), DynamicValue::Struct(src_sub)) => {
                    dst_sub.merge_from(src_sub, PropertySet::ALL);
                    DynamicValue::Struct(dst_sub)
                }
                _ => src_val.clone(),
            };
            self.insert_value(prop.tag, merged);
        }
    }

    /// Swap the masked **non-key** properties of `self` and `other` —
    /// dots-cpp `Struct::_swap`, minus keys. Panics when the
    /// descriptors aren't the same `Arc`.
    pub fn swap(&mut self, other: &mut DynamicStruct, mask: PropertySet) {
        self.assert_shared_descriptor(other);
        let descriptor = self.descriptor.clone();
        for prop in &descriptor.properties {
            if prop.is_key || !mask.has(prop.tag) {
                continue;
            }
            let mine = self.take_value(prop.tag);
            let theirs = other.take_value(prop.tag);
            if let Some(v) = theirs {
                self.insert_value(prop.tag, v);
            }
            if let Some(v) = mine {
                other.insert_value(prop.tag, v);
            }
        }
    }

    /// Clear the masked **non-key** properties — dots-cpp
    /// `Struct::_clear`, minus keys.
    pub fn clear(&mut self, mask: PropertySet) {
        let key_mask = self.descriptor.key_mask();
        let cleared = (mask - key_mask) & self.valid;
        self.properties.retain(|(tag, _)| !cleared.has(*tag));
        self.valid = self.valid - cleared;
    }

    fn assert_shared_descriptor(&self, other: &DynamicStruct) {
        assert!(
            Arc::ptr_eq(&self.descriptor, &other.descriptor),
            "struct value operation requires matching descriptors: `{}` vs `{}`",
            self.descriptor.name,
            other.descriptor.name,
        );
    }

    /// Remove and return the value stored for `tag`, clearing its
    /// valid bit. Internal helper for the mutating operations.
    fn take_value(&mut self, tag: u32) -> Option<DynamicValue> {
        let pos = self.properties.iter().position(|(t, _)| *t == tag)?;
        self.valid = self.valid.without_tag(tag);
        Some(self.properties.remove(pos).1)
    }

    /// Insert `value` for `tag`, keeping `self.properties` in
    /// descriptor order and setting the valid bit. Internal helper for
    /// the mutating operations.
    fn insert_value(&mut self, tag: u32, value: DynamicValue) {
        debug_assert!(self.get(tag).is_none(), "insert_value on an occupied tag");
        // Descriptor order, not tag order: find where `tag` sits in
        // the descriptor's property list and insert before the first
        // stored property that comes later.
        let order_of = |t: u32| {
            self.descriptor
                .properties
                .iter()
                .position(|p| p.tag == t)
                .unwrap_or(usize::MAX)
        };
        let my_order = order_of(tag);
        let pos = self
            .properties
            .iter()
            .position(|(t, _)| order_of(*t) > my_order)
            .unwrap_or(self.properties.len());
        self.properties.insert(pos, (tag, value));
        self.valid = self.valid.with_tag(tag);
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
        DynamicValue::Enum(v) => e.u32(*v).map(|_| ()),
        DynamicValue::Timepoint(v) | DynamicValue::Duration(v) => e.f64(*v).map(|_| ()),
        // Opaque passthrough: re-emit the `[type-name, payload]`
        // envelope verbatim without touching the inner object.
        DynamicValue::Any(a) => a.dots_encode(e),
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
        DynamicFieldKind::Enum(desc) => {
            // The wire carries the enumerator tag; reject tags the
            // descriptor doesn't list, mirroring both the static path
            // and dots-cpp's `enumeratorFromTag` (which throws).
            let tag = d.u32()?;
            if desc.element_by_tag(tag).is_none() {
                return Err(DecodeError::message("unknown DOTS enum tag"));
            }
            Ok(DynamicValue::Enum(tag))
        }
        DynamicFieldKind::Timepoint => Ok(DynamicValue::Timepoint(d.f64()?)),
        DynamicFieldKind::Duration => Ok(DynamicValue::Duration(d.f64()?)),
        // Read the opaque envelope into an `AnyObject` without decoding
        // the contained object (the descriptor for it may not be known).
        DynamicFieldKind::Any => Ok(DynamicValue::Any(crate::AnyObject::dots_decode(d)?)),
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
            // Same text form as `Timepoint`/`Duration` themselves, so
            // it reads back through
            // [`DynamicFieldKind::parse_value`](crate::DynamicFieldKind::parse_value).
            DynamicValue::Timepoint(s) => f.write_str(&crate::text::format_timepoint(*s)),
            DynamicValue::Duration(s) => write!(f, "{s}s"),
            // Opaque: show the type identity and payload size, not the
            // contained object (decoding it needs a registry).
            DynamicValue::Any(a) => {
                write!(f, "any<{}>[{} bytes]", a.type_name(), a.payload().len())
            }
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
                (Some(DynamicValue::Enum(tag)), DynamicFieldKind::Enum(enum_desc)) => {
                    match enum_desc.element_by_tag(*tag) {
                        Some(elem) => f.write_str(&elem.name)?,
                        None => write!(f, "{tag}")?,
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
                DynamicEnumElement { name: "Happy".into(), tag: 1, value: 0 },
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
            // Enum values are stored/transmitted as the enumerator tag
            // (2 = Grumpy), not the metadata value (7).
            properties: alloc::vec![(1, DynamicValue::Enum(2))],
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

/// Key identity for wire-only values.
///
/// Ports the key halves of dots-cpp `TestStaticStruct._KeyProperties` /
/// `TestDynamicStruct._keyProperties` (the key set is exactly the
/// `key`-marked tags) and `TestStaticStruct.same` /
/// `TestDynamicStruct.same` (two instances are "the same" iff their
/// key properties are equal, regardless of non-key differences —
/// here surfaced via `key_bytes()` equality).
#[cfg(test)]
mod key_identity_tests {
    use super::*;

    /// A descriptor with two keys (tags 1, 2) and one non-key (tag 3),
    /// so "exactly the key tags" is distinguishable from "first tag" or
    /// "all tags".
    fn record_descriptor() -> Arc<DynamicStructDescriptor> {
        Arc::new(DynamicStructDescriptor {
            name: "Record".into(),
            flags: StructFlags::NONE,
            properties: alloc::vec![
                DynamicPropertyDescriptor {
                    name: "realm".into(),
                    tag: 1,
                    is_key: true,
                    kind: DynamicFieldKind::U32,
                },
                DynamicPropertyDescriptor {
                    name: "id".into(),
                    tag: 2,
                    is_key: true,
                    kind: DynamicFieldKind::U64,
                },
                DynamicPropertyDescriptor {
                    name: "label".into(),
                    tag: 3,
                    is_key: false,
                    kind: DynamicFieldKind::String,
                },
            ],
        })
    }

    fn record(
        descriptor: Arc<DynamicStructDescriptor>,
        realm: u32,
        id: u64,
        label: Option<&str>,
    ) -> DynamicStruct {
        let mut valid = PropertySet::EMPTY.with_tag(1).with_tag(2);
        let mut properties = alloc::vec![
            (1, DynamicValue::U32(realm)),
            (2, DynamicValue::U64(id)),
        ];
        if let Some(label) = label {
            valid = valid.with_tag(3);
            properties.push((3, DynamicValue::String(label.into())));
        }
        DynamicStruct {
            descriptor,
            valid,
            properties,
        }
    }

    /// Ports `TestDynamicStruct._keyProperties`: the key set is exactly
    /// the set of `key`-marked tags — no more, no less.
    #[test]
    fn key_set_is_exactly_the_key_tags() {
        let value = record(record_descriptor(), 1, 2, Some("x"));
        let expected = PropertySet::EMPTY.with_tag(1).with_tag(2);
        assert_eq!(Transmittable::key_set(&value), expected);
    }

    /// Ports `TestDynamicStruct.same` (positive half): equal keys but
    /// different non-key properties → identical `key_bytes()`.
    #[test]
    fn key_bytes_equal_for_equal_keys_despite_non_key_differences() {
        let desc = record_descriptor();
        let a = record(desc.clone(), 7, 42, Some("foo"));
        let b = record(desc.clone(), 7, 42, Some("bar"));
        // Even a *missing* non-key property doesn't affect key identity.
        let c = record(desc, 7, 42, None);
        assert_eq!(a.key_bytes(), b.key_bytes());
        assert_eq!(a.key_bytes(), c.key_bytes());
    }

    /// Ports `TestDynamicStruct.same` (negative half): any differing
    /// key property → different `key_bytes()`.
    #[test]
    fn key_bytes_differ_when_key_differs() {
        let desc = record_descriptor();
        let a = record(desc.clone(), 7, 42, Some("foo"));
        let other_realm = record(desc.clone(), 8, 42, Some("foo"));
        let other_id = record(desc, 7, 43, Some("foo"));
        assert_ne!(a.key_bytes(), other_realm.key_bytes());
        assert_ne!(a.key_bytes(), other_id.key_bytes());
    }

    /// A missing key property encodes as `null`, so a partial-key
    /// value stays distinguishable from one whose key is set to zero
    /// (the `key_bytes` doc contract).
    #[test]
    fn key_bytes_distinguish_missing_key_from_zero_key() {
        let desc = record_descriptor();
        let zero_key = record(desc.clone(), 0, 42, None);
        let missing_key = DynamicStruct {
            descriptor: desc,
            valid: PropertySet::EMPTY.with_tag(2),
            properties: alloc::vec![(2, DynamicValue::U64(42))],
        };
        assert_ne!(zero_key.key_bytes(), missing_key.key_bytes());
    }
}

/// `DynamicStruct` equality semantics.
///
/// Ports dots-cpp `TestDynamicStruct.equal` (`_equal` compares all
/// property values) onto the hand-written `PartialEq` impl, and pins
/// that impl's descriptor-comparison behavior: descriptors compare by
/// `Arc` pointer identity, not structurally.
#[cfg(test)]
mod partial_eq_tests {
    use super::*;

    fn pair_descriptor() -> Arc<DynamicStructDescriptor> {
        Arc::new(DynamicStructDescriptor {
            name: "Pair".into(),
            flags: StructFlags::NONE,
            properties: alloc::vec![
                DynamicPropertyDescriptor {
                    name: "n".into(),
                    tag: 1,
                    is_key: false,
                    kind: DynamicFieldKind::I32,
                },
                DynamicPropertyDescriptor {
                    name: "s".into(),
                    tag: 2,
                    is_key: false,
                    kind: DynamicFieldKind::String,
                },
            ],
        })
    }

    fn pair(descriptor: Arc<DynamicStructDescriptor>, n: i32, s: Option<&str>) -> DynamicStruct {
        let mut valid = PropertySet::EMPTY.with_tag(1);
        let mut properties = alloc::vec![(1, DynamicValue::I32(n))];
        if let Some(s) = s {
            valid = valid.with_tag(2);
            properties.push((2, DynamicValue::String(s.into())));
        }
        DynamicStruct {
            descriptor,
            valid,
            properties,
        }
    }

    /// Ports `TestDynamicStruct.equal` (positive half): same descriptor
    /// `Arc`, same valid set, same property values → equal.
    #[test]
    fn equal_when_same_descriptor_valid_and_properties() {
        let desc = pair_descriptor();
        let a = pair(desc.clone(), 2, Some("foo"));
        let b = pair(desc, 2, Some("foo"));
        assert_eq!(a, a);
        assert_eq!(a, b);
    }

    /// Ports `TestDynamicStruct.equal` (negative half): a differing
    /// property value → unequal.
    #[test]
    fn unequal_when_property_value_differs() {
        let desc = pair_descriptor();
        let a = pair(desc.clone(), 2, Some("foo"));
        let b = pair(desc, 2, Some("bar"));
        assert_ne!(a, b);
    }

    /// A differing valid set → unequal, even before property values
    /// are considered.
    #[test]
    fn unequal_when_valid_set_differs() {
        let desc = pair_descriptor();
        let a = pair(desc.clone(), 2, Some("foo"));
        let b = pair(desc, 2, None);
        assert_ne!(a, b);
    }

    /// Pins the descriptor-comparison sharp edge: `PartialEq` uses
    /// `Arc::ptr_eq`, so two values built from *structurally identical*
    /// descriptors held in different `Arc`s compare unequal even when
    /// all their data matches. This is deliberate (documented on the
    /// impl: "values built from different descriptor instances (even if
    /// structurally identical) compare unequal") — descriptors are
    /// expected to be shared per type, e.g. via a registry. Do not
    /// "fix" this test by deep-comparing descriptors without deciding
    /// that semantic change on purpose.
    #[test]
    fn structurally_identical_descriptors_in_different_arcs_compare_unequal() {
        let a = pair(pair_descriptor(), 2, Some("foo"));
        let b = pair(pair_descriptor(), 2, Some("foo"));
        assert!(!Arc::ptr_eq(&a.descriptor, &b.descriptor));
        // Same name, same properties, same values — still unequal.
        assert_eq!(a.descriptor.name, b.descriptor.name);
        assert_eq!(a.valid, b.valid);
        assert_eq!(a.properties, b.properties);
        assert_ne!(a, b);
    }
}
