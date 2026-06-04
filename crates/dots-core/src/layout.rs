//! Raw-memory layout primitives and the descriptor-driven CBOR codec.
//!
//! This is the only module in the workspace that opts into `unsafe`.
//! It implements the layout-compatible duality between typed structs
//! (produced by `#[derive(DotsStruct)]`) and dynamic `AnyStruct` instances
//! allocated from a `StructDescriptor` alone.
//!
//! # Memory model
//!
//! For a typed `Foo` declared with `#[derive(DotsStruct)]`, every field
//! is `Option<T>`, and the proc-macro emits typed *thunks* per property
//! plus the static `(size, align, offset)` triple. An [`AnyStruct`]
//! built from the same `StructDescriptor` allocates a heap buffer of
//! exactly that `(size, align)` and zero-initializes it. Zero is a
//! valid bit pattern for `None` of every primitive `T` we support
//! (and for niche-optimized references), so the buffer starts as a
//! valid all-fields-`None` struct without any per-field initialization.
//!
//! # Safety boundary
//!
//! `unsafe` is confined to (a) `alloc`/`dealloc`, (b) the raw pointer
//! arithmetic that hands a per-field pointer to a thunk, and (c) the
//! generic `opt_*` helpers exposed for the proc-macro. All unsafe is
//! local; no `unsafe` types or APIs leak across crate boundaries.

#![allow(unsafe_code)]

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::alloc::Layout;
use core::any::Any;
use core::ptr::NonNull;

use crate::{PropertySet, StructDescriptor, StructValue};

/// CBOR encoder type used by property thunks. Fixed-writer (`&mut Vec<u8>`)
/// so thunk fn-pointer signatures are stable across types.
pub type CborEncoder<'a> = minicbor::Encoder<&'a mut Vec<u8>>;

/// CBOR decoder type used by property thunks.
pub type CborDecoder<'b> = minicbor::Decoder<'b>;

/// Encode error. Writes go to `Vec<u8>`, which never fails — the only
/// real source of errors is `minicbor::encode::Error::Message`.
pub type EncodeError = minicbor::encode::Error<core::convert::Infallible>;

/// Decode error from minicbor.
pub type DecodeError = minicbor::decode::Error;

/// Heap-allocated, layout-compatible storage for any DOTS struct.
///
/// `AnyStruct.data` holds a buffer of exactly `descriptor.size` bytes,
/// aligned to `descriptor.align`, initialized so that every `Option<T>`
/// field begins as `None`. The buffer's bit pattern matches what a
/// typed `Box<Foo>` would contain for the equivalent values.
pub struct AnyStruct {
    descriptor: &'static StructDescriptor,
    valid: PropertySet,
    data: NonNull<u8>,
}

impl AnyStruct {
    /// Allocate a fresh instance for the given descriptor.
    ///
    /// `Option<T>` fields begin as `None` (the zero bit-pattern). Bare-`T`
    /// key fields, whose zeroed bytes may be an invalid `T`, are then
    /// initialized to a valid `T::default()` via their `init` thunk, so
    /// the whole buffer is a valid struct value from this point on — a
    /// precondition every other thunk (`drop`/`decode`/`clone`) and the
    /// `as_typed` reinterpret rely on. The placeholder key value is
    /// transient: it is overwritten by decode/clone, and the `valid` set
    /// (not the bytes) is the source of truth for which properties are
    /// really present.
    pub fn new(descriptor: &'static StructDescriptor) -> Self {
        let layout = descriptor.layout();
        let data = allocate_zeroed(layout);
        for prop in descriptor.properties {
            // SAFETY: `prop.offset` is in-range for `descriptor`'s layout
            // by construction; the slot is freshly zero-allocated, which
            // is exactly the precondition `init` expects.
            unsafe {
                (prop.vtable.init)(data.as_ptr().add(prop.offset));
            }
        }
        Self {
            descriptor,
            valid: PropertySet::EMPTY,
            data,
        }
    }

    /// Decode an `AnyStruct` from CBOR bytes using the supplied descriptor.
    pub fn decode_from_slice(
        descriptor: &'static StructDescriptor,
        bytes: &[u8],
    ) -> Result<Self, DecodeError> {
        let mut decoder = minicbor::Decoder::new(bytes);
        Self::decode_from_decoder(descriptor, &mut decoder)
    }

    /// Decode an `AnyStruct` from an active decoder, advancing it past
    /// the consumed CBOR map. Used by the framing layer to decode the
    /// payload while sharing the decoder with the header pass.
    pub fn decode_from_decoder(
        descriptor: &'static StructDescriptor,
        decoder: &mut CborDecoder<'_>,
    ) -> Result<Self, DecodeError> {
        let mut out = Self::new(descriptor);
        // SAFETY: `out.data` was allocated for `descriptor.layout()` and
        // zero-initialized; per-property writes go through typed thunks.
        unsafe {
            decode_into_raw(descriptor, out.data.as_ptr(), &mut out.valid, decoder)?;
        }
        Ok(out)
    }

    pub fn descriptor(&self) -> &'static StructDescriptor {
        self.descriptor
    }

    pub fn valid_set(&self) -> PropertySet {
        self.valid
    }

    /// Pointer to the start of the value buffer. The buffer follows the
    /// layout described by `descriptor()` — the same layout as the
    /// equivalent typed struct.
    pub fn data_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Borrow the layout-compatible buffer as a typed `&T`. Returns
    /// `None` if `T`'s descriptor is not the same `&'static
    /// StructDescriptor` this `AnyStruct` was allocated for.
    ///
    /// The identity check is `core::ptr::eq` — sound because
    /// `#[derive(DotsStruct)]` emits a single `'static` descriptor per
    /// type, so any two `AnyStruct`s built for `T` carry the same
    /// pointer.
    pub fn as_typed<T: StructValue>(&self) -> Option<&T> {
        if core::ptr::eq(self.descriptor, T::type_descriptor()) {
            // SAFETY: descriptor identity guarantees the buffer was
            // allocated with `T`'s layout and initialized through `T`'s
            // property thunks. A `&T` over the same memory is sound.
            Some(unsafe { &*(self.data.as_ptr() as *const T) })
        } else {
            None
        }
    }

    /// Move the layout-compatible buffer into a typed `Box<T>`,
    /// consuming this `AnyStruct` without running its destructor.
    /// Returns `Err(self)` on a descriptor mismatch so the caller can
    /// recover the value.
    pub fn into_typed<T: StructValue>(self) -> Result<Box<T>, Self> {
        if !core::ptr::eq(self.descriptor, T::type_descriptor()) {
            return Err(self);
        }
        let ptr = self.data.as_ptr() as *mut T;
        // Don't run Drop — the allocation now belongs to the Box.
        // `T`'s own Drop will run when the Box is dropped, and the
        // global allocator will free `descriptor.layout()` (== T's
        // layout) at that time.
        core::mem::forget(self);
        // SAFETY: descriptor identity guarantees `T`'s layout matches;
        // the buffer was allocated through the global allocator with
        // that layout; ownership transfers to the Box.
        Ok(unsafe { Box::from_raw(ptr) })
    }

    /// Mutable typed view, same identity check as [`as_typed`].
    pub fn as_typed_mut<T: StructValue>(&mut self) -> Option<&mut T> {
        if core::ptr::eq(self.descriptor, T::type_descriptor()) {
            // SAFETY: see `as_typed`.
            Some(unsafe { &mut *(self.data.as_ptr() as *mut T) })
        } else {
            None
        }
    }

    /// Build an `AnyStruct` by cloning the contents of any
    /// `&dyn StructValue`. Used to materialise an owning, type-erased
    /// copy from a borrowed typed value (e.g. `&Foo`).
    pub fn from_struct_value(value: &dyn StructValue) -> Self {
        Self::from_struct_value_with_mask(value, PropertySet::from_bits(u32::MAX))
    }

    /// Build an `AnyStruct` by cloning only the properties whose tag
    /// is set in `mask`. Properties outside the mask are left `None`
    /// (zero bit-pattern, set by `Self::new`).
    ///
    /// Used by the broker's publish-with-mask path to materialise a
    /// payload that the cache and re-fanout layers can hand around
    /// without re-encoding the unmasked properties on every replay.
    pub fn from_struct_value_with_mask(value: &dyn StructValue, mask: PropertySet) -> Self {
        let descriptor = value.descriptor();
        let mut out = Self::new(descriptor);
        let effective = value.valid_set() & mask;
        // SAFETY: `value.data_ptr()` is valid for `&value`'s lifetime;
        // `out.data` was allocated for `descriptor.layout()` and starts
        // zero-initialized (`None` for every property). Per-property
        // clone_in_place reads through the typed thunks.
        unsafe {
            for prop in descriptor.properties {
                if !effective.has(prop.tag) {
                    continue;
                }
                (prop.vtable.clone_in_place)(
                    value.data_ptr().add(prop.offset),
                    out.data.as_ptr().add(prop.offset),
                );
            }
        }
        out.valid = effective;
        out
    }
}

impl core::fmt::Debug for AnyStruct {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AnyStruct")
            .field("type", &self.descriptor.name)
            .field("valid", &self.valid)
            .finish_non_exhaustive()
    }
}

// SAFETY: `AnyStruct` owns its allocation; every property type the
// derive macro supports is `Send` (primitives, `String`, `Vec`, nested
// `AnyStruct`-derived structs which are themselves built from `Send`
// fields). The descriptor pointer is `&'static` and thus `Send`.
unsafe impl Send for AnyStruct {}

// SAFETY: `AnyStruct` only exposes `&self` access to its buffer and
// holds no interior mutability. Same reasoning as `Send`.
unsafe impl Sync for AnyStruct {}

impl Clone for AnyStruct {
    fn clone(&self) -> Self {
        Self::from_struct_value(self)
    }
}

impl Drop for AnyStruct {
    fn drop(&mut self) {
        for prop in self.descriptor.properties {
            // SAFETY: `self.data` points to a buffer of `descriptor.layout()`
            // and the property offset is in-range by descriptor construction.
            // `drop_in_place` handles both `Some` and `None` for `Option<T>`.
            unsafe {
                let p = self.data.as_ptr().add(prop.offset);
                (prop.vtable.drop_in_place)(p);
            }
        }
        deallocate(self.data, self.descriptor.layout());
    }
}

impl StructValue for AnyStruct {
    fn descriptor(&self) -> &'static StructDescriptor {
        self.descriptor
    }

    fn type_descriptor() -> &'static StructDescriptor {
        // AnyStruct's descriptor is per-instance, not per-type, so
        // there's no compile-time descriptor to return. Callers using
        // AnyStruct shouldn't reach for this method — they have
        // descriptor access through the instance's field directly.
        panic!("AnyStruct has no compile-time type descriptor; use .descriptor() on an instance")
    }

    fn valid_set(&self) -> PropertySet {
        self.valid
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }
}

/// Encode any `&dyn StructValue` to a freshly allocated `Vec<u8>`.
///
/// Walks the descriptor's properties in declaration order, emitting
/// only those whose tag is set in `valid_set()`. The wire format is
/// a CBOR map keyed by property tag, identical to what the C++ DOTS
/// implementation produces.
pub fn encode_to_vec(value: &dyn StructValue) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    encode_into_vec(value, &mut buf);
    buf
}

/// Append the encoded form of `value` to an existing `Vec<u8>`.
/// Useful when assembling multi-part wire frames (e.g. transmission
/// envelopes containing a header + payload) without an intermediate
/// allocation per part.
pub fn encode_into_vec(value: &dyn StructValue, buf: &mut Vec<u8>) {
    let mut encoder = minicbor::Encoder::new(buf);
    // SAFETY: `value.data_ptr()` is valid for the lifetime of `value`,
    // and the property offsets are within `descriptor().layout().size()`
    // by descriptor construction.
    unsafe {
        encode_from_raw(
            value.descriptor(),
            value.data_ptr(),
            value.valid_set(),
            &mut encoder,
        )
        .expect("Vec<u8> writes are infallible");
    }
}

/// Encode any `&dyn StructValue` directly into an active CBOR encoder.
/// The encoder's writer is generic — useful for encoders writing into
/// shared buffers managed by callers (e.g. the framing layer).
pub fn encode_into_encoder(
    value: &dyn StructValue,
    encoder: &mut CborEncoder<'_>,
) -> Result<(), EncodeError> {
    // SAFETY: same invariants as `encode_into_vec`.
    unsafe {
        encode_from_raw(
            value.descriptor(),
            value.data_ptr(),
            value.valid_set(),
            encoder,
        )
    }
}

/// Encode `value` but only emit properties whose tag is in `mask` AND
/// is actually set on the value. The intersection means missing keys
/// (e.g. removing an instance via key fields, where one of the keys
/// happens to be `None`) silently get dropped — caller is responsible
/// for ensuring all keys are present if that matters.
///
/// The wire form is identical to a regular `encode_into_vec` of a
/// value where only the masked properties had been set. Used by the
/// remove path: `mask = key_properties_mask` produces a key-only
/// payload.
pub fn encode_into_vec_with_mask(
    value: &dyn StructValue,
    mask: PropertySet,
    buf: &mut Vec<u8>,
) {
    let mut encoder = minicbor::Encoder::new(buf);
    // SAFETY: same invariants as `encode_into_vec`.
    unsafe {
        encode_from_raw(
            value.descriptor(),
            value.data_ptr(),
            value.valid_set() & mask,
            &mut encoder,
        )
        .expect("Vec<u8> writes are infallible");
    }
}

/// Encode `value` into an existing CBOR encoder, restricted to a mask.
/// Same semantics as [`encode_into_vec_with_mask`].
pub fn encode_into_encoder_with_mask(
    value: &dyn StructValue,
    mask: PropertySet,
    encoder: &mut CborEncoder<'_>,
) -> Result<(), EncodeError> {
    // SAFETY: same invariants as `encode_into_vec`.
    unsafe {
        encode_from_raw(
            value.descriptor(),
            value.data_ptr(),
            value.valid_set() & mask,
            encoder,
        )
    }
}

/// Compute the bitmask of `value`'s `#[dots(key)]` property tags.
/// Useful for constructing remove headers (`attributes` set to the
/// key bits) and for cache lookup.
pub fn key_set(value: &dyn StructValue) -> PropertySet {
    let mut set = PropertySet::EMPTY;
    for prop in value.descriptor().key_properties() {
        set = set.with_tag(prop.tag);
    }
    set
}

/// Encode the value's *key* properties — those marked `#[dots(key)]` —
/// as a CBOR array, returning the bytes. Suitable as a `BTreeMap` /
/// `HashMap` key for indexing instances by their primary key.
///
/// Properties are encoded in declaration order. A key property whose
/// `Option<T>` is `None` writes a CBOR `null`, so partial-key values
/// stay distinguishable from values where the key is set to a
/// default-encoded zero.
pub fn encode_key_bytes(value: &dyn StructValue) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_key_into(value, &mut buf);
    buf
}

/// Append the key-properties CBOR array to an existing buffer.
pub fn encode_key_into(value: &dyn StructValue, buf: &mut Vec<u8>) {
    let descriptor = value.descriptor();
    let base = value.data_ptr();
    let mut encoder = minicbor::Encoder::new(buf);
    let key_count = descriptor.key_properties().count() as u64;
    encoder
        .array(key_count)
        .expect("Vec<u8> writes are infallible");
    for prop in descriptor.key_properties() {
        // SAFETY: `prop.offset` is in-range for `descriptor`'s layout
        // by descriptor construction; `value.data_ptr()` points at a
        // buffer of that layout for `&self`'s lifetime.
        unsafe {
            let p = base.add(prop.offset);
            if (prop.vtable.is_set)(p) {
                (prop.vtable.encode_value)(p, &mut encoder)
                    .expect("Vec<u8> writes are infallible");
            } else {
                encoder.null().expect("Vec<u8> writes are infallible");
            }
        }
    }
}

/// Walk the descriptor and emit each set property to the encoder.
///
/// # Safety
///
/// `base` must point to a valid struct buffer matching `descriptor`'s
/// layout, and the buffer must remain live for the duration of the call.
unsafe fn encode_from_raw(
    descriptor: &'static StructDescriptor,
    base: *const u8,
    valid: PropertySet,
    encoder: &mut CborEncoder<'_>,
) -> Result<(), EncodeError> {
    encoder.map(u64::from(valid.len()))?;
    for prop in descriptor.properties {
        if !valid.has(prop.tag) {
            continue;
        }
        encoder.u32(prop.tag)?;
        // SAFETY: `prop.offset` is in-range and the field is `Some`
        // (we just checked the valid bit).
        unsafe {
            (prop.vtable.encode_value)(base.add(prop.offset), encoder)?;
        }
    }
    Ok(())
}

/// Decode a CBOR map into a buffer matching `descriptor`'s layout.
///
/// # Safety
///
/// `base` must point to a buffer of `descriptor.layout()`, zero-initialized
/// so that every `Option<T>` field reads as `None` before the first write
/// (or hold a previously-valid struct value). `valid` is updated to reflect
/// the tags successfully decoded.
unsafe fn decode_into_raw(
    descriptor: &'static StructDescriptor,
    base: *mut u8,
    valid: &mut PropertySet,
    decoder: &mut CborDecoder<'_>,
) -> Result<(), DecodeError> {
    let len = decoder.map()?.ok_or_else(|| {
        DecodeError::message("indefinite-length maps are not supported in DOTS structs")
    })?;
    for _ in 0..len {
        let tag = decoder.u32()?;
        match descriptor.property(tag) {
            Some(prop) => {
                // SAFETY: `prop.offset` is in-range; the buffer is initialized
                // (zero-init = a valid `None`, or a previously-written `Some`)
                // so dropping then writing through the thunk is sound.
                unsafe {
                    (prop.vtable.decode_value)(base.add(prop.offset), decoder)?;
                }
                *valid = valid.with_tag(tag);
            }
            None => {
                // Forward-compat: skip properties this descriptor doesn't know.
                decoder.skip()?;
            }
        }
    }
    // Key contract: every `#[dots(key)]` property must be present. This
    // is what dotsd guarantees for real instances; enforcing it here
    // means a malformed/partial wire value is rejected as a decode error
    // rather than producing a struct with a bogus placeholder key (and,
    // for bare-`T` keys, it keeps `as_typed` honest). Keyless types have
    // an empty mask, so this is a no-op for them.
    let key_mask = descriptor.key_mask();
    if (*valid & key_mask) != key_mask {
        return Err(DecodeError::message(
            "decoded DOTS struct is missing one or more `#[dots(key)]` properties",
        ));
    }
    Ok(())
}

/// Decode a CBOR slice into a typed struct.
///
/// Constructs a default-valued `T` (which for DOTS structs is all-`None`)
/// and applies the wire updates onto it via the descriptor.
pub fn decode_typed_from_slice<T>(bytes: &[u8]) -> Result<T, DecodeError>
where
    T: StructValue + Default,
{
    let mut decoder = minicbor::Decoder::new(bytes);
    decode_typed_from_decoder(&mut decoder)
}

/// Decode a typed struct from an active decoder. Useful when the
/// caller is reading a stream of CBOR items and needs to know how
/// many bytes were consumed (via `decoder.position()`).
pub fn decode_typed_from_decoder<T>(decoder: &mut CborDecoder<'_>) -> Result<T, DecodeError>
where
    T: StructValue + Default,
{
    let mut value = T::default();
    let descriptor = StructValue::descriptor(&value);
    let mut valid = PropertySet::EMPTY;
    let base = (&raw mut value) as *mut u8;
    // SAFETY: `T: StructValue` enforces the layout invariant — its
    // descriptor matches `T`'s `(size, align)` and the field offsets
    // come from `offset_of!(T, _)`. Treating `&mut value` as `*mut u8`
    // for byte-precise field writes via typed thunks is sound.
    unsafe {
        decode_into_raw(descriptor, base, &mut valid, decoder)?;
    }
    Ok(value)
}

fn allocate_zeroed(layout: Layout) -> NonNull<u8> {
    if layout.size() == 0 {
        // SAFETY: `align` is non-zero by `Layout`'s invariants.
        unsafe { NonNull::new_unchecked(layout.align() as *mut u8) }
    } else {
        // SAFETY: `layout` came from `Layout::from_size_align` with valid
        // size/align, so alloc_zeroed's preconditions are satisfied.
        let ptr = unsafe { alloc::alloc::alloc_zeroed(layout) };
        match NonNull::new(ptr) {
            Some(p) => p,
            None => alloc::alloc::handle_alloc_error(layout),
        }
    }
}

fn deallocate(ptr: NonNull<u8>, layout: Layout) {
    if layout.size() != 0 {
        // SAFETY: `ptr` was returned by `alloc_zeroed(layout)`, has not
        // been deallocated, and the layout matches.
        unsafe {
            alloc::alloc::dealloc(ptr.as_ptr(), layout);
        }
    }
}

// ===== DotsField: uniform encode/decode trait =====
//
// `DotsField` abstracts over "any type that can occupy a DOTS struct
// field". The per-property thunks dispatch through it. Each leaf type
// has an explicit impl below; `#[derive(DotsStruct)]` adds one per
// derived struct that delegates to the descriptor-driven codec.

/// Trait implemented by every type that can occupy a DOTS property slot.
pub trait DotsField: Sized {
    /// Encode `&self` to the CBOR encoder.
    fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError>;

    /// Decode `Self` from the CBOR decoder.
    fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError>;
}

/// Manual `DotsField` impls for the leaf types that already have
/// `minicbor::Encode + Decode` and whose default minicbor wire format
/// matches what DOTS expects (so we just delegate).
macro_rules! impl_dots_field_via_minicbor {
    ($($t:ty),* $(,)?) => {
        $(
            impl DotsField for $t {
                #[inline]
                fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
                    <Self as minicbor::Encode<()>>::encode(self, e, &mut ())
                }
                #[inline]
                fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
                    <Self as minicbor::Decode<'_, ()>>::decode(d, &mut ())
                }
            }
        )*
    };
}

impl_dots_field_via_minicbor!(
    bool,
    u8, u16, u32, u64,
    i8, i16, i32, i64,
    f32, f64,
    alloc::string::String,
);

/// DOTS `uuid` — exactly 16 raw bytes, encoded as a CBOR ByteString.
/// Matches dots-cpp's `CborWriter::write(std::array<uint8_t, 16>)`.
///
/// `uuid` is the *only* fixed-byte type in DOTS. For arbitrary binary
/// blobs use `Vec<u8>`, which encodes as DOTS `vector<uint8>` (a CBOR
/// array of `uint8`) — the same convention dots-cpp uses. Other
/// `[u8; N]` sizes intentionally have no `DotsField` impl: they would
/// share the uuid wire format but mean something different, and that
/// ambiguity is what we want to prevent at the type level.
impl DotsField for [u8; 16] {
    #[inline]
    fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        e.bytes(self)?;
        Ok(())
    }
    #[inline]
    fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
        let bytes = d.bytes()?;
        bytes.try_into().map_err(|_| {
            minicbor::decode::Error::message("uuid byte-string length mismatch")
        })
    }
}

impl crate::DotsTypeKind for [u8; 16] {
    const KIND: crate::FieldKind = crate::FieldKind::Uuid;
}

/// Safe wrapper used by the manual `DotsField` impl that the proc-macro
/// emits for derived DOTS structs. Encodes via the descriptor-driven path.
pub fn encode_struct_value(
    value: &dyn StructValue,
    encoder: &mut CborEncoder<'_>,
) -> Result<(), EncodeError> {
    // SAFETY: `value.data_ptr()` is valid for `&self`'s lifetime, the
    // descriptor's offsets are within `descriptor().layout().size()`,
    // and only set properties are dereferenced.
    unsafe {
        encode_from_raw(
            value.descriptor(),
            value.data_ptr(),
            value.valid_set(),
            encoder,
        )
    }
}

/// Safe wrapper used by the manual `DotsField` impl that the proc-macro
/// emits for derived DOTS structs. Constructs `T::default()` (an
/// all-`None` instance) and applies wire updates over it.
pub fn decode_struct_default<T>(decoder: &mut CborDecoder<'_>) -> Result<T, DecodeError>
where
    T: StructValue + Default,
{
    let mut value = T::default();
    let descriptor = StructValue::descriptor(&value);
    let mut valid = PropertySet::EMPTY;
    let base = (&raw mut value) as *mut u8;
    // SAFETY: `T: StructValue` enforces layout consistency between
    // `T` and its descriptor (size, align, offsets); writing through
    // typed thunks at those offsets is sound.
    unsafe {
        decode_into_raw(descriptor, base, &mut valid, decoder)?;
    }
    Ok(value)
}

// ===== Generic per-type thunk helpers =====
//
// The proc-macro emits `PropertyVtable` statics whose function-pointer
// fields name these helpers with explicit type parameters, e.g.
//
//     PropertyVtable {
//         is_set: ::dots_core::layout::opt_is_set::<u32>,
//         encode_value: ::dots_core::layout::opt_encode::<u32>,
//         ...
//     }
//
// Generic-fn-with-explicit-type coerces to a concrete fn pointer,
// monomorphizing per `T`. The bound is `T: DotsField`, so primitives
// (via blanket impl) and DOTS structs (via emitted manual impl) both
// fit through the same thunk family.

/// Read whether the `Option<T>` at `ptr` is `Some(_)`.
///
/// # Safety
///
/// `ptr` must point to a valid `Option<T>`.
pub unsafe fn opt_is_set<T>(ptr: *const u8) -> bool {
    // SAFETY: caller-upheld.
    unsafe { (*(ptr as *const Option<T>)).is_some() }
}

/// Encode the inner value of `Option<T>` at `ptr`. Caller must have
/// already verified the field is `Some(_)` via `opt_is_set`.
///
/// # Safety
///
/// `ptr` must point to a valid `Option<T>` whose discriminant is `Some`.
pub unsafe fn opt_encode<T>(ptr: *const u8, e: &mut CborEncoder<'_>) -> Result<(), EncodeError>
where
    T: DotsField,
{
    // SAFETY: caller-upheld.
    let opt = unsafe { &*(ptr as *const Option<T>) };
    let v = opt
        .as_ref()
        .expect("opt_encode invoked on a None field — caller must check is_set first");
    v.dots_encode(e)
}

/// Decode a `T` from the decoder, drop any existing `Option<T>` at `ptr`,
/// and write `Some(value)` in its place.
///
/// # Safety
///
/// `ptr` must point to a valid `Option<T>` (initialized — at minimum
/// zero-initialized, which is a valid `None` for every supported `T`).
pub unsafe fn opt_decode<T>(ptr: *mut u8, d: &mut CborDecoder<'_>) -> Result<(), DecodeError>
where
    T: DotsField,
{
    let value: T = T::dots_decode(d)?;
    let p = ptr as *mut Option<T>;
    // SAFETY: `p` points to a valid `Option<T>` per caller contract;
    // dropping in place is sound because the existing value is
    // initialized (zero-init = `None`, or a previously-decoded `Some`).
    unsafe {
        core::ptr::drop_in_place(p);
        core::ptr::write(p, Some(value));
    }
    Ok(())
}

/// Drop the `Option<T>` at `ptr` in place.
///
/// # Safety
///
/// `ptr` must point to a valid `Option<T>`.
pub unsafe fn opt_drop<T>(ptr: *mut u8) {
    // SAFETY: caller-upheld.
    unsafe {
        core::ptr::drop_in_place(ptr as *mut Option<T>);
    }
}

/// Clone the `Option<T>` at `src` into `dst`, dropping any existing
/// value at `dst` first.
///
/// # Safety
///
/// `src` and `dst` must each point to a valid `Option<T>` (zero-init
/// counts as `None`).
pub unsafe fn opt_clone<T: Clone>(src: *const u8, dst: *mut u8) {
    // SAFETY: caller-upheld.
    let src_opt = unsafe { &*(src as *const Option<T>) };
    let cloned: Option<T> = src_opt.clone();
    let dst_opt = dst as *mut Option<T>;
    unsafe {
        core::ptr::drop_in_place(dst_opt);
        core::ptr::write(dst_opt, cloned);
    }
}

/// Clone the `Option<Vec<T>>` at `src` into `dst`. Same shape as
/// [`opt_clone`] but routed via the `Vec` family so the derive macro
/// can pick a `Vec`-specific clone without a `DotsField` bound.
///
/// # Safety
///
/// `src` and `dst` must point to valid `Option<Vec<T>>`.
pub unsafe fn opt_clone_vec<T: Clone>(src: *const u8, dst: *mut u8) {
    // SAFETY: caller-upheld.
    let src_opt = unsafe { &*(src as *const Option<Vec<T>>) };
    let cloned: Option<Vec<T>> = src_opt.clone();
    let dst_opt = dst as *mut Option<Vec<T>>;
    unsafe {
        core::ptr::drop_in_place(dst_opt);
        core::ptr::write(dst_opt, cloned);
    }
}

// ===== Vec<T> thunk family =====
//
// For every `Option<Vec<X>>` field — including `Vec<u8>` — the proc-macro
// reaches for these helpers instead of `opt_encode`/`opt_decode`. The
// wire format is a CBOR array of `X`, matching dots-cpp.
//
// `is_set` and `drop_in_place` simply use `opt_is_set::<Vec<X>>` and
// `opt_drop::<Vec<X>>` — `Vec<X>` is a normal owned type for those
// purposes.

/// Encode the inner `Vec<T>` of `Option<Vec<T>>` at `ptr` as a CBOR array.
///
/// # Safety
///
/// `ptr` must point to a valid `Option<Vec<T>>` whose discriminant is `Some`.
pub unsafe fn opt_encode_vec<T>(
    ptr: *const u8,
    e: &mut CborEncoder<'_>,
) -> Result<(), EncodeError>
where
    T: DotsField,
{
    // SAFETY: caller-upheld.
    let opt = unsafe { &*(ptr as *const Option<Vec<T>>) };
    let v = opt
        .as_ref()
        .expect("opt_encode_vec invoked on a None field — caller must check is_set first");
    e.array(v.len() as u64)?;
    for item in v {
        item.dots_encode(e)?;
    }
    Ok(())
}

/// Decode a CBOR array into `Vec<T>`, drop any existing `Option<Vec<T>>`
/// at `ptr`, and write `Some(value)` in its place.
///
/// # Safety
///
/// `ptr` must point to a valid `Option<Vec<T>>` (initialized — at minimum
/// zero-initialized, which is `None`).
pub unsafe fn opt_decode_vec<T>(
    ptr: *mut u8,
    d: &mut CborDecoder<'_>,
) -> Result<(), DecodeError>
where
    T: DotsField,
{
    let len = d.array()?.ok_or_else(|| {
        DecodeError::message("indefinite-length arrays are not supported in DOTS Vec fields")
    })?;
    let mut out: Vec<T> = Vec::with_capacity(len as usize);
    for _ in 0..len {
        out.push(T::dots_decode(d)?);
    }
    let p = ptr as *mut Option<Vec<T>>;
    // SAFETY: `p` points to a valid `Option<Vec<T>>` per caller contract;
    // dropping the existing value is sound.
    unsafe {
        core::ptr::drop_in_place(p);
        core::ptr::write(p, Some(out));
    }
    Ok(())
}

// ===== Bare-`T` key thunk family (Approach A) =====
//
// A `#[dots(key)]` property may be stored as a bare `T` rather than
// `Option<T>`: a key is, by contract, always present, so the optional
// wrapper buys nothing and only costs a discriminant. The trade-off is
// that a zeroed buffer is *not* automatically a valid `T` for non-`Copy`
// keys (a zeroed `String` is a null-pointer `String` — UB to read or
// drop). We keep the buffer sound by giving key slots an `init` thunk
// that writes a valid `T::default()` immediately after zero-allocation
// (see `AnyStruct::new`). From that point every slot is a valid `T`, so
// the decode/drop/clone thunks below can drop-then-write exactly like
// their `opt_*` cousins — there is never an invalid-bits window.

/// No-op initializer for `Option<T>` fields: a zeroed slot is already a
/// valid `None`, so nothing to do.
///
/// # Safety
///
/// `ptr` must point to a zero-initialized `Option<T>` slot (trivially
/// satisfied; the body touches nothing).
pub unsafe fn init_noop(_ptr: *mut u8) {}

/// Initialize a bare-`T` key slot to `T::default()`, writing over the
/// (possibly invalid) zeroed bytes *without* dropping them.
///
/// # Safety
///
/// `ptr` must point to a `T`-sized, `T`-aligned slot that is **not yet
/// initialized** (e.g. freshly zero-allocated). The old bytes are
/// overwritten with `core::ptr::write`, so they are never dropped —
/// which is exactly what we need, since zeroed bytes may be an invalid
/// `T`.
pub unsafe fn key_init<T: Default>(ptr: *mut u8) {
    // SAFETY: caller-upheld; `write` does not drop the prior bytes.
    unsafe {
        core::ptr::write(ptr as *mut T, T::default());
    }
}

/// A bare-`T` key is always set.
///
/// # Safety
///
/// Trivially safe; signature matches the vtable slot.
pub unsafe fn key_is_set(_ptr: *const u8) -> bool {
    true
}

/// Encode the bare `T` at `ptr`.
///
/// # Safety
///
/// `ptr` must point to a valid `T`.
pub unsafe fn key_encode<T>(ptr: *const u8, e: &mut CborEncoder<'_>) -> Result<(), EncodeError>
where
    T: DotsField,
{
    // SAFETY: caller-upheld.
    let v = unsafe { &*(ptr as *const T) };
    v.dots_encode(e)
}

/// Decode a `T` from the decoder and write it to the bare-`T` slot at
/// `ptr`, dropping the previous value first.
///
/// # Safety
///
/// `ptr` must point to a valid `T` (the slot is kept valid by `key_init`
/// at allocation and by every prior write), live for the call.
pub unsafe fn key_decode<T>(ptr: *mut u8, d: &mut CborDecoder<'_>) -> Result<(), DecodeError>
where
    T: DotsField,
{
    let value: T = T::dots_decode(d)?;
    let p = ptr as *mut T;
    // SAFETY: `p` points to a valid `T` per caller contract; drop-then-
    // write is sound because the slot is always initialized.
    unsafe {
        core::ptr::drop_in_place(p);
        core::ptr::write(p, value);
    }
    Ok(())
}

/// Drop the bare `T` at `ptr` in place.
///
/// # Safety
///
/// `ptr` must point to a valid `T`.
pub unsafe fn key_drop<T>(ptr: *mut u8) {
    // SAFETY: caller-upheld.
    unsafe {
        core::ptr::drop_in_place(ptr as *mut T);
    }
}

/// Clone the bare `T` at `src` into the bare-`T` slot at `dst`, dropping
/// any existing value at `dst` first.
///
/// # Safety
///
/// `src` and `dst` must each point to a valid `T`.
pub unsafe fn key_clone<T: Clone>(src: *const u8, dst: *mut u8) {
    // SAFETY: caller-upheld.
    let cloned: T = unsafe { (*(src as *const T)).clone() };
    let p = dst as *mut T;
    unsafe {
        core::ptr::drop_in_place(p);
        core::ptr::write(p, cloned);
    }
}
