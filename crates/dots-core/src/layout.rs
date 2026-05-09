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
    /// Allocate a fresh, all-fields-`None` instance for the given descriptor.
    pub fn new(descriptor: &'static StructDescriptor) -> Self {
        let layout = descriptor.layout();
        let data = allocate_zeroed(layout);
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
        let mut out = Self::new(descriptor);
        let mut decoder = minicbor::Decoder::new(bytes);
        // SAFETY: `out.data` was allocated for `descriptor.layout()` and
        // zero-initialized; per-property writes go through typed thunks.
        unsafe {
            decode_into_raw(descriptor, out.data.as_ptr(), &mut out.valid, &mut decoder)?;
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
    {
        let mut encoder = minicbor::Encoder::new(&mut buf);
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
    buf
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
    let mut value = T::default();
    let descriptor = StructValue::descriptor(&value);
    let mut valid = PropertySet::EMPTY;
    let mut decoder = minicbor::Decoder::new(bytes);
    let base = (&raw mut value) as *mut u8;
    // SAFETY: `T: StructValue` enforces the layout invariant — its
    // descriptor matches `T`'s `(size, align)` and the field offsets
    // come from `offset_of!(T, _)`. Treating `&mut value` as `*mut u8`
    // for byte-precise field writes via typed thunks is sound.
    unsafe {
        decode_into_raw(descriptor, base, &mut valid, &mut decoder)?;
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
// field". The per-property thunks dispatch through it.
//
// We do *not* use a blanket impl over `T: minicbor::Encode + Decode`,
// because that would force `Vec<u8>` to encode as a CBOR array (minicbor's
// default for `Vec<T>`) rather than as a CBOR byte string. The wire-format
// compatibility with C++ DOTS requires byte-string for `Vec<u8>`, so we
// write explicit per-leaf-type impls below.
//
// `#[derive(DotsStruct)]` adds one more impl family on top: every derived
// struct gets a manual `DotsField` impl that delegates to the descriptor-
// driven codec via `encode_struct_value` / `decode_struct_default`.

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

/// `Vec<u8>` is a special case: minicbor's default `Vec<T>` impl encodes
/// as a CBOR array, but DOTS uses CBOR byte strings for raw bytes
/// (cross-language compatibility with C++ DOTS). Manual impl writes
/// `bytes` / reads `bytes` directly.
impl DotsField for Vec<u8> {
    #[inline]
    fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        e.bytes(self)?;
        Ok(())
    }
    #[inline]
    fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
        Ok(d.bytes()?.to_vec())
    }
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

// ===== Vec<T> thunk family =====
//
// For `Option<Vec<X>>` fields where `X` is not `u8`, the proc-macro
// reaches for these helpers instead of `opt_encode`/`opt_decode`. The
// wire format is a CBOR array of `X` (whereas `Vec<u8>` stays on the
// byte-string path through the regular `opt_*` thunks).
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
