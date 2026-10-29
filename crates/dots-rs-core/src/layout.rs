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

use crate::{DotsFieldOps, PropertySet, StructDescriptor, StructValue};

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
    /// Every property slot is then seeded via its `init` thunk: an
    /// `Option<T>` field is written to a real `None` and a bare-`T` key
    /// to `T::default()` (the zeroed bytes are not relied on to be a
    /// valid value — see [`opt_init`]/[`key_init`]), so the whole buffer
    /// is a valid struct value from this point on — a
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

    /// Apply a flat, masked mirror of `src` onto `self` — dots-cpp
    /// `Struct::_copy` semantics restricted to non-key properties,
    /// matching `Container::updateWithoutKeys`: the partial-update
    /// operation the cache performs on every non-create transmission.
    ///
    /// For each **non-key** property whose tag is set in `mask`:
    /// - set in `src` → deep-clone it into `self`, dropping any prior
    ///   value (`clone_in_place`);
    /// - unset in `src` → clear it in `self` (`set_none`).
    ///
    /// The "in the mask but unset in `src` means clear" rule is the
    /// DOTS wire-update contract (`DotsHeader.attributes`) — it is how
    /// a publish invalidates a single field. Properties whose tag is
    /// **outside** `mask` are left untouched — that's what preserves
    /// values the update didn't mention. Key properties are never
    /// modified (they define identity and the container keys by them),
    /// so the copy can never move an entry.
    ///
    /// The overlay is **flat**: a sub-struct property is replaced
    /// wholesale, not merged field-by-field — exactly as the C++
    /// container does. (The recursive, non-destructive overlay is
    /// [`merge_from`](Self::merge_from).)
    ///
    /// `src` must share `self`'s descriptor; mismatches are a caller
    /// bug (asserted in debug builds) and the call is a no-op in
    /// release.
    pub fn copy_from(&mut self, src: &dyn StructValue, mask: PropertySet) {
        debug_assert!(
            core::ptr::eq(self.descriptor, src.descriptor()),
            "copy_from requires matching descriptors"
        );
        if !core::ptr::eq(self.descriptor, src.descriptor()) {
            return;
        }
        let src_base = src.data_ptr();
        for prop in self.descriptor.properties {
            if prop.is_key || !mask.has(prop.tag) {
                continue;
            }
            // SAFETY: both buffers share this descriptor's layout, so
            // `prop.offset` is in-range for each; every slot is a valid
            // `Option<T>` (or bare-`T` key, excluded above). The thunks
            // read `src` and drop-then-write `self` in place.
            unsafe {
                let dst_p = self.data.as_ptr().add(prop.offset);
                let src_p = src_base.add(prop.offset);
                if (prop.vtable.is_set)(src_p) {
                    (prop.vtable.clone_in_place)(src_p, dst_p);
                    self.valid = self.valid.with_tag(prop.tag);
                } else {
                    (prop.vtable.set_none)(dst_p);
                    self.valid = self.valid.without_tag(prop.tag);
                }
            }
        }
    }

    /// Like [`copy_from`](Self::copy_from), but **moves** each set
    /// property out of `src` instead of deep-cloning it — `src`'s owned
    /// heap allocations (`String`/`Vec` buffers, nested data) are handed
    /// over to `self`. After the call the moved-out slots of `src` are
    /// `None`, so `src` stays valid and drops cleanly.
    ///
    /// Use this on the cache's update path when the incoming value is
    /// owned and about to be dropped: it saves a deep clone of every
    /// changed property. Semantics (mask, flat overlay, keys untouched,
    /// clear-when-unset-in-mask) are identical to `copy_from`.
    pub fn copy_take(&mut self, src: &mut AnyStruct, mask: PropertySet) {
        debug_assert!(
            core::ptr::eq(self.descriptor, src.descriptor),
            "copy_take requires matching descriptors"
        );
        if !core::ptr::eq(self.descriptor, src.descriptor) {
            return;
        }
        for prop in self.descriptor.properties {
            if prop.is_key || !mask.has(prop.tag) {
                continue;
            }
            // SAFETY: both buffers share this descriptor's layout, so
            // `prop.offset` is in-range for each; every slot is a valid
            // `Option<T>` (keys excluded above). `take_in_place` moves
            // `src`'s value into `self`, leaving `src`'s slot `None`.
            unsafe {
                let dst_p = self.data.as_ptr().add(prop.offset);
                let src_p = src.data.as_ptr().add(prop.offset);
                if (prop.vtable.is_set)(src_p) {
                    (prop.vtable.take_in_place)(src_p, dst_p);
                    self.valid = self.valid.with_tag(prop.tag);
                    src.valid = src.valid.without_tag(prop.tag);
                } else {
                    (prop.vtable.set_none)(dst_p);
                    self.valid = self.valid.without_tag(prop.tag);
                }
            }
        }
    }

    /// Make `self` an exact masked copy of `src` — dots-cpp
    /// `Struct::_assign` semantics, minus keys: every **non-key**
    /// property that is in `mask` *and* set on `src` is deep-cloned in;
    /// every other non-key property is cleared, **including those
    /// outside `mask`** (that's what distinguishes `assign` from
    /// [`copy_from`](Self::copy_from), which leaves out-of-mask
    /// properties untouched).
    ///
    /// Key properties are never modified — unlike C++, where `_assign`
    /// can invalidate a key, dots-rust keeps keys permanently valid
    /// (bare-`T` key storage cannot represent an unset key on the typed
    /// side). Panics on a descriptor mismatch.
    pub fn assign_from(&mut self, src: &dyn StructValue, mask: PropertySet) {
        assert!(
            core::ptr::eq(self.descriptor, src.descriptor()),
            "assign_from requires matching descriptors"
        );
        let src_valid = src.valid_set();
        let src_base = src.data_ptr();
        for prop in self.descriptor.properties {
            if prop.is_key {
                continue;
            }
            // SAFETY: shared descriptor — offsets in-range for both
            // buffers, slots valid for the thunk's type.
            unsafe {
                let dst_p = self.data.as_ptr().add(prop.offset);
                if mask.has(prop.tag) && src_valid.has(prop.tag) {
                    (prop.vtable.clone_in_place)(src_base.add(prop.offset), dst_p);
                    self.valid = self.valid.with_tag(prop.tag);
                } else {
                    (prop.vtable.set_none)(dst_p);
                    self.valid = self.valid.without_tag(prop.tag);
                }
            }
        }
    }

    /// Like [`assign_from`](Self::assign_from), but **moves** each
    /// assigned property out of `src` (the C++ rvalue `_assign`
    /// overload). Moved-out slots of `src` are cleared, so `src` stays
    /// valid and drops cleanly. Panics on a descriptor mismatch.
    pub fn assign_take(&mut self, src: &mut AnyStruct, mask: PropertySet) {
        assert!(
            core::ptr::eq(self.descriptor, src.descriptor),
            "assign_take requires matching descriptors"
        );
        for prop in self.descriptor.properties {
            if prop.is_key {
                continue;
            }
            // SAFETY: shared descriptor — see `assign_from`;
            // `take_in_place` leaves `src`'s slot cleared and valid.
            unsafe {
                let dst_p = self.data.as_ptr().add(prop.offset);
                let src_p = src.data.as_ptr().add(prop.offset);
                if mask.has(prop.tag) && src.valid.has(prop.tag) {
                    (prop.vtable.take_in_place)(src_p, dst_p);
                    self.valid = self.valid.with_tag(prop.tag);
                    src.valid = src.valid.without_tag(prop.tag);
                } else {
                    (prop.vtable.set_none)(dst_p);
                    self.valid = self.valid.without_tag(prop.tag);
                }
            }
        }
    }

    /// Non-destructive recursive overlay of `src` onto `self` —
    /// dots-cpp `Struct::_merge` semantics, minus keys.
    ///
    /// Only **non-key** properties that are in `mask` *and* set on
    /// `src` are touched; nothing is ever cleared (contrast with
    /// [`copy_from`](Self::copy_from), the wire-update operation).
    /// A struct-typed property is merged property-wise, recursively —
    /// fields the source sub-struct doesn't carry survive on the
    /// target. Every other kind of property (including vectors) is
    /// replaced wholesale.
    ///
    /// Panics on a descriptor mismatch.
    pub fn merge_from(&mut self, src: &dyn StructValue, mask: PropertySet) {
        assert!(
            core::ptr::eq(self.descriptor, src.descriptor()),
            "merge_from requires matching descriptors"
        );
        let src_valid = src.valid_set();
        let src_base = src.data_ptr();
        for prop in self.descriptor.properties {
            if prop.is_key || !mask.has(prop.tag) || !src_valid.has(prop.tag) {
                continue;
            }
            // SAFETY: shared descriptor — offsets in-range for both
            // buffers; `merge_in_place` handles set and unset targets.
            unsafe {
                (prop.vtable.merge_in_place)(
                    src_base.add(prop.offset),
                    self.data.as_ptr().add(prop.offset),
                );
            }
            self.valid = self.valid.with_tag(prop.tag);
        }
    }

    /// Swap the masked **non-key** properties of `self` and `other` —
    /// values and validity change sides; owned heap data is handed
    /// over, not cloned. Properties outside `mask` and all keys stay
    /// put. Panics on a descriptor mismatch.
    pub fn swap(&mut self, other: &mut AnyStruct, mask: PropertySet) {
        assert!(
            core::ptr::eq(self.descriptor, other.descriptor),
            "swap requires matching descriptors"
        );
        for prop in self.descriptor.properties {
            if prop.is_key || !mask.has(prop.tag) {
                continue;
            }
            // SAFETY: shared descriptor; the two buffers are distinct
            // allocations (`&mut` on both), so the slots don't alias.
            unsafe {
                (prop.vtable.swap_in_place)(
                    self.data.as_ptr().add(prop.offset),
                    other.data.as_ptr().add(prop.offset),
                );
            }
            let (s, o) = (self.valid.has(prop.tag), other.valid.has(prop.tag));
            self.valid = if o {
                self.valid.with_tag(prop.tag)
            } else {
                self.valid.without_tag(prop.tag)
            };
            other.valid = if s {
                other.valid.with_tag(prop.tag)
            } else {
                other.valid.without_tag(prop.tag)
            };
        }
    }

    /// Clear the masked **non-key** properties of `self` — dots-cpp
    /// `Struct::_clear` semantics, minus keys (dots-rust never
    /// invalidates a key property).
    pub fn clear(&mut self, mask: PropertySet) {
        for prop in self.descriptor.properties {
            if prop.is_key || !mask.has(prop.tag) {
                continue;
            }
            // SAFETY: `self.data` follows this descriptor's layout;
            // `set_none` drops any existing value and writes `None`.
            unsafe {
                (prop.vtable.set_none)(self.data.as_ptr().add(prop.offset));
            }
            self.valid = self.valid.without_tag(prop.tag);
        }
    }
}

/// Whole-value equality: same descriptor (by pointer identity) and
/// every property equal. Values of different DOTS types are unequal,
/// never a panic — mirroring [`DynamicStruct`](crate::DynamicStruct)'s
/// `PartialEq`. For masked comparison use [`equal`].
impl PartialEq for AnyStruct {
    fn eq(&self, other: &Self) -> bool {
        core::ptr::eq(self.descriptor, other.descriptor)
            && equal(self, other, PropertySet::ALL)
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

/// The canonical instance key (wire-format spec §6): a deterministic
/// CBOR array of the `#[dots(key)]` property values in ascending tag
/// order, with `null` for an unset key property and `[]` for keyless
/// types. Byte equality of two keys is instance identity; the bytes
/// also serve as `BTreeMap`/`HashMap` keys for indexing instances.
///
/// Byte-identical to [`DynamicStruct::key_bytes`] for the same
/// logical value.
///
/// [`DynamicStruct::key_bytes`]: crate::DynamicStruct::key_bytes
pub fn encode_key_bytes(value: &dyn StructValue) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_key_into(value, &mut buf);
    buf
}

/// Append the canonical-key CBOR array to an existing buffer.
///
/// Key properties are emitted in ascending tag order — never
/// declaration order, which stays free to change (wire-format spec
/// §6.3).
pub fn encode_key_into(value: &dyn StructValue, buf: &mut Vec<u8>) {
    let descriptor = value.descriptor();
    let base = value.data_ptr();
    let mut encoder = minicbor::Encoder::new(buf);
    // The key mask iterates ascending by tag without allocating —
    // this runs on the container's per-lookup path.
    let key_mask = descriptor.key_mask();
    encoder
        .array(key_mask.len() as u64)
        .expect("Vec<u8> writes are infallible");
    for tag in key_mask.iter() {
        let prop = descriptor
            .property(tag)
            .expect("tag comes from this descriptor's key mask");
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

// ===== Value-level struct operations (dots-cpp `Struct` method family) =====
//
// These free functions accept any two `&dyn StructValue`s — typed
// structs and `AnyStruct` interoperate freely as long as they share the
// same `&'static StructDescriptor`. A descriptor mismatch is a caller
// bug and panics: silently comparing differently-typed values would
// produce garbage orderings.

fn assert_shared_descriptor(
    lhs: &dyn StructValue,
    rhs: &dyn StructValue,
) -> &'static StructDescriptor {
    let d = lhs.descriptor();
    assert!(
        core::ptr::eq(d, rhs.descriptor()),
        "struct value operation requires matching descriptors: `{}` vs `{}`",
        d.name,
        rhs.descriptor().name,
    );
    d
}

/// Masked property-wise equality — the analogue of dots-cpp's
/// `Struct::_equal(rhs, includedProperties)`.
///
/// A property in `mask` is equal when it is unset on both sides, or set
/// on both sides with equal values. Pass [`PropertySet::ALL`] to compare
/// every property. Panics on a descriptor mismatch.
pub fn equal(lhs: &dyn StructValue, rhs: &dyn StructValue, mask: PropertySet) -> bool {
    let descriptor = assert_shared_descriptor(lhs, rhs);
    let (lv, rv) = (lhs.valid_set(), rhs.valid_set());
    for prop in descriptor.properties {
        if !mask.has(prop.tag) {
            continue;
        }
        let (l, r) = (lv.has(prop.tag), rv.has(prop.tag));
        if l != r {
            return false;
        }
        if !l {
            continue;
        }
        // SAFETY: both buffers follow this shared descriptor's layout,
        // so `prop.offset` is in-range for each and the slots are valid
        // for the thunk's `Option<T>` / bare-`T` type.
        if !unsafe {
            (prop.vtable.eq_in_place)(
                lhs.data_ptr().add(prop.offset),
                rhs.data_ptr().add(prop.offset),
            )
        } {
            return false;
        }
    }
    true
}

/// Key-property equality — the analogue of dots-cpp's `Struct::_same`:
/// do the two values identify the *same instance*? Non-key properties
/// are ignored. Panics on a descriptor mismatch.
pub fn same(lhs: &dyn StructValue, rhs: &dyn StructValue) -> bool {
    equal(lhs, rhs, lhs.descriptor().key_mask())
}

/// Masked total order — lexicographic over the properties in `mask`, in
/// descriptor (declaration) order, with an unset property sorting before
/// any set one. Replaces dots-cpp's `_less`/`_lessEqual`/`_greater`/
/// `_greaterEqual` quartet with a single `Ordering`. Coherent with
/// [`equal`]: `Ordering::Equal` ⇔ `equal(lhs, rhs, mask)`.
///
/// Panics on a descriptor mismatch.
pub fn cmp_masked(
    lhs: &dyn StructValue,
    rhs: &dyn StructValue,
    mask: PropertySet,
) -> core::cmp::Ordering {
    let descriptor = assert_shared_descriptor(lhs, rhs);
    let (lv, rv) = (lhs.valid_set(), rhs.valid_set());
    for prop in descriptor.properties {
        if !mask.has(prop.tag) {
            continue;
        }
        let (l, r) = (lv.has(prop.tag), rv.has(prop.tag));
        let ord = match (l, r) {
            (false, false) => core::cmp::Ordering::Equal,
            (false, true) => core::cmp::Ordering::Less,
            (true, false) => core::cmp::Ordering::Greater,
            // SAFETY: see `equal` — shared descriptor layout.
            (true, true) => unsafe {
                (prop.vtable.cmp_in_place)(
                    lhs.data_ptr().add(prop.offset),
                    rhs.data_ptr().add(prop.offset),
                )
            },
        };
        if ord != core::cmp::Ordering::Equal {
            return ord;
        }
    }
    core::cmp::Ordering::Equal
}

/// The set of properties on which the two values differ — the analogue
/// of dots-cpp's `Struct::_diffProperties(other, includedProperties)`.
///
/// A tag lands in the result when it is set on exactly one side
/// (validity difference) or set on both with unequal values. Only tags
/// in `mask` are considered. The result is exactly the update mask that
/// would make a receiver of `lhs` mirror `rhs` when applied via
/// [`AnyStruct::copy_from`] — diff and copy are inverse operations.
///
/// Panics on a descriptor mismatch.
pub fn diff_properties(
    lhs: &dyn StructValue,
    rhs: &dyn StructValue,
    mask: PropertySet,
) -> PropertySet {
    let descriptor = assert_shared_descriptor(lhs, rhs);
    let (lv, rv) = (lhs.valid_set(), rhs.valid_set());
    // Set on exactly one side → different by validity alone.
    let mut diff = (lv ^ rv) & mask;
    // Set on both sides → compare values.
    for prop in descriptor.properties {
        if !((lv & rv) & mask).has(prop.tag) {
            continue;
        }
        // SAFETY: see `equal` — shared descriptor layout.
        if !unsafe {
            (prop.vtable.eq_in_place)(
                lhs.data_ptr().add(prop.offset),
                rhs.data_ptr().add(prop.offset),
            )
        } {
            diff = diff.with_tag(prop.tag);
        }
    }
    diff
}

/// True when every property named in `mask` is set on `value` —
/// dots-cpp `Struct::_hasProperties` (subset flavor). For an exact
/// match compare `value.valid_set() == mask` directly.
pub fn has_properties(value: &dyn StructValue, mask: PropertySet) -> bool {
    mask.is_subset_of(value.valid_set())
}

/// Validate that every property named in `mask` is set on `value`,
/// returning a [`MissingProperties`] error naming the absent ones —
/// the `Result`-shaped analogue of dots-cpp's
/// `Struct::_assertHasProperties`.
pub fn expect_properties(
    value: &dyn StructValue,
    mask: PropertySet,
) -> Result<(), MissingProperties> {
    let missing = mask - value.valid_set();
    if missing.is_empty() {
        return Ok(());
    }
    let descriptor = value.descriptor();
    Err(MissingProperties {
        type_name: descriptor.name.into(),
        missing: descriptor
            .properties
            .iter()
            .filter(|p| missing.has(p.tag))
            .map(|p| p.name.into())
            .collect(),
    })
}

/// Error returned by [`expect_properties`]: the value is missing
/// properties the caller requires. Lists the missing property names
/// for diagnostics, like dots-cpp's `_assertHasProperties` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingProperties {
    /// DOTS type name of the offending value.
    pub type_name: alloc::string::String,
    /// Names of the required-but-unset properties, in declaration
    /// order.
    pub missing: Vec<alloc::string::String>,
}

impl core::fmt::Display for MissingProperties {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{} instance is missing expected properties: ", self.type_name)?;
        for (i, name) in self.missing.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(name)?;
        }
        Ok(())
    }
}

impl core::error::Error for MissingProperties {}

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
/// Seeds a zeroed `T` (all-`None`, with bare-`T` keys initialized via
/// their `init` thunks) and applies the wire updates onto it via the
/// descriptor. Does not require `T: Default`.
pub fn decode_typed_from_slice<T>(bytes: &[u8]) -> Result<T, DecodeError>
where
    T: StructValue,
{
    let mut decoder = minicbor::Decoder::new(bytes);
    decode_typed_from_decoder(&mut decoder)
}

/// Decode a typed struct from an active decoder. Useful when the
/// caller is reading a stream of CBOR items and needs to know how
/// many bytes were consumed (via `decoder.position()`).
///
/// Mirrors [`AnyStruct::new`] on a stack buffer: a zero-initialized `T`
/// is a valid `None` for every `Option<T>` field, then each property's
/// `init` thunk seeds its slot (a bare-`T` key becomes `T::default()`),
/// leaving the whole buffer a valid `T` before the first wire write.
/// This is what lets us drop the `T: Default` bound — the seed is built
/// from the descriptor's thunks, not the type's `Default` impl.
pub fn decode_typed_from_decoder<T>(decoder: &mut CborDecoder<'_>) -> Result<T, DecodeError>
where
    T: StructValue,
{
    decode_seeded_typed::<T>(|descriptor, base| {
        let mut valid = PropertySet::EMPTY;
        // SAFETY: decode_seeded_typed supplies initialized storage matching
        // the descriptor for T.
        unsafe { decode_into_raw(descriptor, base, &mut valid, decoder) }
    })
}

/// Decode a complete canonical key directly into T's key property slots.
/// Non-key slots retain their initialized, absent state.
pub(crate) fn decode_typed_key<T: StructValue>(bytes: &[u8]) -> Result<T, DecodeError> {
    let mut decoder = minicbor::Decoder::new(bytes);
    let key_mask = T::type_descriptor().key_mask();
    let len = decoder.array()?.ok_or_else(|| {
        DecodeError::message("indefinite-length array not supported in a canonical key")
    })?;
    if len != key_mask.len() as u64 {
        return Err(DecodeError::message(
            "canonical key length does not match the type's key property count",
        ));
    }
    decode_seeded_typed::<T>(|descriptor, base| {
        for tag in key_mask.iter() {
            if decoder.datatype()? == minicbor::data::Type::Null {
                return Err(DecodeError::message(
                    "invalid instance reference: canonical key must not contain null elements",
                ));
            }
            let prop = descriptor
                .property(tag)
                .expect("tag comes from this descriptor's key mask");
            // SAFETY: decode_seeded_typed supplies initialized storage for T;
            // each property offset and decode thunk belong to its descriptor.
            unsafe {
                (prop.vtable.decode_value)(base.add(prop.offset), &mut decoder)?;
            }
        }
        if decoder.position() != bytes.len() {
            return Err(DecodeError::message("trailing bytes after canonical key"));
        }
        Ok(())
    })
}

// Share initialization and error cleanup between map and key-array decoding.
fn decode_seeded_typed<T: StructValue>(
    decode: impl FnOnce(&'static StructDescriptor, *mut u8) -> Result<(), DecodeError>,
) -> Result<T, DecodeError> {
    let descriptor = T::type_descriptor();
    let mut slot = core::mem::MaybeUninit::<T>::zeroed();
    let base = slot.as_mut_ptr() as *mut u8;
    // SAFETY: `T: StructValue` enforces the layout invariant — `descriptor`
    // matches `T`'s `(size, align)` and the offsets come from
    // `offset_of!(T, _)`. The buffer is zero-initialized (a valid `None`
    // for every `Option<T>`); each `init` thunk then seeds its slot, so
    // from here on every byte is a valid `T`.
    unsafe {
        for prop in descriptor.properties {
            (prop.vtable.init)(base.add(prop.offset));
        }
    }
    // If decode bails (bad wire, missing key), the seeded buffer is a
    // fully-valid `T` that must be dropped through the property thunks —
    // `assume_init().drop()` can't run because we never reach it. The
    // guard handles that; it's `forget`-ten on the success path.
    let guard = RawStructGuard { descriptor, base };
    decode(descriptor, base)?;
    core::mem::forget(guard);
    // SAFETY: decode succeeded, so the buffer is a fully-initialized `T`.
    Ok(unsafe { slot.assume_init() })
}

/// Drops every property slot of a raw, descriptor-seeded struct buffer.
/// Used to clean up a partially-decoded stack buffer when decode errors
/// before the value is moved out (see [`decode_typed_from_decoder`]).
struct RawStructGuard {
    descriptor: &'static StructDescriptor,
    base: *mut u8,
}

impl Drop for RawStructGuard {
    fn drop(&mut self) {
        for prop in self.descriptor.properties {
            // SAFETY: every slot was seeded via its `init` thunk and is
            // left a valid value by the decode thunks even on error
            // (`dots_decode` reads the new value before overwriting the
            // old), so dropping in place through the thunk is sound.
            unsafe {
                (prop.vtable.drop_in_place)(self.base.add(prop.offset));
            }
        }
    }
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

// ===== Generic per-type thunk helpers =====
//
// The proc-macro emits `PropertyVtable` statics whose function-pointer
// fields name these helpers with explicit type parameters, e.g.
//
//     PropertyVtable {
//         is_set: ::dots_rs_core::layout::opt_is_set::<u32>,
//         encode_value: ::dots_rs_core::layout::opt_encode::<u32>,
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

/// Clear the `Option<T>` at `ptr`: drop any existing value and write a
/// real `None`. Unlike zeroing, this is correct for niche-optimized
/// layouts (e.g. `Option<bool>`, whose `None` is *not* the zero
/// pattern, and `Option<AnyObject>`, whose niche lives in a length
/// field).
///
/// # Safety
///
/// `ptr` must point to a valid `Option<T>`.
pub unsafe fn opt_set_none<T>(ptr: *mut u8) {
    let p = ptr as *mut Option<T>;
    // SAFETY: caller-upheld; the slot is a valid `Option<T>`, so
    // drop-then-write is sound.
    unsafe {
        core::ptr::drop_in_place(p);
        core::ptr::write(p, None);
    }
}

/// Move the `Option<T>` at `src` into `dst`: drop any existing value at
/// `dst`, transfer `src`'s value (handing over owned heap allocations
/// rather than deep-cloning them), and leave `src` as `None`. This is
/// the move counterpart of [`opt_clone`]; it needs no `T: Clone` bound
/// because nothing is copied. Works uniformly for `Option<Vec<_>>` too
/// — moving an `Option` is type-agnostic.
///
/// # Safety
///
/// `src` and `dst` must each point to a valid `Option<T>`.
pub unsafe fn opt_take<T>(src: *mut u8, dst: *mut u8) {
    let s = src as *mut Option<T>;
    let d = dst as *mut Option<T>;
    // SAFETY: caller-upheld. `replace` reads `src`'s value out and
    // writes `None` back; `dst`'s prior value is dropped before the
    // moved value is written.
    unsafe {
        let moved = core::ptr::replace(s, None);
        core::ptr::drop_in_place(d);
        core::ptr::write(d, moved);
    }
}

/// Compare the `Option<T>` slots at `lhs` and `rhs` for equality —
/// validity and value, per [`opt_field_eq`](crate::field_ops::opt_field_eq).
///
/// # Safety
///
/// `lhs` and `rhs` must each point to a valid `Option<T>`.
pub unsafe fn opt_eq<T: DotsFieldOps>(lhs: *const u8, rhs: *const u8) -> bool {
    // SAFETY: caller-upheld.
    let (a, b) = unsafe { (&*(lhs as *const Option<T>), &*(rhs as *const Option<T>)) };
    crate::field_ops::opt_field_eq(a, b)
}

/// Order the `Option<T>` slots at `lhs` and `rhs` — `None` first, per
/// [`opt_field_cmp`](crate::field_ops::opt_field_cmp).
///
/// # Safety
///
/// `lhs` and `rhs` must each point to a valid `Option<T>`.
pub unsafe fn opt_cmp<T: DotsFieldOps>(lhs: *const u8, rhs: *const u8) -> core::cmp::Ordering {
    // SAFETY: caller-upheld.
    let (a, b) = unsafe { (&*(lhs as *const Option<T>), &*(rhs as *const Option<T>)) };
    crate::field_ops::opt_field_cmp(a, b)
}

/// Swap the `Option<T>` slots at `a` and `b` in place.
///
/// # Safety
///
/// `a` and `b` must each point to a valid `Option<T>`, and must not
/// alias.
pub unsafe fn opt_swap<T>(a: *mut u8, b: *mut u8) {
    // SAFETY: caller-upheld; `swap` moves both values without dropping.
    unsafe {
        core::ptr::swap(a as *mut Option<T>, b as *mut Option<T>);
    }
}

/// Merge the `Option<T>` slot at `src` into `dst`, per
/// [`opt_field_merge`](crate::field_ops::opt_field_merge) — recursive
/// for struct-typed `T`, clone-assign otherwise, no-op when `src` is
/// `None`.
///
/// # Safety
///
/// `src` and `dst` must each point to a valid `Option<T>` and must not
/// alias.
pub unsafe fn opt_merge<T: DotsFieldOps>(src: *const u8, dst: *mut u8) {
    // SAFETY: caller-upheld.
    let (s, d) = unsafe { (&*(src as *const Option<T>), &mut *(dst as *mut Option<T>)) };
    crate::field_ops::opt_field_merge(d, s);
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

/// Initializer for an `Option<T>` slot: writes a real `None`, over the
/// (possibly invalid) zeroed bytes *without* dropping them.
///
/// A zeroed slot is **not** universally a valid `None`. For most types
/// the niche that `Option` uses is at the all-zero value, but it need
/// not be: `Option<AnyObject>`, for instance, puts its niche in a
/// `String`/`Vec` length field, so the zero pattern is neither `None`
/// nor a valid `Some` — reading or dropping it is UB. Writing `None`
/// explicitly makes every `Option<T>` slot valid regardless of where
/// the niche lives.
///
/// # Safety
///
/// `ptr` must point to an `Option<T>`-sized, `Option<T>`-aligned slot
/// that is safe to overwrite without dropping (e.g. freshly
/// zero-allocated). `write` does not drop the prior bytes.
pub unsafe fn opt_init<T>(ptr: *mut u8) {
    // SAFETY: caller-upheld; `write` does not drop the prior bytes.
    unsafe {
        core::ptr::write(ptr as *mut Option<T>, None);
    }
}

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

/// "Clear" a bare-`T` key slot by restoring `T::default()`, dropping the
/// previous value first. Key properties are never cleared by
/// [`AnyStruct::copy_from`] (key tags are skipped), but the vtable slot
/// must be total and sound, so this keeps the bare-`T` slot a valid,
/// droppable `T` should it ever be invoked.
///
/// # Safety
///
/// `ptr` must point to a valid `T`.
pub unsafe fn key_set_none<T: Default>(ptr: *mut u8) {
    let p = ptr as *mut T;
    // SAFETY: caller-upheld; drop-then-write keeps the slot valid.
    unsafe {
        core::ptr::drop_in_place(p);
        core::ptr::write(p, T::default());
    }
}

/// Move the bare `T` at `src` into `dst`, dropping `dst`'s prior value
/// and restoring `src` to `T::default()` so it stays a valid, droppable
/// `T`. Key properties are never moved by [`AnyStruct::copy_take`]
/// (key tags are skipped), but the vtable slot must be total and
/// sound.
///
/// # Safety
///
/// `src` and `dst` must each point to a valid `T`.
pub unsafe fn key_take<T: Default>(src: *mut u8, dst: *mut u8) {
    let s = src as *mut T;
    let d = dst as *mut T;
    // SAFETY: caller-upheld; `replace` keeps `src` valid, drop-then-
    // write keeps `dst` valid.
    unsafe {
        let moved = core::ptr::replace(s, T::default());
        core::ptr::drop_in_place(d);
        core::ptr::write(d, moved);
    }
}

/// Compare the bare-`T` key slots at `lhs` and `rhs` for equality. A
/// key is always set, so this is a plain value comparison.
///
/// # Safety
///
/// `lhs` and `rhs` must each point to a valid `T`.
pub unsafe fn key_eq<T: DotsFieldOps>(lhs: *const u8, rhs: *const u8) -> bool {
    // SAFETY: caller-upheld.
    unsafe { (*(lhs as *const T)).dots_eq(&*(rhs as *const T)) }
}

/// Order the bare-`T` key slots at `lhs` and `rhs`.
///
/// # Safety
///
/// `lhs` and `rhs` must each point to a valid `T`.
pub unsafe fn key_cmp<T: DotsFieldOps>(lhs: *const u8, rhs: *const u8) -> core::cmp::Ordering {
    // SAFETY: caller-upheld.
    unsafe { (*(lhs as *const T)).dots_cmp(&*(rhs as *const T)) }
}

/// Swap the bare-`T` key slots at `a` and `b` in place.
///
/// # Safety
///
/// `a` and `b` must each point to a valid `T`, and must not alias.
pub unsafe fn key_swap<T>(a: *mut u8, b: *mut u8) {
    // SAFETY: caller-upheld; `swap` moves both values without dropping.
    unsafe {
        core::ptr::swap(a as *mut T, b as *mut T);
    }
}

/// Merge the bare-`T` key slot at `src` into `dst`. A key is always
/// set on both sides, so this is [`DotsFieldOps::dots_merge`] on the
/// values — a clone-assign for every key-eligible type.
///
/// # Safety
///
/// `src` and `dst` must each point to a valid `T` and must not alias.
pub unsafe fn key_merge<T: DotsFieldOps>(src: *const u8, dst: *mut u8) {
    // SAFETY: caller-upheld.
    unsafe {
        (*(dst as *mut T)).dots_merge(&*(src as *const T));
    }
}
