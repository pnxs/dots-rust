//! CBOR codec helpers for DOTS structs.
//!
//! Generated `#[derive(DotsStruct)]` code emits `minicbor::Encode<()>` and
//! `minicbor::Decode<'_, ()>` impls; these helpers provide the common
//! "to bytes" / "from bytes" entry points without making callers
//! reach into `minicbor` directly.
//!
//! # Wire format
//!
//! A DOTS struct serializes as a CBOR map keyed by the property *tag*
//! (a positive integer). Only properties whose [`PropertySet`] bit is
//! set — i.e. fields that are `Some(_)` — appear in the map. Unknown
//! tags encountered during decode are skipped, providing forward
//! compatibility when peers add new properties.
//!
//! [`PropertySet`]: crate::PropertySet

use alloc::vec::Vec;

use minicbor::{Decode, Encode, decode, encode};

/// Encode a value into a freshly allocated `Vec<u8>`.
///
/// The `Vec<u8>` writer never fails, so this returns the buffer directly.
pub fn encode_to_vec<T: Encode<()>>(value: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    minicbor::encode(value, &mut buf).expect("Vec<u8> writes are infallible");
    buf
}

/// Encode a value into an existing buffer, appending to it.
pub fn encode_into_vec<T: Encode<()>>(value: &T, buf: &mut Vec<u8>) {
    minicbor::encode(value, buf).expect("Vec<u8> writes are infallible");
}

/// Decode a value from a CBOR byte slice. The slice must contain the
/// complete encoded value; trailing bytes are not allowed.
pub fn decode_from_slice<'a, T: Decode<'a, ()>>(bytes: &'a [u8]) -> Result<T, decode::Error> {
    minicbor::decode(bytes)
}

/// Encode error alias for `minicbor::encode::Error<W::Error>`.
pub type EncodeError<E> = encode::Error<E>;

/// Decode error alias for `minicbor::decode::Error`.
pub type DecodeError = decode::Error;
