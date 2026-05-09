//! v2 transmission framing.
//!
//! The wire format is a 5-byte size prefix followed by the body:
//!
//! ```text
//!   [0x1A] [BE u32: body_size] [CBOR DotsHeader] [CBOR payload]
//!   └─────────── 5 bytes ──────────┘
//! ```
//!
//! - `0x1A` is CBOR's "uint32 follows" marker (major type 0,
//!   additional info 26). The C++ DOTS implementation always emits
//!   this fixed form regardless of the actual size value, so the
//!   reader can read exactly 5 bytes to learn the body length.
//! - `body_size` is the byte count of the header + payload combined,
//!   *not* including the 5-byte prefix itself.
//! - The body is two concatenated CBOR maps. The reader advances
//!   through them in sequence using a single CBOR decoder.
//!
//! This module is purely synchronous and I/O-free — encode produces
//! `Vec<u8>`, decode operates on `&[u8]` and returns the consumed
//! byte count alongside the parsed transmission. The transport layer
//! (TCP, UDS, ...) layers async I/O on top of these primitives.

use std::sync::Arc;

use dots_core::{
    DecodeError, DynamicStruct, DynamicStructDescriptor, StructValue, decode_typed_from_decoder,
    encode_into_vec,
};

use crate::{DotsHeader, Registry};

/// Length of the size prefix in bytes (`0x1A` + 4-byte big-endian uint32).
pub const SIZE_PREFIX_LEN: usize = 5;

/// Maximum body size accepted by the receiver, matching the C++ default.
/// Frames larger than this are rejected before allocating a buffer.
pub const MAX_BODY_SIZE: u32 = 10 * 1024 * 1024;

/// CBOR marker byte for "uint32 argument follows" — the first byte
/// of every v2 size prefix.
pub const SIZE_PREFIX_MARKER: u8 = 0x1A;

/// A complete v2 transmission: header + payload.
///
/// The payload is held in [`DynamicStruct`] form regardless of how it
/// arrived on the wire. Typed receivers can convert via
/// [`decode_typed_transmission`] instead of going through `Transmission`.
#[derive(Debug, Clone)]
pub struct Transmission {
    pub header: DotsHeader,
    pub payload: DynamicStruct,
}

/// Errors produced by the framing layer.
#[derive(Debug)]
pub enum FramingError {
    /// Buffer doesn't yet contain a complete frame. Caller should read
    /// more bytes and retry. Carries the number of bytes still needed.
    NeedMoreData {
        have: usize,
        need: usize,
    },
    /// Size prefix didn't start with `0x1A`. Likely a desynced stream
    /// or a peer using an unsupported framing format.
    InvalidSizePrefix(u8),
    /// Body size exceeds [`MAX_BODY_SIZE`].
    BodyTooLarge {
        size: u32,
    },
    /// CBOR decode of the header or payload failed.
    Decode(DecodeError),
    /// Header arrived without a `type_name` set, so the receiver can't
    /// pick a payload descriptor.
    HeaderMissingTypeName,
    /// Header named a type that isn't in the receiver's registry.
    UnknownType(String),
}

impl core::fmt::Display for FramingError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NeedMoreData { have, need } => {
                write!(f, "incomplete frame: have {have} bytes, need {need}")
            }
            Self::InvalidSizePrefix(b) => write!(f, "invalid size prefix marker: 0x{b:02x}"),
            Self::BodyTooLarge { size } => {
                write!(f, "body size {size} exceeds maximum {}", MAX_BODY_SIZE)
            }
            Self::Decode(e) => write!(f, "CBOR decode error: {e}"),
            Self::HeaderMissingTypeName => f.write_str("header missing required `type_name`"),
            Self::UnknownType(name) => write!(f, "unknown payload type `{name}`"),
        }
    }
}

impl std::error::Error for FramingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(e) => Some(e),
            _ => None,
        }
    }
}

impl From<DecodeError> for FramingError {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

// ===== Encoding =====

/// Encode a transmission with a typed payload, producing the full v2
/// frame (size prefix + header + payload).
///
/// Caller is responsible for setting `header.type_name` to match
/// `payload`'s descriptor name — the framer itself does not validate
/// or override it.
pub fn encode_typed_transmission(header: &DotsHeader, payload: &dyn StructValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(SIZE_PREFIX_LEN + 64);
    out.extend_from_slice(&[SIZE_PREFIX_MARKER, 0, 0, 0, 0]);
    encode_into_vec(header, &mut out);
    encode_into_vec(payload, &mut out);
    patch_size_prefix(&mut out);
    out
}

impl Transmission {
    /// Encode this transmission (with its dynamic payload) into a v2 frame.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(SIZE_PREFIX_LEN + 64);
        out.extend_from_slice(&[SIZE_PREFIX_MARKER, 0, 0, 0, 0]);
        encode_into_vec(&self.header, &mut out);
        self.payload.encode_into(&mut out);
        patch_size_prefix(&mut out);
        out
    }
}

fn patch_size_prefix(buf: &mut [u8]) {
    let body_size = (buf.len() - SIZE_PREFIX_LEN) as u32;
    buf[1..5].copy_from_slice(&body_size.to_be_bytes());
}

// ===== Decoding =====

/// Parse just the 5-byte size prefix at the head of `bytes`. Returns
/// the body size advertised by the peer.
///
/// Use this from a streaming reader to decide how many more bytes to
/// fetch before attempting [`Transmission::decode`] /
/// [`decode_typed_transmission`].
pub fn parse_size_prefix(bytes: &[u8]) -> Result<u32, FramingError> {
    if bytes.len() < SIZE_PREFIX_LEN {
        return Err(FramingError::NeedMoreData {
            have: bytes.len(),
            need: SIZE_PREFIX_LEN,
        });
    }
    if bytes[0] != SIZE_PREFIX_MARKER {
        return Err(FramingError::InvalidSizePrefix(bytes[0]));
    }
    let size = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    if size > MAX_BODY_SIZE {
        return Err(FramingError::BodyTooLarge { size });
    }
    Ok(size)
}

impl Transmission {
    /// Decode a complete v2 frame into a dynamic transmission.
    ///
    /// `registry` resolves the payload's type by name (taken from
    /// `header.type_name`). Returns the parsed transmission together
    /// with the total number of bytes consumed (`SIZE_PREFIX_LEN +
    /// body_size`), so callers can advance their read buffer.
    pub fn decode(bytes: &[u8], registry: &Registry) -> Result<(Self, usize), FramingError> {
        let body_size = parse_size_prefix(bytes)? as usize;
        let total = SIZE_PREFIX_LEN + body_size;
        if bytes.len() < total {
            return Err(FramingError::NeedMoreData {
                have: bytes.len(),
                need: total,
            });
        }
        let body = &bytes[SIZE_PREFIX_LEN..total];

        let mut decoder = dots_core::minicbor::Decoder::new(body);
        let header: DotsHeader = decode_typed_from_decoder(&mut decoder)?;

        let type_name = header
            .type_name
            .as_deref()
            .ok_or(FramingError::HeaderMissingTypeName)?;
        let descriptor = lookup_struct(registry, type_name)?;
        let payload = DynamicStruct::decode_from_decoder(descriptor, &mut decoder)?;

        Ok((Self { header, payload }, total))
    }
}

/// Decode a v2 frame whose payload type is statically known.
///
/// Skips the registry lookup since `T` is known at the call site.
/// Caller is responsible for any `header.type_name` validation.
pub fn decode_typed_transmission<T>(bytes: &[u8]) -> Result<(DotsHeader, T, usize), FramingError>
where
    T: StructValue + Default,
{
    let body_size = parse_size_prefix(bytes)? as usize;
    let total = SIZE_PREFIX_LEN + body_size;
    if bytes.len() < total {
        return Err(FramingError::NeedMoreData {
            have: bytes.len(),
            need: total,
        });
    }
    let body = &bytes[SIZE_PREFIX_LEN..total];
    let mut decoder = dots_core::minicbor::Decoder::new(body);
    let header: DotsHeader = decode_typed_from_decoder(&mut decoder)?;
    let payload: T = decode_typed_from_decoder(&mut decoder)?;
    Ok((header, payload, total))
}

fn lookup_struct(
    registry: &Registry,
    name: &str,
) -> Result<Arc<DynamicStructDescriptor>, FramingError> {
    use crate::DescriptorEntry;
    match registry.lookup(name) {
        Some(DescriptorEntry::Struct(d)) => Ok(d.clone()),
        Some(DescriptorEntry::Enum(_)) => Err(FramingError::UnknownType(format!(
            "{name} is registered as an enum, not a struct"
        ))),
        None => Err(FramingError::UnknownType(name.into())),
    }
}

