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

use bytes::Bytes;
use dots_core::{
    DecodeError, DynamicStruct, DynamicStructDescriptor, PropertySet, StructValue, Transmittable,
    decode_typed_from_decoder, encode_into_vec,
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

/// Encode a transmission, producing the full v2 frame (size prefix +
/// header + payload).
///
/// Accepts any [`Transmittable`] payload — typed Rust structs,
/// `AnyStruct`, or `DynamicStruct`. Caller is responsible for setting
/// `header.type_name` to match `payload.type_name()`; the framer
/// itself does not validate or override it.
pub fn encode_transmission(header: &DotsHeader, payload: &dyn Transmittable) -> Vec<u8> {
    let mut out = Vec::with_capacity(SIZE_PREFIX_LEN + 64);
    encode_transmission_into(header, payload, &mut out);
    out
}

/// Append a transmission to an existing buffer.
///
/// Lets callers (e.g. the async transport's encoder) reuse a scratch
/// buffer across many sends, eliminating the per-send allocation. Also
/// usable for building a single buffer of back-to-back frames — each
/// call appends one complete frame whose size prefix references only
/// that frame's body.
pub fn encode_transmission_into(
    header: &DotsHeader,
    payload: &dyn Transmittable,
    out: &mut Vec<u8>,
) {
    encode_transmission_with_mask_into(header, payload, payload.valid_set(), out);
}

/// Same as [`encode_transmission_into`], but emits only the payload
/// properties whose tag is in `mask`. Used by the remove path to
/// publish a key-only payload alongside `header.remove_obj = true`.
pub fn encode_transmission_with_mask_into(
    header: &DotsHeader,
    payload: &dyn Transmittable,
    mask: PropertySet,
    out: &mut Vec<u8>,
) {
    let frame_start = out.len();
    out.extend_from_slice(&[SIZE_PREFIX_MARKER, 0, 0, 0, 0]);
    encode_into_vec(header, out);
    let mut encoder = dots_core::minicbor::Encoder::new(&mut *out);
    payload
        .encode_into(mask, &mut encoder)
        .expect("Vec<u8> writes are infallible");
    patch_size_prefix(out, frame_start);
}

impl Transmission {
    /// Encode this transmission into a v2 frame.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(SIZE_PREFIX_LEN + 64);
        self.encode_into(&mut out);
        out
    }

    /// Append this transmission's frame bytes to an existing buffer.
    /// Same scratch-buffer / batching benefits as
    /// [`encode_transmission_into`].
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        encode_transmission_into(&self.header, &self.payload, out);
    }
}

/// Patch the 4-byte big-endian size field of a frame whose 5-byte
/// prefix begins at `frame_start` in `buf`.
fn patch_size_prefix(buf: &mut [u8], frame_start: usize) {
    let frame_end = buf.len();
    let body_size = (frame_end - frame_start - SIZE_PREFIX_LEN) as u32;
    buf[frame_start + 1..frame_start + SIZE_PREFIX_LEN]
        .copy_from_slice(&body_size.to_be_bytes());
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

// ===== RawTransmission: header decoded, payload kept as raw bytes =====

/// Inbound transmission with the header eagerly decoded and the
/// payload retained as raw `Bytes`.
///
/// This is the broker's preferred form on the receive side: routing,
/// cache lookup, and is_from_myself stamping all consult `header`,
/// while fan-out forwards `payload` verbatim — eliminating the
/// per-message `DynamicStruct` decode/clone/re-encode round-trip
/// that [`Transmission`] forces when the broker only needs to rewrite
/// the header.
///
/// `payload` is a refcounted slice of the inbound buffer (no copy).
/// The internal-type dispatch and the cache-update path can still
/// materialise a [`DynamicStruct`] on demand via
/// [`RawTransmission::decode_payload`].
#[derive(Debug, Clone)]
pub struct RawTransmission {
    pub header: DotsHeader,
    /// Raw CBOR bytes of the payload struct (no size prefix, no header).
    pub payload: Bytes,
}

impl RawTransmission {
    /// Decode a complete v2 frame into a `RawTransmission`.
    ///
    /// `frame` must contain exactly one full frame (5-byte prefix +
    /// header + payload). The codec is responsible for length-checking
    /// against the size prefix and slicing out one frame's worth of
    /// bytes before calling this. If the buffer is short, returns
    /// [`FramingError::NeedMoreData`] so callers that pass partial
    /// buffers still get a useful diagnostic.
    pub fn decode(frame: Bytes) -> Result<Self, FramingError> {
        let body_size = parse_size_prefix(&frame)? as usize;
        let total = SIZE_PREFIX_LEN + body_size;
        if frame.len() < total {
            return Err(FramingError::NeedMoreData {
                have: frame.len(),
                need: total,
            });
        }
        let body = &frame[SIZE_PREFIX_LEN..total];
        let mut decoder = dots_core::minicbor::Decoder::new(body);
        let header: DotsHeader = decode_typed_from_decoder(&mut decoder)?;
        let payload_start_in_body = decoder.position();
        let payload_start = SIZE_PREFIX_LEN + payload_start_in_body;
        let payload = frame.slice(payload_start..total);
        Ok(Self { header, payload })
    }

    /// Decode the payload bytes into a [`DynamicStruct`] using the type
    /// named in `header.type_name`. Materialises only on demand — the
    /// hot fan-out path doesn't need this.
    pub fn decode_payload(&self, registry: &Registry) -> Result<DynamicStruct, FramingError> {
        let type_name = self
            .header
            .type_name
            .as_deref()
            .ok_or(FramingError::HeaderMissingTypeName)?;
        let descriptor = lookup_struct(registry, type_name)?;
        Ok(DynamicStruct::decode(descriptor, &self.payload)?)
    }
}

/// Append a v2 frame with `new_header` and the given raw payload bytes
/// to `out`. Mirrors [`encode_transmission_into`] but takes the
/// payload pre-encoded — used by the broker to rewrite a transmission's
/// header (sender, server_sent_time) while reusing the original
/// payload bytes verbatim.
pub fn encode_frame_with_header(header: &DotsHeader, payload_bytes: &[u8], out: &mut Vec<u8>) {
    let frame_start = out.len();
    out.extend_from_slice(&[SIZE_PREFIX_MARKER, 0, 0, 0, 0]);
    encode_into_vec(header, out);
    out.extend_from_slice(payload_bytes);
    patch_size_prefix(out, frame_start);
}

