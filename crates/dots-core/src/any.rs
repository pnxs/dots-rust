//! The open `any` field type.
//!
//! [`AnyObject`] stores an arbitrary DOTS object inside a single
//! property. Unlike a nested struct — whose concrete type is fully
//! determined by the parent's [`StructDescriptor`] — an `any` field's
//! contained type is not derivable from the static schema, so the type
//! identity has to travel *inside* the value. That self-describing
//! value is the new thing this type adds.
//!
//! # Wire format
//!
//! `AnyObject` is an **opaque envelope**: a 2-element CBOR array of
//! `[ type-name (text), payload (byte string) ]`. The payload is the
//! contained object pre-serialized to canonical CBOR (the same
//! tag-keyed map every DOTS struct uses). A broker can read the array,
//! see the type identity in the clear, and forward the payload
//! verbatim *without ever needing the inner type's descriptor* —
//! routing stays transparent and the inner object's lifecycle is
//! decoupled from the outer.
//!
//! ```text
//! any payload = Ping{ id: 42 }      ; Ping serializes to A1 01 18 2A
//!     82                            # array(2)
//!        64 50696E67               # text(4) "Ping"   <- type name
//!        44 A101182A               # bytes(4)         <- inner object (opaque)
//! ```
//!
//! # Decode is a separate step
//!
//! `AnyObject` is intentionally **decode-free**: serializing an `any`
//! field just moves `(type_name, payload)`; it never decodes the inner
//! object. Recovering the stored struct is an explicit operation that
//! resolves the type name against a registry — see the broker's
//! `Registry::from_any`. A receiver that doesn't know the contained
//! type simply keeps the bytes opaque and passes them through.
//!
//! # As a struct field
//!
//! A `#[derive(DotsStruct)]` field of type `Option<AnyObject>` works
//! through the same scalar property thunks as any other leaf type —
//! `AnyObject` is a normal [`DotsField`]. Marking such a field
//! `#[dots(key)]` is rejected by the derive: comparing opaque
//! heterogeneous blobs as keys is a footgun.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::{
    AnyStruct, DotsTypeKind, FieldKind, StructDescriptor, StructValue,
    layout::{CborDecoder, CborEncoder, DecodeError, DotsField, EncodeError, encode_to_vec},
};

/// An arbitrary DOTS object stored as an opaque, self-describing CBOR
/// envelope: the contained type's name plus its canonical-CBOR payload.
///
/// This is serialization-agnostic pure storage — it carries no
/// descriptor and never decodes its payload. See the module docs for
/// the wire format and the decode contract.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AnyObject {
    type_name: String,
    payload: Vec<u8>,
}

impl AnyObject {
    /// Construct from a type name and a pre-serialized canonical-CBOR
    /// payload. Most callers want [`to_any`] / [`from_struct_value`]
    /// instead, which produce the payload from a live value.
    ///
    /// [`from_struct_value`]: AnyObject::from_struct_value
    pub fn new(type_name: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            type_name: type_name.into(),
            payload: payload.into(),
        }
    }

    /// The contained type's DOTS name. Always available without a
    /// descriptor — this is the type identity carried in the clear.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// The raw canonical-CBOR payload bytes of the contained object.
    /// Suitable for verbatim reroute by a broker that doesn't know the
    /// inner type.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consume `self`, returning the owned payload bytes.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }

    /// `true` for a default-constructed (empty) `AnyObject`. Mirrors
    /// dots-cpp `AnyObject::empty()`, which keys off the type name.
    pub fn is_empty(&self) -> bool {
        self.type_name.is_empty()
    }

    /// Wrap a live DOTS struct into an `AnyObject` by serializing its
    /// set properties to canonical CBOR. The type name is taken from
    /// the value's descriptor. See also the free function [`to_any`].
    pub fn from_struct_value(value: &dyn StructValue) -> Self {
        Self {
            type_name: value.descriptor().name.to_string(),
            payload: encode_to_vec(value),
        }
    }

    /// Recover the contained object as a layout-compatible
    /// [`AnyStruct`], given the contained type's static descriptor.
    ///
    /// This is the registry-free, low-level decode: the caller supplies
    /// the descriptor directly (e.g. `Foo::DESCRIPTOR`). The broker's
    /// `Registry::from_any` is the convenience wrapper that resolves the
    /// descriptor from the [`type_name`](AnyObject::type_name) first.
    pub fn decode(&self, descriptor: &'static StructDescriptor) -> Result<AnyStruct, DecodeError> {
        AnyStruct::decode_from_slice(descriptor, &self.payload)
    }
}

impl DotsField for AnyObject {
    #[inline]
    fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        // Opaque envelope: [ type-name, payload-bytes ].
        e.array(2)?;
        e.str(&self.type_name)?;
        e.bytes(&self.payload)?;
        Ok(())
    }

    #[inline]
    fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
        let len = d.array()?.ok_or_else(|| {
            DecodeError::message("indefinite-length array not supported in `any` envelope")
        })?;
        if len != 2 {
            return Err(DecodeError::message(
                "invalid `any` envelope: expected a 2-element array",
            ));
        }
        let type_name = d.str()?.to_string();
        let payload = d.bytes()?.to_vec();
        Ok(Self { type_name, payload })
    }
}

impl DotsTypeKind for AnyObject {
    const KIND: FieldKind = FieldKind::Any;
}

/// Wrap a live DOTS struct into an [`AnyObject`] by serializing its set
/// properties to canonical CBOR. Free-function form of
/// [`AnyObject::from_struct_value`]; mirrors dots-cpp `to_any`.
pub fn to_any(value: &dyn StructValue) -> AnyObject {
    AnyObject::from_struct_value(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn default_is_empty() {
        let any = AnyObject::default();
        assert!(any.is_empty());
        assert!(any.type_name().is_empty());
        assert!(any.payload().is_empty());
    }

    #[test]
    fn cbor_wire_format_is_opaque_envelope() {
        // Mirrors dots-cpp TestAnyObject.cborWireFormat_IsOpaqueEnvelope:
        // [ "Ping", h'A101182A' ]
        let any = AnyObject::new("Ping", vec![0xA1, 0x01, 0x18, 0x2A]);
        let mut buf: Vec<u8> = Vec::new();
        let mut enc = minicbor::Encoder::new(&mut buf);
        any.dots_encode(&mut enc).unwrap();
        assert_eq!(
            buf,
            vec![
                0x82, // array(2)
                0x64, b'P', b'i', b'n', b'g', // text(4) "Ping"
                0x44, 0xA1, 0x01, 0x18, 0x2A, // bytes(4)
            ]
        );
    }

    #[test]
    fn cbor_round_trip_preserves_type_name_and_payload() {
        let any = AnyObject::new("Ping", vec![0xA1, 0x01, 0x18, 0x2A]);
        let mut buf: Vec<u8> = Vec::new();
        any.dots_encode(&mut minicbor::Encoder::new(&mut buf)).unwrap();
        let decoded = AnyObject::dots_decode(&mut minicbor::Decoder::new(&buf)).unwrap();
        assert_eq!(decoded, any);
        assert_eq!(decoded.type_name(), "Ping");
        assert_eq!(decoded.payload(), &[0xA1, 0x01, 0x18, 0x2A]);
    }

    #[test]
    fn wrong_array_size_is_rejected() {
        // A 3-element array is not a valid envelope.
        let mut buf: Vec<u8> = Vec::new();
        let mut enc = minicbor::Encoder::new(&mut buf);
        enc.array(3).unwrap();
        enc.str("Ping").unwrap();
        enc.bytes(&[0xA0]).unwrap();
        enc.u8(0).unwrap();
        assert!(AnyObject::dots_decode(&mut minicbor::Decoder::new(&buf)).is_err());
    }
}
