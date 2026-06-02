//! Open `any` field support.
//!
//! Verifies that:
//! - A struct field of type `Option<AnyObject>` round-trips through the
//!   descriptor-driven codec (it flows through the scalar property
//!   thunks like any other leaf type).
//! - The descriptor reports `FieldKind::Any` for the property.
//! - `to_any` wraps a live struct and the wrapped object survives the
//!   outer encode/decode, then decodes back to the original via its
//!   static descriptor.

use dots_core::{AnyObject, FieldKind, decode_typed_from_slice, encode_to_vec, to_any};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Ping")]
struct Ping {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    note: Option<String>,
}

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Envelope")]
struct Envelope {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    payload: Option<AnyObject>,
}

#[test]
fn any_field_kind_is_any() {
    let payload_prop = Envelope::DESCRIPTOR
        .property(2)
        .expect("payload property must exist");
    assert!(matches!(payload_prop.kind, FieldKind::Any));
}

#[test]
fn envelope_with_any_roundtrips() {
    let ping = Ping {
        id: Some(42),
        note: Some("hi".into()),
    };
    let original = Envelope {
        id: Some(1),
        payload: Some(to_any(&ping)),
    };

    let bytes = encode_to_vec(&original);
    let decoded: Envelope = decode_typed_from_slice(&bytes).expect("typed decode succeeds");
    assert_eq!(original, decoded);

    // The contained type identity survives in the clear.
    let any = decoded.payload.expect("payload set");
    assert_eq!(any.type_name(), "Ping");

    // And the opaque payload decodes back to the original Ping via the
    // contained type's static descriptor.
    let recovered = any.decode(Ping::DESCRIPTOR).expect("decode contained Ping");
    let recovered: &Ping = recovered.as_typed::<Ping>().expect("downcast to Ping");
    assert_eq!(recovered, &ping);
}

#[test]
fn empty_any_field_roundtrips() {
    // An Envelope with no payload set: the `any` field is simply absent
    // from the CBOR map.
    let original = Envelope {
        id: Some(7),
        payload: None,
    };
    let bytes = encode_to_vec(&original);
    let decoded: Envelope = decode_typed_from_slice(&bytes).expect("typed decode succeeds");
    assert_eq!(original, decoded);
    assert!(decoded.payload.is_none());
}
