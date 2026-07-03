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

use dots_rs_core::{FieldKind, decode_typed_from_slice, encode_to_vec, to_any};

mod model {
    use dots_rs_core::AnyObject;
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Ping")]
    pub struct Ping {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub note: Option<String>,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Envelope")]
    pub struct Envelope {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub payload: Option<AnyObject>,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Batch")]
    pub struct Batch {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub items: Option<Vec<AnyObject>>,
    }
}
use model::*;

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

/// Ports the descriptor half of dots-cpp
/// `TestAnyObject.vectorOfAny_DescriptorAndCborRoundTrip`: a
/// `vector<any>` field routes through `FieldKind::Vec` with an `Any`
/// element kind.
#[test]
fn vector_of_any_field_kind_is_vec_of_any() {
    let items_prop = Batch::DESCRIPTOR
        .property(2)
        .expect("items property must exist");
    assert!(matches!(items_prop.kind, FieldKind::Vec(&FieldKind::Any)));
}

/// Ports the roundtrip half of dots-cpp
/// `TestAnyObject.vectorOfAny_DescriptorAndCborRoundTrip`: every
/// element of a `vector<any>` survives the CBOR roundtrip with its
/// type name and payload intact.
#[test]
fn vector_of_any_cbor_roundtrip_preserves_each_element() {
    use dots_rs_core::AnyObject;

    let ping = Ping {
        id: Some(42),
        note: Some("hi".into()),
    };
    let original = Batch {
        id: Some(1),
        items: Some(vec![
            // A live-wrapped object and a hand-built envelope, matching
            // the cpp test's heterogeneous `{ "A", ... }, { "B", ... }`.
            to_any(&ping),
            AnyObject::new("B", vec![0x02, 0x03]),
        ]),
    };

    let bytes = encode_to_vec(&original);
    let decoded: Batch = decode_typed_from_slice(&bytes).expect("typed decode succeeds");
    assert_eq!(original, decoded);

    let items = decoded.items.expect("items set");
    assert_eq!(items.len(), 2);

    // Element 0: type identity in the clear, payload decodes back to
    // the original Ping.
    assert_eq!(items[0].type_name(), "Ping");
    let recovered = items[0].decode(Ping::DESCRIPTOR).expect("decode contained Ping");
    assert_eq!(recovered.as_typed::<Ping>().expect("downcast"), &ping);

    // Element 1: opaque envelope passes through verbatim.
    assert_eq!(items[1].type_name(), "B");
    assert_eq!(items[1].payload(), &[0x02, 0x03]);
}

/// An unset / empty `vector<any>` field behaves like any other vector
/// property: absent stays absent, empty stays empty (and non-equal to
/// absent).
#[test]
fn vector_of_any_empty_and_absent_roundtrip() {
    let absent = Batch {
        id: Some(2),
        items: None,
    };
    let bytes = encode_to_vec(&absent);
    let decoded: Batch = decode_typed_from_slice(&bytes).expect("typed decode succeeds");
    assert_eq!(absent, decoded);

    let empty = Batch {
        id: Some(2),
        items: Some(vec![]),
    };
    let bytes = encode_to_vec(&empty);
    let decoded: Batch = decode_typed_from_slice(&bytes).expect("typed decode succeeds");
    assert_eq!(empty, decoded);
    assert_ne!(absent, empty);
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
