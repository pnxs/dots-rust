//! Wire-only descriptor codec.
//!
//! Validates that a process with only the *metadata* of a DOTS struct
//! (a `DynamicStructDescriptor`) can decode bytes encoded by a process
//! with the full compiled type, and re-encode them to byte-identical
//! output. This is the foundation of `dotsd`-style routing of types
//! the broker has never been compiled against.

use std::sync::Arc;

use dots_rs_core::{DynamicStruct, DynamicStructDescriptor, DynamicValue, encode_to_vec};

mod model {
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Pin")]
    pub struct Pin {
        #[dots(tag = 1)]
        pub label: Option<String>,
        #[dots(tag = 2)]
        pub weight: Option<u32>,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Wire")]
    pub struct Wire {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub label: Option<String>,
        #[dots(tag = 3)]
        pub flag: Option<bool>,
        #[dots(tag = 4)]
        pub raw: Option<Vec<u8>>,
        #[dots(tag = 5)]
        pub counters: Option<Vec<u32>>,
        #[dots(tag = 6)]
        pub pins: Option<Vec<Pin>>,
        #[dots(tag = 7)]
        pub primary_pin: Option<Pin>,
    }
}
use model::*;

fn dyn_descriptor() -> Arc<DynamicStructDescriptor> {
    Arc::new(DynamicStructDescriptor::from_static(Wire::DESCRIPTOR))
}

#[test]
fn primitives_cross_roundtrip() {
    let original = Wire {
        id: Some(42),
        label: Some("hello".into()),
        flag: Some(true),
        ..Default::default()
    };
    let typed_bytes = encode_to_vec(&original);

    let dyn_value = DynamicStruct::decode(dyn_descriptor(), &typed_bytes)
        .expect("dynamic decode succeeds");
    let dyn_bytes = dyn_value.encode();

    assert_eq!(typed_bytes, dyn_bytes, "wire bytes must match across paths");
    assert_eq!(dyn_value.valid.len(), 3);
}

#[test]
fn vec_of_primitives_cross_roundtrip() {
    let original = Wire {
        id: Some(1),
        counters: Some(vec![10, 20, 30]),
        ..Default::default()
    };
    let typed_bytes = encode_to_vec(&original);

    let dyn_value = DynamicStruct::decode(dyn_descriptor(), &typed_bytes).unwrap();
    let dyn_bytes = dyn_value.encode();
    assert_eq!(typed_bytes, dyn_bytes);

    // Verify the value enum representation is what we expect.
    let (_, counters) = dyn_value
        .properties
        .iter()
        .find(|(t, _)| *t == 5)
        .expect("counters must be present");
    match counters {
        DynamicValue::Vec(items) => {
            assert_eq!(items.len(), 3);
            assert!(matches!(items[0], DynamicValue::U32(10)));
            assert!(matches!(items[1], DynamicValue::U32(20)));
            assert!(matches!(items[2], DynamicValue::U32(30)));
        }
        other => panic!("expected Vec, got {other:?}"),
    }
}

#[test]
fn vec_of_bytes_cross_roundtrip() {
    // `Vec<u8>` encodes as a CBOR array of u8 (matching dots-cpp), so
    // the dynamic decoder yields `DynamicValue::Vec` of `U8` items —
    // not `DynamicValue::Uuid`. (`Uuid` is the only DOTS type that
    // uses a CBOR byte string on the wire, and it's always 16 bytes.)
    let original = Wire {
        id: Some(2),
        raw: Some(vec![0xde, 0xad, 0xbe, 0xef]),
        ..Default::default()
    };
    let typed_bytes = encode_to_vec(&original);

    let dyn_value = DynamicStruct::decode(dyn_descriptor(), &typed_bytes).unwrap();
    let dyn_bytes = dyn_value.encode();
    assert_eq!(typed_bytes, dyn_bytes);

    let (_, raw) = dyn_value
        .properties
        .iter()
        .find(|(t, _)| *t == 4)
        .unwrap();
    match raw {
        DynamicValue::Vec(items) => {
            assert_eq!(items.len(), 4);
            assert!(matches!(items[0], DynamicValue::U8(0xde)));
            assert!(matches!(items[1], DynamicValue::U8(0xad)));
            assert!(matches!(items[2], DynamicValue::U8(0xbe)));
            assert!(matches!(items[3], DynamicValue::U8(0xef)));
        }
        other => panic!("expected Vec, got {other:?}"),
    }
}

#[test]
fn nested_struct_cross_roundtrip() {
    let original = Wire {
        id: Some(3),
        primary_pin: Some(Pin {
            label: Some("vcc".into()),
            weight: Some(7),
        }),
        ..Default::default()
    };
    let typed_bytes = encode_to_vec(&original);

    let dyn_value = DynamicStruct::decode(dyn_descriptor(), &typed_bytes).unwrap();
    let dyn_bytes = dyn_value.encode();
    assert_eq!(typed_bytes, dyn_bytes);

    let (_, pin_value) = dyn_value
        .properties
        .iter()
        .find(|(t, _)| *t == 7)
        .unwrap();
    match pin_value {
        DynamicValue::Struct(inner) => {
            assert_eq!(inner.descriptor.name, "Pin");
            assert_eq!(inner.valid.len(), 2);
        }
        other => panic!("expected Struct, got {other:?}"),
    }
}

#[test]
fn vec_of_nested_structs_cross_roundtrip() {
    let original = Wire {
        id: Some(4),
        pins: Some(vec![
            Pin {
                label: Some("a".into()),
                weight: Some(1),
            },
            Pin {
                label: Some("b".into()),
                weight: None,
            },
            Pin {
                label: None,
                weight: Some(3),
            },
        ]),
        ..Default::default()
    };
    let typed_bytes = encode_to_vec(&original);

    let dyn_value = DynamicStruct::decode(dyn_descriptor(), &typed_bytes).unwrap();
    let dyn_bytes = dyn_value.encode();
    assert_eq!(typed_bytes, dyn_bytes);
}

#[test]
fn unknown_tags_skipped_in_dynamic_decode() {
    // Hand-craft bytes containing tag=1 (id=99) plus a tag the
    // descriptor doesn't list (tag=200).
    let bytes = [
        0xa2, // map of 2 pairs
        0x01, 0x18, 0x63, // tag 1 -> 99
        0x18, 0xc8, // tag 200
        0x65, b'g', b'h', b'o', b's', b't', // text "ghost"
    ];
    let dyn_value = DynamicStruct::decode(dyn_descriptor(), &bytes).expect("decode skips unknown tag");
    assert_eq!(dyn_value.valid.len(), 1);
    assert!(dyn_value.valid.has(1));
}

#[test]
fn empty_struct_cross_roundtrip() {
    let original = Wire::default();
    let typed_bytes = encode_to_vec(&original);

    let dyn_value = DynamicStruct::decode(dyn_descriptor(), &typed_bytes).unwrap();
    let dyn_bytes = dyn_value.encode();
    assert_eq!(typed_bytes, dyn_bytes);
    assert_eq!(typed_bytes, [0xa0]);
    assert!(dyn_value.valid.is_empty());
}

#[test]
fn full_field_set_cross_roundtrip() {
    let original = Wire {
        id: Some(5),
        label: Some("everything".into()),
        flag: Some(false),
        raw: Some(vec![1, 2, 3]),
        counters: Some(vec![100, 200]),
        pins: Some(vec![Pin {
            label: Some("p".into()),
            weight: Some(9),
        }]),
        primary_pin: Some(Pin {
            label: Some("center".into()),
            weight: Some(0),
        }),
    };
    let typed_bytes = encode_to_vec(&original);

    let dyn_value = DynamicStruct::decode(dyn_descriptor(), &typed_bytes).unwrap();
    let dyn_bytes = dyn_value.encode();
    assert_eq!(typed_bytes, dyn_bytes);
    assert_eq!(dyn_value.valid.len(), 7);
}
