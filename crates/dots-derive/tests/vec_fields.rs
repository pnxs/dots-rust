//! `Vec<T>` field support.
//!
//! Every `Vec<X>` — including `Vec<u8>` — encodes as a CBOR array,
//! with each element going through its own `DotsField::dots_encode`.
//! Matches dots-cpp, whose `CborSerializer::visitVectorBeginDerived`
//! has no byte-string special case for `vector_t<uint8_t>`.
//!
//! Element types covered:
//! - primitives (`u32`)
//! - owned strings (`String`)
//! - nested DOTS structs (`Vec<Inner>`)

use dots_core::{AnyStruct, FieldKind, decode_typed_from_slice, encode_to_vec};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Tag")]
struct Tag {
    #[dots(tag = 1)]
    name: Option<String>,
    #[dots(tag = 2)]
    weight: Option<u32>,
}

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Catalog", cached)]
struct Catalog {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    raw: Option<Vec<u8>>,           // array of u8 (matches dots-cpp)
    #[dots(tag = 3)]
    counters: Option<Vec<u32>>,     // array of primitives
    #[dots(tag = 4)]
    labels: Option<Vec<String>>,    // array of owned strings
    #[dots(tag = 5)]
    tags: Option<Vec<Tag>>,         // array of nested DOTS structs
}

#[test]
fn field_kinds_route_correctly() {
    let p = |tag| Catalog::DESCRIPTOR.property(tag).unwrap();
    match p(2).kind {
        FieldKind::Vec(inner) => assert!(matches!(inner, FieldKind::U8)),
        other => panic!("raw: expected Vec(U8), got {other:?}"),
    }
    match p(3).kind {
        FieldKind::Vec(inner) => assert!(matches!(inner, FieldKind::U32)),
        other => panic!("counters: expected Vec(U32), got {other:?}"),
    }
    match p(4).kind {
        FieldKind::Vec(inner) => assert!(matches!(inner, FieldKind::String)),
        other => panic!("labels: expected Vec(String), got {other:?}"),
    }
    match p(5).kind {
        FieldKind::Vec(inner) => match inner {
            FieldKind::Struct(d) => assert_eq!(d.name, "Tag"),
            other => panic!("tags: inner kind expected Struct, got {other:?}"),
        },
        other => panic!("tags: expected Vec(Struct), got {other:?}"),
    }
}

#[test]
fn vec_of_primitives_roundtrip() {
    let original = Catalog {
        id: Some(1),
        counters: Some(vec![10, 20, 30]),
        ..Default::default()
    };
    let bytes = encode_to_vec(&original);
    let decoded: Catalog = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
}

#[test]
fn vec_of_strings_roundtrip() {
    let original = Catalog {
        id: Some(2),
        labels: Some(vec!["one".into(), "two".into(), "three".into()]),
        ..Default::default()
    };
    let bytes = encode_to_vec(&original);
    let decoded: Catalog = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
}

#[test]
fn vec_of_nested_structs_roundtrip() {
    let original = Catalog {
        id: Some(3),
        tags: Some(vec![
            Tag {
                name: Some("alpha".into()),
                weight: Some(1),
            },
            Tag {
                name: Some("beta".into()),
                weight: None,
            },
        ]),
        ..Default::default()
    };
    let bytes = encode_to_vec(&original);
    let decoded: Catalog = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
}

#[test]
fn vec_u8_uses_array_wire_format() {
    // dots-cpp encodes `vector<uint8>` as a CBOR array of u8 (each
    // element a single-byte CBOR uint), not a byte string. Verify
    // dots-rust does the same.
    let s = Catalog {
        raw: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    let bytes = encode_to_vec(&s);
    // Map(1) + tag(2) prefix is `0xa1 0x02`.
    assert_eq!(&bytes[..2], &[0xa1, 0x02]);
    // Array header for length 3 is `0x83`.
    assert_eq!(bytes[2], 0x83);
    // Three u8 values 1, 2, 3 each fit in single-byte CBOR uint.
    assert_eq!(&bytes[3..], &[0x01, 0x02, 0x03]);
}

#[test]
fn vec_u32_uses_array_wire_format() {
    // CBOR array (length 3) header is 0x83.
    let s = Catalog {
        counters: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    let bytes = encode_to_vec(&s);
    // Map(1) + tag(3) prefix
    assert_eq!(&bytes[..2], &[0xa1, 0x03]);
    // Array header for length 3
    assert_eq!(bytes[2], 0x83);
    // Three u32 values 1, 2, 3 each fit in single-byte CBOR uint
    assert_eq!(&bytes[3..6], &[0x01, 0x02, 0x03]);
}

#[test]
fn empty_vec_is_distinct_from_unset() {
    // An empty Vec is `Some(vec![])` — encoded but with array length 0.
    // Unset is `None` — not in the map at all.
    let with_empty = Catalog {
        counters: Some(vec![]),
        ..Default::default()
    };
    let with_unset = Catalog::default();

    let b1 = encode_to_vec(&with_empty);
    let b2 = encode_to_vec(&with_unset);
    assert_ne!(b1, b2);

    let d1: Catalog = decode_typed_from_slice(&b1).unwrap();
    assert_eq!(d1.counters, Some(vec![]));

    let d2: Catalog = decode_typed_from_slice(&b2).unwrap();
    assert_eq!(d2.counters, None);
}

#[test]
fn vec_dynamic_anystruct_roundtrip() {
    let original = Catalog {
        id: Some(99),
        raw: Some(vec![0xde, 0xad, 0xbe, 0xef]),
        counters: Some(vec![1000, 2000]),
        labels: Some(vec!["x".into()]),
        tags: Some(vec![Tag {
            name: Some("only".into()),
            weight: Some(7),
        }]),
    };
    let typed_bytes = encode_to_vec(&original);
    let any = AnyStruct::decode_from_slice(Catalog::DESCRIPTOR, &typed_bytes)
        .expect("dynamic decode succeeds");
    let dynamic_bytes = encode_to_vec(&any);
    assert_eq!(typed_bytes, dynamic_bytes);

    // And re-decode dynamic-encoded bytes back into the typed struct.
    let back: Catalog = decode_typed_from_slice(&dynamic_bytes).unwrap();
    assert_eq!(original, back);
}
