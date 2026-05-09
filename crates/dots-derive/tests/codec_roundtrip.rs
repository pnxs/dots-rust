//! End-to-end CBOR codec tests for `#[derive(DotsStruct)]` output.
//!
//! These exercise the wire format (sparse CBOR map keyed by property tag)
//! and the round-trip encode → decode → equality contract.

use dots_core::{StructValue, decode_from_slice, encode_to_vec};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, PartialEq)]
#[dots(name = "Sample", cached)]
struct Sample {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    payload: Option<String>,
    #[dots(tag = 3)]
    counter: Option<u64>,
    #[dots(tag = 4)]
    flag: Option<bool>,
    #[dots(tag = 5)]
    ratio: Option<f64>,
}

#[test]
fn roundtrip_all_fields_set() {
    let original = Sample {
        id: Some(42),
        payload: Some("hello".into()),
        counter: Some(9000),
        flag: Some(true),
        ratio: Some(1.25),
    };
    let bytes = encode_to_vec(&original);
    let decoded: Sample = decode_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
}

#[test]
fn roundtrip_partial_object() {
    let original = Sample {
        id: Some(7),
        payload: None,
        counter: Some(1),
        flag: None,
        ratio: None,
    };
    let bytes = encode_to_vec(&original);
    let decoded: Sample = decode_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
    assert_eq!(decoded.valid_set().len(), 2);
}

#[test]
fn roundtrip_empty_object() {
    let original = Sample::default();
    let bytes = encode_to_vec(&original);
    // Empty CBOR map is a single byte: 0xa0.
    assert_eq!(bytes, [0xa0]);
    let decoded: Sample = decode_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
    assert!(decoded.valid_set().is_empty());
}

#[test]
fn wire_format_is_sparse_map_keyed_by_tag() {
    // id=42, no other fields -> map of length 1 with key 1, value 42
    let s = Sample {
        id: Some(42),
        ..Default::default()
    };
    let bytes = encode_to_vec(&s);
    // 0xa1     = map of 1 pair
    // 0x01     = unsigned 1 (the property tag)
    // 0x18 0x2a = unsigned 42 (CBOR encodes 24..=255 with the 0x18 prefix)
    assert_eq!(bytes, [0xa1, 0x01, 0x18, 0x2a]);
}

#[test]
fn unknown_tags_are_skipped_for_forward_compat() {
    // Hand-craft bytes representing a map with id=1 plus a property at tag 99
    // that the current Sample type does not know about. Decode must skip it.
    //
    // 0xa2          = map of 2 pairs
    // 0x01 0x01     = tag 1 -> 1
    // 0x18 0x63     = tag 99
    // 0x65 'e''x''t''r''a' = text string "extra"
    let bytes = [
        0xa2, 0x01, 0x01, 0x18, 0x63, 0x65, b'e', b'x', b't', b'r', b'a',
    ];
    let decoded: Sample = decode_from_slice(&bytes).expect("decode skips unknown tag");
    assert_eq!(decoded.id, Some(1));
    assert!(decoded.payload.is_none());
}

#[test]
fn map_size_matches_valid_set_len() {
    let s = Sample {
        id: Some(1),
        flag: Some(false),
        ..Default::default()
    };
    let bytes = encode_to_vec(&s);
    // First byte encodes the map size; for sizes 0..=23 it's 0xa0 + size.
    assert_eq!(bytes[0], 0xa0 | 2);
    assert_eq!(s.valid_set().len(), 2);
}
