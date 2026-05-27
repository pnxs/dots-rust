//! End-to-end CBOR codec tests for `#[derive(DotsStruct)]` output.
//!
//! These exercise the wire format (sparse CBOR map keyed by property tag)
//! and the round-trip encode → decode → equality contract — both the
//! typed path and the dynamic `AnyStruct` path. The two paths must
//! produce byte-identical wire output for the same logical value.

use dots_core::{AnyStruct, StructValue, decode_typed_from_slice, encode_to_vec};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
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
fn typed_roundtrip_all_fields_set() {
    let original = Sample {
        id: Some(42),
        payload: Some("hello".into()),
        counter: Some(9000),
        flag: Some(true),
        ratio: Some(1.25),
    };
    let bytes = encode_to_vec(&original);
    let decoded: Sample = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
}

#[test]
fn typed_roundtrip_partial_object() {
    let original = Sample {
        id: Some(7),
        payload: None,
        counter: Some(1),
        flag: None,
        ratio: None,
    };
    let bytes = encode_to_vec(&original);
    let decoded: Sample = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
    assert_eq!(decoded.valid_set().len(), 2);
}

#[test]
fn typed_roundtrip_empty_object() {
    let original = Sample::default();
    let bytes = encode_to_vec(&original);
    // Empty CBOR map is a single byte: 0xa0.
    assert_eq!(bytes, [0xa0]);
    let decoded: Sample = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
    assert!(decoded.valid_set().is_empty());
}

#[test]
fn wire_format_is_sparse_map_keyed_by_tag() {
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
    // Hand-craft bytes with id=1 plus a property at tag 99 that the
    // current Sample type does not know about. Decode must skip it.
    let bytes = [
        0xa2, 0x01, 0x01, 0x18, 0x63, 0x65, b'e', b'x', b't', b'r', b'a',
    ];
    let decoded: Sample = decode_typed_from_slice(&bytes).expect("decode skips unknown tag");
    assert_eq!(decoded.id, Some(1));
    assert!(decoded.payload.is_none());
}

#[test]
fn dynamic_decode_yields_same_logical_value() {
    let original = Sample {
        id: Some(11),
        payload: Some("dyn".into()),
        flag: Some(false),
        ..Default::default()
    };
    let typed_bytes = encode_to_vec(&original);

    let any = AnyStruct::decode_from_slice(Sample::DESCRIPTOR, &typed_bytes)
        .expect("dynamic decode must succeed");
    assert_eq!(StructValue::descriptor(&any).name, "Sample");
    assert_eq!(any.valid_set(), original.valid_set());
}

#[test]
fn typed_and_dynamic_paths_produce_identical_bytes() {
    let original = Sample {
        id: Some(123),
        payload: Some("identical".into()),
        counter: Some(456),
        flag: Some(true),
        ratio: Some(-3.5),
    };

    let typed_bytes = encode_to_vec(&original);
    let any = AnyStruct::decode_from_slice(Sample::DESCRIPTOR, &typed_bytes)
        .expect("decode succeeds");
    let dynamic_bytes = encode_to_vec(&any);

    // The descriptor-driven codec is the single source of truth for
    // wire format, so the two paths must agree byte-for-byte.
    assert_eq!(typed_bytes, dynamic_bytes);
}

#[test]
fn anystruct_as_typed_returns_matching_pointer() {
    let original = Sample {
        id: Some(7),
        payload: Some("zero-cost".into()),
        flag: Some(true),
        counter: Some(42),
        ratio: Some(2.5),
    };
    let typed_bytes = encode_to_vec(&original);
    let any =
        AnyStruct::decode_from_slice(Sample::DESCRIPTOR, &typed_bytes).expect("decode succeeds");

    // `as_typed::<T>()` is the free cast — descriptor identity is
    // the only check; the returned `&T` aliases `AnyStruct`'s buffer.
    let viewed: &Sample = any.as_typed::<Sample>().expect("descriptor matches");
    assert!(core::ptr::eq(
        viewed as *const Sample as *const u8,
        any.data_ptr(),
    ));
    assert_eq!(*viewed, original);
}

#[test]
fn anystruct_as_typed_rejects_wrong_t() {
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Other")]
    struct Other {
        #[dots(tag = 1)]
        x: Option<u32>,
    }

    let any = AnyStruct::new(Sample::DESCRIPTOR);
    assert!(any.as_typed::<Other>().is_none());
}

#[test]
fn dynamic_anystruct_zeroinit_decodes_safely() {
    // Decode bytes that touch only some properties; verify the
    // AnyStruct's all-fields-None starting state plus per-tag writes
    // don't trip Drop or leak when the value goes out of scope.
    let bytes = encode_to_vec(&Sample {
        payload: Some("only payload".into()),
        ..Default::default()
    });
    let any = AnyStruct::decode_from_slice(Sample::DESCRIPTOR, &bytes).expect("decode succeeds");
    assert_eq!(any.valid_set().len(), 1);
    drop(any);
}
