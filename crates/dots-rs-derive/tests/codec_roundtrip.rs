//! End-to-end CBOR codec tests for `#[derive(DotsStruct)]` output.
//!
//! These exercise the wire format (sparse CBOR map keyed by property tag)
//! and the round-trip encode → decode → equality contract — both the
//! typed path and the dynamic `AnyStruct` path. The two paths must
//! produce byte-identical wire output for the same logical value.

use dots_rs_core::dots;
use dots_rs_core::{AnyStruct, StructValue, decode_typed_from_slice, encode_to_vec};

mod model {
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Sample", cached)]
    pub struct Sample {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub payload: Option<String>,
        #[dots(tag = 3)]
        pub counter: Option<u64>,
        #[dots(tag = 4)]
        pub flag: Option<bool>,
        #[dots(tag = 5)]
        pub ratio: Option<f64>,
    }
}
use model::*;

#[test]
fn typed_roundtrip_all_fields_set() {
    let original = dots!(Sample {
        id: 42u32,
        payload: "hello".into(),
        counter: 9000u64,
        flag: true,
        ratio: 1.25,
    });
    let bytes = encode_to_vec(&original);
    let decoded: Sample = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
}

#[test]
fn typed_roundtrip_partial_object() {
    let original = dots!(Sample {
        id: 7u32,
        counter: 1u64,
    });
    let bytes = encode_to_vec(&original);
    let decoded: Sample = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
    assert_eq!(decoded.valid_set().len(), 2);
}

#[test]
fn empty_keyed_object_encodes_but_decode_rejects_missing_key() {
    // A keyed struct with nothing set still *encodes* to an empty map
    // (encode doesn't validate)...
    let original = Sample::default();
    let bytes = encode_to_vec(&original);
    // Empty CBOR map is a single byte: 0xa0.
    assert_eq!(bytes, [0xa0]);
    // ...but decoding now rejects it: every DOTS instance must carry its
    // `#[dots(key)]` properties (tag 1 here). This holds for `Option<T>`
    // keys as well as bare-`T` keys — it's the key *contract*, not the
    // storage form.
    assert!(decode_typed_from_slice::<Sample>(&bytes).is_err());
}

#[test]
fn missing_key_decode_error_drops_already_decoded_owned_fields() {
    // Hand-encode {2: "hello"} — the owned `payload` is set but the key
    // (tag 1) is absent. Decode writes a real heap `String` into the
    // seeded buffer, then rejects the value for the missing key. The
    // typed decoder must drop that partially-built buffer through the
    // property thunks; a leak or double-free would trip the allocator /
    // debug UB checks here.
    let mut buf = Vec::new();
    let mut e = dots_rs_core::minicbor::Encoder::new(&mut buf);
    e.map(1).unwrap();
    e.u32(2).unwrap();
    e.str("hello").unwrap();

    assert!(decode_typed_from_slice::<Sample>(&buf).is_err());
}

/// Port of dots-cpp `TestCborSerializer.serializerException`: feeding
/// the decoder malformed or type-mismatched CBOR must surface a
/// `DecodeError`, never panic or read out of bounds.
#[test]
fn malformed_cbor_yields_error_not_panic() {
    // (a) Truncated: a map header promising one pair, then nothing.
    assert!(decode_typed_from_slice::<Sample>(&[0xa1]).is_err());

    // (b) Truncated mid-value: key tag 1 present, but its u32 value is
    // cut off (0x1a announces a 4-byte int with no bytes following).
    assert!(decode_typed_from_slice::<Sample>(&[0xa1, 0x01, 0x1a]).is_err());

    // (c) Type mismatch: tag 1 (`id: u32`) carries a text string instead
    // of an integer.
    let mut buf = Vec::new();
    let mut e = dots_rs_core::minicbor::Encoder::new(&mut buf);
    e.map(1).unwrap();
    e.u32(1).unwrap();
    e.str("not-an-int").unwrap();
    assert!(decode_typed_from_slice::<Sample>(&buf).is_err());

    // (d) Wrong top-level shape: a bare integer where a struct map is
    // expected.
    assert!(decode_typed_from_slice::<Sample>(&[0x01]).is_err());

    // (e) Empty input.
    assert!(decode_typed_from_slice::<Sample>(&[]).is_err());
}

#[test]
fn wire_format_is_sparse_map_keyed_by_tag() {
    let s = dots!(Sample {
        id: 42_u32,
    });
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
    let original = dots!(Sample {
        id: 11_u32,
        payload: "dyn".into(),
        flag: false,
    });
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
    use dots_rs_derive::DotsStruct;
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
    // Decode bytes that touch only some properties (plus the required
    // key); verify the AnyStruct's init + per-tag writes don't trip Drop
    // or leak when the value goes out of scope.
    let bytes = encode_to_vec(&Sample {
        id: Some(1),
        payload: Some("only payload".into()),
        ..Default::default()
    });
    let any = AnyStruct::decode_from_slice(Sample::DESCRIPTOR, &bytes).expect("decode succeeds");
    assert_eq!(any.valid_set().len(), 2);
    drop(any);
}
