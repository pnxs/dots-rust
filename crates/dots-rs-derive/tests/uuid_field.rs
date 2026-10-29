//! `[u8; 16]` fields — wire format for DOTS `uuid`.
//!
//! Generated UUIDs use `dots_rs::Uuid`; handwritten array fields remain
//! supported. On the wire either representation is a
//! CBOR ByteString of length 16 — matching dots-cpp's
//! `CborWriter::write(std::array<uint8_t, 16>)`, which writes a
//! ByteString rather than the array path used by `vector_t<T>`. For
//! arbitrary binary blobs use `Vec<u8>` (DOTS `vector<uint8>`).

use dots_rs_core::{FieldKind, decode_typed_from_slice, encode_to_vec};

mod model {
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Token")]
    pub struct Token {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub fingerprint: Option<[u8; 16]>,
    }
}
use model::*;

#[test]
fn uuid_field_kind_is_uuid() {
    let p = Token::DESCRIPTOR.property(2).unwrap();
    assert!(matches!(p.kind, FieldKind::Uuid));
}

#[test]
fn uuid_uses_byte_string_wire_format() {
    let token = Token {
        id: Some(7),
        fingerprint: Some([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]),
    };
    let bytes = encode_to_vec(&token);
    // CBOR ByteString of length 16 has header `0x50` (major-type-2,
    // additional-info = 16). Find it after the map+tag prefix.
    // Map(2) header is `0xa2`; tag-1 = `0x01`, then u32 value (one
    // byte for 7), tag-2 = `0x02`, then 0x50 + 16 raw bytes.
    let bs_header_pos = bytes
        .iter()
        .position(|&b| b == 0x50)
        .expect("byte-string header");
    assert_eq!(
        &bytes[bs_header_pos + 1..bs_header_pos + 17],
        &token.fingerprint.unwrap()
    );
}

#[test]
fn uuid_field_roundtrips() {
    let original = Token {
        id: Some(42),
        fingerprint: Some([0xab; 16]),
    };
    let bytes = encode_to_vec(&original);
    let decoded: Token = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
}

#[test]
fn uuid_decode_rejects_wrong_length() {
    // Forge a payload with a 15-byte CBOR ByteString instead of 16.
    // Map(1) + tag(2) + ByteString(15) + 15 zero bytes.
    let mut bytes = vec![0xa1, 0x02, 0x4f];
    bytes.extend_from_slice(&[0u8; 15]);
    let result: Result<Token, _> = decode_typed_from_slice(&bytes);
    assert!(result.is_err(), "decode should reject length mismatch");
}

mod typed_model {
    use dots_rs::{DotsStruct, Uuid};

    #[derive(DotsStruct, Clone)]
    pub struct UuidToken {
        #[dots(tag = 1, key)]
        pub id: Uuid,
        #[dots(tag = 2)]
        pub history: Option<Vec<Uuid>>,
    }
}

#[test]
fn uuid_wrapper_preserves_wire_keys_vectors_and_filters() {
    use dots_rs::filter::IntoPredicateValue;
    use dots_rs::{InstanceRef, Uuid, dots};
    let id: Uuid = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6".parse().unwrap();
    let value = dots!(typed_model::UuidToken {
        id: id.into_bytes(),
        history: vec![id]
    });
    assert!(matches!(
        typed_model::UuidToken::DESCRIPTOR.property(1).unwrap().kind,
        FieldKind::Uuid
    ));
    let encoded = encode_to_vec(&value);
    let decoded: typed_model::UuidToken = decode_typed_from_slice(&encoded).unwrap();
    assert_eq!(decoded.id, id);
    assert_eq!(decoded.history, Some(vec![id]));
    let reference = InstanceRef::of(&value);
    assert_eq!(reference.key_bytes()[0..2], [0x81, 0x50]);
    assert_eq!(&reference.key_bytes()[2..], id.as_bytes());
    assert_eq!(reference.materialize_key().unwrap().id, id);
    let _predicate = typed_model::UuidToken::ID.eq(id);
    assert_eq!(id.into_scalar().uuid_val, Some(id.into_bytes()));
    assert_eq!(
        Uuid::into_list(vec![id]).uuid_list,
        Some(vec![id.into_bytes()])
    );
}
