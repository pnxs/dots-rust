//! `[u8; 16]` fields — wire format for DOTS `uuid`.
//!
//! `uuid` in `.dots` lowers to `[u8; 16]` (see
//! `dots-build/src/codegen.rs::map_primitive`). On the wire it's a
//! CBOR ByteString of length 16 — matching dots-cpp's
//! `CborWriter::write(std::array<uint8_t, 16>)`, which writes a
//! ByteString rather than the array path used by `vector_t<T>`. For
//! arbitrary binary blobs use `Vec<u8>` (DOTS `vector<uint8>`).

use dots_core::{FieldKind, decode_typed_from_slice, encode_to_vec};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Token")]
struct Token {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    fingerprint: Option<[u8; 16]>,
}

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
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
            0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
        ]),
    };
    let bytes = encode_to_vec(&token);
    // CBOR ByteString of length 16 has header `0x50` (major-type-2,
    // additional-info = 16). Find it after the map+tag prefix.
    // Map(2) header is `0xa2`; tag-1 = `0x01`, then u32 value (one
    // byte for 7), tag-2 = `0x02`, then 0x50 + 16 raw bytes.
    let bs_header_pos = bytes.iter().position(|&b| b == 0x50).expect("byte-string header");
    assert_eq!(&bytes[bs_header_pos + 1..bs_header_pos + 17], &token.fingerprint.unwrap());
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
