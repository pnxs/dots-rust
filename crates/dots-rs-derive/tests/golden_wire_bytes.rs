//! Golden wire-byte tests — byte-exact expectations ported from dots-cpp.
//!
//! Source of truth: `dots-cpp/tests/src/serialization/TestCborSerializer.cpp`
//! (`CborSerializerTestDataEncoded`) and the typed tests in
//! `dots-cpp/tests/src/serialization/TestSerializer.h`. The mirrored test
//! types come from `dots-cpp/tests/src/serialization/serialization.dots`
//! (same property tags, same field types, same declaration order — encode
//! order follows declaration order on both sides).
//!
//! Every expected byte sequence below is copied verbatim from the cpp
//! test data; a Rust encoder change that alters any of these bytes is a
//! wire-compatibility break with dots-cpp peers.

// The float literals are copied verbatim from the cpp test data and must
// stay bit-exact to match the golden bytes — they are not stand-ins for
// the mathematical constants clippy suggests.
#![allow(clippy::approx_constant)]

use dots_rs_core::{
    Duration, PropertySet, StructValue, Timepoint, decode_typed_from_decoder,
    decode_typed_from_slice, encode_into_vec_with_mask, encode_to_vec,
};

mod model {
    use dots_rs_core::{Duration, PropertySet, Timepoint};
    use dots_rs_derive::{DotsEnum, DotsStruct};

    /// Mirror of cpp `SerializationEnum` (serialization.dots):
    /// `{ 1: foo, 3: bar = 42, 5: baz = 21, 19: qux = 73 }`.
    /// On the wire an enum is its enumerator *tag*, never the value.
    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(name = "SerializationEnum")]
    pub enum SerializationEnum {
        #[default]
        #[dots(tag = 1)]
        Foo,
        #[dots(tag = 3, value = 42)]
        Bar,
        #[dots(tag = 5, value = 21)]
        Baz,
        #[dots(tag = 19, value = 73)]
        Qux,
    }

    /// Mirror of cpp `SerializationStructSimple`.
    ///
    /// cpp marks `int32Property` as `[key]`, but its test data decodes
    /// instances *without* the key set (e.g. the nested
    /// `{ boolProperty }` value inside `structComplex1`), which the Rust
    /// decoder rejects by contract (`#[dots(key)]` properties are
    /// required at decode). Key-ness never reaches the wire, so the
    /// mirror is keyless — the bytes are identical either way.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "SerializationStructSimple")]
    pub struct SerializationStructSimple {
        #[dots(tag = 1)]
        pub int32_property: Option<i32>,
        #[dots(tag = 2)]
        pub string_property: Option<String>,
        #[dots(tag = 3)]
        pub bool_property: Option<bool>,
        #[dots(tag = 4)]
        pub float32_property: Option<f32>,
    }

    /// Mirror of cpp `SerializationStructComplex` (keyless for the same
    /// reason — cpp's `structComplex1`/`structComplex2` each carry only
    /// one of the two `[key]` properties).
    ///
    /// Declaration order matches the `.dots` file, because encoding
    /// walks properties in declaration order on both sides.
    /// `steadyTimepointProperty` (tag 10, `steady_timepoint`) has no
    /// dedicated dots-rs type and is never set in the cpp golden data,
    /// so it is omitted — absent properties contribute no bytes.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "SerializationStructComplex")]
    pub struct SerializationStructComplex {
        #[dots(tag = 7)]
        pub enum_property: Option<SerializationEnum>,
        #[dots(tag = 2)]
        pub uint32_property: Option<u32>,
        #[dots(tag = 4)]
        pub float64_property: Option<f64>,
        #[dots(tag = 25)]
        pub timepoint_property: Option<Timepoint>,
        #[dots(tag = 15)]
        pub struct_simple_property: Option<SerializationStructSimple>,
        #[dots(tag = 3)]
        pub property_set_property: Option<PropertySet>,
        #[dots(tag = 9)]
        pub duration_vector_property: Option<Vec<Duration>>,
        #[dots(tag = 8)]
        pub int64_property: Option<i64>,
        #[dots(tag = 6)]
        pub uuid_property: Option<[u8; 16]>,
    }

    /// Field-level golden-byte host: one optional field per primitive
    /// type from cpp's `serialize_TypedArgument` data. Setting exactly
    /// one field yields `0xA1 <tag> <value bytes>`.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "GoldenPrimitives")]
    pub struct GoldenPrimitives {
        #[dots(tag = 1)]
        pub bool_f: Option<bool>,
        #[dots(tag = 2)]
        pub i8_f: Option<i8>,
        #[dots(tag = 3)]
        pub i64_f: Option<i64>,
        #[dots(tag = 4)]
        pub u64_f: Option<u64>,
        #[dots(tag = 5)]
        pub f32_f: Option<f32>,
        #[dots(tag = 6)]
        pub f64_f: Option<f64>,
        #[dots(tag = 7)]
        pub string_f: Option<String>,
    }
}
use model::*;

// ===== Golden byte constants (copied from CborSerializerTestDataEncoded) =====

/// cpp `structSimple1_Valid`:
/// `{1: 12345789, 2: "foobar", 4: 3.1415f}`.
const STRUCT_SIMPLE1_VALID: &[u8] = &[
    0xA3, //
    0x01, 0x1A, 0x00, 0xBC, 0x61, 0xBD, // int32Property = 12345789
    0x02, 0x66, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // stringProperty = "foobar"
    0x04, 0xFA, 0x40, 0x49, 0x0E, 0x56, // float32Property = 3.1415f
];

/// cpp `structComplex1_Valid`. The timepoint property sits at tag 25,
/// pinning the two-byte CBOR map key `0x18 0x19` (tags > 23 need the
/// uint8 header byte).
const STRUCT_COMPLEX1_VALID: &[u8] = &[
    0xA4, //
    0x07, 0x05, // enumProperty = baz — enumerator *tag* 5, not value 21 (0x15)
    0x04, 0xFB, 0xC0, 0x05, 0xBF, 0x0A, 0x8B, 0x14, 0x5F, 0xCF, // float64 = -2.71828182846
    0x18, 0x19, // property tag 25 (two-byte key)
    0xFB, 0x41, 0xD7, 0x9A, 0x54, 0xCB, 0x60, 0x00, 0x00, // timepoint 1583960877.5
    0x0F, 0xA1, 0x03, 0xF4, // structSimpleProperty = { boolProperty: false }
];

/// cpp `structComplex2_Valid`.
const STRUCT_COMPLEX2_VALID: &[u8] = &[
    0xA3, //
    0x03, 0x1A, 0xAA, 0xFF, 0x00,
    0x55, // propertySetProperty = 0b10101010_11111111_00000000_01010101
    0x09, 0x82, // durationVectorProperty = [123.456, 342.073]
    0xFB, 0x40, 0x5E, 0xDD, 0x2F, 0x1A, 0x9F, 0xBE, 0x77, //
    0xFB, 0x40, 0x75, 0x61, 0x2B, 0x02, 0x0C, 0x49, 0xBA, //
    0x06, 0x50, // uuidProperty = 8c96148e-58bd-11eb-ae93-0242ac130002
    0x8C, 0x96, 0x14, 0x8E, 0x58, 0xBD, 0x11, 0xEB, //
    0xAE, 0x93, 0x02, 0x42, 0xAC, 0x13, 0x00, 0x02, //
];

/// cpp `string5` — non-ASCII (contains `©` = 0xC2 0xA9), escapes, newline.
const STRING5: &[u8] = &[
    0x70, 0x66, 0x6F, 0x6F, 0x5C, 0x20, 0x62, 0x61, 0x72, 0xC2, 0xA9, 0x0A, 0x20, 0x62, 0x5C, 0x61,
    0x7A,
];

// ===== Decoded values (mirrors TestSerializerDataDecoded) =====

fn struct_simple1() -> SerializationStructSimple {
    SerializationStructSimple {
        int32_property: Some(12345789),
        string_property: Some("foobar".into()),
        bool_property: None,
        float32_property: Some(3.1415_f32),
    }
}

fn struct_complex1() -> SerializationStructComplex {
    SerializationStructComplex {
        enum_property: Some(SerializationEnum::Baz),
        float64_property: Some(-2.71828182846_f64),
        // "2020-03-11T21:07:57.500+00:00" as seconds since the epoch.
        timepoint_property: Some(Timepoint(1583960877.5)),
        struct_simple_property: Some(SerializationStructSimple {
            bool_property: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn struct_complex2() -> SerializationStructComplex {
    SerializationStructComplex {
        property_set_property: Some(PropertySet::from_bits(
            0b1010_1010_1111_1111_0000_0000_0101_0101,
        )),
        // duration2 is cpp `5min + 42s + 73ms`.
        duration_vector_property: Some(vec![Duration(123.456), Duration(342.073)]),
        uuid_property: Some([
            0x8C, 0x96, 0x14, 0x8E, 0x58, 0xBD, 0x11, 0xEB, 0xAE, 0x93, 0x02, 0x42, 0xAC, 0x13,
            0x00, 0x02,
        ]),
        ..Default::default()
    }
}

fn concat(parts: &[&[u8]]) -> Vec<u8> {
    parts.iter().flat_map(|p| p.iter().copied()).collect()
}

// ===== Full-struct golden bytes (serialize_/deserialize_*StructArgument) =====

/// Port of `serialize_SimpleStructArgument` / `deserialize_SimpleStructArgument`
/// (the `structSimple1_Valid` cases).
#[test]
fn struct_simple1_matches_cpp_golden_bytes() {
    let value = struct_simple1();
    assert_eq!(encode_to_vec(&value), STRUCT_SIMPLE1_VALID);

    let decoded: SerializationStructSimple =
        decode_typed_from_slice(STRUCT_SIMPLE1_VALID).expect("decode cpp bytes");
    assert_eq!(decoded, value);
}

/// Port of `serialize_ComplexStructArgument` / `deserialize_ComplexStructArgument`
/// (`structComplex1_Valid`): enum, float64, timepoint at tag 25 (two-byte
/// map key), nested struct.
#[test]
fn struct_complex1_matches_cpp_golden_bytes() {
    let value = struct_complex1();
    assert_eq!(encode_to_vec(&value), STRUCT_COMPLEX1_VALID);

    let decoded: SerializationStructComplex =
        decode_typed_from_slice(STRUCT_COMPLEX1_VALID).expect("decode cpp bytes");
    assert_eq!(decoded, value);
}

/// Port of `serialize_ComplexStructArgument` / `deserialize_ComplexStructArgument`
/// (`structComplex2_Valid`): property_set, vector<duration>, uuid.
#[test]
fn struct_complex2_matches_cpp_golden_bytes() {
    let value = struct_complex2();
    assert_eq!(encode_to_vec(&value), STRUCT_COMPLEX2_VALID);

    let decoded: SerializationStructComplex =
        decode_typed_from_slice(STRUCT_COMPLEX2_VALID).expect("decode cpp bytes");
    assert_eq!(decoded, value);
}

/// Port of `deserialize_SimpleStructArgument` (`structSimple1_None`): an
/// empty CBOR map decodes to a value with no valid properties.
#[test]
fn empty_map_decodes_to_no_valid_properties() {
    let decoded: SerializationStructSimple =
        decode_typed_from_slice(&[0xA0]).expect("empty map decodes for keyless struct");
    assert_eq!(decoded.valid_set().len(), 0);
    assert_eq!(decoded, SerializationStructSimple::default());
}

/// The wire carries the enumerator *tag* even when tag and value differ
/// widely — `qux` is tag 19 (0x13), value 73 (0x49). Matches dots-cpp's
/// `CborSerializer`, which writes `enumeratorFromValue(value).tag()`.
#[test]
fn enum_wire_byte_is_tag_not_value() {
    let value = SerializationStructComplex {
        enum_property: Some(SerializationEnum::Qux),
        ..Default::default()
    };
    assert_eq!(encode_to_vec(&value), [0xA1, 0x07, 0x13]);
}

// ===== Field-level primitive golden bytes (serialize_TypedArgument) =====

#[test]
fn golden_bool() {
    // cpp boolTrue / boolFalse.
    let t = GoldenPrimitives {
        bool_f: Some(true),
        ..Default::default()
    };
    assert_eq!(encode_to_vec(&t), [0xA1, 0x01, 0xF5]);

    let f = GoldenPrimitives {
        bool_f: Some(false),
        ..Default::default()
    };
    assert_eq!(encode_to_vec(&f), [0xA1, 0x01, 0xF4]);
}

#[test]
fn golden_i8() {
    // cpp int8Negative: -42 → CBOR major type 1: 0x38 0x29.
    let neg = GoldenPrimitives {
        i8_f: Some(-42),
        ..Default::default()
    };
    let bytes = encode_to_vec(&neg);
    assert_eq!(bytes, [0xA1, 0x02, 0x38, 0x29]);
    let decoded: GoldenPrimitives = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(decoded.i8_f, Some(-42));

    // cpp int8Positive: 42 → 0x18 0x2A.
    let pos = GoldenPrimitives {
        i8_f: Some(42),
        ..Default::default()
    };
    assert_eq!(encode_to_vec(&pos), [0xA1, 0x02, 0x18, 0x2A]);
}

#[test]
fn golden_i64_large_negative() {
    // cpp int64Negative: -12345678910111213 → 0x3B <n-1 big-endian>.
    let v = GoldenPrimitives {
        i64_f: Some(-12345678910111213),
        ..Default::default()
    };
    let bytes = encode_to_vec(&v);
    assert_eq!(
        bytes,
        [
            0xA1, 0x03, 0x3B, 0x00, 0x2B, 0xDC, 0x54, 0x5D, 0xF2, 0xBD, 0xEC
        ]
    );
    let decoded: GoldenPrimitives = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(decoded.i64_f, Some(-12345678910111213));
}

#[test]
fn golden_u64_above_i64_max() {
    // cpp uint64Positive2: 18434398394799440403 (> i64::MAX) must stay a
    // CBOR unsigned int, not overflow into the signed range.
    let v = GoldenPrimitives {
        u64_f: Some(18434398394799440403),
        ..Default::default()
    };
    let bytes = encode_to_vec(&v);
    assert_eq!(
        bytes,
        [
            0xA1, 0x04, 0x1B, 0xFF, 0xD4, 0x23, 0xAB, 0xA2, 0x0D, 0x42, 0x13
        ]
    );
    let decoded: GoldenPrimitives = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(decoded.u64_f, Some(18434398394799440403));
}

#[test]
fn golden_f32() {
    // cpp float32Zero: 0.0f must be a full 4-byte float32 (0xFA), NOT
    // shrunk to a half-float (0xF9) — dots-cpp always writes float32
    // for float32 values.
    let zero = GoldenPrimitives {
        f32_f: Some(0.0_f32),
        ..Default::default()
    };
    assert_eq!(
        encode_to_vec(&zero),
        [0xA1, 0x05, 0xFA, 0x00, 0x00, 0x00, 0x00]
    );

    // cpp float32Positive: 3.1415f.
    let pos = GoldenPrimitives {
        f32_f: Some(3.1415_f32),
        ..Default::default()
    };
    assert_eq!(
        encode_to_vec(&pos),
        [0xA1, 0x05, 0xFA, 0x40, 0x49, 0x0E, 0x56]
    );

    // cpp float32Negative: -2.7183f.
    let neg = GoldenPrimitives {
        f32_f: Some(-2.7183_f32),
        ..Default::default()
    };
    let bytes = encode_to_vec(&neg);
    assert_eq!(bytes, [0xA1, 0x05, 0xFA, 0xC0, 0x2D, 0xF8, 0xA1]);
    let decoded: GoldenPrimitives = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(decoded.f32_f, Some(-2.7183_f32));
}

#[test]
fn golden_f64() {
    // cpp float64Positive: 3.14159265359.
    let pos = GoldenPrimitives {
        f64_f: Some(3.14159265359_f64),
        ..Default::default()
    };
    assert_eq!(
        encode_to_vec(&pos),
        [
            0xA1, 0x06, 0xFB, 0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2E, 0xEA
        ]
    );

    // cpp float64Negative: -2.71828182846.
    let neg = GoldenPrimitives {
        f64_f: Some(-2.71828182846_f64),
        ..Default::default()
    };
    assert_eq!(
        encode_to_vec(&neg),
        [
            0xA1, 0x06, 0xFB, 0xC0, 0x05, 0xBF, 0x0A, 0x8B, 0x14, 0x5F, 0xCF
        ]
    );
}

#[test]
fn golden_string_non_ascii() {
    // cpp string5: "foo\ bar©\n b\az" — © is the two UTF-8 bytes C2 A9.
    let v = GoldenPrimitives {
        string_f: Some("foo\\ bar\u{a9}\n b\\az".into()),
        ..Default::default()
    };
    let bytes = encode_to_vec(&v);
    assert_eq!(bytes, concat(&[&[0xA1, 0x07], STRING5]));
    let decoded: GoldenPrimitives = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(decoded.string_f.as_deref(), Some("foo\\ bar\u{a9}\n b\\az"));
}

// ===== Unknown-property skip robustness =====

/// Port of `deserialize_UnknownProperties` (`structSimple1_Unknown`): an
/// unknown property (tag 5) carrying a string value is skipped and the
/// known properties decode normally.
#[test]
fn unknown_string_property_is_skipped() {
    let bytes = concat(&[
        &[0xA4],
        &STRUCT_SIMPLE1_VALID[1..], // the three known property pairs
        &[0x05],                    // unknown tag 5...
        STRING5,                    // ...carrying string5
    ]);
    let decoded: SerializationStructSimple =
        decode_typed_from_slice(&bytes).expect("unknown string property must be skipped");
    assert_eq!(decoded, struct_simple1());
}

/// Port of `deserialize_UnknownProperties` (`structComplex1_Unknown`): the
/// unknown property's value is a whole nested struct map (an entire
/// `structComplex2_Valid` encoding), which must be skipped recursively.
#[test]
fn unknown_nested_struct_property_is_skipped() {
    let bytes = concat(&[
        &[0xA5],
        &[0x07, 0x05],                                                 // enumProperty
        &[0x04, 0xFB, 0xC0, 0x05, 0xBF, 0x0A, 0x8B, 0x14, 0x5F, 0xCF], // float64Property
        &[0x05],                                                       // unknown tag 5...
        STRUCT_COMPLEX2_VALID, // ...carrying a full nested map
        &[
            0x18, 0x19, 0xFB, 0x41, 0xD7, 0x9A, 0x54, 0xCB, 0x60, 0x00, 0x00,
        ], // timepointProperty
        &[0x0F, 0xA1, 0x03, 0xF4], // structSimpleProperty
    ]);
    let decoded: SerializationStructComplex =
        decode_typed_from_slice(&bytes).expect("unknown nested-map property must be skipped");
    assert_eq!(decoded, struct_complex1());
}

/// Hand-encoded variant covering the remaining skip shapes from
/// `TestCborSerializer.skip`: an unknown array, an unknown map, and an
/// unknown text string longer than 23 bytes (one-byte length header,
/// `0x78 <len>`), followed by a known property that must still decode.
#[test]
fn unknown_array_map_and_long_string_properties_are_skipped() {
    use dots_rs_core::minicbor::Encoder;

    let long: String = "x".repeat(33);
    let mut buf = Vec::new();
    let mut e = Encoder::new(&mut buf);
    e.map(4).unwrap();
    e.u32(20).unwrap(); // unknown tag: array value
    e.array(3).unwrap();
    e.bool(true).unwrap();
    e.bool(false).unwrap();
    e.bool(false).unwrap();
    e.u32(21).unwrap(); // unknown tag: nested map value
    e.map(2).unwrap();
    e.u32(1).unwrap();
    e.u32(1).unwrap();
    e.u32(2).unwrap();
    e.str("inner").unwrap();
    e.u32(22).unwrap(); // unknown tag: long string (0x78 0x21 header)
    e.str(&long).unwrap();
    e.u32(1).unwrap(); // known tag 1: int32Property
    e.i32(12345789).unwrap();

    // Pin the long-string header: 33 bytes needs the one-byte-length
    // form `0x78 0x21`, exercising a skip path no <=23-byte string hits.
    let header_pos = buf
        .windows(2)
        .position(|w| w == [0x78, 0x21])
        .expect("long string must use the 0x78 one-byte length header");
    assert_eq!(&buf[header_pos + 2..header_pos + 2 + 4], b"xxxx");

    let decoded: SerializationStructSimple =
        decode_typed_from_slice(&buf).expect("unknown properties must be skipped");
    assert_eq!(decoded.int32_property, Some(12345789));
    assert_eq!(decoded.valid_set().len(), 1);
}

/// Port of `TestCborSerializer.skip`: skip four leading top-level values
/// (two struct maps, an array, a long string) from a concatenated stream,
/// then decode the final struct — byte positions must line up exactly.
#[test]
fn skip_leading_values_then_decode_struct() {
    // cpp `longString` from TestCborSerializer.skip (33 bytes, 0x78 header).
    const LONG_STRING: &[u8] = &[
        0x78, 0x21, 0x66, 0x6F, 0x6F, 0x5C, 0x20, 0x62, 0x61, 0x72, 0xC2, 0xA9, 0x0A, 0x20, 0x62,
        0x5C, 0x61, 0x7A, 0x70, 0x66, 0x6F, 0x6F, 0x5C, 0x20, 0x62, 0x61, 0x72, 0xC2, 0xA9, 0x0A,
        0x20, 0x62, 0x5C, 0x61, 0x7A,
    ];
    // cpp `vectorBool`: [true, false, false].
    const VECTOR_BOOL: &[u8] = &[0x83, 0xF5, 0xF4, 0xF4];

    let stream = concat(&[
        STRUCT_SIMPLE1_VALID,
        STRUCT_COMPLEX2_VALID,
        VECTOR_BOOL,
        LONG_STRING,
        STRUCT_COMPLEX1_VALID,
    ]);

    let mut decoder = dots_rs_core::minicbor::Decoder::new(&stream);
    for _ in 0..4 {
        decoder.skip().expect("skipping a top-level value succeeds");
    }
    let decoded: SerializationStructComplex =
        decode_typed_from_decoder(&mut decoder).expect("decode after skips");
    assert_eq!(decoded, struct_complex1());
    // Everything consumed — mirrors cpp's `EXPECT_FALSE(serializer.inputAvailable())`.
    assert_eq!(decoder.position(), stream.len());
}

// ===== Masked encode (serialize_SimpleStructArgument Specific/None) =====

/// Port of `serialize_SimpleStructArgument`: `encode_into_vec_with_mask`
/// emits the intersection of the mask and the set properties — no
/// automatic key inclusion (mirrors dots-cpp `Serialize(value, includedProperties)`).
#[test]
fn masked_encode_matches_cpp_specific_and_none() {
    let value = struct_simple1();

    // Mask {boolProperty, float32Property}: boolProperty is not set on
    // structSimple1, so only float32 (tag 4) is emitted — cpp
    // `structSimple1_Specific` = 0xA1 + float32 pair.
    let mask = SerializationStructSimple::PROP_BOOL_PROPERTY
        | SerializationStructSimple::PROP_FLOAT32_PROPERTY;
    let mut buf = Vec::new();
    encode_into_vec_with_mask(&value, mask, &mut buf);
    assert_eq!(buf, [0xA1, 0x04, 0xFA, 0x40, 0x49, 0x0E, 0x56]);

    // Mask None: cpp `structSimple1_None` = empty map.
    let mut buf = Vec::new();
    encode_into_vec_with_mask(&value, PropertySet::EMPTY, &mut buf);
    assert_eq!(buf, [0xA0]);

    // Mask All: cpp `structSimple1_All` = the full encoding.
    let mut buf = Vec::new();
    encode_into_vec_with_mask(&value, PropertySet::from_bits(u32::MAX), &mut buf);
    assert_eq!(buf, STRUCT_SIMPLE1_VALID);
}

/// Port of `serialize_ComplexStructArgument` (the Specific cases):
/// masked encodes of the complex structs, pinning the two-byte tag-25
/// map key in isolation and a masked vector property.
#[test]
fn masked_encode_matches_cpp_complex_specific() {
    // cpp structComplex1_Specific: mask {timepointProperty, propertySetProperty};
    // only the timepoint is set.
    let mut buf = Vec::new();
    encode_into_vec_with_mask(
        &struct_complex1(),
        SerializationStructComplex::PROP_TIMEPOINT_PROPERTY
            | SerializationStructComplex::PROP_PROPERTY_SET_PROPERTY,
        &mut buf,
    );
    assert_eq!(
        buf,
        [
            0xA1, 0x18, 0x19, 0xFB, 0x41, 0xD7, 0x9A, 0x54, 0xCB, 0x60, 0x00, 0x00
        ]
    );

    // cpp structComplex2_Specific: mask {enumProperty, durationVectorProperty};
    // only the duration vector is set.
    let mut buf = Vec::new();
    encode_into_vec_with_mask(
        &struct_complex2(),
        SerializationStructComplex::PROP_ENUM_PROPERTY
            | SerializationStructComplex::PROP_DURATION_VECTOR_PROPERTY,
        &mut buf,
    );
    assert_eq!(
        buf,
        [
            0xA1, 0x09, 0x82, //
            0xFB, 0x40, 0x5E, 0xDD, 0x2F, 0x1A, 0x9F, 0xBE, 0x77, //
            0xFB, 0x40, 0x75, 0x61, 0x2B, 0x02, 0x0C, 0x49, 0xBA, //
        ]
    );
}
