//! `#[derive(DotsEnum)]` and enum-as-field support.
//!
//! Verifies:
//! - Enum descriptors expose the right metadata (variants, tags, values).
//! - Typed roundtrip — encode enum field, decode, equality.
//! - Wire format — enums encode as a single CBOR `int32`.
//! - Default `tag → value` mapping (matches `.dots` `1: foo` convention).
//! - Explicit `#[dots(tag = N, value = M)]` lets tag and wire value differ.
//! - `AnyStruct` (layout-compatible) and `DynamicStruct` (wire-only)
//!   both round-trip enum-bearing structs to byte-identical output.
//! - Decoding an unknown wire value yields a `DecodeError`, not UB.

use std::sync::Arc;

use dots_rs_core::{
    AnyStruct, DynamicStruct, DynamicStructDescriptor, DynamicValue, FieldKind,
    decode_typed_from_slice, encode_to_vec,
};
mod model {
    use dots_rs_derive::{DotsEnum, DotsStruct};

    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(name = "Status")]
    pub enum Status {
        #[default]
        #[dots(tag = 1)]
        Idle,
        #[dots(tag = 2)]
        Running,
        #[dots(tag = 3)]
        Failed,
    }

    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(name = "Errno")]
    pub enum Errno {
        #[default]
        #[dots(tag = 1, value = 0)]
        Ok,
        #[dots(tag = 2, value = -1)]
        Refused,
        #[dots(tag = 3, value = -42)]
        BadMessage,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Job")]
    pub struct Job {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub status: Option<Status>,
        #[dots(tag = 3)]
        pub last_error: Option<Errno>,
        #[dots(tag = 4)]
        pub history: Option<Vec<Status>>,
    }
}
use model::*;

// ----- Descriptor metadata -----

#[test]
fn enum_descriptor_lists_variants() {
    let d = Status::DESCRIPTOR;
    assert_eq!(d.name, "Status");
    assert_eq!(d.elements.len(), 3);
    assert_eq!(d.elements[0].name, "Idle");
    assert_eq!(d.elements[0].tag, 1);
    assert_eq!(d.elements[0].value, 1);
    assert_eq!(d.elements[2].name, "Failed");
    assert_eq!(d.elements[2].value, 3);
}

/// Mirrors dots-cpp `TestEnumDescriptor.enumeratorFromTag`: a known tag
/// resolves to its enumerator; an unknown tag yields `None` rather than
/// panicking.
#[test]
fn element_by_tag_resolves_known_and_rejects_unknown() {
    let d = Status::DESCRIPTOR;
    assert_eq!(d.element_by_tag(1).unwrap().name, "Idle");
    assert_eq!(d.element_by_tag(2).unwrap().name, "Running");
    assert_eq!(d.element_by_tag(3).unwrap().value, 3);
    assert!(d.element_by_tag(0).is_none());
    assert!(d.element_by_tag(99).is_none());
}

/// Mirrors dots-cpp `TestEnumDescriptor.enumeratorFromValue`: lookup by
/// the on-the-wire integer value. Exercises the negative-value enum
/// (`Errno`) so the `i32` value path — not the tag — is what matches.
#[test]
fn element_by_value_resolves_known_and_rejects_unknown() {
    let d = Status::DESCRIPTOR;
    assert_eq!(d.element_by_value(1).unwrap().name, "Idle");
    assert_eq!(d.element_by_value(3).unwrap().tag, 3);
    assert!(d.element_by_value(0).is_none());
    assert!(d.element_by_value(99).is_none());

    let e = Errno::DESCRIPTOR;
    assert_eq!(e.element_by_value(0).unwrap().name, "Ok");
    assert_eq!(e.element_by_value(-1).unwrap().name, "Refused");
    assert_eq!(e.element_by_value(-42).unwrap().name, "BadMessage");
    // The tags (1,2,3) are not the wire values here, so value 1 misses.
    assert!(e.element_by_value(1).is_none());
}

#[test]
fn explicit_value_overrides_tag_default() {
    let d = Errno::DESCRIPTOR;
    assert_eq!(d.elements[0].tag, 1);
    assert_eq!(d.elements[0].value, 0);
    assert_eq!(d.elements[1].tag, 2);
    assert_eq!(d.elements[1].value, -1);
    assert_eq!(d.elements[2].value, -42);
}

#[test]
fn parent_descriptor_routes_enum_field_to_field_kind_enum() {
    let p = Job::DESCRIPTOR.property(2).unwrap();
    match p.kind {
        FieldKind::Enum(d) => assert_eq!(d.name, "Status"),
        other => panic!("expected FieldKind::Enum, got {other:?}"),
    }
}

#[test]
fn vec_of_enum_field_kind_is_vec_of_enum() {
    let p = Job::DESCRIPTOR.property(4).unwrap();
    match p.kind {
        FieldKind::Vec(inner) => match inner {
            FieldKind::Enum(d) => assert_eq!(d.name, "Status"),
            other => panic!("expected inner Enum, got {other:?}"),
        },
        other => panic!("expected Vec, got {other:?}"),
    }
}

// ----- Wire format -----

#[test]
fn enum_encodes_as_single_int32() {
    let job = Job {
        id: Some(1),
        status: Some(Status::Running),
        ..Default::default()
    };
    let bytes = encode_to_vec(&job);
    // Map(2): {1: 1, 2: 2}
    //   0xa2     map of 2 pairs
    //   0x01     tag 1
    //   0x01     u32 1
    //   0x02     tag 2
    //   0x02     i32 2  (positive small int, same byte as u8)
    assert_eq!(bytes, [0xa2, 0x01, 0x01, 0x02, 0x02]);
}

#[test]
fn negative_enum_value_uses_cbor_negative_int() {
    let job = Job {
        last_error: Some(Errno::Refused),
        ..Default::default()
    };
    let bytes = encode_to_vec(&job);
    // CBOR negative integer -1 is encoded as 0x20.
    // Map(1): {3: -1}
    assert_eq!(bytes, [0xa1, 0x03, 0x20]);
}

// ----- Roundtrips -----

#[test]
fn typed_enum_roundtrip() {
    let original = Job {
        id: Some(42),
        status: Some(Status::Failed),
        last_error: Some(Errno::BadMessage),
        history: Some(vec![Status::Idle, Status::Running, Status::Failed]),
    };
    let bytes = encode_to_vec(&original);
    let decoded: Job = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn anystruct_cross_roundtrip_with_enum() {
    let original = Job {
        id: Some(7),
        status: Some(Status::Running),
        last_error: Some(Errno::Ok),
        history: Some(vec![Status::Idle, Status::Running]),
    };
    let typed_bytes = encode_to_vec(&original);
    let any = AnyStruct::decode_from_slice(Job::DESCRIPTOR, &typed_bytes).unwrap();
    let dyn_bytes = encode_to_vec(&any);
    assert_eq!(typed_bytes, dyn_bytes);
}

#[test]
fn wire_only_dynamic_struct_decodes_enum_field() {
    let original = Job {
        id: Some(11),
        status: Some(Status::Running),
        last_error: Some(Errno::Refused),
        ..Default::default()
    };
    let typed_bytes = encode_to_vec(&original);

    let dyn_desc = Arc::new(DynamicStructDescriptor::from_static(Job::DESCRIPTOR));
    let dyn_value = DynamicStruct::decode(dyn_desc, &typed_bytes).unwrap();
    let re_encoded = dyn_value.encode();
    assert_eq!(typed_bytes, re_encoded);

    // Spot-check that the decoded enum field is DynamicValue::Enum(2)
    // (Status::Running has value 2).
    let (_, status_value) = dyn_value
        .properties
        .iter()
        .find(|(t, _)| *t == 2)
        .unwrap();
    match status_value {
        DynamicValue::Enum(v) => assert_eq!(*v, 2),
        other => panic!("expected DynamicValue::Enum, got {other:?}"),
    }
}

#[test]
fn unknown_enum_value_returns_decode_error() {
    use dots_rs_core::minicbor::Encoder;
    // Construct a Job with status = i32 value 99 (not a valid Status).
    let mut buf = Vec::new();
    let mut e = Encoder::new(&mut buf);
    e.map(1).unwrap();
    e.u32(2).unwrap(); // tag for `status`
    e.i32(99).unwrap();
    let result: Result<Job, _> = decode_typed_from_slice(&buf);
    assert!(result.is_err(), "decode of unknown enum value must fail");
}
