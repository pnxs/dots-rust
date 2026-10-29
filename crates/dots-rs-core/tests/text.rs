//! Public text-conversion surface: the `FromStr` impls, the uuid
//! helpers, and the kind-directed pair as an external consumer sees
//! them.

use std::sync::Arc;

use dots_rs_core::{
    Duration, DynamicEnumDescriptor, DynamicEnumElement, DynamicFieldKind,
    DynamicPropertyDescriptor, DynamicStruct, DynamicStructDescriptor, DynamicValue,
    ParseErrorReason, PropertySet, StructFlags, Timepoint, uuid_from_str, uuid_to_string,
};

#[test]
fn timepoint_from_str_accepts_seconds_and_iso8601() {
    // dots-cpp's `timePoint1`: 2020-03-11T21:07:57.500+00:00.
    let expected = Timepoint(1583960877.5);
    assert_eq!("1583960877.5".parse::<Timepoint>().unwrap(), expected);
    assert_eq!(
        "2020-03-11T21:07:57.5Z".parse::<Timepoint>().unwrap(),
        expected
    );
    assert_eq!(
        "2020-03-11T22:07:57.5+01:00".parse::<Timepoint>().unwrap(),
        expected
    );
    assert_eq!(expected.to_string(), "2020-03-11T21:07:57.5Z");
    assert_eq!(
        expected.to_string().parse::<Timepoint>().unwrap(),
        expected,
        "Display and FromStr must be inverses"
    );

    let err = "not a time".parse::<Timepoint>().unwrap_err();
    assert_eq!(err.type_name, "timepoint");
    assert_eq!(
        err.to_string(),
        "invalid timepoint: expected seconds or ISO-8601"
    );
}

#[test]
fn duration_from_str_accepts_optional_suffix() {
    assert_eq!("123.456s".parse::<Duration>().unwrap(), Duration(123.456));
    assert_eq!("123.456".parse::<Duration>().unwrap(), Duration(123.456));
    assert_eq!(Duration(342.073).to_string(), "342.073s");
    assert_eq!(
        Duration(342.073).to_string().parse::<Duration>().unwrap(),
        Duration(342.073)
    );
    assert!("later".parse::<Duration>().is_err());
}

#[test]
fn property_set_from_str_accepts_tag_list_and_bitmask() {
    let set = PropertySet::EMPTY.with_tag(1).with_tag(3);
    assert_eq!("PropertySet{1,3}".parse::<PropertySet>().unwrap(), set);
    assert_eq!("0b1010".parse::<PropertySet>().unwrap(), set);
    assert_eq!("10".parse::<PropertySet>().unwrap(), set);
    assert_eq!(format!("{set:?}").parse::<PropertySet>().unwrap(), set);
    assert_eq!(
        "PropertySet{32}".parse::<PropertySet>().unwrap_err().reason,
        ParseErrorReason::OutOfRange
    );
}

#[test]
fn uuid_helpers_round_trip() {
    let bytes = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32,
        0x10,
    ];
    let text = uuid_to_string(&bytes);
    assert_eq!(text, "01234567-89ab-cdef-fedc-ba9876543210");
    assert_eq!(uuid_from_str(&text).unwrap(), bytes);
    // The dashed form other DOTS tooling prints is accepted too.
    assert_eq!(
        uuid_from_str("01234567-89ab-cdef-fedc-ba9876543210").unwrap(),
        bytes
    );
}

/// A descriptor-driven edit of one property: parse user text against
/// the property's kind, then store it — the flow a publish dialog or
/// CLI needs.
#[test]
fn parses_user_text_into_a_struct_property() {
    let mood = Arc::new(DynamicEnumDescriptor {
        name: "Mood".into(),
        elements: vec![
            DynamicEnumElement {
                name: "Happy".into(),
                tag: 1,
                value: 0,
            },
            DynamicEnumElement {
                name: "Grumpy".into(),
                tag: 2,
                value: 7,
            },
        ],
    });
    let descriptor = Arc::new(DynamicStructDescriptor {
        name: "Person".into(),
        flags: StructFlags::NONE,
        properties: vec![
            DynamicPropertyDescriptor {
                name: "id".into(),
                tag: 1,
                is_key: true,
                kind: DynamicFieldKind::U32,
            },
            DynamicPropertyDescriptor {
                name: "name".into(),
                tag: 2,
                is_key: false,
                kind: DynamicFieldKind::String,
            },
            DynamicPropertyDescriptor {
                name: "mood".into(),
                tag: 3,
                is_key: false,
                kind: DynamicFieldKind::Enum(mood),
            },
            DynamicPropertyDescriptor {
                name: "scores".into(),
                tag: 4,
                is_key: false,
                kind: DynamicFieldKind::Vec(Box::new(DynamicFieldKind::I16)),
            },
        ],
    });

    let inputs = ["7", "Ada", "Grumpy", "{ -1, 2, 3 }"];
    let mut valid = PropertySet::EMPTY;
    let mut properties = Vec::new();
    for (prop, input) in descriptor.properties.iter().zip(inputs) {
        properties.push((prop.tag, prop.kind.parse_value(input).unwrap()));
        valid = valid.with_tag(prop.tag);
    }

    let person = DynamicStruct {
        descriptor: descriptor.clone(),
        valid,
        properties,
    };
    assert_eq!(
        person.to_string(),
        r#"Person{ id: 7, name: "Ada", mood: Grumpy, scores: [-1, 2, 3] }"#
    );
    assert_eq!(person.get(3), Some(&DynamicValue::Enum(2)));

    // The parsed value survives a wire round-trip, so parsing produced
    // the variant the property's kind encodes.
    let bytes = person.encode();
    let decoded = DynamicStruct::decode(descriptor, &bytes).unwrap();
    assert!(decoded.same(&person));
    assert_eq!(decoded.get(4), person.get(4));
}

#[test]
fn per_kind_text_round_trips_through_the_public_api() {
    let cases = [
        (DynamicFieldKind::Bool, DynamicValue::Bool(false)),
        (DynamicFieldKind::U64, DynamicValue::U64(u64::MAX)),
        (DynamicFieldKind::I32, DynamicValue::I32(i32::MIN)),
        (DynamicFieldKind::F64, DynamicValue::F64(-2.5e-9)),
        (
            DynamicFieldKind::String,
            DynamicValue::String("comma, quote \" and tab \t".into()),
        ),
        (DynamicFieldKind::Uuid, DynamicValue::Uuid([0x5a; 16])),
        (
            DynamicFieldKind::Timepoint,
            DynamicValue::Timepoint(1583960877.5),
        ),
        (DynamicFieldKind::Duration, DynamicValue::Duration(0.001)),
        (
            DynamicFieldKind::PropertySet,
            DynamicValue::PropertySet(PropertySet::EMPTY.with_tag(2)),
        ),
    ];

    for (kind, value) in cases {
        let text = kind.format_value(&value);
        assert_eq!(
            kind.parse_value(&text).unwrap(),
            value,
            "{} round-trip via {text:?}",
            kind.type_name()
        );
    }
}

#[test]
fn uuid_display_and_from_str_use_standard_text() {
    use dots_rs_core::Uuid;
    let canonical = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6";
    let uuid = canonical.parse::<Uuid>().unwrap();
    assert_eq!(uuid.to_string(), canonical);
    assert_eq!(canonical.to_uppercase().parse::<Uuid>().unwrap(), uuid);
    assert_eq!(
        "0xf81d4fae7dec11d0a76500a0c91e6bf6"
            .parse::<Uuid>()
            .unwrap(),
        uuid
    );
    assert_eq!(Uuid::from_bytes(uuid.into_bytes()), uuid);
    assert_eq!(DynamicValue::Uuid(uuid.into_bytes()).to_string(), canonical);
    assert_eq!(
        DynamicFieldKind::Uuid.format_value(&DynamicValue::Uuid(uuid.into_bytes())),
        canonical
    );
    for invalid in [
        "",
        "not-a-uuid",
        "f81d4fae-7dec-11d0-a765-00a0c91e6bf",
        "z81d4fae-7dec-11d0-a765-00a0c91e6bf6",
    ] {
        assert!(invalid.parse::<Uuid>().is_err());
    }
}
