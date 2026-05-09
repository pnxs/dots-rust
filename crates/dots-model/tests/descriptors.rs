//! Round-trip and conversion tests for descriptor data types.

use std::sync::Arc;

use dots_core::{
    DynamicStruct, DynamicStructDescriptor, DynamicValue, decode_typed_from_slice, encode_to_vec,
};
use dots_derive::DotsStruct;
use dots_model::{
    DotsStructFlags, EnumDescriptorData, EnumElementDescriptor, StructDescriptorData,
    StructPropertyData,
};

#[derive(DotsStruct, Default, Debug, PartialEq)]
#[dots(name = "Inner")]
struct Inner {
    #[dots(tag = 1)]
    label: Option<String>,
}

#[derive(DotsStruct, Default, Debug, PartialEq)]
#[dots(name = "Sample", cached, persistent)]
struct Sample {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    payload: Option<String>,
    #[dots(tag = 3)]
    flag: Option<bool>,
    #[dots(tag = 4)]
    raw: Option<Vec<u8>>,
    #[dots(tag = 5)]
    counters: Option<Vec<u32>>,
    #[dots(tag = 6)]
    inner: Option<Inner>,
    #[dots(tag = 7)]
    inners: Option<Vec<Inner>>,
}

// ----- DotsStructFlags -----

#[test]
fn struct_flags_roundtrip() {
    let original = DotsStructFlags {
        cached: Some(true),
        persistent: Some(true),
        substruct_only: Some(false),
        ..Default::default()
    };
    let bytes = encode_to_vec(&original);
    let decoded: DotsStructFlags = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn struct_flags_from_static_matches_descriptor() {
    let flags = DotsStructFlags::from_static(Sample::DESCRIPTOR.flags);
    assert_eq!(flags.cached, Some(true));
    assert_eq!(flags.persistent, Some(true));
    assert_eq!(flags.internal, Some(false));
    assert_eq!(flags.cleanup, Some(false));
    assert_eq!(flags.local, Some(false));
    assert_eq!(flags.substruct_only, Some(false));
}

// ----- StructPropertyData -----

#[test]
fn property_data_roundtrip() {
    let original = StructPropertyData {
        name: Some("count".into()),
        tag: Some(7),
        is_key: Some(false),
        type_name: Some("uint32".into()),
        type_id: None,
    };
    let bytes = encode_to_vec(&original);
    let decoded: StructPropertyData = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn property_data_from_static_emits_correct_type_names() {
    let by_tag: std::collections::HashMap<u32, StructPropertyData> = Sample::DESCRIPTOR
        .properties
        .iter()
        .map(|p| (p.tag, StructPropertyData::from_static(p)))
        .collect();

    assert_eq!(by_tag[&1].type_name.as_deref(), Some("uint32"));
    assert_eq!(by_tag[&1].is_key, Some(true));
    assert_eq!(by_tag[&2].type_name.as_deref(), Some("string"));
    assert_eq!(by_tag[&3].type_name.as_deref(), Some("bool"));
    assert_eq!(by_tag[&4].type_name.as_deref(), Some("vector<uint8>"));
    assert_eq!(by_tag[&5].type_name.as_deref(), Some("vector<uint32>"));
    assert_eq!(by_tag[&6].type_name.as_deref(), Some("Inner"));
    assert_eq!(by_tag[&7].type_name.as_deref(), Some("vector<Inner>"));
}

// ----- StructDescriptorData -----

#[test]
fn descriptor_data_from_static_full_struct() {
    let data = StructDescriptorData::from_static(Sample::DESCRIPTOR);
    assert_eq!(data.name.as_deref(), Some("Sample"));
    assert_eq!(data.publisher_id, None);
    assert!(data.documentation.is_none());

    let flags = data.flags.expect("flags must be set");
    assert_eq!(flags.cached, Some(true));
    assert_eq!(flags.persistent, Some(true));

    let props = data.properties.expect("properties must be set");
    assert_eq!(props.len(), 7);
    assert_eq!(props[0].name.as_deref(), Some("id"));
    assert_eq!(props[0].tag, Some(1));
    assert_eq!(props[0].is_key, Some(true));
}

#[test]
fn descriptor_data_roundtrip() {
    let original = StructDescriptorData::from_static(Sample::DESCRIPTOR);
    let bytes = encode_to_vec(&original);
    let decoded: StructDescriptorData = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn descriptor_data_decodes_via_wire_only_path() {
    // The descriptor data type is itself a DOTS struct; verify the
    // wire-only `DynamicStruct` path can decode it from typed-encoded
    // bytes given just `StructDescriptorData::DESCRIPTOR`'s metadata.
    let original = StructDescriptorData::from_static(Sample::DESCRIPTOR);
    let typed_bytes = encode_to_vec(&original);

    let dyn_desc = Arc::new(DynamicStructDescriptor::from_static(
        StructDescriptorData::DESCRIPTOR,
    ));
    let dyn_value =
        DynamicStruct::decode(dyn_desc, &typed_bytes).expect("wire-only decode succeeds");
    assert_eq!(dyn_value.encode(), typed_bytes);

    // Spot-check that the decoded properties array reflects Sample's
    // tags/types — i.e. dotsd would now know enough to route Sample.
    let (_, props) = dyn_value
        .properties
        .iter()
        .find(|(t, _)| *t == 2)
        .unwrap();
    let DynamicValue::Vec(items) = props else {
        panic!("properties should be a Vec");
    };
    assert_eq!(items.len(), 7);
}

// ----- EnumElementDescriptor / EnumDescriptorData -----

#[test]
fn enum_element_roundtrip() {
    let original = EnumElementDescriptor {
        enum_value: Some(-3),
        name: Some("retired".into()),
        tag: Some(2),
    };
    let bytes = encode_to_vec(&original);
    let decoded: EnumElementDescriptor = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn enum_descriptor_data_roundtrip() {
    let original = EnumDescriptorData {
        name: Some("DotsConnectionState".into()),
        elements: Some(vec![
            EnumElementDescriptor {
                enum_value: Some(1),
                name: Some("connecting".into()),
                tag: Some(1),
            },
            EnumElementDescriptor {
                enum_value: Some(3),
                name: Some("connected".into()),
                tag: Some(2),
            },
            EnumElementDescriptor {
                enum_value: Some(5),
                name: Some("closed".into()),
                tag: Some(3),
            },
        ]),
        publisher_id: Some(42),
    };
    let bytes = encode_to_vec(&original);
    let decoded: EnumDescriptorData = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn enum_descriptor_data_skipped_tag3_is_forward_compat() {
    // Hand-craft bytes that include a value at tag 3 (deprecated). Our
    // EnumDescriptorData has no field at tag 3, so decode must skip it.
    //
    //   0xa3                      = map of 3 pairs
    //   0x01 0x64 'foo'            = tag 1 -> "foo" (text len 4 = 0x64; truncate to 3)
    //
    // For simplicity, use minicbor to construct realistic bytes: name=foo,
    // a junk value at tag 3, and publisher_id=7.
    use dots_core::minicbor::Encoder;
    let mut buf = Vec::new();
    let mut e = Encoder::new(&mut buf);
    e.map(3).unwrap();
    e.u32(1).unwrap();
    e.str("foo").unwrap();
    e.u32(3).unwrap();
    e.u32(99).unwrap(); // junk at deprecated tag
    e.u32(4).unwrap();
    e.u32(7).unwrap();

    let decoded: EnumDescriptorData = decode_typed_from_slice(&buf).unwrap();
    assert_eq!(decoded.name.as_deref(), Some("foo"));
    assert_eq!(decoded.publisher_id, Some(7));
    assert!(decoded.elements.is_none());
}
