//! Conformance tests for the canonical instance key and the
//! `instance_ref` field types — the golden vectors of wire-format spec
//! §6.6, byte for byte. A change to any expected byte sequence here is
//! a compatibility break with every other implementation of the spec.

use dots_rs_core::{
    AnyInstance, DynamicStruct, InstanceRef, encode_key_bytes, encode_to_vec, instance_ref,
    materialize_key,
};

mod model {
    use dots_rs_derive::DotsStruct;

    /// Spec §6.5: `struct ExampleType { 1: [key] string name; 2: bool enabled; }`
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "ExampleType")]
    pub struct ExampleType {
        #[dots(tag = 1, key)]
        pub name: Option<String>,
        #[dots(tag = 2)]
        pub enabled: Option<bool>,
    }

    /// Spec §6.5: `struct RouteType { 1/2: [key] string; 3: string via; }`
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "RouteType")]
    pub struct RouteType {
        #[dots(tag = 1, key)]
        pub parent: Option<String>,
        #[dots(tag = 2, key)]
        pub name: Option<String>,
        #[dots(tag = 3)]
        pub via: Option<String>,
    }

    /// Spec §6.5: keyless singleton.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "TimeType")]
    pub struct TimeType {
        #[dots(tag = 1)]
        pub tz: Option<String>,
    }

    /// Spec §6.6: a generic reference used as a key.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "StatusType")]
    pub struct StatusType {
        #[dots(tag = 1, key)]
        pub subject: Option<dots_rs_core::InstanceRef>,
        #[dots(tag = 2)]
        pub applied: Option<bool>,
    }

    /// Both reference forms side by side, for round-trip coverage.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "RefPair")]
    pub struct RefPair {
        #[dots(tag = 1)]
        pub generic: Option<dots_rs_core::InstanceRef>,
        #[dots(tag = 2)]
        pub typed: Option<dots_rs_core::InstanceRef<ExampleType>>,
    }
}
use model::{ExampleType, RefPair, RouteType, StatusType, TimeType};

fn eth0() -> ExampleType {
    ExampleType {
        name: Some("eth0".into()),
        enabled: Some(true),
    }
}

const ETH0_KEY: &[u8] = &[0x81, 0x64, b'e', b't', b'h', b'0'];
const ETH0_REF: &[u8] = &[
    0x82, 0x6B, b'E', b'x', b'a', b'm', b'p', b'l', b'e', b'T', b'y', b'p', b'e', 0x81, 0x64, b'e',
    b't', b'h', b'0',
];

/// Spec §6.6 table for `ExampleType { name: "eth0", enabled: true }`.
#[test]
fn golden_vectors_example_type() {
    let value = eth0();

    // Instance with both properties: {1: "eth0", 2: true}.
    assert_eq!(
        encode_to_vec(&value),
        [0xA2, 0x01, 0x64, b'e', b't', b'h', b'0', 0x02, 0xF5]
    );
    // Canonical key: ["eth0"].
    assert_eq!(encode_key_bytes(&value), ETH0_KEY);
    // Generic `instance_ref` value: ["ExampleType", ["eth0"]].
    let mut buf = Vec::new();
    instance_ref(&value).encode_into(&mut buf);
    assert_eq!(buf, ETH0_REF);
    // Typed `instance_ref<ExampleType>` value: bare canonical key.
    let mut buf = Vec::new();
    InstanceRef::of(&value).encode_into(&mut buf);
    assert_eq!(buf, ETH0_KEY);
}

/// Spec §6.6: composite key `["eth0","r1"]`, and the keyless `[]`.
#[test]
fn golden_vectors_composite_and_keyless() {
    let route = RouteType {
        parent: Some("eth0".into()),
        name: Some("r1".into()),
        via: Some("192.0.2.1".into()),
    };
    assert_eq!(
        encode_key_bytes(&route),
        [0x82, 0x64, b'e', b't', b'h', b'0', 0x62, b'r', b'1']
    );

    let time = TimeType {
        tz: Some("UTC".into()),
    };
    assert_eq!(encode_key_bytes(&time), [0x80]);
}

/// Spec §6.6: a reference used as a key — canonical keys nest.
#[test]
fn golden_vector_ref_as_key() {
    let status = StatusType {
        subject: Some(instance_ref(&eth0())),
        applied: Some(true),
    };
    let expected: Vec<u8> = core::iter::once(0x81u8)
        .chain(ETH0_REF.iter().copied())
        .collect();
    assert_eq!(encode_key_bytes(&status), expected);

    // And the whole thing survives a wire round-trip.
    let bytes = encode_to_vec(&status);
    let decoded: StatusType = dots_rs_core::decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(decoded, status);
}

/// Static and dynamic paths must produce identical canonical keys.
#[test]
fn static_and_dynamic_keys_agree() {
    for (static_key, value) in [
        (
            encode_key_bytes(&eth0()),
            DynamicStruct::from_struct_value(&eth0()),
        ),
        (
            encode_key_bytes(&RouteType {
                parent: Some("eth0".into()),
                name: Some("r1".into()),
                via: None,
            }),
            DynamicStruct::from_struct_value(&RouteType {
                parent: Some("eth0".into()),
                name: Some("r1".into()),
                via: None,
            }),
        ),
    ] {
        assert_eq!(static_key, value.key_bytes());
    }
}

#[test]
fn both_ref_forms_round_trip_inside_a_struct() {
    let pair = RefPair {
        generic: Some(instance_ref(&eth0())),
        typed: Some(InstanceRef::of(&eth0())),
    };
    let bytes = encode_to_vec(&pair);
    let decoded: RefPair = dots_rs_core::decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(decoded, pair);

    let typed = decoded.typed.unwrap();
    assert_eq!(typed.type_name(), "ExampleType");
    assert_eq!(typed.key_bytes(), ETH0_KEY);
    // Widening a typed ref materializes the name and yields the same
    // identity the generic form carries.
    assert_eq!(typed.into_any(), decoded.generic.unwrap());
}

#[test]
fn typed_ref_materializes_back_to_key_properties() {
    let typed: InstanceRef<ExampleType> = InstanceRef::of(&eth0());
    let descriptor = std::sync::Arc::new(dots_rs_core::DynamicStructDescriptor::from_static(
        ExampleType::DESCRIPTOR,
    ));
    let partial = materialize_key(&descriptor, typed.key_bytes()).unwrap();
    assert_eq!(
        partial.get(1),
        Some(&dots_rs_core::DynamicValue::String("eth0".into()))
    );
    assert_eq!(partial.get(2), None, "non-key properties stay unset");
    // Materialization preserves identity: key → partial instance → key
    // is a fixed point.
    assert_eq!(partial.key_bytes(), typed.key_bytes());
}

#[test]
fn display_is_diagnostic() {
    assert_eq!(
        format!("{}", instance_ref(&eth0())),
        r#"ExampleType["eth0"]"#
    );
    assert_eq!(
        format!("{}", InstanceRef::of(&eth0())),
        r#"ExampleType["eth0"]"#
    );
}

/// References compare by encoded identity — usable as map keys.
#[test]
fn refs_are_identity_comparable() {
    use std::collections::HashSet;

    let a = instance_ref(&eth0());
    let b = instance_ref(&ExampleType {
        name: Some("eth0".into()),
        enabled: Some(false), // non-key difference: same identity
    });
    let c = instance_ref(&ExampleType {
        name: Some("eth1".into()),
        enabled: Some(true),
    });
    assert_eq!(a, b);
    assert_ne!(a, c);

    let set: HashSet<InstanceRef<AnyInstance>> = [a, b, c].into_iter().collect();
    assert_eq!(set.len(), 2);
}
