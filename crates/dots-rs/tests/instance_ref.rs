use dots_rs::{DotsStruct, InstanceRef, StructValue, dots};

mod model {
    use super::*;
    // Deliberately no Default; key declaration order differs from tag order.
    #[derive(DotsStruct, Debug, PartialEq, Clone)]
    #[dots(cached)]
    pub struct Device {
        #[dots(tag = 3, key)]
        pub name: String,
        #[dots(tag = 2)]
        pub label: Option<String>,
        #[dots(tag = 1, key)]
        pub site: Option<u32>,
    }

    #[derive(DotsStruct, Clone)]
    #[dots(cached)]
    pub struct Singleton {
        #[dots(tag = 1)]
        pub value: Option<String>,
    }
}
use model::*;

#[test]
fn materialize_typed_key() {
    let value = dots!(Device {
        name: "eth0",
        site: 7_u32,
        label: "uplink"
    });
    let reference = InstanceRef::of(&value);
    let decoded = InstanceRef::<Device>::decode(&reference.encode()).unwrap();
    let key = decoded.materialize_key().unwrap();

    assert_eq!(
        key,
        dots!(Device {
            name: "eth0",
            site: 7_u32
        })
    );
    assert_eq!(key.valid_set(), Device::PROP_SITE | Device::PROP_NAME);
    assert_eq!(InstanceRef::of(&key), reference);
}

#[test]
fn materialize_rejects_keys_that_do_not_match_target() {
    assert!(InstanceRef::<Device>::default().materialize_key().is_err());
    // Reference decoding checks the key array's shape, but not target types.
    for bytes in [&[0x81, 0x01][..], &[0x82, 0x01, 0x02][..]] {
        let reference = InstanceRef::<Device>::decode(bytes).unwrap();
        assert!(reference.materialize_key().is_err());
    }
}

#[test]
fn materialize_singleton_key() {
    let reference = InstanceRef::of(&dots!(Singleton { value: "present" }));
    let key = reference.materialize_key().unwrap();
    assert!(key.value.is_none());
    assert!(key.valid_set().is_empty());
}

#[test]
fn display_preserves_escaped_string_keys() {
    let value = dots!(Device { site: 7_u32, name: "a\"b\\c\n\t\0\u{1b}é" });
    let reference = InstanceRef::of(&value);
    let expected = r#"Device[7,"a\"b\\c\n\t\0\u{1b}é"]"#;
    assert_eq!(reference.to_string(), expected);
    let generic = reference.into_any();
    assert_eq!(generic.to_string(), expected);
    let dynamic = dots_rs::DynamicValue::InstanceRef {
        type_name: Some("Device".into()),
        key: generic.key_bytes().to_vec(),
    };
    assert_eq!(dynamic.to_string(), expected);
}
