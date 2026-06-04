//! End-to-end registry + descriptor exchange tests.
//!
//! Simulates the broker scenario: a peer learns about user-defined
//! types entirely from the wire (via `StructDescriptorData` /
//! `EnumDescriptorData`), registers them, then decodes actual
//! instances using only what's been registered. Cross-roundtrip
//! through this path must produce byte-identical output to the
//! sender's typed-encoded bytes.

use std::sync::Arc;

use dots_core::{DynamicStruct, DynamicStructDescriptor, decode_typed_from_slice, encode_to_vec};
use dots_model::{
    EnumDescriptorData, Registry, RegistryError, StructDescriptorData,
};

// ----- Fixture types (the "sender" side has these compiled) -----

mod model {
    use dots_derive::{DotsEnum, DotsStruct};

    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(name = "Severity")]
    pub enum Severity {
        #[default]
        #[dots(tag = 1)]
        Info,
        #[dots(tag = 2)]
        Warning,
        #[dots(tag = 3)]
        Error,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Address")]
    pub struct Address {
        #[dots(tag = 1)]
        pub street: Option<String>,
        #[dots(tag = 2)]
        pub number: Option<u32>,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "LogEntry", cached)]
    pub struct LogEntry {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub message: Option<String>,
        #[dots(tag = 3)]
        pub severity: Option<Severity>,
        #[dots(tag = 4)]
        pub payload: Option<Vec<u8>>,
        #[dots(tag = 5)]
        pub counters: Option<Vec<u32>>,
        #[dots(tag = 6)]
        pub sender: Option<Address>,
        #[dots(tag = 7)]
        pub cc: Option<Vec<Address>>,
        #[dots(tag = 8)]
        pub severities_seen: Option<Vec<Severity>>,
    }
}
use model::*;

// ----- Build a "received" registry the way a broker would -----

/// Simulates dotsd's startup: receive descriptors over the wire
/// (here, faked by encoding from static descriptors), decode them,
/// and register. Returns a registry where the broker has learned
/// about every type LogEntry transitively references.
fn registry_from_wire() -> Registry {
    let reg = Registry::new();

    // Dependency order: enum first, then nested struct, then top-level.
    let severity_data = EnumDescriptorData::from_static(Severity::DESCRIPTOR);
    reg.register_enum_dynamic(Arc::new(reg.build_dynamic_enum(&severity_data).unwrap()));

    let address_data = StructDescriptorData::from_static(Address::DESCRIPTOR);
    let address_dyn = reg.build_dynamic_struct(&address_data).unwrap();
    reg.register_struct_dynamic(Arc::new(address_dyn));

    let log_entry_data = StructDescriptorData::from_static(LogEntry::DESCRIPTOR);
    // At this point, the registry already has Severity (enum) and
    // Address (struct), so building LogEntry succeeds — its property
    // type names ("Severity", "Address", "vector<Address>", etc.)
    // resolve cleanly.
    let log_entry_dyn = reg.build_dynamic_struct(&log_entry_data).unwrap();
    reg.register_struct_dynamic(Arc::new(log_entry_dyn));

    reg
}

// ----- Tests -----

#[test]
fn build_dynamic_struct_resolves_primitive_fields() {
    let reg = Registry::new();
    let data = StructDescriptorData::from_static(Address::DESCRIPTOR);
    let dyn_desc = reg.build_dynamic_struct(&data).unwrap();
    assert_eq!(dyn_desc.name, "Address");
    assert_eq!(dyn_desc.properties.len(), 2);
    assert_eq!(dyn_desc.properties[0].name, "street");
    assert_eq!(dyn_desc.properties[1].tag, 2);
}

#[test]
fn build_dynamic_struct_fails_for_unregistered_nested_type() {
    let reg = Registry::new();
    let data = StructDescriptorData::from_static(LogEntry::DESCRIPTOR);
    // Severity and Address aren't registered yet.
    let err = reg.build_dynamic_struct(&data).unwrap_err();
    match err {
        RegistryError::UnknownType(name) => {
            assert!(name == "Severity" || name == "Address");
        }
        other => panic!("expected UnknownType, got {other:?}"),
    }
}

#[test]
fn dependency_order_registration_succeeds() {
    let reg = registry_from_wire();
    assert!(reg.lookup("Severity").is_some());
    assert!(reg.lookup("Address").is_some());
    assert!(reg.lookup("LogEntry").is_some());
}

#[test]
fn end_to_end_broker_scenario() {
    // SENDER side: build an actual LogEntry instance, encode it.
    let original = LogEntry {
        id: Some(7),
        message: Some("hello".into()),
        severity: Some(Severity::Warning),
        payload: Some(vec![0xde, 0xad, 0xbe, 0xef]),
        counters: Some(vec![1, 2, 3]),
        sender: Some(Address {
            street: Some("Main".into()),
            number: Some(42),
        }),
        cc: Some(vec![
            Address {
                street: Some("Other".into()),
                number: Some(1),
            },
            Address {
                street: None,
                number: Some(2),
            },
        ]),
        severities_seen: Some(vec![Severity::Info, Severity::Error]),
    };
    let typed_bytes = encode_to_vec(&original);

    // RECEIVER side: build a registry exclusively from wire-form
    // descriptors (no compile-time knowledge of LogEntry/Address/Severity).
    let reg = registry_from_wire();

    // Receiver fetches the LogEntry descriptor it just registered.
    let log_entry_desc = match reg.lookup("LogEntry").unwrap() {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!("LogEntry should be a struct"),
    };

    // Decode the typed bytes using only the registry-built descriptor.
    let dyn_value =
        DynamicStruct::decode(log_entry_desc, &typed_bytes).expect("dynamic decode succeeds");

    // Re-encode and verify byte equality with sender's output.
    let dyn_bytes = dyn_value.encode();
    assert_eq!(typed_bytes, dyn_bytes);

    // Receiver can also re-decode into the typed shape — proving the
    // wire stays compatible across the registry roundtrip.
    let back: LogEntry = decode_typed_from_slice(&dyn_bytes).unwrap();
    assert_eq!(original, back);
}

#[test]
fn building_with_enum_field_resolves_through_registry() {
    let reg = registry_from_wire();
    // Inspect the LogEntry's `severity` property kind.
    let log_entry = match reg.lookup("LogEntry").unwrap() {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let severity_prop = log_entry
        .properties
        .iter()
        .find(|p| p.tag == 3)
        .expect("severity property must exist");
    match &severity_prop.kind {
        dots_core::DynamicFieldKind::Enum(d) => assert_eq!(d.name, "Severity"),
        other => panic!("expected DynamicFieldKind::Enum, got {other:?}"),
    }
}

#[test]
fn vector_of_struct_resolves_correctly() {
    let reg = registry_from_wire();
    let log_entry = match reg.lookup("LogEntry").unwrap() {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let cc_prop = log_entry
        .properties
        .iter()
        .find(|p| p.tag == 7)
        .expect("cc property must exist");
    match &cc_prop.kind {
        dots_core::DynamicFieldKind::Vec(inner) => match inner.as_ref() {
            dots_core::DynamicFieldKind::Struct(d) => assert_eq!(d.name, "Address"),
            other => panic!("expected inner Struct, got {other:?}"),
        },
        other => panic!("expected Vec, got {other:?}"),
    }
}

#[test]
fn registering_static_then_looking_up_works() {
    let reg = Registry::new();
    reg.register_struct_static(Address::DESCRIPTOR);
    let entry = reg.lookup("Address").expect("must be present");
    match entry {
        dots_model::DescriptorEntry::Struct(d) => assert_eq!(d.name, "Address"),
        _ => panic!("wrong entry kind"),
    }
}

#[test]
fn enum_descriptor_data_reverse_conversion() {
    let reg = Registry::new();
    let data = EnumDescriptorData::from_static(Severity::DESCRIPTOR);
    let dyn_desc = reg.build_dynamic_enum(&data).unwrap();
    assert_eq!(dyn_desc.name, "Severity");
    assert_eq!(dyn_desc.elements.len(), 3);
    assert_eq!(dyn_desc.elements[0].name, "Info");
    assert_eq!(dyn_desc.elements[2].value, 3);
}

#[test]
fn struct_descriptor_data_missing_name_errors() {
    let reg = Registry::new();
    let data = StructDescriptorData::default();
    assert_eq!(
        reg.build_dynamic_struct(&data).unwrap_err(),
        RegistryError::MissingField("name")
    );
}

#[test]
fn struct_descriptor_data_with_nested_dynamic_struct_in_registry() {
    // Round-trip a Foo containing a registered-via-wire nested struct.
    // First, register Address dynamically (not from static).
    let reg = Registry::new();
    let address_data = StructDescriptorData::from_static(Address::DESCRIPTOR);
    let address_dyn = Arc::new(reg.build_dynamic_struct(&address_data).unwrap());
    reg.register_struct_dynamic(address_dyn.clone());

    // Now register a hypothetical struct that uses Address by name.
    // Use LogEntry's actual descriptor for ergonomic test data — just
    // need Severity registered too.
    let severity_data = EnumDescriptorData::from_static(Severity::DESCRIPTOR);
    reg.register_enum_dynamic(Arc::new(reg.build_dynamic_enum(&severity_data).unwrap()));

    let log_entry_data = StructDescriptorData::from_static(LogEntry::DESCRIPTOR);
    let log_entry_dyn = reg.build_dynamic_struct(&log_entry_data).unwrap();

    // The nested Address Arc inside LogEntry's `sender` property
    // should be the same Arc we registered earlier.
    let sender_prop = log_entry_dyn
        .properties
        .iter()
        .find(|p| p.tag == 6)
        .unwrap();
    match &sender_prop.kind {
        dots_core::DynamicFieldKind::Struct(d) => {
            assert!(Arc::ptr_eq(d, &address_dyn), "nested ref should share Arc");
        }
        _ => panic!(),
    }
}

#[test]
fn registering_overwrites_existing_entry() {
    let reg = Registry::new();
    reg.register_struct_static(Address::DESCRIPTOR);
    assert_eq!(reg.len(), 1);

    // Register a "different" Address (just rebuilt) — should overwrite.
    let a2 = Arc::new(DynamicStructDescriptor::from_static(Address::DESCRIPTOR));
    reg.register_struct_dynamic(a2.clone());
    assert_eq!(reg.len(), 1, "register replaces, not duplicates");
    match reg.lookup("Address").unwrap() {
        dots_model::DescriptorEntry::Struct(d) => assert!(Arc::ptr_eq(&d, &a2)),
        _ => panic!(),
    }
}
