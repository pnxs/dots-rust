//! End-to-end registry + descriptor exchange tests.
//!
//! Simulates the broker scenario: a peer learns about user-defined
//! types entirely from the wire (via `StructDescriptorData` /
//! `EnumDescriptorData`), registers them, then decodes actual
//! instances using only what's been registered. Cross-roundtrip
//! through this path must produce byte-identical output to the
//! sender's typed-encoded bytes.

use std::sync::Arc;

use dots_rs_core::{
    DynamicStruct, DynamicStructDescriptor, decode_typed_from_slice, encode_to_vec,
};
use dots_rs_model::{
    EnumDescriptorData, Registry, RegistryError, StaticTypePolicy, StructDescriptorData,
};

// ----- Fixture types (the "sender" side has these compiled) -----

mod model {
    use dots_rs_derive::{DotsEnum, DotsStruct};

    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(rt = dots_rs_model::__rt)]
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
    #[dots(rt = dots_rs_model::__rt)]
    #[dots(name = "Address")]
    pub struct Address {
        #[dots(tag = 1)]
        pub street: Option<String>,
        #[dots(tag = 2)]
        pub number: Option<u32>,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(rt = dots_rs_model::__rt)]
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
        dots_rs_model::DescriptorEntry::Struct(d) => d.clone(),
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
        dots_rs_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let severity_prop = log_entry
        .properties
        .iter()
        .find(|p| p.tag == 3)
        .expect("severity property must exist");
    match &severity_prop.kind {
        dots_rs_core::DynamicFieldKind::Enum(d) => assert_eq!(d.name, "Severity"),
        other => panic!("expected DynamicFieldKind::Enum, got {other:?}"),
    }
}

#[test]
fn vector_of_struct_resolves_correctly() {
    let reg = registry_from_wire();
    let log_entry = match reg.lookup("LogEntry").unwrap() {
        dots_rs_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let cc_prop = log_entry
        .properties
        .iter()
        .find(|p| p.tag == 7)
        .expect("cc property must exist");
    match &cc_prop.kind {
        dots_rs_core::DynamicFieldKind::Vec(inner) => match inner.as_ref() {
            dots_rs_core::DynamicFieldKind::Struct(d) => assert_eq!(d.name, "Address"),
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
        dots_rs_model::DescriptorEntry::Struct(d) => assert_eq!(d.name, "Address"),
        _ => panic!("wrong entry kind"),
    }
}

/// The `StaticTypePolicy` levels: `FundamentalOnly` yields an empty
/// registry (fundamentals are codec-native), `InternalOnly` seeds the
/// DOTS-internal types but no user types, `All` additionally seeds
/// every compiled-in type from the link-time slices.
#[test]
fn static_type_policy_levels() {
    use dots_rs_core::GlobalRegistration;
    // Runtime no-ops, but their presence anchors the slice entries at
    // link time — the same way `container::<T>` / `subscribe::<T>` /
    // `view::<T>` call sites do.
    Address::register_as_subscribed();
    LogEntry::register_as_subscribed_filtered();

    let fundamental = Registry::with_static_types(StaticTypePolicy::FundamentalOnly);
    assert!(fundamental.is_empty());

    let internal = Registry::with_static_types(StaticTypePolicy::InternalOnly);
    assert!(internal.lookup("DotsHeader").is_some());
    assert!(
        internal.lookup("Address").is_none(),
        "InternalOnly must not seed user types"
    );

    let all = Registry::with_static_types(StaticTypePolicy::All);
    assert!(all.lookup("DotsHeader").is_some());
    assert!(
        all.lookup_static_struct("Address").is_some(),
        "All must seed compiled-in user types from the slices"
    );
    assert!(
        all.lookup_static_struct("LogEntry").is_some(),
        "All must also seed view types (SUBSCRIBED_FILTERED_TYPES)"
    );
}

/// Re-registering the same `&'static StructDescriptor` must keep the
/// existing dynamic projection: `DynamicStruct` operations compare
/// descriptors by `Arc` identity, so a fresh Arc per registration
/// would make values decoded before the re-registration incomparable
/// with values decoded after.
#[test]
fn re_registering_same_static_descriptor_is_idempotent() {
    let reg = Registry::new();
    reg.register_struct_static(Address::DESCRIPTOR);
    let first = match reg.lookup("Address").unwrap() {
        dots_rs_model::DescriptorEntry::Struct(d) => d,
        _ => panic!("wrong entry kind"),
    };

    reg.register_struct_static(Address::DESCRIPTOR);
    let second = match reg.lookup("Address").unwrap() {
        dots_rs_model::DescriptorEntry::Struct(d) => d,
        _ => panic!("wrong entry kind"),
    };

    assert!(
        Arc::ptr_eq(&first, &second),
        "repeated static registration must not mint a new descriptor Arc"
    );
}

/// A wire-learned dynamic entry is still replaced when the static
/// descriptor for the same name shows up (promotion to static), and
/// the promoted entry then stays stable.
#[test]
fn static_registration_promotes_over_dynamic_entry() {
    let reg = Registry::new();
    let data = StructDescriptorData::from_static(Address::DESCRIPTOR);
    let wire = Arc::new(reg.build_dynamic_struct(&data).unwrap());
    reg.register_struct_dynamic(wire.clone());
    assert!(reg.lookup_static_struct("Address").is_none());

    reg.register_struct_static(Address::DESCRIPTOR);
    assert!(reg.lookup_static_struct("Address").is_some());
    let promoted = match reg.lookup("Address").unwrap() {
        dots_rs_model::DescriptorEntry::Struct(d) => d,
        _ => panic!("wrong entry kind"),
    };
    assert!(
        !Arc::ptr_eq(&promoted, &wire),
        "static registration must replace the wire-learned entry"
    );

    reg.register_struct_static(Address::DESCRIPTOR);
    let after = match reg.lookup("Address").unwrap() {
        dots_rs_model::DescriptorEntry::Struct(d) => d,
        _ => panic!("wrong entry kind"),
    };
    assert!(Arc::ptr_eq(&promoted, &after));
}

#[test]
fn enum_descriptor_data_reverse_conversion() {
    let reg = Registry::new();
    let data = EnumDescriptorData::from_static(Severity::DESCRIPTOR);
    let dyn_desc = reg.build_dynamic_enum(&data).unwrap();
    assert_eq!(dyn_desc.name, "Severity");
    assert_eq!(dyn_desc.elements.len(), 3);
    assert_eq!(dyn_desc.elements[0].name, "Info");
    // Metadata value defaults to `tag - 1` (dots-cpp generator default).
    assert_eq!(dyn_desc.elements[2].value, 2);
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
        dots_rs_core::DynamicFieldKind::Struct(d) => {
            assert!(Arc::ptr_eq(d, &address_dyn), "nested ref should share Arc");
        }
        _ => panic!(),
    }
}

/// Ports dots-cpp `TestRegistry.forEach` (the
/// `forEach<StructDescriptor>` / `forEach<EnumDescriptor>` filtered
/// walks): with a mix of static structs, dynamic structs, and enums
/// registered, `iter_structs` yields exactly the structs and
/// `iter_enums` exactly the enums.
#[test]
fn iter_structs_and_iter_enums_partition_the_registry() {
    use dots_rs_core::{
        DynamicEnumDescriptor, DynamicEnumElement, DynamicFieldKind, DynamicPropertyDescriptor,
        StructFlags,
    };

    let reg = Registry::new();

    // Static structs.
    reg.register_struct_static(Address::DESCRIPTOR);
    reg.register_struct_static(LogEntry::DESCRIPTOR);

    // A purely dynamic struct (no compile-time counterpart).
    reg.register_struct_dynamic(Arc::new(DynamicStructDescriptor {
        name: "WireOnly".into(),
        flags: StructFlags::NONE,
        properties: vec![DynamicPropertyDescriptor {
            name: "id".into(),
            tag: 1,
            is_key: true,
            kind: DynamicFieldKind::U32,
        }],
    }));

    // A static enum and a purely dynamic enum.
    reg.register_enum_static(Severity::DESCRIPTOR);
    reg.register_enum_dynamic(Arc::new(DynamicEnumDescriptor {
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
                value: 1,
            },
        ],
    }));

    assert_eq!(reg.len(), 5);

    // iter_structs: exactly the three structs, no enums.
    let structs = reg.iter_structs();
    assert_eq!(structs.len(), 3);
    let mut struct_names: Vec<&str> = structs.iter().map(|d| d.name.as_str()).collect();
    struct_names.sort_unstable();
    assert_eq!(struct_names, ["Address", "LogEntry", "WireOnly"]);

    // iter_enums: exactly the two enums, no structs.
    let enums = reg.iter_enums();
    assert_eq!(enums.len(), 2);
    let mut enum_names: Vec<&str> = enums.iter().map(|d| d.name.as_str()).collect();
    enum_names.sort_unstable();
    assert_eq!(enum_names, ["Mood", "Severity"]);
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
        dots_rs_model::DescriptorEntry::Struct(d) => assert!(Arc::ptr_eq(&d, &a2)),
        _ => panic!(),
    }
}

#[test]
fn parse_type_name_resolves_instance_refs() {
    use dots_rs_core::DynamicFieldKind;

    let reg = Registry::new();
    assert!(matches!(
        reg.parse_type_name("instance_ref").unwrap(),
        DynamicFieldKind::InstanceRef
    ));
    // Typed targets stay names — no registry lookup, so an unknown
    // target type resolves fine (the key bytes are opaque anyway).
    match reg.parse_type_name("instance_ref<NetworkRoute>").unwrap() {
        DynamicFieldKind::InstanceRefTo(target) => assert_eq!(target, "NetworkRoute"),
        other => panic!("expected InstanceRefTo, got {other:?}"),
    }
    match reg
        .parse_type_name("vector<instance_ref<Address>>")
        .unwrap()
    {
        DynamicFieldKind::Vec(inner) => {
            assert!(matches!(*inner, DynamicFieldKind::InstanceRefTo(ref t) if t == "Address"));
        }
        other => panic!("expected Vec, got {other:?}"),
    }
    assert_eq!(
        reg.parse_type_name("instance_ref<>").unwrap_err(),
        RegistryError::MalformedTypeName("instance_ref<>".into())
    );
}

#[test]
fn build_dynamic_struct_accepts_foreign_key_kinds() {
    use dots_rs_core::DynamicFieldKind;
    use dots_rs_model::StructPropertyData;

    // A wire descriptor with a float-typed key: no dots-rust program
    // can publish this (the derive rejects it), but a foreign peer may.
    // Receivers don't police spec §6.3 — the type must still register
    // so it is cached and routed; the kind predicate is there for
    // tools that want to flag it.
    let reg = Registry::new();
    let data = StructDescriptorData {
        name: Some("Rogue".into()),
        properties: Some(vec![StructPropertyData {
            name: Some("value".into()),
            tag: Some(1),
            is_key: Some(true),
            type_name: Some("float64".into()),
            type_id: None,
        }]),
        ..Default::default()
    };
    let rogue = reg.build_dynamic_struct(&data).unwrap();
    let key = &rogue.properties[0];
    assert!(key.is_key);
    assert!(matches!(key.kind, DynamicFieldKind::F64));
    assert!(!key.kind.is_valid_key_kind());

    // A typed reference is a legal key kind.
    let data = StructDescriptorData {
        name: Some("Fine".into()),
        properties: Some(vec![StructPropertyData {
            name: Some("subject".into()),
            tag: Some(1),
            is_key: Some(true),
            type_name: Some("instance_ref<NetworkRoute>".into()),
            type_id: None,
        }]),
        ..Default::default()
    };
    let fine = reg.build_dynamic_struct(&data).unwrap();
    assert!(fine.properties[0].kind.is_valid_key_kind());
}
