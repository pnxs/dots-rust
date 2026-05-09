//! Nested DOTS struct support.
//!
//! Verifies that:
//! - A struct field of type `Option<Inner>` (where `Inner` is itself a
//!   `#[derive(DotsStruct)]` type) round-trips correctly.
//! - The descriptor reports `FieldKind::Struct(&Inner::DESCRIPTOR)` for
//!   the nested property.
//! - Dynamic decode via `AnyStruct` works when the nested struct's
//!   descriptor is reachable through the parent's descriptor tree.

use dots_core::{AnyStruct, FieldKind, StructValue, decode_typed_from_slice, encode_to_vec};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, PartialEq)]
#[dots(name = "Address")]
struct Address {
    #[dots(tag = 1)]
    street: Option<String>,
    #[dots(tag = 2)]
    number: Option<u32>,
}

#[derive(DotsStruct, Default, Debug, PartialEq)]
#[dots(name = "Person", cached)]
struct Person {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    name: Option<String>,
    #[dots(tag = 3)]
    home: Option<Address>,
}

#[test]
fn nested_field_kind_is_struct() {
    let home_prop = Person::DESCRIPTOR
        .property(3)
        .expect("home property must exist");
    match home_prop.kind {
        FieldKind::Struct(d) => assert_eq!(d.name, "Address"),
        other => panic!("expected FieldKind::Struct, got {other:?}"),
    }
}

#[test]
fn nested_typed_roundtrip() {
    let original = Person {
        id: Some(1),
        name: Some("Ada".into()),
        home: Some(Address {
            street: Some("Lovelace Lane".into()),
            number: Some(42),
        }),
    };
    let bytes = encode_to_vec(&original);
    let decoded: Person = decode_typed_from_slice(&bytes).expect("typed decode succeeds");
    assert_eq!(original, decoded);
}

#[test]
fn nested_partial_inner_roundtrip() {
    // Inner struct with only one field set — nested decode must respect
    // the inner type's partial-object semantics.
    let original = Person {
        id: Some(2),
        name: None,
        home: Some(Address {
            street: None,
            number: Some(7),
        }),
    };
    let bytes = encode_to_vec(&original);
    let decoded: Person = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
    assert_eq!(decoded.home().unwrap().valid_set().len(), 1);
}

#[test]
fn nested_dynamic_decode_via_anystruct() {
    let original = Person {
        id: Some(3),
        name: Some("Linus".into()),
        home: Some(Address {
            street: Some("Helsinki".into()),
            number: Some(1991),
        }),
    };
    let typed_bytes = encode_to_vec(&original);

    // Dynamic decode into AnyStruct, then re-encode — bytes must match.
    let any = AnyStruct::decode_from_slice(Person::DESCRIPTOR, &typed_bytes)
        .expect("dynamic decode succeeds");
    let dynamic_bytes = encode_to_vec(&any);
    assert_eq!(typed_bytes, dynamic_bytes);
}

#[test]
fn nested_anystruct_roundtrip_back_to_typed() {
    let original = Person {
        id: Some(4),
        name: Some("Grace".into()),
        home: Some(Address {
            street: Some("Hopper Hall".into()),
            number: Some(1906),
        }),
    };
    let typed_bytes = encode_to_vec(&original);

    let any = AnyStruct::decode_from_slice(Person::DESCRIPTOR, &typed_bytes)
        .expect("dynamic decode");
    let dynamic_bytes = encode_to_vec(&any);
    let back: Person = decode_typed_from_slice(&dynamic_bytes).expect("typed redecode");
    assert_eq!(original, back);
}

#[test]
fn nested_field_with_no_inner_value() {
    let original = Person {
        id: Some(5),
        name: Some("Anonymous".into()),
        home: None,
    };
    let bytes = encode_to_vec(&original);
    let decoded: Person = decode_typed_from_slice(&bytes).expect("decode succeeds");
    assert_eq!(original, decoded);
    assert!(decoded.home().is_none());
}
