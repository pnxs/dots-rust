//! Static `StructDescriptor` metadata.
//!
//! Ports the descriptor-introspection halves of dots-cpp's
//! `TestStaticStruct` / `TestStaticDescriptor`:
//!
//! - `size` / `alignment` / `name` match the underlying Rust type
//!   (`TestStaticDescriptor.size`, `.alignment`, `.name`).
//! - Property `tag`s and `name`s match the `#[dots(tag = N)]` schema
//!   (`TestStaticStruct.PropertiesHaveExpectedTags` / `...Names`).
//! - Each property's reported `offset` equals the real in-memory field
//!   offset (`TestStaticStruct.PropertyOffsetsMatchActualOffsets` and
//!   `_Descriptor_PropertyOffsetsMatchActualOffsets`). This is the
//!   contract that lets `AnyStruct` reinterpret a layout-compatible
//!   buffer through the descriptor.
//! - `key_mask` is exactly the set of `#[dots(key)]` tags
//!   (`TestStaticStruct._KeyProperties`).
//!
//! Operations with no Rust equivalent (`_assign`/`_copy`/`_merge`/
//! `_swap`/`_clear`/`diffProperties`) are intentionally not ported —
//! dots-rust has no in-place struct-mutation API; those flows go
//! through `#[derive]`ed field access or fresh re-encoding instead.

use core::mem::{align_of, offset_of, size_of};

mod model {
    use dots_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Record")]
    pub struct Record {
        #[dots(tag = 1, key)]
        pub realm: Option<u32>,
        #[dots(tag = 2, key)]
        pub id: Option<u64>,
        #[dots(tag = 3)]
        pub label: Option<String>,
        #[dots(tag = 4)]
        pub flag: Option<bool>,
    }
}
use model::*;

#[test]
fn descriptor_size_align_name_match_the_type() {
    let d = Record::DESCRIPTOR;
    assert_eq!(d.name, "Record");
    assert_eq!(d.size, size_of::<Record>());
    assert_eq!(d.align, align_of::<Record>());
}

#[test]
fn properties_have_expected_tags_and_names() {
    let d = Record::DESCRIPTOR;
    let by_tag: Vec<(u32, &str)> = d.properties.iter().map(|p| (p.tag, p.name)).collect();
    assert_eq!(
        by_tag,
        [(1, "realm"), (2, "id"), (3, "label"), (4, "flag")]
    );
}

#[test]
fn property_offsets_match_actual_field_offsets() {
    let d = Record::DESCRIPTOR;
    let offset = |tag: u32| d.property(tag).unwrap().offset;
    assert_eq!(offset(1), offset_of!(Record, realm));
    assert_eq!(offset(2), offset_of!(Record, id));
    assert_eq!(offset(3), offset_of!(Record, label));
    assert_eq!(offset(4), offset_of!(Record, flag));
}

#[test]
fn key_mask_is_exactly_the_key_tags() {
    let d = Record::DESCRIPTOR;
    // Tags 1 and 2 are `#[dots(key)]`; 3 and 4 are not.
    let key_tags: Vec<u32> = d.key_mask().iter().collect();
    assert_eq!(key_tags, [1, 2]);

    let key_names: Vec<&str> = d.key_properties().map(|p| p.name).collect();
    assert_eq!(key_names, ["realm", "id"]);

    assert!(d.property(1).unwrap().is_key);
    assert!(d.property(2).unwrap().is_key);
    assert!(!d.property(3).unwrap().is_key);
    assert!(!d.property(4).unwrap().is_key);
}
