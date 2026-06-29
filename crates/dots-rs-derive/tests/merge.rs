//! `AnyStruct::merge_from` — the flat, masked partial-update overlay
//! that backs the cache's update path (matching dots-cpp
//! `Container::updateWithoutKeys`).
//!
//! For each non-key property whose tag is in the mask: set in the
//! source → deep-clone it into the target; unset in the source → clear
//! it in the target (`set_none`). Properties outside the mask, and all
//! key properties, are untouched.
//!
//! These exercise the soundness-sensitive parts: owned fields
//! (`String`/`Vec`) are dropped-then-written without leak or
//! double-free, and niche-optimized `Option<bool>` clears to a real
//! `None` rather than `Some(false)`.

use std::sync::Arc;

use dots_rs_core::{
    AnyStruct, DynamicStruct, DynamicStructDescriptor, PropertySet, decode_typed_from_slice,
    encode_to_vec,
};

mod model {
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Rec")]
    pub struct Rec {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub label: Option<String>,
        #[dots(tag = 3)]
        pub count: Option<u64>,
        #[dots(tag = 4)]
        pub flag: Option<bool>,
        #[dots(tag = 5)]
        pub items: Option<Vec<u32>>,
    }
}
use model::*;

/// `PropertySet` containing exactly the given tags.
fn mask(tags: &[u32]) -> PropertySet {
    tags.iter()
        .fold(PropertySet::EMPTY, |acc, &t| acc.with_tag(t))
}

/// Merge `src` (as a typed value) into `base` over `m`, returning the
/// merged `Rec` read back out of the buffer.
fn merge(base: &Rec, src: &Rec, m: PropertySet) -> Rec {
    let mut target = AnyStruct::from_struct_value(base);
    target.merge_from(src, m);
    target.as_typed::<Rec>().unwrap().clone()
}

/// `merge_take` counterpart of [`merge`]: builds an owned `AnyStruct`
/// for `src`, moves its masked properties into `base`, and returns both
/// the merged target and what remains of the (drained) source.
fn merge_take(base: &Rec, src: &Rec, m: PropertySet) -> (Rec, Rec) {
    let mut target = AnyStruct::from_struct_value(base);
    let mut source = AnyStruct::from_struct_value(src);
    target.merge_take(&mut source, m);
    (
        target.as_typed::<Rec>().unwrap().clone(),
        source.as_typed::<Rec>().unwrap().clone(),
    )
}

#[test]
fn overlays_set_property_and_preserves_out_of_mask() {
    let base = Rec {
        id: Some(1),
        label: Some("old".into()),
        count: Some(10),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        label: Some("new".into()),
        ..Default::default()
    };
    // Mask names only tag 2 (`label`).
    let merged = merge(&base, &update, mask(&[2]));
    assert_eq!(merged.label.as_deref(), Some("new")); // overlaid
    assert_eq!(merged.count, Some(10)); // out of mask → preserved
    assert_eq!(merged.id, Some(1)); // key untouched
}

#[test]
fn clears_property_in_mask_but_unset_in_source() {
    let base = Rec {
        id: Some(1),
        label: Some("present".into()),
        count: Some(10),
        ..Default::default()
    };
    // Source carries neither label nor count, but the mask addresses
    // both → explicit clear (drops the owned String soundly).
    let update = Rec {
        id: Some(1),
        ..Default::default()
    };
    let merged = merge(&base, &update, mask(&[2, 3]));
    assert_eq!(merged.label, None);
    assert_eq!(merged.count, None);
    assert_eq!(merged.id, Some(1));
}

/// The niche guard: `Option<bool>` does not place `None` at the zero
/// bit-pattern, so clearing must write a real `None` — not leave
/// `Some(false)` behind.
#[test]
fn clearing_bool_yields_none_not_some_false() {
    let base = Rec {
        id: Some(1),
        flag: Some(false),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        ..Default::default()
    };
    let merged = merge(&base, &update, mask(&[4]));
    assert_eq!(merged.flag, None);
}

#[test]
fn overlaying_false_bool_is_distinct_from_clearing() {
    let base = Rec {
        id: Some(1),
        flag: Some(true),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        flag: Some(false),
        ..Default::default()
    };
    let merged = merge(&base, &update, mask(&[4]));
    assert_eq!(merged.flag, Some(false));
}

#[test]
fn key_properties_are_never_modified() {
    let base = Rec {
        id: Some(5),
        label: Some("a".into()),
        ..Default::default()
    };
    let update = Rec {
        id: Some(999),
        label: Some("b".into()),
        ..Default::default()
    };
    // Full mask — even so, the key (tag 1) must stay as it was.
    let merged = merge(&base, &update, PropertySet::from_bits(u32::MAX));
    assert_eq!(merged.id, Some(5));
    assert_eq!(merged.label.as_deref(), Some("b"));
}

#[test]
fn owned_vec_overwrite_then_clear() {
    let base = Rec {
        id: Some(1),
        items: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    // Overwrite the Vec.
    let overwrite = Rec {
        id: Some(1),
        items: Some(vec![9]),
        ..Default::default()
    };
    let merged = merge(&base, &overwrite, mask(&[5]));
    assert_eq!(merged.items, Some(vec![9]));

    // Clear the Vec (drops the owned allocation soundly).
    let clear = Rec {
        id: Some(1),
        ..Default::default()
    };
    let cleared = merge(&base, &clear, mask(&[5]));
    assert_eq!(cleared.items, None);
}

// ===== merge_take (move instead of clone) =====

#[test]
fn take_moves_owned_field_and_drains_source() {
    let base = Rec {
        id: Some(1),
        label: Some("old".into()),
        count: Some(10),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        label: Some("moved".into()),
        ..Default::default()
    };
    // Mask names only `label` (tag 2).
    let (merged, drained) = merge_take(&base, &update, mask(&[2]));

    // Target received the moved value; out-of-mask `count` preserved.
    assert_eq!(merged.label.as_deref(), Some("moved"));
    assert_eq!(merged.count, Some(10));
    assert_eq!(merged.id, Some(1));

    // The source's moved-out slot is now None; its key is untouched
    // (so the drained source is still a valid, droppable value).
    assert_eq!(drained.label, None);
    assert_eq!(drained.id, Some(1));
}

#[test]
fn take_clears_property_in_mask_but_unset_in_source() {
    let base = Rec {
        id: Some(1),
        label: Some("present".into()),
        count: Some(10),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        ..Default::default()
    };
    let (merged, _drained) = merge_take(&base, &update, mask(&[2, 3]));
    assert_eq!(merged.label, None);
    assert_eq!(merged.count, None);
}

#[test]
fn take_clearing_bool_yields_none_not_some_false() {
    let base = Rec {
        id: Some(1),
        flag: Some(false),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        ..Default::default()
    };
    let (merged, _) = merge_take(&base, &update, mask(&[4]));
    assert_eq!(merged.flag, None);
}

#[test]
fn take_moves_vec_without_cloning_then_drains() {
    let base = Rec {
        id: Some(1),
        items: Some(vec![1, 2, 3]),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        items: Some(vec![9, 9]),
        ..Default::default()
    };
    let (merged, drained) = merge_take(&base, &update, mask(&[5]));
    assert_eq!(merged.items, Some(vec![9, 9]));
    assert_eq!(drained.items, None); // moved out of the source
}

#[test]
fn take_never_modifies_key_or_source_key() {
    let base = Rec {
        id: Some(5),
        label: Some("a".into()),
        ..Default::default()
    };
    let update = Rec {
        id: Some(999),
        label: Some("b".into()),
        ..Default::default()
    };
    let (merged, drained) = merge_take(&base, &update, PropertySet::from_bits(u32::MAX));
    assert_eq!(merged.id, Some(5)); // target key unchanged
    assert_eq!(merged.label.as_deref(), Some("b"));
    assert_eq!(drained.id, Some(999)); // source key not moved out
}

// ===== DynamicStruct merge (wire-only representation) =====

/// Build the wire-only `DynamicStruct` form of a `Rec`.
fn dyn_of(r: &Rec) -> DynamicStruct {
    let desc = Arc::new(DynamicStructDescriptor::from_static(Rec::DESCRIPTOR));
    DynamicStruct::decode(desc, &encode_to_vec(r)).unwrap()
}

/// `merge_take` two `DynamicStruct`s and decode the result back to a
/// `Rec`, plus the drained source.
fn dyn_merge_take(base: &Rec, src: &Rec, m: PropertySet) -> (Rec, Rec) {
    let mut b = dyn_of(base);
    let mut s = dyn_of(src);
    b.merge_take(&mut s, m);
    (
        decode_typed_from_slice::<Rec>(&b.encode()).unwrap(),
        // The drained source keeps its key (tag 1), so it's still a
        // decodable Rec.
        decode_typed_from_slice::<Rec>(&s.encode()).unwrap(),
    )
}

#[test]
fn dyn_overlays_set_and_preserves_out_of_mask() {
    let base = Rec {
        id: Some(1),
        label: Some("old".into()),
        count: Some(10),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        label: Some("new".into()),
        ..Default::default()
    };
    let (merged, drained) = dyn_merge_take(&base, &update, mask(&[2]));
    assert_eq!(merged.label.as_deref(), Some("new")); // overlaid
    assert_eq!(merged.count, Some(10)); // out of mask → preserved
    assert_eq!(merged.id, Some(1)); // key kept
    assert_eq!(drained.label, None); // moved out of source
}

#[test]
fn dyn_clears_property_in_mask_but_unset_in_source() {
    let base = Rec {
        id: Some(1),
        label: Some("present".into()),
        count: Some(10),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        ..Default::default()
    };
    let (merged, _) = dyn_merge_take(&base, &update, mask(&[2, 3]));
    assert_eq!(merged.label, None);
    assert_eq!(merged.count, None);
    assert_eq!(merged.id, Some(1));
}

#[test]
fn dyn_merge_from_clones_without_draining_source() {
    let base = Rec {
        id: Some(1),
        count: Some(5),
        ..Default::default()
    };
    let update = Rec {
        id: Some(1),
        label: Some("added".into()),
        ..Default::default()
    };
    let mut b = dyn_of(&base);
    let s = dyn_of(&update);
    b.merge_from(&s, mask(&[2]));
    let merged = decode_typed_from_slice::<Rec>(&b.encode()).unwrap();
    assert_eq!(merged.label.as_deref(), Some("added"));
    assert_eq!(merged.count, Some(5)); // preserved
    // Source untouched by the clone variant.
    let src_back = decode_typed_from_slice::<Rec>(&s.encode()).unwrap();
    assert_eq!(src_back.label.as_deref(), Some("added"));
}

#[test]
fn valid_set_tracks_overlay_and_clear() {
    let base = Rec {
        id: Some(1),
        label: Some("x".into()),
        count: Some(7),
        ..Default::default()
    };
    let mut target = AnyStruct::from_struct_value(&base);
    // Overlay tag 4, clear tag 2 (mask covers 2 and 4; src has only 4).
    let update = Rec {
        id: Some(1),
        flag: Some(true),
        ..Default::default()
    };
    target.merge_from(&update, mask(&[2, 4]));
    let valid: Vec<u32> = target.valid_set().iter().collect();
    // tag 1 (key) + 3 (untouched) + 4 (overlaid); tag 2 cleared.
    assert_eq!(valid, [1, 3, 4]);
}
