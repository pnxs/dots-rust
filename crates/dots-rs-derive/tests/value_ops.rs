//! Value-level struct operations — the dots-cpp `Struct` method family
//! (`_equal`/`_same`/`_less`/`_diffProperties`/`_assign`/`_merge`/
//! `_swap`/`_clear`), ported scenario-by-scenario from dots-cpp's
//! `tests/src/type/TestStaticStruct.cpp` and `TestDynamicStruct.cpp`.
//!
//! Every comparison scenario is exercised twice: on the static path
//! (`AnyStruct` / typed values via the `layout` free functions) and on
//! the dynamic path (`DynamicStruct` methods), and the results are
//! cross-checked — the two representations must always agree.

use std::cmp::Ordering;
use std::sync::Arc;

use dots_rs_core::{
    AnyStruct, DotsFieldOps, DynamicStruct, DynamicStructDescriptor, PropertySet, cmp_masked,
    diff_properties, equal, expect_properties, has_properties, same,
};
mod model {
    use dots_rs_derive::{DotsEnum, DotsStruct};

    #[derive(DotsEnum, Clone, Copy, Debug, PartialEq)]
    #[dots(name = "Grade")]
    pub enum Grade {
        // Values deliberately reversed w.r.t. tags: ordering must follow
        // `value` (dots-cpp semantics), not the wire tag.
        #[dots(tag = 1, value = 30)]
        Low,
        #[dots(tag = 2, value = 20)]
        Mid,
        #[dots(tag = 3, value = 10)]
        High,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "SubSection", substruct_only)]
    pub struct SubSection {
        #[dots(tag = 1)]
        pub p1: Option<i64>,
        #[dots(tag = 2)]
        pub p2: Option<String>,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "TestRec")]
    pub struct TestRec {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub name: Option<String>,
        #[dots(tag = 3)]
        pub score: Option<f64>,
        #[dots(tag = 4)]
        pub grade: Option<Grade>,
        #[dots(tag = 5)]
        pub sub: Option<SubSection>,
        #[dots(tag = 6)]
        pub items: Option<Vec<u32>>,
    }
}
use model::*;

fn mask(tags: &[u32]) -> PropertySet {
    tags.iter().fold(PropertySet::EMPTY, |acc, &t| acc.with_tag(t))
}

fn dyn_descriptor() -> Arc<DynamicStructDescriptor> {
    Arc::new(DynamicStructDescriptor::from_static(TestRec::DESCRIPTOR))
}

/// Project a typed value onto the shared dynamic descriptor so two
/// projections are `Arc`-identical (required by the dynamic ops).
fn to_dynamic(value: &TestRec, descriptor: &Arc<DynamicStructDescriptor>) -> DynamicStruct {
    DynamicStruct::decode(descriptor.clone(), &dots_rs_core::encode_to_vec(value))
        .expect("roundtrip through CBOR")
}

/// Assert that the static and dynamic paths agree on equal/cmp/diff
/// for the given pair and mask, and return the static results.
fn cross_check(a: &TestRec, b: &TestRec, m: PropertySet) -> (bool, Ordering, PropertySet) {
    let (eq_s, ord_s, diff_s) = (equal(a, b, m), cmp_masked(a, b, m), diff_properties(a, b, m));
    let d = dyn_descriptor();
    let (da, db) = (to_dynamic(a, &d), to_dynamic(b, &d));
    assert_eq!(eq_s, da.equal(&db, m), "equal: static vs dynamic disagree");
    assert_eq!(ord_s, da.cmp_masked(&db, m), "cmp: static vs dynamic disagree");
    assert_eq!(diff_s, da.diff_properties(&db, m), "diff: static vs dynamic disagree");
    (eq_s, ord_s, diff_s)
}

// ===== equal / same =====

#[test]
fn equal_full_mask_and_masked_subset() {
    let a = TestRec { id: Some(1), name: Some("x".into()), score: Some(1.5), ..Default::default() };
    let mut b = a.clone();
    let (eq, ord, diff) = cross_check(&a, &b, PropertySet::ALL);
    assert!(eq);
    assert_eq!(ord, Ordering::Equal);
    assert!(diff.is_empty());

    b.score = Some(2.5);
    let (eq, _, _) = cross_check(&a, &b, PropertySet::ALL);
    assert!(!eq);
    // Masked away, the difference disappears.
    let (eq, ord, diff) = cross_check(&a, &b, mask(&[1, 2]));
    assert!(eq);
    assert_eq!(ord, Ordering::Equal);
    assert!(diff.is_empty());
}

#[test]
fn equal_distinguishes_unset_from_set() {
    let a = TestRec { id: Some(1), name: Some("x".into()), ..Default::default() };
    let b = TestRec { id: Some(1), ..Default::default() };
    let (eq, ord, diff) = cross_check(&a, &b, PropertySet::ALL);
    assert!(!eq);
    // Set sorts after unset.
    assert_eq!(ord, Ordering::Greater);
    assert_eq!(diff, mask(&[2]));
}

#[test]
fn same_compares_keys_only() {
    let a = TestRec { id: Some(7), name: Some("a".into()), ..Default::default() };
    let b = TestRec { id: Some(7), name: Some("b".into()), ..Default::default() };
    let c = TestRec { id: Some(8), name: Some("a".into()), ..Default::default() };
    assert!(same(&a, &b));
    assert!(!same(&a, &c));

    let d = dyn_descriptor();
    assert!(to_dynamic(&a, &d).same(&to_dynamic(&b, &d)));
    assert!(!to_dynamic(&a, &d).same(&to_dynamic(&c, &d)));
}

#[test]
fn nan_equals_bit_identical_nan() {
    // total_cmp semantics: a NaN-valued property is equal to itself,
    // so diff stays empty (C++ would report it changed forever).
    let a = TestRec { id: Some(1), score: Some(f64::NAN), ..Default::default() };
    let b = a.clone();
    let (eq, ord, diff) = cross_check(&a, &b, PropertySet::ALL);
    assert!(eq);
    assert_eq!(ord, Ordering::Equal);
    assert!(diff.is_empty());
}

// ===== cmp (lexicographic, declaration order, unset < set) =====

#[test]
fn cmp_orders_by_first_differing_property() {
    let a = TestRec { id: Some(1), name: Some("a".into()), score: Some(9.0), ..Default::default() };
    let b = TestRec { id: Some(1), name: Some("b".into()), score: Some(1.0), ..Default::default() };
    // `name` (tag 2) decides before `score` (tag 3) gets a say.
    let (_, ord, _) = cross_check(&a, &b, PropertySet::ALL);
    assert_eq!(ord, Ordering::Less);
    // Restricted to `score`, the order flips.
    let (_, ord, _) = cross_check(&a, &b, mask(&[3]));
    assert_eq!(ord, Ordering::Greater);
    // Empty mask → equal (C++ `_less` returns false both ways).
    let (eq, ord, _) = cross_check(&a, &b, PropertySet::EMPTY);
    assert!(eq);
    assert_eq!(ord, Ordering::Equal);
}

#[test]
fn enum_orders_by_value_not_tag() {
    // Grade::Low has the smallest tag (1) but the LARGEST value (30).
    let a = TestRec { id: Some(1), grade: Some(Grade::Low), ..Default::default() };
    let b = TestRec { id: Some(1), grade: Some(Grade::High), ..Default::default() };
    assert_eq!(Grade::Low.dots_cmp(&Grade::High), Ordering::Greater);
    let (_, ord, _) = cross_check(&a, &b, mask(&[4]));
    assert_eq!(ord, Ordering::Greater);
}

#[test]
fn vectors_compare_lexicographically() {
    let a = TestRec { id: Some(1), items: Some(vec![1, 2, 3]), ..Default::default() };
    let b = TestRec { id: Some(1), items: Some(vec![1, 2, 4]), ..Default::default() };
    let c = TestRec { id: Some(1), items: Some(vec![1, 2]), ..Default::default() };
    assert_eq!(cross_check(&a, &b, PropertySet::ALL).1, Ordering::Less);
    assert_eq!(cross_check(&a, &c, PropertySet::ALL).1, Ordering::Greater);
}

#[test]
fn substructs_compare_recursively() {
    let mk = |p1: i64| TestRec {
        id: Some(1),
        sub: Some(SubSection { p1: Some(p1), p2: None }),
        ..Default::default()
    };
    let (eq, ord, diff) = cross_check(&mk(5), &mk(5), PropertySet::ALL);
    assert!(eq && ord == Ordering::Equal && diff.is_empty());
    let (eq, ord, diff) = cross_check(&mk(5), &mk(6), PropertySet::ALL);
    assert!(!eq);
    assert_eq!(ord, Ordering::Less);
    // The sub-struct difference surfaces as its (flat) tag.
    assert_eq!(diff, mask(&[5]));
}

// ===== diff_properties =====

#[test]
fn diff_covers_validity_and_value_differences() {
    let a = TestRec {
        id: Some(1),
        name: Some("a".into()),
        score: Some(1.0),
        ..Default::default()
    };
    let b = TestRec {
        id: Some(1),
        name: Some("b".into()), // value diff → tag 2
        grade: Some(Grade::Mid), // validity diff → tag 4
        ..Default::default()     // score unset → tag 3
    };
    let (_, _, diff) = cross_check(&a, &b, PropertySet::ALL);
    assert_eq!(diff, mask(&[2, 3, 4]));
    // Masking narrows the verdict.
    let (_, _, diff) = cross_check(&a, &b, mask(&[2]));
    assert_eq!(diff, mask(&[2]));
}

#[test]
fn diff_is_the_update_mask_that_reconciles() {
    // diff(a→b) fed into copy_from makes a mirror b (non-key props).
    let a = TestRec { id: Some(1), name: Some("a".into()), score: Some(1.0), ..Default::default() };
    let b = TestRec { id: Some(1), grade: Some(Grade::Low), score: Some(2.0), ..Default::default() };
    let diff = diff_properties(&a, &b, PropertySet::ALL);
    let mut target = AnyStruct::from_struct_value(&a);
    target.copy_from(&b, diff);
    assert!(equal(&target, &b, PropertySet::ALL));
    assert_eq!(target.as_typed::<TestRec>().unwrap(), &b);
}

// ===== assign =====

#[test]
fn assign_mirrors_masked_and_clears_the_rest() {
    let base = TestRec {
        id: Some(1),
        name: Some("keep?".into()),
        score: Some(1.0),
        items: Some(vec![1]),
        ..Default::default()
    };
    let src = TestRec {
        id: Some(9), // keys are never assigned
        name: Some("new".into()),
        grade: Some(Grade::Mid),
        ..Default::default()
    };
    let mut target = AnyStruct::from_struct_value(&base);
    target.assign_from(&src, mask(&[2, 4]));
    let out = target.as_typed::<TestRec>().unwrap();
    assert_eq!(out.id, Some(1)); // key preserved
    assert_eq!(out.name.as_deref(), Some("new")); // masked + set → assigned
    assert_eq!(out.grade, Some(Grade::Mid)); // masked + set → assigned
    assert_eq!(out.score, None); // outside mask → cleared (assign ≠ copy)
    assert_eq!(out.items, None); // outside mask → cleared

    // Dynamic mirror.
    let d = dyn_descriptor();
    let mut dt = to_dynamic(&base, &d);
    dt.assign_from(&to_dynamic(&src, &d), mask(&[2, 4]));
    assert!(dt.equal(&to_dynamic(out, &d), PropertySet::ALL));
}

#[test]
fn assign_take_drains_the_source() {
    let base = TestRec { id: Some(1), score: Some(1.0), ..Default::default() };
    let src = TestRec { id: Some(1), name: Some("moved".into()), ..Default::default() };
    let mut target = AnyStruct::from_struct_value(&base);
    let mut source = AnyStruct::from_struct_value(&src);
    target.assign_take(&mut source, PropertySet::ALL);
    assert_eq!(
        target.as_typed::<TestRec>().unwrap().name.as_deref(),
        Some("moved")
    );
    assert_eq!(source.as_typed::<TestRec>().unwrap().name, None);
}

// ===== merge (recursive, non-destructive) =====

#[test]
fn merge_overlays_without_clearing() {
    let base = TestRec {
        id: Some(1),
        name: Some("base".into()),
        score: Some(1.0),
        ..Default::default()
    };
    let overlay = TestRec {
        id: Some(1),
        name: Some("over".into()),
        ..Default::default() // score unset → must SURVIVE on target
    };
    let mut target = AnyStruct::from_struct_value(&base);
    target.merge_from(&overlay, PropertySet::ALL);
    let out = target.as_typed::<TestRec>().unwrap();
    assert_eq!(out.name.as_deref(), Some("over"));
    assert_eq!(out.score, Some(1.0)); // contrast: copy_from would clear this

    let d = dyn_descriptor();
    let mut dt = to_dynamic(&base, &d);
    dt.merge_from(&to_dynamic(&overlay, &d), PropertySet::ALL);
    assert!(dt.equal(&to_dynamic(out, &d), PropertySet::ALL));
}

#[test]
fn merge_recurses_into_substructs() {
    let base = TestRec {
        id: Some(1),
        sub: Some(SubSection { p1: Some(10), p2: Some("keep".into()) }),
        ..Default::default()
    };
    let overlay = TestRec {
        id: Some(1),
        sub: Some(SubSection { p1: Some(20), p2: None }),
        ..Default::default()
    };
    let mut target = AnyStruct::from_struct_value(&base);
    target.merge_from(&overlay, PropertySet::ALL);
    let out = target.as_typed::<TestRec>().unwrap();
    let sub = out.sub.as_ref().unwrap();
    assert_eq!(sub.p1, Some(20)); // overlaid
    assert_eq!(sub.p2.as_deref(), Some("keep")); // survived the merge

    // copy_from, by contrast, replaces the sub-struct wholesale.
    let mut flat = AnyStruct::from_struct_value(&base);
    flat.copy_from(&overlay, PropertySet::ALL);
    let flat_sub = flat.as_typed::<TestRec>().unwrap().sub.clone().unwrap();
    assert_eq!(flat_sub.p2, None);

    // Dynamic mirror agrees with the static merge.
    let d = dyn_descriptor();
    let mut dt = to_dynamic(&base, &d);
    dt.merge_from(&to_dynamic(&overlay, &d), PropertySet::ALL);
    assert!(dt.equal(&to_dynamic(out, &d), PropertySet::ALL));
}

#[test]
fn merge_emplaces_missing_substruct() {
    let base = TestRec { id: Some(1), ..Default::default() };
    let overlay = TestRec {
        id: Some(1),
        sub: Some(SubSection { p1: Some(1), p2: None }),
        ..Default::default()
    };
    let mut target = AnyStruct::from_struct_value(&base);
    target.merge_from(&overlay, PropertySet::ALL);
    assert_eq!(target.as_typed::<TestRec>().unwrap().sub, overlay.sub);
}

#[test]
fn typed_dots_merge_matches_anystruct_merge() {
    // The derive-generated `DotsFieldOps::dots_merge` and the thunk
    // path must implement the same operation.
    let mut typed = TestRec {
        id: Some(1),
        name: Some("base".into()),
        sub: Some(SubSection { p1: Some(1), p2: Some("x".into()) }),
        ..Default::default()
    };
    let overlay = TestRec {
        id: Some(1),
        sub: Some(SubSection { p1: Some(2), p2: None }),
        ..Default::default()
    };
    let mut erased = AnyStruct::from_struct_value(&typed);
    erased.merge_from(&overlay, PropertySet::ALL);
    typed.dots_merge(&overlay);
    assert_eq!(erased.as_typed::<TestRec>().unwrap(), &typed);
}

// ===== swap / clear =====

#[test]
fn swap_exchanges_masked_properties_and_validity() {
    let a = TestRec { id: Some(1), name: Some("a".into()), score: Some(1.0), ..Default::default() };
    let b = TestRec { id: Some(2), name: None, score: Some(2.0), ..Default::default() };
    let mut sa = AnyStruct::from_struct_value(&a);
    let mut sb = AnyStruct::from_struct_value(&b);
    sa.swap(&mut sb, mask(&[2]));
    let (ta, tb) = (
        sa.as_typed::<TestRec>().unwrap().clone(),
        sb.as_typed::<TestRec>().unwrap().clone(),
    );
    assert_eq!(ta.name, None); // a's name moved out
    assert_eq!(tb.name.as_deref(), Some("a")); // ...into b
    assert_eq!(ta.id, Some(1)); // keys stay put
    assert_eq!(tb.id, Some(2));
    assert_eq!(ta.score, Some(1.0)); // outside mask → untouched
    assert_eq!(tb.score, Some(2.0));

    // Dynamic mirror.
    let d = dyn_descriptor();
    let (mut da, mut db) = (to_dynamic(&a, &d), to_dynamic(&b, &d));
    da.swap(&mut db, mask(&[2]));
    assert!(da.equal(&to_dynamic(&ta, &d), PropertySet::ALL));
    assert!(db.equal(&to_dynamic(&tb, &d), PropertySet::ALL));
}

#[test]
fn clear_drops_masked_non_key_properties() {
    let a = TestRec {
        id: Some(1),
        name: Some("x".into()),
        score: Some(1.0),
        items: Some(vec![1, 2]),
        ..Default::default()
    };
    let mut s = AnyStruct::from_struct_value(&a);
    s.clear(mask(&[1, 2, 6])); // includes the key tag — must be ignored
    let out = s.as_typed::<TestRec>().unwrap();
    assert_eq!(out.id, Some(1)); // key survives even when masked
    assert_eq!(out.name, None);
    assert_eq!(out.items, None);
    assert_eq!(out.score, Some(1.0)); // outside mask

    let d = dyn_descriptor();
    let mut dt = to_dynamic(&a, &d);
    dt.clear(mask(&[1, 2, 6]));
    assert!(dt.equal(&to_dynamic(out, &d), PropertySet::ALL));
}

// ===== PartialEq / has / expect =====

#[test]
fn anystruct_partial_eq_and_type_mismatch() {
    let a = TestRec { id: Some(1), name: Some("x".into()), ..Default::default() };
    let s1 = AnyStruct::from_struct_value(&a);
    let s2 = AnyStruct::from_struct_value(&a);
    assert_eq!(s1, s2);
    let sub = SubSection { p1: Some(1), p2: None };
    // Different descriptors → unequal, not a panic.
    assert!(s1 != AnyStruct::from_struct_value(&sub));
}

#[test]
fn has_and_expect_properties() {
    let a = TestRec { id: Some(1), name: Some("x".into()), ..Default::default() };
    assert!(has_properties(&a, mask(&[1, 2])));
    assert!(!has_properties(&a, mask(&[1, 3])));
    assert!(expect_properties(&a, mask(&[1, 2])).is_ok());
    let err = expect_properties(&a, mask(&[2, 3, 4])).unwrap_err();
    assert_eq!(err.type_name, "TestRec");
    assert_eq!(err.missing, vec!["score".to_string(), "grade".to_string()]);
    assert_eq!(
        err.to_string(),
        "TestRec instance is missing expected properties: score, grade"
    );

    // Dynamic mirror.
    let d = dyn_descriptor();
    let dt = to_dynamic(&a, &d);
    assert!(dt.has_properties(mask(&[1, 2])));
    let dyn_err = dt.expect_properties(mask(&[2, 3, 4])).unwrap_err();
    assert_eq!(dyn_err, err);
}

// ===== property_mask / key_mask parity =====

#[test]
fn descriptor_masks_agree_across_representations() {
    let static_d = TestRec::DESCRIPTOR;
    let dyn_d = dyn_descriptor();
    assert_eq!(static_d.property_mask(), dyn_d.property_mask());
    assert_eq!(static_d.key_mask(), dyn_d.key_mask());
    assert_eq!(static_d.property_mask(), mask(&[1, 2, 3, 4, 5, 6]));
    assert_eq!(static_d.key_mask(), mask(&[1]));
}
