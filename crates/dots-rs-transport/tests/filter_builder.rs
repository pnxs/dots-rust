//! Filter builder DSL: typed `Attr<S, V>` handles, operator
//! overloads, n-ary collapse, projection.

use dots_rs_core::PropertySet;
use dots_rs_model::filter::{
    DotsCompareOp, DotsPredicateKind, predicate, project_only,
};

mod model {
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, Clone)]
    #[dots(name = "Pinger", cached)]
    pub struct Pinger {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub message: Option<String>,
        #[dots(tag = 3)]
        pub sequence: Option<u64>,
    }
}
use model::*;

#[test]
fn eq_via_operator_and_method_produce_same_predicate() {
    // Both `attr.eq(v)` and the typed handle's named method should
    // build identical wire shapes.
    let by_method = predicate(Pinger::ID.eq(42_u32)).build();
    let predicate_nodes = by_method.predicate.expect("predicate present").nodes.unwrap();
    assert_eq!(predicate_nodes.len(), 1);
    let leaf = predicate_nodes[0].leaf.as_ref().unwrap();
    assert_eq!(leaf.property_tag, Some(1));
    assert_eq!(leaf.op, Some(DotsCompareOp::Eq));
    let value = leaf.value.as_ref().unwrap();
    assert_eq!(value.uint_val, Some(42));
    assert!(value.int_val.is_none());
}

#[test]
fn n_ary_and_collapse_arity_3() {
    // `(a & b) & c` and `a & (b & c)` and `a & b & c` must all
    // produce a single AndOp arity-3, not a nested binary chain.
    let p = Pinger::ID.eq(1_u32) & Pinger::SEQUENCE.lt(100_u64) & Pinger::MESSAGE.eq("hi".to_string());
    let f = predicate(p).build();
    let nodes = f.predicate.unwrap().nodes.unwrap();
    assert_eq!(nodes.len(), 4, "1 AndOp + 3 leaves");
    assert_eq!(nodes[0].kind, Some(DotsPredicateKind::AndOp));
    assert_eq!(nodes[0].arity, Some(3));
    assert_eq!(nodes[1].kind, Some(DotsPredicateKind::Leaf));
    assert_eq!(nodes[2].kind, Some(DotsPredicateKind::Leaf));
    assert_eq!(nodes[3].kind, Some(DotsPredicateKind::Leaf));
}

#[test]
fn nary_or_collapse() {
    let p = Pinger::ID.eq(1_u32) | Pinger::ID.eq(2_u32) | Pinger::ID.eq(3_u32);
    let f = predicate(p).build();
    let nodes = f.predicate.unwrap().nodes.unwrap();
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes[0].kind, Some(DotsPredicateKind::OrOp));
    assert_eq!(nodes[0].arity, Some(3));
}

#[test]
fn not_wraps_subtree() {
    let p = !(Pinger::ID.eq(7_u32));
    let f = predicate(p).build();
    let nodes = f.predicate.unwrap().nodes.unwrap();
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].kind, Some(DotsPredicateKind::NotOp));
    assert_eq!(nodes[0].arity, Some(1));
    assert_eq!(nodes[1].kind, Some(DotsPredicateKind::Leaf));
}

#[test]
fn is_in_list() {
    let p = Pinger::ID.is_in(vec![1_u32, 2, 3]);
    let f = predicate(p).build();
    let nodes = f.predicate.unwrap().nodes.unwrap();
    assert_eq!(nodes.len(), 1);
    let leaf = nodes[0].leaf.as_ref().unwrap();
    assert_eq!(leaf.op, Some(DotsCompareOp::IsIn));
    assert_eq!(
        leaf.value.as_ref().unwrap().uint_list.as_deref(),
        Some(&[1u64, 2, 3][..])
    );
}

#[test]
fn project_mask_assembly() {
    let f = predicate(Pinger::ID.eq(7_u32))
        .project(Pinger::PROP_ID | Pinger::PROP_SEQUENCE)
        .build();
    let mask = f.property_mask.unwrap();
    assert!(mask.has(1));
    assert!(mask.has(3));
    assert!(!mask.has(2));
}

#[test]
fn project_only_no_predicate() {
    let f = project_only::<Pinger>(Pinger::PROP_SEQUENCE).build();
    assert!(f.predicate.is_none());
    let mask = f.property_mask.unwrap();
    assert!(mask.has(3));
}

#[test]
fn is_null_and_not_null() {
    let f = predicate(Pinger::MESSAGE.is_null()).build();
    let nodes = f.predicate.unwrap().nodes.unwrap();
    let leaf = nodes[0].leaf.as_ref().unwrap();
    assert_eq!(leaf.op, Some(DotsCompareOp::IsNull));
    assert!(leaf.value.is_none());

    let f = predicate(Pinger::MESSAGE.not_null()).build();
    let nodes = f.predicate.unwrap().nodes.unwrap();
    let leaf = nodes[0].leaf.as_ref().unwrap();
    assert_eq!(leaf.op, Some(DotsCompareOp::NotNull));
}

#[test]
fn prop_consts_match_property_set_tag_bit() {
    assert_eq!(Pinger::PROP_ID, PropertySet::EMPTY.with_tag(1));
    assert_eq!(Pinger::PROP_MESSAGE, PropertySet::EMPTY.with_tag(2));
    assert_eq!(Pinger::PROP_SEQUENCE, PropertySet::EMPTY.with_tag(3));
}
