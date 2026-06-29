//! Predicate evaluator: compile + matches against a synthetic
//! `Sample` type.

use dots_rs_core::{DynamicStruct, DynamicStructDescriptor, DynamicValue, PropertySet, dots};
use dots_rs_model::filter::{
    DotsCompareOp, DotsPredicate, DotsPredicateKind, DotsPredicateLeaf, DotsPredicateNode,
    DotsPredicateValue,
};
#[allow(unused_imports)]
use dots_rs_model::*;
use dots_rs_transport::filter::CompiledPredicate;

mod model {
    use dots_rs_core::Timepoint;
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, Clone)]
    #[dots(name = "Sample", cached)]
    pub struct Sample {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub name: Option<String>,
        #[dots(tag = 3)]
        pub score: Option<i64>,
        #[dots(tag = 4)]
        pub flag: Option<bool>,
        #[dots(tag = 5)]
        pub ratio: Option<f64>,
        #[dots(tag = 6)]
        pub moment: Option<Timepoint>,
        #[dots(tag = 7)]
        pub badge: Option<[u8; 16]>,
    }
}
use model::*;

fn descriptor() -> DynamicStructDescriptor {
    DynamicStructDescriptor::from_static(Sample::DESCRIPTOR)
}

fn payload_of(s: &Sample) -> DynamicStruct {
    let bytes = dots_rs_core::encode_to_vec(s);
    DynamicStruct::decode(std::sync::Arc::new(descriptor()), &bytes).expect("decode")
}

fn leaf(tag: u32, op: DotsCompareOp, value: Option<DotsPredicateValue>) -> DotsPredicateNode {
    dots!(DotsPredicateNode {
        kind: DotsPredicateKind::Leaf,
        leaf: DotsPredicateLeaf {
            property_tag: tag,
            op: op,
            value: value,
        },
    })
}

fn op_node(kind: DotsPredicateKind, arity: u32) -> DotsPredicateNode {
    dots!(DotsPredicateNode {
        kind: kind,
        arity: arity,
    })
}

#[test]
fn empty_predicate_matches_everything() {
    let s = Sample::default();
    let pred = DotsPredicate { nodes: None };
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.is_empty());
    assert!(compiled.matches(&payload_of(&s)));
}

#[test]
fn eq_on_u32_key() {
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            1,
            DotsCompareOp::Eq,
            Some(dots!(DotsPredicateValue { uint_val: 42_u64 })),
        )],
    });
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&dots!(Sample { id: 42_u32 }))));
    assert!(!compiled.matches(&payload_of(&dots!(Sample { id: 7_u32 }))));
}

#[test]
fn lt_on_i64_score() {
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            3,
            DotsCompareOp::Lt,
            Some(dots!(DotsPredicateValue { int_val: 100_i64 })),
        )],
    });
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&dots!(Sample { score: 99_i64 }))));
    assert!(!compiled.matches(&payload_of(&dots!(Sample { score: 100_i64 }))));
    assert!(!compiled.matches(&payload_of(&dots!(Sample { score: 101_i64 }))));
}

#[test]
fn neq_string() {
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            2,
            DotsCompareOp::Neq,
            Some(dots!(DotsPredicateValue { string_val: "alice" })),
        )],
    });
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&dots!(Sample { name: "bob" }))));
    assert!(!compiled.matches(&payload_of(&dots!(Sample { name: "alice" }))));
}

#[test]
fn is_null_and_not_null() {
    let is_null = dots!(DotsPredicate {
        nodes: vec![leaf(2, DotsCompareOp::IsNull, None)],
    });
    let not_null = dots!(DotsPredicate {
        nodes: vec![leaf(2, DotsCompareOp::NotNull, None)],
    });
    let c_null = CompiledPredicate::compile(&is_null, &descriptor()).unwrap();
    let c_notnull = CompiledPredicate::compile(&not_null, &descriptor()).unwrap();
    let empty = Sample::default();
    let filled = dots!(Sample { name: "x" });
    assert!(c_null.matches(&payload_of(&empty)));
    assert!(!c_null.matches(&payload_of(&filled)));
    assert!(!c_notnull.matches(&payload_of(&empty)));
    assert!(c_notnull.matches(&payload_of(&filled)));
}

#[test]
fn is_in_on_u32() {
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            1,
            DotsCompareOp::IsIn,
            Some(dots!(DotsPredicateValue {
                uint_list: vec![10_u64, 20, 30],
            })),
        )],
    });
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&dots!(Sample { id: 20_u32 }))));
    assert!(!compiled.matches(&payload_of(&dots!(Sample { id: 40_u32 }))));
}

#[test]
fn n_ary_and_or_not() {
    // (id == 1 && score < 100) — n-ary And with arity 2
    let pred = dots!(DotsPredicate {
        nodes: vec![
            op_node(DotsPredicateKind::AndOp, 2),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(dots!(DotsPredicateValue { uint_val: 1_u64 })),
            ),
            leaf(
                3,
                DotsCompareOp::Lt,
                Some(dots!(DotsPredicateValue { int_val: 100_i64 })),
            ),
        ],
    });
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&dots!(Sample {
        id: 1_u32,
        score: 50_i64,
    }))));
    assert!(!compiled.matches(&payload_of(&dots!(Sample {
        id: 2_u32,
        score: 50_i64,
    }))));
    assert!(!compiled.matches(&payload_of(&dots!(Sample {
        id: 1_u32,
        score: 200_i64,
    }))));

    // !(id == 1) — NotOp with arity 1
    let pred = dots!(DotsPredicate {
        nodes: vec![
            op_node(DotsPredicateKind::NotOp, 1),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(dots!(DotsPredicateValue { uint_val: 1_u64 })),
            ),
        ],
    });
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(!compiled.matches(&payload_of(&dots!(Sample { id: 1_u32 }))));
    assert!(compiled.matches(&payload_of(&dots!(Sample { id: 2_u32 }))));
}

#[test]
fn n_ary_collapsed_and() {
    // (a & b & c) → arity-3 And, not nested 2,2
    let pred = dots!(DotsPredicate {
        nodes: vec![
            op_node(DotsPredicateKind::AndOp, 3),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(dots!(DotsPredicateValue { uint_val: 7_u64 })),
            ),
            leaf(
                3,
                DotsCompareOp::Gt,
                Some(dots!(DotsPredicateValue { int_val: 0_i64 })),
            ),
            leaf(
                2,
                DotsCompareOp::Eq,
                Some(dots!(DotsPredicateValue { string_val: "hi" })),
            ),
        ],
    });
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&dots!(Sample {
        id: 7_u32,
        score: 10_i64,
        name: "hi",
    }))));
    assert!(!compiled.matches(&payload_of(&dots!(Sample {
        id: 7_u32,
        score: 10_i64,
        name: "nope",
    }))));
}

#[test]
fn rejects_unknown_property_tag() {
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            99,
            DotsCompareOp::Eq,
            Some(dots!(DotsPredicateValue { uint_val: 0_u64 })),
        )],
    });
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_wrong_value_slot() {
    // u32 property compared against an int_val slot (should be uint_val)
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            1,
            DotsCompareOp::Eq,
            Some(dots!(DotsPredicateValue { int_val: 1_i64 })),
        )],
    });
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_ordered_op_on_bool() {
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            4,
            DotsCompareOp::Lt,
            Some(dots!(DotsPredicateValue { bool_val: true })),
        )],
    });
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_truncated_tree() {
    // AndOp arity=2 with only one child
    let pred = dots!(DotsPredicate {
        nodes: vec![
            op_node(DotsPredicateKind::AndOp, 2),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(dots!(DotsPredicateValue { uint_val: 0_u64 })),
            ),
        ],
    });
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_bad_not_arity() {
    let pred = dots!(DotsPredicate {
        nodes: vec![
            op_node(DotsPredicateKind::NotOp, 2),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(dots!(DotsPredicateValue { uint_val: 0_u64 })),
            ),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(dots!(DotsPredicateValue { uint_val: 0_u64 })),
            ),
        ],
    });
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_empty_list() {
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            1,
            DotsCompareOp::IsIn,
            Some(dots!(DotsPredicateValue {
                uint_list: Vec::<u64>::new(),
            })),
        )],
    });
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn uuid_eq_and_is_in() {
    let a: [u8; 16] = [1; 16];
    let b: [u8; 16] = [2; 16];
    let pred_eq = dots!(DotsPredicate {
        nodes: vec![leaf(
            7,
            DotsCompareOp::Eq,
            Some(dots!(DotsPredicateValue { uuid_val: a })),
        )],
    });
    let c = CompiledPredicate::compile(&pred_eq, &descriptor()).unwrap();
    assert!(c.matches(&payload_of(&dots!(Sample { badge: a }))));
    assert!(!c.matches(&payload_of(&dots!(Sample { badge: b }))));

    // is_in
    let pred_in = dots!(DotsPredicate {
        nodes: vec![leaf(
            7,
            DotsCompareOp::IsIn,
            Some(dots!(DotsPredicateValue { uuid_list: vec![a, b] })),
        )],
    });
    let c = CompiledPredicate::compile(&pred_in, &descriptor()).unwrap();
    assert!(c.matches(&payload_of(&dots!(Sample { badge: a }))));
    assert!(c.matches(&payload_of(&dots!(Sample { badge: b }))));
    assert!(!c.matches(&payload_of(&dots!(Sample { badge: [3_u8; 16] }))));

    // ordered op on uuid — rejected at compile time
    let pred_bad = dots!(DotsPredicate {
        nodes: vec![leaf(
            7,
            DotsCompareOp::Lt,
            Some(dots!(DotsPredicateValue { uuid_val: a })),
        )],
    });
    assert!(CompiledPredicate::compile(&pred_bad, &descriptor()).is_err());
}

#[test]
fn float_compares() {
    let pred = dots!(DotsPredicate {
        nodes: vec![leaf(
            5,
            DotsCompareOp::Ge,
            Some(dots!(DotsPredicateValue { float_val: 0.5_f64 })),
        )],
    });
    let c = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(c.matches(&payload_of(&dots!(Sample { ratio: 0.5_f64 }))));
    assert!(c.matches(&payload_of(&dots!(Sample { ratio: 0.9_f64 }))));
    assert!(!c.matches(&payload_of(&dots!(Sample { ratio: 0.1_f64 }))));
}

#[test]
fn _ensure_property_set_unused_warning_quiet() {
    // The PropertySet import is exercised indirectly via descriptor
    // construction; this is just a placeholder to silence dead-code
    // analysis on the import in environments that build tests alone.
    let _ = PropertySet::EMPTY;
    let _: Option<DynamicValue> = None;
}
