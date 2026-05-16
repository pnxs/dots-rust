//! Predicate evaluator: compile + matches against a synthetic
//! `Sample` type.

use dots_core::{DynamicStruct, DynamicStructDescriptor, DynamicValue, PropertySet, Timepoint};
use dots_derive::DotsStruct;
use dots_model::filter::{
    DotsCompareOp, DotsPredicate, DotsPredicateKind, DotsPredicateLeaf, DotsPredicateNode,
    DotsPredicateValue,
};
use dots_transport::filter::CompiledPredicate;

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "Sample", cached)]
struct Sample {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    name: Option<String>,
    #[dots(tag = 3)]
    score: Option<i64>,
    #[dots(tag = 4)]
    flag: Option<bool>,
    #[dots(tag = 5)]
    ratio: Option<f64>,
    #[dots(tag = 6)]
    moment: Option<Timepoint>,
    #[dots(tag = 7)]
    badge: Option<[u8; 16]>,
}

fn descriptor() -> DynamicStructDescriptor {
    DynamicStructDescriptor::from_static(Sample::DESCRIPTOR)
}

fn payload_of(s: &Sample) -> DynamicStruct {
    let bytes = dots_core::encode_to_vec(s);
    DynamicStruct::decode(std::sync::Arc::new(descriptor()), &bytes).expect("decode")
}

fn leaf(tag: u32, op: DotsCompareOp, value: Option<DotsPredicateValue>) -> DotsPredicateNode {
    DotsPredicateNode {
        kind: Some(DotsPredicateKind::Leaf),
        leaf: Some(DotsPredicateLeaf {
            property_tag: Some(tag),
            op: Some(op),
            value,
        }),
        ..Default::default()
    }
}

fn op_node(kind: DotsPredicateKind, arity: u32) -> DotsPredicateNode {
    DotsPredicateNode {
        kind: Some(kind),
        arity: Some(arity),
        ..Default::default()
    }
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
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            1,
            DotsCompareOp::Eq,
            Some(DotsPredicateValue {
                uint_val: Some(42),
                ..Default::default()
            }),
        )]),
    };
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&Sample { id: Some(42), ..Default::default() })));
    assert!(!compiled.matches(&payload_of(&Sample { id: Some(7), ..Default::default() })));
}

#[test]
fn lt_on_i64_score() {
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            3,
            DotsCompareOp::Lt,
            Some(DotsPredicateValue {
                int_val: Some(100),
                ..Default::default()
            }),
        )]),
    };
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&Sample { score: Some(99), ..Default::default() })));
    assert!(!compiled.matches(&payload_of(&Sample { score: Some(100), ..Default::default() })));
    assert!(!compiled.matches(&payload_of(&Sample { score: Some(101), ..Default::default() })));
}

#[test]
fn neq_string() {
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            2,
            DotsCompareOp::Neq,
            Some(DotsPredicateValue {
                string_val: Some("alice".into()),
                ..Default::default()
            }),
        )]),
    };
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&Sample { name: Some("bob".into()), ..Default::default() })));
    assert!(!compiled.matches(&payload_of(&Sample {
        name: Some("alice".into()),
        ..Default::default()
    })));
}

#[test]
fn is_null_and_not_null() {
    let is_null = DotsPredicate {
        nodes: Some(vec![leaf(2, DotsCompareOp::IsNull, None)]),
    };
    let not_null = DotsPredicate {
        nodes: Some(vec![leaf(2, DotsCompareOp::NotNull, None)]),
    };
    let c_null = CompiledPredicate::compile(&is_null, &descriptor()).unwrap();
    let c_notnull = CompiledPredicate::compile(&not_null, &descriptor()).unwrap();
    let empty = Sample::default();
    let filled = Sample { name: Some("x".into()), ..Default::default() };
    assert!(c_null.matches(&payload_of(&empty)));
    assert!(!c_null.matches(&payload_of(&filled)));
    assert!(!c_notnull.matches(&payload_of(&empty)));
    assert!(c_notnull.matches(&payload_of(&filled)));
}

#[test]
fn is_in_on_u32() {
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            1,
            DotsCompareOp::IsIn,
            Some(DotsPredicateValue {
                uint_list: Some(vec![10, 20, 30]),
                ..Default::default()
            }),
        )]),
    };
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&Sample { id: Some(20), ..Default::default() })));
    assert!(!compiled.matches(&payload_of(&Sample { id: Some(40), ..Default::default() })));
}

#[test]
fn n_ary_and_or_not() {
    // (id == 1 && score < 100) — n-ary And with arity 2
    let pred = DotsPredicate {
        nodes: Some(vec![
            op_node(DotsPredicateKind::AndOp, 2),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(DotsPredicateValue {
                    uint_val: Some(1),
                    ..Default::default()
                }),
            ),
            leaf(
                3,
                DotsCompareOp::Lt,
                Some(DotsPredicateValue {
                    int_val: Some(100),
                    ..Default::default()
                }),
            ),
        ]),
    };
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&Sample {
        id: Some(1),
        score: Some(50),
        ..Default::default()
    })));
    assert!(!compiled.matches(&payload_of(&Sample {
        id: Some(2),
        score: Some(50),
        ..Default::default()
    })));
    assert!(!compiled.matches(&payload_of(&Sample {
        id: Some(1),
        score: Some(200),
        ..Default::default()
    })));

    // !(id == 1) — NotOp with arity 1
    let pred = DotsPredicate {
        nodes: Some(vec![
            op_node(DotsPredicateKind::NotOp, 1),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(DotsPredicateValue {
                    uint_val: Some(1),
                    ..Default::default()
                }),
            ),
        ]),
    };
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(!compiled.matches(&payload_of(&Sample { id: Some(1), ..Default::default() })));
    assert!(compiled.matches(&payload_of(&Sample { id: Some(2), ..Default::default() })));
}

#[test]
fn n_ary_collapsed_and() {
    // (a & b & c) → arity-3 And, not nested 2,2
    let pred = DotsPredicate {
        nodes: Some(vec![
            op_node(DotsPredicateKind::AndOp, 3),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(DotsPredicateValue {
                    uint_val: Some(7),
                    ..Default::default()
                }),
            ),
            leaf(
                3,
                DotsCompareOp::Gt,
                Some(DotsPredicateValue {
                    int_val: Some(0),
                    ..Default::default()
                }),
            ),
            leaf(
                2,
                DotsCompareOp::Eq,
                Some(DotsPredicateValue {
                    string_val: Some("hi".into()),
                    ..Default::default()
                }),
            ),
        ]),
    };
    let compiled = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(compiled.matches(&payload_of(&Sample {
        id: Some(7),
        score: Some(10),
        name: Some("hi".into()),
        ..Default::default()
    })));
    assert!(!compiled.matches(&payload_of(&Sample {
        id: Some(7),
        score: Some(10),
        name: Some("nope".into()),
        ..Default::default()
    })));
}

#[test]
fn rejects_unknown_property_tag() {
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            99,
            DotsCompareOp::Eq,
            Some(DotsPredicateValue {
                uint_val: Some(0),
                ..Default::default()
            }),
        )]),
    };
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_wrong_value_slot() {
    // u32 property compared against an int_val slot (should be uint_val)
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            1,
            DotsCompareOp::Eq,
            Some(DotsPredicateValue {
                int_val: Some(1),
                ..Default::default()
            }),
        )]),
    };
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_ordered_op_on_bool() {
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            4,
            DotsCompareOp::Lt,
            Some(DotsPredicateValue {
                bool_val: Some(true),
                ..Default::default()
            }),
        )]),
    };
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_truncated_tree() {
    // AndOp arity=2 with only one child
    let pred = DotsPredicate {
        nodes: Some(vec![
            op_node(DotsPredicateKind::AndOp, 2),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(DotsPredicateValue {
                    uint_val: Some(0),
                    ..Default::default()
                }),
            ),
        ]),
    };
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_bad_not_arity() {
    let pred = DotsPredicate {
        nodes: Some(vec![
            op_node(DotsPredicateKind::NotOp, 2),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(DotsPredicateValue {
                    uint_val: Some(0),
                    ..Default::default()
                }),
            ),
            leaf(
                1,
                DotsCompareOp::Eq,
                Some(DotsPredicateValue {
                    uint_val: Some(0),
                    ..Default::default()
                }),
            ),
        ]),
    };
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn rejects_empty_list() {
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            1,
            DotsCompareOp::IsIn,
            Some(DotsPredicateValue {
                uint_list: Some(vec![]),
                ..Default::default()
            }),
        )]),
    };
    assert!(CompiledPredicate::compile(&pred, &descriptor()).is_err());
}

#[test]
fn uuid_eq_and_is_in() {
    let a: [u8; 16] = [1; 16];
    let b: [u8; 16] = [2; 16];
    let pred_eq = DotsPredicate {
        nodes: Some(vec![leaf(
            7,
            DotsCompareOp::Eq,
            Some(DotsPredicateValue {
                uuid_val: Some(a),
                ..Default::default()
            }),
        )]),
    };
    let c = CompiledPredicate::compile(&pred_eq, &descriptor()).unwrap();
    assert!(c.matches(&payload_of(&Sample { badge: Some(a), ..Default::default() })));
    assert!(!c.matches(&payload_of(&Sample { badge: Some(b), ..Default::default() })));

    // is_in
    let pred_in = DotsPredicate {
        nodes: Some(vec![leaf(
            7,
            DotsCompareOp::IsIn,
            Some(DotsPredicateValue {
                uuid_list: Some(vec![a, b]),
                ..Default::default()
            }),
        )]),
    };
    let c = CompiledPredicate::compile(&pred_in, &descriptor()).unwrap();
    assert!(c.matches(&payload_of(&Sample { badge: Some(a), ..Default::default() })));
    assert!(c.matches(&payload_of(&Sample { badge: Some(b), ..Default::default() })));
    assert!(!c.matches(&payload_of(&Sample { badge: Some([3; 16]), ..Default::default() })));

    // ordered op on uuid — rejected at compile time
    let pred_bad = DotsPredicate {
        nodes: Some(vec![leaf(
            7,
            DotsCompareOp::Lt,
            Some(DotsPredicateValue {
                uuid_val: Some(a),
                ..Default::default()
            }),
        )]),
    };
    assert!(CompiledPredicate::compile(&pred_bad, &descriptor()).is_err());
}

#[test]
fn float_compares() {
    let pred = DotsPredicate {
        nodes: Some(vec![leaf(
            5,
            DotsCompareOp::Ge,
            Some(DotsPredicateValue {
                float_val: Some(0.5),
                ..Default::default()
            }),
        )]),
    };
    let c = CompiledPredicate::compile(&pred, &descriptor()).unwrap();
    assert!(c.matches(&payload_of(&Sample { ratio: Some(0.5), ..Default::default() })));
    assert!(c.matches(&payload_of(&Sample { ratio: Some(0.9), ..Default::default() })));
    assert!(!c.matches(&payload_of(&Sample { ratio: Some(0.1), ..Default::default() })));
}

#[test]
fn _ensure_property_set_unused_warning_quiet() {
    // The PropertySet import is exercised indirectly via descriptor
    // construction; this is just a placeholder to silence dead-code
    // analysis on the import in environments that build tests alone.
    let _ = PropertySet::EMPTY;
    let _: Option<DynamicValue> = None;
}
