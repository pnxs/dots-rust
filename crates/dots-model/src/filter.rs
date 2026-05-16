//! Server-side subscription filtering — wire types.
//!
//! Mirrors `external/dots/model/filter.dots` from dots-cpp. A
//! [`DotsFilter`] is attached to a `DotsMember(join)` and carries two
//! orthogonal axes of selection:
//!
//! - **predicate** — row selection: which instances of the type are
//!   forwarded to this subscription.
//! - **property_mask** — column selection: which properties of a
//!   matching instance are present on the wire. Key properties are
//!   always added implicitly by the server.
//!
//! Either axis may be omitted independently. An empty [`DotsFilter`]
//! is equivalent to an unfiltered subscription on rows + all
//! properties on columns.
//!
//! The predicate is encoded as a pre-order traversal of an n-ary tree
//! where each node carries an arity (the number of children that
//! follow it linearly in the same vector). This avoids self-referential
//! structs while keeping the wire form compact and the evaluator
//! simple — a recursive walk with a moving cursor.

use dots_core::{PropertySet, Timepoint};
use dots_derive::{DotsEnum, DotsStruct};

/// Kind tag on a [`DotsPredicateNode`].
#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(name = "DotsPredicateKind")]
pub enum DotsPredicateKind {
    /// Node is a single comparison; uses [`DotsPredicateNode::leaf`].
    #[default]
    #[dots(tag = 1)]
    Leaf,
    /// N-ary conjunction; consumes the next `arity` children linearly.
    #[dots(tag = 2)]
    AndOp,
    /// N-ary disjunction; consumes the next `arity` children linearly.
    #[dots(tag = 3)]
    OrOp,
    /// Unary negation; `arity` is always 1.
    #[dots(tag = 4)]
    NotOp,
}

/// Comparison operator on a leaf.
#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(name = "DotsCompareOp")]
pub enum DotsCompareOp {
    #[default]
    #[dots(tag = 1)]
    Eq,
    #[dots(tag = 2)]
    Neq,
    #[dots(tag = 3)]
    Lt,
    #[dots(tag = 4)]
    Le,
    #[dots(tag = 5)]
    Gt,
    #[dots(tag = 6)]
    Ge,
    /// Property is contained in the value's vector slot.
    #[dots(tag = 7)]
    IsIn,
    /// Property is not contained in the value's vector slot.
    #[dots(tag = 8)]
    NotIn,
    /// Property is not set on the instance (value field unused).
    #[dots(tag = 9)]
    IsNull,
    /// Property is set on the instance (value field unused).
    #[dots(tag = 10)]
    NotNull,
}

/// Width-unified leaf value. Exactly one slot is populated for scalar
/// ops, and exactly one of the `*_list` slots for `IsIn` / `NotIn`.
///
/// The server validates at subscribe time that the populated slot
/// matches the target property's type as declared in its descriptor.
/// Integer slots cover the full signed/unsigned width ranges and are
/// narrowed against the descriptor.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsPredicateValue", internal)]
pub struct DotsPredicateValue {
    #[dots(tag = 1)]
    pub bool_val: Option<bool>,
    /// Covers int8..int64; narrowed against the descriptor server-side.
    #[dots(tag = 2)]
    pub int_val: Option<i64>,
    /// Covers uint8..uint64; narrowed against the descriptor server-side.
    #[dots(tag = 3)]
    pub uint_val: Option<u64>,
    /// Covers float32 and float64.
    #[dots(tag = 4)]
    pub float_val: Option<f64>,
    #[dots(tag = 5)]
    pub string_val: Option<String>,
    #[dots(tag = 6)]
    pub timepoint_val: Option<Timepoint>,
    #[dots(tag = 7)]
    pub duration_val: Option<dots_core::Duration>,
    #[dots(tag = 8)]
    pub uuid_val: Option<[u8; 16]>,

    /// Vector slot for `IsIn` / `NotIn` against int8..int64 properties.
    #[dots(tag = 20)]
    pub int_list: Option<Vec<i64>>,
    /// Vector slot for `IsIn` / `NotIn` against uint8..uint64 properties.
    #[dots(tag = 21)]
    pub uint_list: Option<Vec<u64>>,
    /// Vector slot for `IsIn` / `NotIn` against float32/float64 properties.
    #[dots(tag = 22)]
    pub float_list: Option<Vec<f64>>,
    #[dots(tag = 23)]
    pub string_list: Option<Vec<String>>,
    #[dots(tag = 24)]
    pub timepoint_list: Option<Vec<Timepoint>>,
    #[dots(tag = 25)]
    pub uuid_list: Option<Vec<[u8; 16]>>,
}

/// A single comparison: `property op value`.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsPredicateLeaf", internal)]
pub struct DotsPredicateLeaf {
    /// Tag of the target property within the subscribed type.
    #[dots(tag = 1)]
    pub property_tag: Option<u32>,
    #[dots(tag = 2)]
    pub op: Option<DotsCompareOp>,
    /// Comparison RHS. Omitted when `op` is `IsNull` or `NotNull`.
    #[dots(tag = 3)]
    pub value: Option<DotsPredicateValue>,
}

/// One node in the pre-order predicate vector.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsPredicateNode", internal)]
pub struct DotsPredicateNode {
    #[dots(tag = 1)]
    pub kind: Option<DotsPredicateKind>,
    /// Present iff `kind == Leaf`.
    #[dots(tag = 2)]
    pub leaf: Option<DotsPredicateLeaf>,
    /// Present iff `kind` is `AndOp` / `OrOp` / `NotOp`. `NotOp` arity
    /// is always 1.
    #[dots(tag = 3)]
    pub arity: Option<u32>,
}

/// Row-selection predicate. Encoded as a pre-order traversal of an
/// n-ary tree; `nodes[0]` is the root.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsPredicate", internal)]
pub struct DotsPredicate {
    #[dots(tag = 1)]
    pub nodes: Option<Vec<DotsPredicateNode>>,
}

/// Row predicate + column projection. Attached to a
/// `DotsMember(join)` with a `subscription_id` to open a filtered
/// subscription.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsFilter", internal)]
pub struct DotsFilter {
    #[dots(tag = 1)]
    pub predicate: Option<DotsPredicate>,
    /// Column projection mask. Keys are added implicitly by the server.
    #[dots(tag = 2)]
    pub property_mask: Option<PropertySet>,
}
