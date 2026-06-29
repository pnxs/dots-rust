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

use std::marker::PhantomData;
use std::ops::{BitAnd, BitOr, Not};

use dots_rs_core::{Duration, PropertySet, Timepoint};
use dots_rs_derive::{DotsEnum, DotsStruct};

/// Kind tag on a [`DotsPredicateNode`].
#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(rt_internal)]
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
#[dots(rt_internal)]
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
#[dots(rt_internal)]
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
    pub duration_val: Option<dots_rs_core::Duration>,
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
#[dots(rt_internal)]
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
#[dots(rt_internal)]
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
#[dots(rt_internal)]
#[dots(name = "DotsPredicate", internal)]
pub struct DotsPredicate {
    #[dots(tag = 1)]
    pub nodes: Option<Vec<DotsPredicateNode>>,
}

/// Row predicate + column projection. Attached to a
/// `DotsMember(join)` with a `subscription_id` to open a filtered
/// subscription.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(rt_internal)]
#[dots(name = "DotsFilter", internal)]
pub struct DotsFilter {
    #[dots(tag = 1)]
    pub predicate: Option<DotsPredicate>,
    /// Column projection mask. Keys are added implicitly by the server.
    #[dots(tag = 2)]
    pub property_mask: Option<PropertySet>,
}

// ===== Builder DSL =====
//
// The DSL produces wire-shaped [`DotsFilter`] values via fluent
// composition. It's typed end-to-end:
//
//   - [`Attr<S, V>`] is a zero-sized handle for a property of value
//     type `V` on struct `S`. Constants of this shape are emitted by
//     `#[derive(DotsStruct)]` so users can write `Pinger::SEQUENCE`
//     and have the resulting predicate validate at compile time:
//     comparing `Pinger::SEQUENCE.lt(0i32)` against a `u64` property
//     fails to type-check because `Attr<Self, u64>::lt` requires
//     `V = u64`.
//   - [`Predicate<S>`] accumulates nodes in the same pre-order shape
//     used on the wire. The `&` / `|` / `!` operators perform n-ary
//     collapse so `a & b & c` produces a single AndOp arity-3 rather
//     than nested binaries.
//   - [`FilterBuilder<S>`] composes a [`Predicate<S>`] with an
//     optional projection mask and emits a wire [`DotsFilter`] via
//     [`FilterBuilder::build`].

/// Types that can be the RHS of a predicate leaf comparison.
///
/// Implementations populate the right slot of a
/// [`DotsPredicateValue`] for both scalar and list shapes. The
/// trait is closed in practice — only the categories supported by
/// the wire schema have impls.
pub trait IntoPredicateValue: Sized {
    /// Populate the scalar slot for a single value comparison.
    fn into_scalar(self) -> DotsPredicateValue;
    /// Populate the list slot for `IsIn` / `NotIn` comparisons.
    fn into_list(items: Vec<Self>) -> DotsPredicateValue;
}

macro_rules! impl_into_pred_value_int_signed {
    ($($t:ty),*) => {$(
        impl IntoPredicateValue for $t {
            fn into_scalar(self) -> DotsPredicateValue {
                DotsPredicateValue {
                    int_val: Some(self as i64),
                    ..Default::default()
                }
            }
            fn into_list(items: Vec<Self>) -> DotsPredicateValue {
                DotsPredicateValue {
                    int_list: Some(items.into_iter().map(|v| v as i64).collect()),
                    ..Default::default()
                }
            }
        }
    )*};
}
impl_into_pred_value_int_signed!(i8, i16, i32, i64);

macro_rules! impl_into_pred_value_int_unsigned {
    ($($t:ty),*) => {$(
        impl IntoPredicateValue for $t {
            fn into_scalar(self) -> DotsPredicateValue {
                DotsPredicateValue {
                    uint_val: Some(self as u64),
                    ..Default::default()
                }
            }
            fn into_list(items: Vec<Self>) -> DotsPredicateValue {
                DotsPredicateValue {
                    uint_list: Some(items.into_iter().map(|v| v as u64).collect()),
                    ..Default::default()
                }
            }
        }
    )*};
}
impl_into_pred_value_int_unsigned!(u8, u16, u32, u64);

impl IntoPredicateValue for bool {
    fn into_scalar(self) -> DotsPredicateValue {
        DotsPredicateValue {
            bool_val: Some(self),
            ..Default::default()
        }
    }
    // bool list is not part of the wire schema; using bool with
    // is_in/not_in is rejected at predicate-compile time.
    fn into_list(_items: Vec<Self>) -> DotsPredicateValue {
        DotsPredicateValue::default()
    }
}

impl IntoPredicateValue for f32 {
    fn into_scalar(self) -> DotsPredicateValue {
        DotsPredicateValue {
            float_val: Some(self as f64),
            ..Default::default()
        }
    }
    fn into_list(items: Vec<Self>) -> DotsPredicateValue {
        DotsPredicateValue {
            float_list: Some(items.into_iter().map(|v| v as f64).collect()),
            ..Default::default()
        }
    }
}

impl IntoPredicateValue for f64 {
    fn into_scalar(self) -> DotsPredicateValue {
        DotsPredicateValue {
            float_val: Some(self),
            ..Default::default()
        }
    }
    fn into_list(items: Vec<Self>) -> DotsPredicateValue {
        DotsPredicateValue {
            float_list: Some(items),
            ..Default::default()
        }
    }
}

impl IntoPredicateValue for String {
    fn into_scalar(self) -> DotsPredicateValue {
        DotsPredicateValue {
            string_val: Some(self),
            ..Default::default()
        }
    }
    fn into_list(items: Vec<Self>) -> DotsPredicateValue {
        DotsPredicateValue {
            string_list: Some(items),
            ..Default::default()
        }
    }
}

impl IntoPredicateValue for Timepoint {
    fn into_scalar(self) -> DotsPredicateValue {
        DotsPredicateValue {
            timepoint_val: Some(self),
            ..Default::default()
        }
    }
    fn into_list(items: Vec<Self>) -> DotsPredicateValue {
        DotsPredicateValue {
            timepoint_list: Some(items),
            ..Default::default()
        }
    }
}

impl IntoPredicateValue for Duration {
    fn into_scalar(self) -> DotsPredicateValue {
        DotsPredicateValue {
            duration_val: Some(self),
            ..Default::default()
        }
    }
    // Duration list is not part of the wire schema.
    fn into_list(_items: Vec<Self>) -> DotsPredicateValue {
        DotsPredicateValue::default()
    }
}

impl IntoPredicateValue for [u8; 16] {
    fn into_scalar(self) -> DotsPredicateValue {
        DotsPredicateValue {
            uuid_val: Some(self),
            ..Default::default()
        }
    }
    fn into_list(items: Vec<Self>) -> DotsPredicateValue {
        DotsPredicateValue {
            uuid_list: Some(items),
            ..Default::default()
        }
    }
}

/// Zero-sized typed handle to a struct property.
///
/// Constructed at compile time via `Attr::new(tag)`; `#[derive(DotsStruct)]`
/// emits one constant per scalar-typed field so users can write
/// `MyType::FIELD.eq(value)` with full type-checking against the
/// property's actual `V`.
#[derive(Debug)]
pub struct Attr<S, V> {
    tag: u32,
    _phantom: PhantomData<fn() -> (S, V)>,
}

impl<S, V> Clone for Attr<S, V> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<S, V> Copy for Attr<S, V> {}

impl<S, V> Attr<S, V> {
    /// Build a typed handle for the property at `tag`.
    pub const fn new(tag: u32) -> Self {
        Self {
            tag,
            _phantom: PhantomData,
        }
    }

    /// DOTS wire tag of this property.
    pub const fn tag(self) -> u32 {
        self.tag
    }
}

impl<S, V: IntoPredicateValue> Attr<S, V> {
    fn scalar_leaf(self, op: DotsCompareOp, v: V) -> Predicate<S> {
        Predicate::leaf(self.tag, op, Some(v.into_scalar()))
    }

    fn list_leaf(self, op: DotsCompareOp, items: Vec<V>) -> Predicate<S> {
        Predicate::leaf(self.tag, op, Some(V::into_list(items)))
    }

    /// Property equal to `v`.
    pub fn eq(self, v: V) -> Predicate<S> {
        self.scalar_leaf(DotsCompareOp::Eq, v)
    }
    /// Property not equal to `v`.
    pub fn neq(self, v: V) -> Predicate<S> {
        self.scalar_leaf(DotsCompareOp::Neq, v)
    }
    /// Property strictly less than `v`.
    pub fn lt(self, v: V) -> Predicate<S> {
        self.scalar_leaf(DotsCompareOp::Lt, v)
    }
    /// Property less than or equal to `v`.
    pub fn le(self, v: V) -> Predicate<S> {
        self.scalar_leaf(DotsCompareOp::Le, v)
    }
    /// Property strictly greater than `v`.
    pub fn gt(self, v: V) -> Predicate<S> {
        self.scalar_leaf(DotsCompareOp::Gt, v)
    }
    /// Property greater than or equal to `v`.
    pub fn ge(self, v: V) -> Predicate<S> {
        self.scalar_leaf(DotsCompareOp::Ge, v)
    }
    /// Property's value is in `items`.
    pub fn is_in(self, items: Vec<V>) -> Predicate<S> {
        self.list_leaf(DotsCompareOp::IsIn, items)
    }
    /// Property's value is not in `items`.
    pub fn not_in(self, items: Vec<V>) -> Predicate<S> {
        self.list_leaf(DotsCompareOp::NotIn, items)
    }
}

impl<S, V> Attr<S, V> {
    /// Property is not set on the instance.
    pub fn is_null(self) -> Predicate<S> {
        Predicate::leaf(self.tag, DotsCompareOp::IsNull, None)
    }
    /// Property is set on the instance.
    pub fn not_null(self) -> Predicate<S> {
        Predicate::leaf(self.tag, DotsCompareOp::NotNull, None)
    }
}

/// Typed predicate accumulator. The `S` parameter pins the predicate
/// to the source struct's type so cross-type composition fails to
/// compile.
#[derive(Debug, Clone)]
pub struct Predicate<S> {
    nodes: Vec<DotsPredicateNode>,
    _phantom: PhantomData<fn() -> S>,
}

impl<S> Predicate<S> {
    fn leaf(tag: u32, op: DotsCompareOp, value: Option<DotsPredicateValue>) -> Self {
        Self {
            nodes: vec![DotsPredicateNode {
                kind: Some(DotsPredicateKind::Leaf),
                leaf: Some(DotsPredicateLeaf {
                    property_tag: Some(tag),
                    op: Some(op),
                    value,
                }),
                arity: None,
            }],
            _phantom: PhantomData,
        }
    }

    fn root_kind(&self) -> DotsPredicateKind {
        self.nodes
            .first()
            .and_then(|n| n.kind)
            .unwrap_or(DotsPredicateKind::Leaf)
    }

    fn root_arity(&self) -> u32 {
        self.nodes.first().and_then(|n| n.arity).unwrap_or(1)
    }

    fn into_dots_predicate(self) -> DotsPredicate {
        DotsPredicate {
            nodes: Some(self.nodes),
        }
    }
}

/// N-ary collapse for `&` / `|`: if the root of `lhs` already
/// matches `op`, splice rhs's nodes onto the existing arity rather
/// than introducing a nested AndOp/OrOp. Mirrors the C++ DSL's
/// behaviour and keeps the wire form compact.
fn combine<S>(op: DotsPredicateKind, mut lhs: Predicate<S>, mut rhs: Predicate<S>) -> Predicate<S> {
    let lhs_same = lhs.root_kind() == op;
    let rhs_same = rhs.root_kind() == op;
    let lhs_arity = if lhs_same { lhs.root_arity() } else { 1 };
    let rhs_arity = if rhs_same { rhs.root_arity() } else { 1 };

    let mut out_nodes = Vec::with_capacity(1 + lhs.nodes.len() + rhs.nodes.len());
    out_nodes.push(DotsPredicateNode {
        kind: Some(op),
        leaf: None,
        arity: Some(lhs_arity + rhs_arity),
    });
    if lhs_same {
        // Strip lhs's head; splice its children.
        lhs.nodes.remove(0);
    }
    if rhs_same {
        rhs.nodes.remove(0);
    }
    out_nodes.append(&mut lhs.nodes);
    out_nodes.append(&mut rhs.nodes);
    Predicate {
        nodes: out_nodes,
        _phantom: PhantomData,
    }
}

impl<S> BitAnd for Predicate<S> {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        combine(DotsPredicateKind::AndOp, self, rhs)
    }
}

impl<S> BitOr for Predicate<S> {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        combine(DotsPredicateKind::OrOp, self, rhs)
    }
}

impl<S> Not for Predicate<S> {
    type Output = Self;
    fn not(self) -> Self {
        let mut out_nodes = Vec::with_capacity(1 + self.nodes.len());
        out_nodes.push(DotsPredicateNode {
            kind: Some(DotsPredicateKind::NotOp),
            leaf: None,
            arity: Some(1),
        });
        out_nodes.extend(self.nodes);
        Predicate {
            nodes: out_nodes,
            _phantom: PhantomData,
        }
    }
}

/// Fluent terminator: pass a [`Predicate<S>`] to start building a
/// [`DotsFilter`].
///
/// ```ignore
/// let filter = filter::predicate(MyType::ID.eq(42_u32) & MyType::SEQ.lt(100_u64))
///     .project(MyType::PROP_ID | MyType::PROP_SEQ)
///     .build();
/// ```
pub fn predicate<S>(p: Predicate<S>) -> FilterBuilder<S> {
    FilterBuilder {
        pred: Some(p),
        mask: None,
        _phantom: PhantomData,
    }
}

/// Build a [`DotsFilter`] with column projection only (no row
/// predicate). Useful when you want a view that mirrors all rows but
/// strips out non-relevant columns.
pub fn project_only<S>(mask: PropertySet) -> FilterBuilder<S> {
    FilterBuilder {
        pred: None,
        mask: Some(mask),
        _phantom: PhantomData,
    }
}

/// Mutable composition handle. Use [`FilterBuilder::project`] to add
/// a column mask and [`FilterBuilder::build`] to emit the wire
/// [`DotsFilter`].
#[derive(Debug)]
pub struct FilterBuilder<S> {
    pred: Option<Predicate<S>>,
    mask: Option<PropertySet>,
    _phantom: PhantomData<fn() -> S>,
}

impl<S> FilterBuilder<S> {
    /// Set the column projection mask. Subsequent `project` calls
    /// overwrite — call once with the full mask.
    pub fn project(mut self, mask: PropertySet) -> Self {
        self.mask = Some(mask);
        self
    }

    /// Emit the wire [`DotsFilter`].
    pub fn build(self) -> DotsFilter {
        DotsFilter {
            predicate: self.pred.map(Predicate::into_dots_predicate),
            property_mask: self.mask,
        }
    }
}
