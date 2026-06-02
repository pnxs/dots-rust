//! Predicate evaluator for server-side filtered subscriptions.
//!
//! [`CompiledPredicate`] is the compiled-against-descriptor form of a
//! wire [`DotsPredicate`]. Compilation validates well-formedness
//! (every leaf names a known property, the value slot matches the
//! property's type, the op is applicable to that type) and narrows
//! the wire's width-unified integer / float slots to the property's
//! actual type so per-event evaluation is a simple variant-match on
//! [`DynamicValue`].
//!
//! Both the host (during cache merge + fan-out) and the guest's
//! `View<T>` operate through this single compiled form. The matcher
//! takes a [`DynamicStruct`] payload — which both sides already have
//! available (host: cache pool; guest: about to be decoded into the
//! view's typed container).

use std::fmt;

use dots_core::{DynamicFieldKind, DynamicPropertyDescriptor, DynamicStruct,
    DynamicStructDescriptor, DynamicValue};
use dots_model::filter::{DotsCompareOp, DotsPredicate, DotsPredicateKind, DotsPredicateLeaf,
    DotsPredicateNode, DotsPredicateValue};

/// Compiled, descriptor-resolved form of a predicate. Cheap to clone
/// (Vec of small enums) and re-evaluable against any number of
/// payloads of the predicate's source type.
#[derive(Debug, Clone)]
pub struct CompiledPredicate {
    nodes: Vec<CompiledNode>,
}

#[derive(Debug, Clone)]
enum CompiledNode {
    Leaf {
        tag: u32,
        op: DotsCompareOp,
        rhs: CompiledRhs,
    },
    And { arity: u32 },
    Or { arity: u32 },
    Not,
}

#[derive(Debug, Clone)]
enum CompiledRhs {
    /// `IsNull` / `NotNull` — no value compared.
    None,
    Scalar(DynamicValue),
    List(Vec<DynamicValue>),
}

/// What went wrong compiling a predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompileError {
    /// A leaf's `kind` / `op` / `property_tag` was missing.
    MalformedLeaf,
    /// A non-leaf node didn't carry an `arity`.
    MissingArity,
    /// `NotOp` must have arity exactly 1.
    BadNotArity(u32),
    /// `AndOp` / `OrOp` must have arity ≥ 1.
    BadOpArity(u32),
    /// The predicate vector ended mid-tree (parent expected more
    /// children).
    TruncatedTree,
    /// Leaf's `property_tag` doesn't exist on the target type.
    UnknownPropertyTag(u32),
    /// Leaf's value slot doesn't match the property's wire type.
    WrongValueSlot { tag: u32, expected: &'static str },
    /// The compare op isn't applicable to the property's type
    /// (e.g. `Lt` on bool, `IsIn` requiring a list slot).
    OpNotApplicable { tag: u32, op: DotsCompareOp },
    /// A non-null op was missing its `value`.
    MissingValue { tag: u32, op: DotsCompareOp },
    /// `IsIn` / `NotIn` was given an empty list.
    EmptyList { tag: u32 },
    /// A leaf referenced a non-leaf field type (struct / vec / enum
    /// / property_set) for which comparisons aren't supported.
    UnsupportedPropertyType { tag: u32 },
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedLeaf => f.write_str("leaf node missing kind/op/property_tag"),
            Self::MissingArity => f.write_str("non-leaf node missing arity"),
            Self::BadNotArity(a) => write!(f, "NotOp must have arity 1, got {a}"),
            Self::BadOpArity(a) => write!(f, "AndOp/OrOp must have arity ≥ 1, got {a}"),
            Self::TruncatedTree => f.write_str("predicate vector ended mid-tree"),
            Self::UnknownPropertyTag(t) => write!(f, "unknown property tag {t}"),
            Self::WrongValueSlot { tag, expected } => {
                write!(f, "leaf at tag {tag} expected {expected} value slot")
            }
            Self::OpNotApplicable { tag, op } => {
                write!(f, "op {op:?} not applicable to property at tag {tag}")
            }
            Self::MissingValue { tag, op } => {
                write!(f, "leaf at tag {tag} with op {op:?} missing value")
            }
            Self::EmptyList { tag } => write!(f, "leaf at tag {tag} got empty list"),
            Self::UnsupportedPropertyType { tag } => {
                write!(f, "comparisons not supported on property at tag {tag}")
            }
        }
    }
}

impl std::error::Error for CompileError {}

impl CompiledPredicate {
    /// Compile a wire predicate against the target type's descriptor.
    /// An empty (or missing-nodes) predicate compiles to a trivial
    /// match-everything; passing such a `DotsPredicate` to a
    /// filtered subscription is therefore equivalent to subscribing
    /// to all rows.
    pub fn compile(
        predicate: &DotsPredicate,
        descriptor: &DynamicStructDescriptor,
    ) -> Result<Self, CompileError> {
        let nodes_in: &[DotsPredicateNode] = match predicate.nodes.as_deref() {
            Some(n) => n,
            None => return Ok(Self { nodes: Vec::new() }),
        };
        if nodes_in.is_empty() {
            return Ok(Self { nodes: Vec::new() });
        }
        let mut out = Vec::with_capacity(nodes_in.len());
        let mut cursor = 0usize;
        compile_subtree(nodes_in, &mut cursor, descriptor, &mut out)?;
        if cursor != nodes_in.len() {
            // Extra trailing nodes after the root subtree — malformed.
            return Err(CompileError::TruncatedTree);
        }
        Ok(Self { nodes: out })
    }

    /// True if the compiled predicate matches everything (no nodes).
    /// An empty predicate is the "unfiltered" sentinel; passing it
    /// through is still useful — projection-only filters are
    /// well-formed.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Evaluate the predicate against a payload.
    ///
    /// Returns `true` if the payload satisfies the predicate. An
    /// empty predicate (no nodes) returns `true` for every payload.
    pub fn matches(&self, payload: &DynamicStruct) -> bool {
        if self.nodes.is_empty() {
            return true;
        }
        let mut cursor = 0usize;
        eval_subtree(&self.nodes, &mut cursor, payload)
    }
}

// ===== compilation =====

fn compile_subtree(
    src: &[DotsPredicateNode],
    cursor: &mut usize,
    descriptor: &DynamicStructDescriptor,
    out: &mut Vec<CompiledNode>,
) -> Result<(), CompileError> {
    if *cursor >= src.len() {
        return Err(CompileError::TruncatedTree);
    }
    let node = &src[*cursor];
    *cursor += 1;
    let kind = node.kind.ok_or(CompileError::MalformedLeaf)?;
    match kind {
        DotsPredicateKind::Leaf => {
            let leaf = node.leaf.as_ref().ok_or(CompileError::MalformedLeaf)?;
            out.push(compile_leaf(leaf, descriptor)?);
            Ok(())
        }
        DotsPredicateKind::AndOp | DotsPredicateKind::OrOp => {
            let arity = node.arity.ok_or(CompileError::MissingArity)?;
            if arity == 0 {
                return Err(CompileError::BadOpArity(arity));
            }
            out.push(if matches!(kind, DotsPredicateKind::AndOp) {
                CompiledNode::And { arity }
            } else {
                CompiledNode::Or { arity }
            });
            for _ in 0..arity {
                compile_subtree(src, cursor, descriptor, out)?;
            }
            Ok(())
        }
        DotsPredicateKind::NotOp => {
            let arity = node.arity.unwrap_or(1);
            if arity != 1 {
                return Err(CompileError::BadNotArity(arity));
            }
            out.push(CompiledNode::Not);
            compile_subtree(src, cursor, descriptor, out)?;
            Ok(())
        }
    }
}

fn compile_leaf(
    leaf: &DotsPredicateLeaf,
    descriptor: &DynamicStructDescriptor,
) -> Result<CompiledNode, CompileError> {
    let tag = leaf.property_tag.ok_or(CompileError::MalformedLeaf)?;
    let op = leaf.op.ok_or(CompileError::MalformedLeaf)?;
    let prop = descriptor
        .property(tag)
        .ok_or(CompileError::UnknownPropertyTag(tag))?;
    let cat = PropertyCategory::from_kind(&prop.kind)
        .ok_or(CompileError::UnsupportedPropertyType { tag })?;

    let rhs = match op {
        DotsCompareOp::IsNull | DotsCompareOp::NotNull => {
            // Null tests apply to every category; value slot ignored.
            CompiledRhs::None
        }
        DotsCompareOp::IsIn | DotsCompareOp::NotIn => {
            if !cat.allows_equality() {
                return Err(CompileError::OpNotApplicable { tag, op });
            }
            let value = leaf.value.as_ref().ok_or(CompileError::MissingValue { tag, op })?;
            let list = narrow_list(value, prop, cat).ok_or(CompileError::WrongValueSlot {
                tag,
                expected: cat.list_slot_name(),
            })?;
            if list.is_empty() {
                return Err(CompileError::EmptyList { tag });
            }
            CompiledRhs::List(list)
        }
        DotsCompareOp::Eq | DotsCompareOp::Neq => {
            if !cat.allows_equality() {
                return Err(CompileError::OpNotApplicable { tag, op });
            }
            let value = leaf.value.as_ref().ok_or(CompileError::MissingValue { tag, op })?;
            let v = narrow_scalar(value, prop, cat).ok_or(CompileError::WrongValueSlot {
                tag,
                expected: cat.scalar_slot_name(),
            })?;
            CompiledRhs::Scalar(v)
        }
        DotsCompareOp::Lt | DotsCompareOp::Le | DotsCompareOp::Gt | DotsCompareOp::Ge => {
            if !cat.allows_ordering() {
                return Err(CompileError::OpNotApplicable { tag, op });
            }
            let value = leaf.value.as_ref().ok_or(CompileError::MissingValue { tag, op })?;
            let v = narrow_scalar(value, prop, cat).ok_or(CompileError::WrongValueSlot {
                tag,
                expected: cat.scalar_slot_name(),
            })?;
            CompiledRhs::Scalar(v)
        }
    };

    Ok(CompiledNode::Leaf { tag, op, rhs })
}

/// Coarse category used both for op-applicability checks and to know
/// which slot of `DotsPredicateValue` to read.
#[derive(Debug, Clone, Copy)]
enum PropertyCategory {
    Bool,
    SignedInt,    // i8..i64
    UnsignedInt,  // u8..u64
    Float,        // f32 / f64
    Timepoint,    // f64 wire, orderable
    Duration,     // f64 wire, orderable
    String,
    Uuid,         // equatable, not orderable
}

impl PropertyCategory {
    fn from_kind(kind: &DynamicFieldKind) -> Option<Self> {
        use DynamicFieldKind::*;
        Some(match kind {
            Bool => Self::Bool,
            I8 | I16 | I32 | I64 => Self::SignedInt,
            U8 | U16 | U32 | U64 => Self::UnsignedInt,
            F32 | F64 => Self::Float,
            Timepoint => Self::Timepoint,
            Duration => Self::Duration,
            String => Self::String,
            Uuid => Self::Uuid,
            // `any` payloads are opaque, so they can't be filtered on
            // server-side (the type identity is in the clear, but the
            // value is not). Treat as non-comparable.
            PropertySet | Vec(_) | Struct(_) | Enum(_) | Any => return None,
        })
    }

    fn allows_equality(self) -> bool {
        // All categories support equality.
        let _ = self;
        true
    }

    fn allows_ordering(self) -> bool {
        !matches!(self, Self::Bool | Self::Uuid)
    }

    fn scalar_slot_name(self) -> &'static str {
        match self {
            Self::Bool => "bool_val",
            Self::SignedInt => "int_val",
            Self::UnsignedInt => "uint_val",
            Self::Float => "float_val",
            Self::Timepoint => "timepoint_val",
            Self::Duration => "duration_val",
            Self::String => "string_val",
            Self::Uuid => "uuid_val",
        }
    }

    fn list_slot_name(self) -> &'static str {
        match self {
            Self::SignedInt => "int_list",
            Self::UnsignedInt => "uint_list",
            Self::Float => "float_list",
            Self::String => "string_list",
            Self::Timepoint => "timepoint_list",
            Self::Uuid => "uuid_list",
            Self::Bool => "(no bool_list)",
            Self::Duration => "(no duration_list)",
        }
    }
}

/// Read the value's scalar slot for `cat`, narrowing the wire-uniform
/// type to the property's actual width.
fn narrow_scalar(
    value: &DotsPredicateValue,
    prop: &DynamicPropertyDescriptor,
    cat: PropertyCategory,
) -> Option<DynamicValue> {
    use DynamicFieldKind::*;
    Some(match cat {
        PropertyCategory::Bool => DynamicValue::Bool(value.bool_val?),
        PropertyCategory::SignedInt => {
            let v = value.int_val?;
            match prop.kind {
                I8 => DynamicValue::I8(v as i8),
                I16 => DynamicValue::I16(v as i16),
                I32 => DynamicValue::I32(v as i32),
                I64 => DynamicValue::I64(v),
                _ => return None,
            }
        }
        PropertyCategory::UnsignedInt => {
            let v = value.uint_val?;
            match prop.kind {
                U8 => DynamicValue::U8(v as u8),
                U16 => DynamicValue::U16(v as u16),
                U32 => DynamicValue::U32(v as u32),
                U64 => DynamicValue::U64(v),
                _ => return None,
            }
        }
        PropertyCategory::Float => {
            let v = value.float_val?;
            match prop.kind {
                F32 => DynamicValue::F32(v as f32),
                F64 => DynamicValue::F64(v),
                _ => return None,
            }
        }
        PropertyCategory::Timepoint => DynamicValue::Timepoint(value.timepoint_val?.0),
        PropertyCategory::Duration => DynamicValue::Duration(value.duration_val?.0),
        PropertyCategory::String => DynamicValue::String(value.string_val.clone()?),
        PropertyCategory::Uuid => DynamicValue::Uuid(value.uuid_val?),
    })
}

/// Read the value's list slot for `cat`, narrowing every element.
fn narrow_list(
    value: &DotsPredicateValue,
    prop: &DynamicPropertyDescriptor,
    cat: PropertyCategory,
) -> Option<Vec<DynamicValue>> {
    use DynamicFieldKind::*;
    Some(match cat {
        PropertyCategory::SignedInt => {
            let items = value.int_list.as_ref()?;
            items
                .iter()
                .map(|&v| match prop.kind {
                    I8 => DynamicValue::I8(v as i8),
                    I16 => DynamicValue::I16(v as i16),
                    I32 => DynamicValue::I32(v as i32),
                    I64 => DynamicValue::I64(v),
                    _ => DynamicValue::I64(v),
                })
                .collect()
        }
        PropertyCategory::UnsignedInt => {
            let items = value.uint_list.as_ref()?;
            items
                .iter()
                .map(|&v| match prop.kind {
                    U8 => DynamicValue::U8(v as u8),
                    U16 => DynamicValue::U16(v as u16),
                    U32 => DynamicValue::U32(v as u32),
                    U64 => DynamicValue::U64(v),
                    _ => DynamicValue::U64(v),
                })
                .collect()
        }
        PropertyCategory::Float => {
            let items = value.float_list.as_ref()?;
            items
                .iter()
                .map(|&v| match prop.kind {
                    F32 => DynamicValue::F32(v as f32),
                    F64 => DynamicValue::F64(v),
                    _ => DynamicValue::F64(v),
                })
                .collect()
        }
        PropertyCategory::String => value
            .string_list
            .as_ref()?
            .iter()
            .map(|s| DynamicValue::String(s.clone()))
            .collect(),
        PropertyCategory::Timepoint => value
            .timepoint_list
            .as_ref()?
            .iter()
            .map(|t| DynamicValue::Timepoint(t.0))
            .collect(),
        PropertyCategory::Uuid => value
            .uuid_list
            .as_ref()?
            .iter()
            .map(|u| DynamicValue::Uuid(*u))
            .collect(),
        PropertyCategory::Bool | PropertyCategory::Duration => return None,
    })
}

// ===== evaluation =====

fn eval_subtree(
    nodes: &[CompiledNode],
    cursor: &mut usize,
    payload: &DynamicStruct,
) -> bool {
    let node = &nodes[*cursor];
    *cursor += 1;
    match node {
        CompiledNode::Leaf { tag, op, rhs } => eval_leaf(*tag, *op, rhs, payload),
        CompiledNode::And { arity } => {
            let mut all = true;
            for _ in 0..*arity {
                // Always advance cursor; can't early-exit without
                // a skip-subtree walk.
                let v = eval_subtree(nodes, cursor, payload);
                all &= v;
            }
            all
        }
        CompiledNode::Or { arity } => {
            let mut any = false;
            for _ in 0..*arity {
                let v = eval_subtree(nodes, cursor, payload);
                any |= v;
            }
            any
        }
        CompiledNode::Not => !eval_subtree(nodes, cursor, payload),
    }
}

fn eval_leaf(
    tag: u32,
    op: DotsCompareOp,
    rhs: &CompiledRhs,
    payload: &DynamicStruct,
) -> bool {
    let lhs = payload.properties.iter().find(|(t, _)| *t == tag).map(|(_, v)| v);
    match op {
        DotsCompareOp::IsNull => lhs.is_none(),
        DotsCompareOp::NotNull => lhs.is_some(),
        DotsCompareOp::Eq => match (lhs, rhs) {
            (Some(l), CompiledRhs::Scalar(r)) => value_eq(l, r),
            _ => false,
        },
        DotsCompareOp::Neq => match (lhs, rhs) {
            (Some(l), CompiledRhs::Scalar(r)) => !value_eq(l, r),
            // null-on-lhs is a non-match for !=  (matches C++ semantics:
            // missing property compares unequal to anything, but the
            // wire form has no representation for that — keep it
            // conservative).
            _ => false,
        },
        DotsCompareOp::Lt | DotsCompareOp::Le | DotsCompareOp::Gt | DotsCompareOp::Ge => {
            let (Some(l), CompiledRhs::Scalar(r)) = (lhs, rhs) else {
                return false;
            };
            value_cmp(l, r).map(|ord| match op {
                DotsCompareOp::Lt => ord.is_lt(),
                DotsCompareOp::Le => ord.is_le(),
                DotsCompareOp::Gt => ord.is_gt(),
                DotsCompareOp::Ge => ord.is_ge(),
                _ => unreachable!(),
            })
            .unwrap_or(false)
        }
        DotsCompareOp::IsIn => match (lhs, rhs) {
            (Some(l), CompiledRhs::List(items)) => items.iter().any(|r| value_eq(l, r)),
            _ => false,
        },
        DotsCompareOp::NotIn => match (lhs, rhs) {
            (Some(l), CompiledRhs::List(items)) => !items.iter().any(|r| value_eq(l, r)),
            _ => false,
        },
    }
}

fn value_eq(a: &DynamicValue, b: &DynamicValue) -> bool {
    use DynamicValue::*;
    match (a, b) {
        (Bool(x), Bool(y)) => x == y,
        (I8(x), I8(y)) => x == y,
        (I16(x), I16(y)) => x == y,
        (I32(x), I32(y)) => x == y,
        (I64(x), I64(y)) => x == y,
        (U8(x), U8(y)) => x == y,
        (U16(x), U16(y)) => x == y,
        (U32(x), U32(y)) => x == y,
        (U64(x), U64(y)) => x == y,
        (F32(x), F32(y)) => x == y,
        (F64(x), F64(y)) => x == y,
        (Timepoint(x), Timepoint(y)) => x == y,
        (Duration(x), Duration(y)) => x == y,
        (String(x), String(y)) => x == y,
        (Uuid(x), Uuid(y)) => x == y,
        // Cross-variant equality not supported; compile-time narrowing
        // ensures both sides have the same variant for any well-formed
        // CompiledPredicate.
        _ => false,
    }
}

fn value_cmp(a: &DynamicValue, b: &DynamicValue) -> Option<core::cmp::Ordering> {
    use DynamicValue::*;
    match (a, b) {
        (I8(x), I8(y)) => Some(x.cmp(y)),
        (I16(x), I16(y)) => Some(x.cmp(y)),
        (I32(x), I32(y)) => Some(x.cmp(y)),
        (I64(x), I64(y)) => Some(x.cmp(y)),
        (U8(x), U8(y)) => Some(x.cmp(y)),
        (U16(x), U16(y)) => Some(x.cmp(y)),
        (U32(x), U32(y)) => Some(x.cmp(y)),
        (U64(x), U64(y)) => Some(x.cmp(y)),
        (F32(x), F32(y)) => x.partial_cmp(y),
        (F64(x), F64(y)) => x.partial_cmp(y),
        (Timepoint(x), Timepoint(y)) => x.partial_cmp(y),
        (Duration(x), Duration(y)) => x.partial_cmp(y),
        (String(x), String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}
