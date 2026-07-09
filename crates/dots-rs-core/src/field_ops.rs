//! Value-level operations on DOTS field types: equality, total order,
//! and recursive merge.
//!
//! [`DotsFieldOps`] is the companion of [`DotsField`](crate::DotsField)
//! (the encode/decode trait): where `DotsField` says how a type crosses
//! the wire, `DotsFieldOps` says how two values of it compare and merge.
//! The struct-level operations — `equal`/`cmp`/`diff_properties` on
//! [`AnyStruct`](crate::AnyStruct) and the derive-generated impls —
//! dispatch through it via the `PropertyVtable` thunks.
//!
//! # Ordering semantics
//!
//! `dots_cmp` is a **total** order, and `dots_eq` is coherent with it
//! (`dots_eq(a, b) ⇔ dots_cmp(a, b) == Ordering::Equal`). This follows
//! dots-cpp's property ordering (invalid < valid, lexicographic struct
//! and vector comparison) with one deliberate divergence: floating-point
//! fields compare via `f64::total_cmp`, so NaN participates in the order
//! deterministically instead of poisoning it. Consequences: `NaN`
//! equals a bit-identical `NaN`, and `-0.0 < +0.0`.

use core::cmp::Ordering;

use alloc::string::String;
use alloc::vec::Vec;

use crate::{AnyObject, Duration, PropertySet, Timepoint};

/// Value-level equality, total order, and merge for every type that can
/// occupy a DOTS property slot.
///
/// Implemented manually for the leaf types below; `#[derive(DotsStruct)]`
/// and `#[derive(DotsEnum)]` emit impls for derived structs (recursive,
/// property-wise in descriptor order) and enums (ordered by the
/// enumerator's `int32` value, matching dots-cpp).
pub trait DotsFieldOps: Clone {
    /// Value equality. Coherent with [`dots_cmp`](Self::dots_cmp).
    fn dots_eq(&self, other: &Self) -> bool {
        self.dots_cmp(other) == Ordering::Equal
    }

    /// Total order. See the module docs for float semantics.
    fn dots_cmp(&self, other: &Self) -> Ordering;

    /// Merge `other` into `self` with dots-cpp `_merge` semantics.
    ///
    /// For plain values this is a clone-assign — the source value wins
    /// wholesale. Derived structs override it with the recursive
    /// property-wise merge (properties set on `other` overlay `self`;
    /// properties unset on `other` survive).
    fn dots_merge(&mut self, other: &Self) {
        *self = other.clone();
    }
}

/// Leaf impls for types with a native `Ord`.
macro_rules! impl_field_ops_via_ord {
    ($($t:ty),* $(,)?) => {
        $(
            impl DotsFieldOps for $t {
                #[inline]
                fn dots_eq(&self, other: &Self) -> bool {
                    self == other
                }
                #[inline]
                fn dots_cmp(&self, other: &Self) -> Ordering {
                    Ord::cmp(self, other)
                }
            }
        )*
    };
}

impl_field_ops_via_ord!(
    bool,
    u8, u16, u32, u64,
    i8, i16, i32, i64,
    String,
    [u8; 16],
);

impl DotsFieldOps for f32 {
    #[inline]
    fn dots_cmp(&self, other: &Self) -> Ordering {
        self.total_cmp(other)
    }
}

impl DotsFieldOps for f64 {
    #[inline]
    fn dots_cmp(&self, other: &Self) -> Ordering {
        self.total_cmp(other)
    }
}

impl DotsFieldOps for PropertySet {
    #[inline]
    fn dots_eq(&self, other: &Self) -> bool {
        self == other
    }
    #[inline]
    fn dots_cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(&self.bits(), &other.bits())
    }
}

impl DotsFieldOps for Timepoint {
    #[inline]
    fn dots_cmp(&self, other: &Self) -> Ordering {
        self.as_secs_f64().total_cmp(&other.as_secs_f64())
    }
}

impl DotsFieldOps for Duration {
    #[inline]
    fn dots_cmp(&self, other: &Self) -> Ordering {
        self.as_secs_f64().total_cmp(&other.as_secs_f64())
    }
}

/// `any` fields compare on their envelope — type name first, then the
/// canonical-CBOR payload bytes. Payload bytes are a faithful equality
/// proxy because encoding is deterministic (descriptor-ordered map).
impl DotsFieldOps for AnyObject {
    #[inline]
    fn dots_eq(&self, other: &Self) -> bool {
        self == other
    }
    #[inline]
    fn dots_cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(self, other)
    }
}

/// Vectors compare lexicographically, element-wise — same as dots-cpp's
/// `VectorDescriptor` ordering. Merge is a wholesale clone-assign (the
/// C++ `_merge` recurses into struct-typed properties only, never into
/// vector elements).
impl<T: DotsFieldOps> DotsFieldOps for Vec<T> {
    fn dots_eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self.iter().zip(other).all(|(a, b)| a.dots_eq(b))
    }

    fn dots_cmp(&self, other: &Self) -> Ordering {
        for (a, b) in self.iter().zip(other) {
            match a.dots_cmp(b) {
                Ordering::Equal => continue,
                non_eq => return non_eq,
            }
        }
        Ord::cmp(&self.len(), &other.len())
    }
}

// ===== Option<T> slot helpers =====
//
// A DOTS property slot is `Option<T>` (or bare `T` for keys). These
// helpers define the slot-level semantics — `None` sorts before any
// `Some(_)`, mirroring dots-cpp's "invalid < valid" property ordering —
// and are shared by the derive-generated `DotsFieldOps` impls and the
// `PropertyVtable` thunks in `layout`.

/// Slot equality: two `None`s are equal, `None ≠ Some(_)`, two `Some`s
/// compare their inner values.
#[inline]
pub fn opt_field_eq<T: DotsFieldOps>(a: &Option<T>, b: &Option<T>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => x.dots_eq(y),
        _ => false,
    }
}

/// Slot order: `None < Some(_)`, two `Some`s compare their inner values.
#[inline]
pub fn opt_field_cmp<T: DotsFieldOps>(a: &Option<T>, b: &Option<T>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => x.dots_cmp(y),
    }
}

/// Slot merge with dots-cpp `_merge` semantics: `src` unset → `dst`
/// untouched; both set → recursive [`DotsFieldOps::dots_merge`]; `src`
/// set and `dst` unset → deep-clone into place.
#[inline]
pub fn opt_field_merge<T: DotsFieldOps>(dst: &mut Option<T>, src: &Option<T>) {
    match (dst.as_mut(), src) {
        (_, None) => {}
        (Some(d), Some(s)) => d.dots_merge(s),
        (None, Some(s)) => *dst = Some(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn float_total_order_handles_nan_and_signed_zero() {
        assert_eq!(f64::NAN.dots_cmp(&f64::NAN), Ordering::Equal);
        assert!(f64::NAN.dots_eq(&f64::NAN));
        assert_eq!((-0.0f64).dots_cmp(&0.0), Ordering::Less);
        assert_eq!(1.5f64.dots_cmp(&2.5), Ordering::Less);
    }

    #[test]
    fn vec_compares_lexicographically() {
        let a = vec![1u32, 2, 3];
        let b = vec![1u32, 2, 4];
        let c = vec![1u32, 2];
        assert_eq!(a.dots_cmp(&b), Ordering::Less);
        assert_eq!(a.dots_cmp(&c), Ordering::Greater);
        assert!(a.dots_eq(&a.clone()));
    }

    #[test]
    fn option_slot_ordering_puts_none_first() {
        assert_eq!(opt_field_cmp::<u32>(&None, &Some(0)), Ordering::Less);
        assert_eq!(opt_field_cmp::<u32>(&None, &None), Ordering::Equal);
        assert_eq!(opt_field_cmp(&Some(3u32), &Some(2)), Ordering::Greater);
        assert!(opt_field_eq::<u32>(&None, &None));
        assert!(!opt_field_eq(&None, &Some(1u32)));
    }

    #[test]
    fn option_slot_merge_keeps_dst_when_src_unset() {
        let mut dst = Some(7u32);
        opt_field_merge(&mut dst, &None);
        assert_eq!(dst, Some(7));
        opt_field_merge(&mut dst, &Some(9));
        assert_eq!(dst, Some(9));
        let mut empty: Option<u32> = None;
        opt_field_merge(&mut empty, &Some(1));
        assert_eq!(empty, Some(1));
    }
}
