use core::fmt;
use crate::{DecodeError, DotsField, DotsTypeKind, EncodeError, FieldKind};
use crate::layout::{CborDecoder, CborEncoder};

/// Bitmask of valid properties, indexed by property tag.
///
/// **Wire layout matches C++ DOTS:** bit `n` of the underlying `u32`
/// represents tag `n`. Tag `0` is unused (DOTS tags are 1-based, so
/// bit 0 is always zero in valid encodings); tag `1` is bit `1`,
/// up to tag `31` at bit `31`.
///
/// This 1:1 tag→bit mapping is what `PropertySet::FromIndex(tag)` does
/// in dots-cpp (`lib/include/dots/type/PropertySet.h`). Diverging from
/// it would silently corrupt the `DotsHeader.attributes` field — a
/// peer would read our bitmask one bit "off", treat properties at the
/// wrong tags as valid/invalid, and propagate broken cache merges.
#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub struct PropertySet(u32);

impl DotsTypeKind for PropertySet {
    const KIND: FieldKind = FieldKind::PropertySet;
}

impl DotsField for PropertySet {
    #[inline]
    fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        <u32 as DotsField>::dots_encode(&self.0, e)
    }
    #[inline]
    fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
        <u32 as DotsField>::dots_decode(d).map(Self)
    }
}

impl PropertySet {
    /// All properties invalid.
    pub const EMPTY: Self = Self(0);

    /// Maximum tag value supported by this representation.
    /// Tag = bit-position, so we lose tag 0 (unused in DOTS) and the
    /// max usable tag is 31.
    pub const MAX_TAG: u32 = 31;

    /// Construct from a raw bitmask. Bit `n` corresponds to tag `n`.
    #[inline]
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    /// Raw bitmask access.
    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Test whether `tag` is set. Tags outside `1..=MAX_TAG` return `false`.
    #[inline]
    pub const fn has(self, tag: u32) -> bool {
        if tag == 0 || tag > Self::MAX_TAG {
            return false;
        }
        (self.0 & (1u32 << tag)) != 0
    }

    /// Return a new set with `tag` added.
    #[inline]
    #[must_use]
    pub const fn with_tag(self, tag: u32) -> Self {
        debug_assert!(tag != 0 && tag <= Self::MAX_TAG);
        Self(self.0 | (1u32 << tag))
    }

    /// Return a new set with `tag` removed.
    #[inline]
    #[must_use]
    pub const fn without_tag(self, tag: u32) -> Self {
        debug_assert!(tag != 0 && tag <= Self::MAX_TAG);
        Self(self.0 & !(1u32 << tag))
    }

    /// True if no tags are set.
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Number of tags set.
    #[inline]
    pub const fn len(self) -> u32 {
        self.0.count_ones()
    }

    /// Iterate the tags that are set, in ascending order.
    pub fn iter(self) -> impl Iterator<Item = u32> {
        let mut bits = self.0;
        core::iter::from_fn(move || {
            if bits == 0 {
                None
            } else {
                let tz = bits.trailing_zeros();
                bits &= bits - 1;
                Some(tz)
            }
        })
    }
}

impl fmt::Debug for PropertySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PropertySet{")?;
        let mut first = true;
        for tag in self.iter() {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            write!(f, "{tag}")?;
        }
        f.write_str("}")
    }
}

impl core::ops::BitOr for PropertySet {
    type Output = Self;
    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl core::ops::BitAnd for PropertySet {
    type Output = Self;
    #[inline]
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

impl core::ops::Sub for PropertySet {
    type Output = Self;
    /// Set difference: `self - rhs` removes any tags present in `rhs`.
    #[inline]
    fn sub(self, rhs: Self) -> Self {
        Self(self.0 & !rhs.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec::Vec;

    #[test]
    fn empty_set_has_no_tags() {
        let s = PropertySet::EMPTY;
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert!(!s.has(1));
        assert!(!s.has(31));
    }

    #[test]
    fn add_and_query_tags() {
        let s = PropertySet::EMPTY.with_tag(1).with_tag(7).with_tag(31);
        assert!(s.has(1));
        assert!(s.has(7));
        assert!(s.has(31));
        assert!(!s.has(2));
        assert_eq!(s.len(), 3);
    }

    #[test]
    fn iter_yields_ascending_tags() {
        let s = PropertySet::EMPTY.with_tag(7).with_tag(1).with_tag(3);
        let v: Vec<u32> = s.iter().collect();
        assert_eq!(v, [1, 3, 7]);
    }

    #[test]
    fn out_of_range_tags_are_not_set() {
        let s = PropertySet::EMPTY;
        assert!(!s.has(0));
        assert!(!s.has(32));
        assert!(!s.has(u32::MAX));
    }

    #[test]
    fn without_tag_clears_only_that_bit() {
        let s = PropertySet::EMPTY.with_tag(2).with_tag(5);
        let t = s.without_tag(2);
        assert!(!t.has(2));
        assert!(t.has(5));
    }

    #[test]
    fn set_algebra() {
        let a = PropertySet::EMPTY.with_tag(1).with_tag(2);
        let b = PropertySet::EMPTY.with_tag(2).with_tag(3);
        assert_eq!((a | b).iter().collect::<Vec<_>>(), [1, 2, 3]);
        assert_eq!((a & b).iter().collect::<Vec<_>>(), [2]);
        assert_eq!((a - b).iter().collect::<Vec<_>>(), [1]);
    }

    /// Wire-format spot-check: tag→bit mapping matches C++
    /// `PropertySet::FromIndex(tag)` which is `1 << tag`.
    #[test]
    fn bit_layout_matches_cpp_from_index() {
        assert_eq!(PropertySet::EMPTY.with_tag(1).bits(), 0b10);
        assert_eq!(PropertySet::EMPTY.with_tag(2).bits(), 0b100);
        assert_eq!(PropertySet::EMPTY.with_tag(3).bits(), 0b1000);
        // Pinger-shaped (tags 1, 2, 3) → bits 1, 2, 3 set → 0b1110 = 14.
        let s = PropertySet::EMPTY.with_tag(1).with_tag(2).with_tag(3);
        assert_eq!(s.bits(), 0b1110);
    }
}
