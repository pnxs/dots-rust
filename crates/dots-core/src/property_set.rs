use core::fmt;

/// Bitmask of valid properties, indexed by property tag.
///
/// Tags are 1-based on the wire (matching DOTS / `.dots` conventions).
/// Internally bit `n` of the underlying `u64` represents tag `n + 1`,
/// so tag `1` is bit `0`, tag `64` is bit `63`. Tags above 64 are not
/// supported in this iteration — DOTS allows up to 256 in principle,
/// but no real type approaches that and a `u64` keeps `PropertySet`
/// `Copy`-cheap.
#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub struct PropertySet(u64);

impl PropertySet {
    /// All properties invalid.
    pub const EMPTY: Self = Self(0);

    /// Maximum tag value supported by this representation.
    pub const MAX_TAG: u32 = 64;

    /// Construct from a raw bitmask. Bit `n` corresponds to tag `n + 1`.
    #[inline]
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// Raw bitmask access.
    #[inline]
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Test whether `tag` is set. Tags outside `1..=MAX_TAG` return `false`.
    #[inline]
    pub const fn has(self, tag: u32) -> bool {
        if tag == 0 || tag > Self::MAX_TAG {
            return false;
        }
        (self.0 & (1u64 << (tag - 1))) != 0
    }

    /// Return a new set with `tag` added.
    #[inline]
    #[must_use]
    pub const fn with_tag(self, tag: u32) -> Self {
        debug_assert!(tag != 0 && tag <= Self::MAX_TAG);
        Self(self.0 | (1u64 << (tag - 1)))
    }

    /// Return a new set with `tag` removed.
    #[inline]
    #[must_use]
    pub const fn without_tag(self, tag: u32) -> Self {
        debug_assert!(tag != 0 && tag <= Self::MAX_TAG);
        Self(self.0 & !(1u64 << (tag - 1)))
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
                Some(tz + 1)
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
        assert!(!s.has(64));
    }

    #[test]
    fn add_and_query_tags() {
        let s = PropertySet::EMPTY.with_tag(1).with_tag(7).with_tag(64);
        assert!(s.has(1));
        assert!(s.has(7));
        assert!(s.has(64));
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
        assert!(!s.has(65));
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
}
