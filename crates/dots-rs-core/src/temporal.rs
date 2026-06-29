//! Temporal value types — `Timepoint` and `Duration`.
//!
//! Both wrap `f64` (fractional seconds) — wire-compatible with the
//! C++ DOTS `timepoint_t` / `duration_t` types defined in
//! `dots-cpp/lib/include/dots/type/Chrono.h`. The wire encoding is
//! identical to a plain CBOR `float64`; the distinction lives in the
//! [`FieldKind`] tag, which surfaces as `"timepoint"` / `"duration"`
//! in `StructDescriptorData.type` strings so other DOTS peers can
//! tell them apart from raw floats.
//!
//! [`FieldKind`]: crate::FieldKind

use core::time::Duration as StdDuration;

use crate::layout::{CborDecoder, CborEncoder, DecodeError, DotsField, EncodeError};
use crate::{DotsTypeKind, FieldKind};

/// Absolute wall-clock time as fractional seconds since the Unix epoch.
///
/// Wire-compatible with C++ DOTS `timepoint_t`. On the wire it's a
/// CBOR `float64`; in descriptor metadata its kind is reported as
/// `"timepoint"`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct Timepoint(pub f64);

impl Timepoint {
    /// Raw fractional-seconds-since-epoch value.
    ///
    /// `Timepoint::now()` lives in `dots-transport` (which requires
    /// `std::time`); dots-core stays no_std and exposes only the
    /// type itself.
    #[inline]
    pub const fn as_secs_f64(self) -> f64 {
        self.0
    }
}

impl From<f64> for Timepoint {
    fn from(v: f64) -> Self {
        Self(v)
    }
}

impl From<Timepoint> for f64 {
    fn from(v: Timepoint) -> Self {
        v.0
    }
}

impl DotsField for Timepoint {
    #[inline]
    fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        <f64 as DotsField>::dots_encode(&self.0, e)
    }
    #[inline]
    fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
        <f64 as DotsField>::dots_decode(d).map(Self)
    }
}

impl DotsTypeKind for Timepoint {
    const KIND: FieldKind = FieldKind::Timepoint;
}

/// A duration as fractional seconds.
///
/// Wire-compatible with C++ DOTS `duration_t`. On the wire it's a
/// CBOR `float64`; in descriptor metadata its kind is reported as
/// `"duration"`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct Duration(pub f64);

impl Duration {
    #[inline]
    pub const fn as_secs_f64(self) -> f64 {
        self.0
    }

    pub fn from_std(d: StdDuration) -> Self {
        Self(d.as_secs_f64())
    }

    pub fn to_std(self) -> Option<StdDuration> {
        if self.0 < 0.0 || !self.0.is_finite() {
            None
        } else {
            Some(StdDuration::from_secs_f64(self.0))
        }
    }
}

impl From<f64> for Duration {
    fn from(v: f64) -> Self {
        Self(v)
    }
}

impl From<Duration> for f64 {
    fn from(v: Duration) -> Self {
        v.0
    }
}

impl From<StdDuration> for Duration {
    fn from(d: StdDuration) -> Self {
        Self::from_std(d)
    }
}

impl DotsField for Duration {
    #[inline]
    fn dots_encode(&self, e: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        <f64 as DotsField>::dots_encode(&self.0, e)
    }
    #[inline]
    fn dots_decode(d: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
        <f64 as DotsField>::dots_decode(d).map(Self)
    }
}

impl DotsTypeKind for Duration {
    const KIND: FieldKind = FieldKind::Duration;
}
