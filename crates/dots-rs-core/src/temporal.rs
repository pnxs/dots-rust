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

impl core::str::FromStr for Timepoint {
    type Err = crate::text::ParseValueError;

    /// Accepts fractional seconds since the Unix epoch, or an ISO-8601
    /// date-time — read as UTC when it carries no offset. See
    /// [`crate::text::parse_timepoint`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::text::parse_timepoint(s).map(Self)
    }
}

impl core::fmt::Display for Timepoint {
    /// ISO-8601 UTC instant, to nanosecond precision with trailing
    /// zeros trimmed — the inverse of the [`FromStr`](core::str::FromStr)
    /// impl.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&crate::text::format_timepoint(self.0))
    }
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

impl core::str::FromStr for Duration {
    type Err = crate::text::ParseValueError;

    /// Accepts fractional seconds, with an optional `s` suffix.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::text::parse_duration(s).map(Self)
    }
}

impl core::fmt::Display for Duration {
    /// Fractional seconds with an `s` suffix.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}s", self.0)
    }
}

#[cfg(test)]
mod tests {
    //! Wire-byte expectations copied from dots-cpp's
    //! `tests/src/serialization/TestCborSerializer.cpp`
    //! (`CborSerializerTestDataEncoded::timePoint1` / `duration1` /
    //! `duration2` / `float64Zero`). Both temporal types must encode as
    //! a plain CBOR float64 (`0xFB` + 8 bytes IEEE 754) — never shrunk
    //! to float32/float16 — to stay byte-compatible with dots-cpp.

    use alloc::vec::Vec;

    use super::*;

    fn encode_field<T: DotsField>(value: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut e: CborEncoder<'_> = minicbor::Encoder::new(&mut buf);
        value
            .dots_encode(&mut e)
            .expect("Vec writes are infallible");
        buf
    }

    fn decode_field<T: DotsField>(bytes: &[u8]) -> T {
        let mut d: CborDecoder<'_> = minicbor::Decoder::new(bytes);
        let value = T::dots_decode(&mut d).expect("decode succeeds");
        assert_eq!(d.position(), bytes.len(), "decode must consume all bytes");
        value
    }

    /// cpp `timePoint1`: "2020-03-11T21:07:57.500+00:00" is
    /// 1583960877.5 seconds since the Unix epoch.
    #[test]
    fn timepoint_golden_wire_bytes() {
        const TIMEPOINT1: &[u8] = &[0xFB, 0x41, 0xD7, 0x9A, 0x54, 0xCB, 0x60, 0x00, 0x00];
        let tp = Timepoint(1583960877.5);
        let bytes = encode_field(&tp);
        assert_eq!(bytes, TIMEPOINT1);
        assert_eq!(decode_field::<Timepoint>(&bytes), tp);
    }

    /// cpp `duration1` (123.456 s) and `duration2` (5min + 42s + 73ms).
    #[test]
    fn duration_golden_wire_bytes() {
        const DURATION1: &[u8] = &[0xFB, 0x40, 0x5E, 0xDD, 0x2F, 0x1A, 0x9F, 0xBE, 0x77];
        const DURATION2: &[u8] = &[0xFB, 0x40, 0x75, 0x61, 0x2B, 0x02, 0x0C, 0x49, 0xBA];

        let d1 = Duration(123.456);
        let bytes = encode_field(&d1);
        assert_eq!(bytes, DURATION1);
        assert_eq!(decode_field::<Duration>(&bytes), d1);

        let d2 = Duration(342.073);
        let bytes = encode_field(&d2);
        assert_eq!(bytes, DURATION2);
        assert_eq!(decode_field::<Duration>(&bytes), d2);
    }

    /// cpp `float64Zero`: 0.0 stays a full float64 (`0xFB` + 8 zero
    /// bytes) — no shrinking to a shorter float encoding.
    #[test]
    fn zero_values_stay_float64() {
        const FLOAT64_ZERO: &[u8] = &[0xFB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(encode_field(&Timepoint(0.0)), FLOAT64_ZERO);
        assert_eq!(encode_field(&Duration(0.0)), FLOAT64_ZERO);
    }

    /// The temporal types are distinguishable from raw `f64` only via
    /// their `FieldKind` — descriptor metadata, never wire bytes.
    #[test]
    fn field_kind_routing() {
        assert!(matches!(
            <Timepoint as DotsTypeKind>::KIND,
            FieldKind::Timepoint
        ));
        assert!(matches!(
            <Duration as DotsTypeKind>::KIND,
            FieldKind::Duration
        ));
    }
}
