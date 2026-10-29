//! DOTS UUID values with standard text formatting and unchanged binary encoding.

use core::{fmt, str::FromStr};

use crate::layout::{CborDecoder, CborEncoder, DecodeError, EncodeError};
use crate::{DotsField, DotsFieldOps, DotsTypeKind, FieldKind, ParseValueError};

/// A 128-bit UUID in network byte order.
///
/// Display uses lowercase `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` text.
/// Parsing also accepts the legacy compact hexadecimal forms, optionally
/// prefixed with `0x`. No UUID version or variant restrictions are imposed.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Uuid([u8; 16]);

impl Uuid {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl From<[u8; 16]> for Uuid {
    fn from(bytes: [u8; 16]) -> Self {
        Self::from_bytes(bytes)
    }
}

impl From<Uuid> for [u8; 16] {
    fn from(uuid: Uuid) -> Self {
        uuid.into_bytes()
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, byte) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Uuid {
    type Err = ParseValueError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        crate::uuid_from_str(text).map(Self)
    }
}

impl DotsTypeKind for Uuid {
    const KIND: FieldKind = FieldKind::Uuid;
}

impl DotsField for Uuid {
    fn dots_encode(&self, encoder: &mut CborEncoder<'_>) -> Result<(), EncodeError> {
        self.0.dots_encode(encoder)
    }

    fn dots_decode(decoder: &mut CborDecoder<'_>) -> Result<Self, DecodeError> {
        <[u8; 16]>::dots_decode(decoder).map(Self)
    }
}

impl DotsFieldOps for Uuid {
    fn dots_cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.cmp(other)
    }
}
