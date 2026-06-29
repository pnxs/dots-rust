use std::io;

use dots_rs_model::FramingError;

/// Errors surfaced by [`TransmissionCodec`] through `tokio_util::codec::Framed`.
///
/// `Framed` requires the codec's error type to implement
/// `From<std::io::Error>` so it can lift transport-level read/write
/// failures into the same error channel as protocol-level decoding.
///
/// [`TransmissionCodec`]: crate::TransmissionCodec
#[derive(Debug)]
pub enum TransportError {
    /// Frame-level decode/encode failure.
    Framing(FramingError),
    /// Underlying byte-stream I/O failure (read, write, EOF).
    Io(io::Error),
}

impl core::fmt::Display for TransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Framing(e) => write!(f, "framing error: {e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Framing(e) => Some(e),
            Self::Io(e) => Some(e),
        }
    }
}

impl From<FramingError> for TransportError {
    fn from(e: FramingError) -> Self {
        Self::Framing(e)
    }
}

impl From<io::Error> for TransportError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
