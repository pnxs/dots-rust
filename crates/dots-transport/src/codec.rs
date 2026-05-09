use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use dots_model::{FramingError, Registry, Transmission};
use tokio_util::codec::{Decoder, Encoder};

use crate::TransportError;

/// `tokio_util::codec` adapter for the v2 transmission framing.
///
/// `Framed<S, TransmissionCodec>` over any `AsyncRead+AsyncWrite` stream
/// `S` gives a `Stream<Item = Result<Transmission, TransportError>>` plus
/// `Sink<Transmission, Error = TransportError>`. The codec carries only an
/// `Arc<Registry>` for resolving payload type names during decode, so it's
/// `Clone` + `Send` + `Sync` and can be shared across connections.
#[derive(Debug, Clone)]
pub struct TransmissionCodec {
    registry: Arc<Registry>,
}

impl TransmissionCodec {
    /// Build a codec backed by the given registry.
    pub fn new(registry: Arc<Registry>) -> Self {
        Self { registry }
    }

    /// Read-only handle to the registry. Callers can reach inside if
    /// they need to consult or extend it (e.g. registering a peer's
    /// descriptor before its first transmission arrives).
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
}

impl Decoder for TransmissionCodec {
    type Item = Transmission;
    type Error = TransportError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // We deliberately re-enter Transmission::decode on every call
        // even when only the size prefix is available; the framing
        // layer maps "incomplete" to NeedMoreData and we lift that to
        // `Ok(None)`, asking the buffer to reserve room for the rest.
        match Transmission::decode(src.as_ref(), &self.registry) {
            Ok((txn, consumed)) => {
                src.advance(consumed);
                Ok(Some(txn))
            }
            Err(FramingError::NeedMoreData { have, need }) => {
                // Hint the buffer to size up to the full frame so the
                // next read fills it in one go.
                let additional = need.saturating_sub(have);
                src.reserve(additional);
                Ok(None)
            }
            Err(other) => Err(TransportError::Framing(other)),
        }
    }
}

impl Encoder<Transmission> for TransmissionCodec {
    type Error = TransportError;

    fn encode(&mut self, item: Transmission, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Transmission::encode produces a complete v2 frame (size
        // prefix + header + payload). Append it to the output buffer.
        let frame = item.encode();
        dst.reserve(frame.len());
        dst.put_slice(&frame);
        Ok(())
    }
}
