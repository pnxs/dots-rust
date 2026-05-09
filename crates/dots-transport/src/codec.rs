use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use dots_model::{FramingError, Registry, Transmission};
use tokio_util::codec::{Decoder, Encoder};

use crate::TransportError;

/// `tokio_util::codec` adapter for the v2 transmission framing.
///
/// `Framed<S, TransmissionCodec>` over any `AsyncRead+AsyncWrite` stream
/// `S` gives a `Stream<Item = Result<Transmission, TransportError>>` plus
/// `Sink<Transmission, Error = TransportError>`. The codec carries an
/// `Arc<Registry>` for resolving payload type names during decode, so it
/// can be shared across connections via `Clone`.
///
/// The encoder reuses a per-codec scratch buffer across calls so that
/// streaming many transmissions doesn't allocate-per-send. Cloning a
/// codec gives the clone its own independent scratch buffer (the
/// existing one isn't shared).
#[derive(Debug)]
pub struct TransmissionCodec {
    registry: Arc<Registry>,
    /// Encoder scratch — reused across `encode` calls to amortize
    /// allocation. Cleared at the start of each frame.
    scratch: Vec<u8>,
}

impl TransmissionCodec {
    /// Build a codec backed by the given registry.
    pub fn new(registry: Arc<Registry>) -> Self {
        Self {
            registry,
            scratch: Vec::new(),
        }
    }

    /// Read-only handle to the registry. Callers can reach inside if
    /// they need to consult or extend it (e.g. registering a peer's
    /// descriptor before its first transmission arrives).
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
}

impl Clone for TransmissionCodec {
    /// Clones the registry handle but starts the new codec with a
    /// fresh empty scratch buffer — copying it is wasted work since
    /// it gets cleared on the first encode.
    fn clone(&self) -> Self {
        Self::new(self.registry.clone())
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
        // Encode into the per-codec scratch buffer first, then copy
        // the bytes into the framed output. The scratch keeps its
        // capacity across calls so streaming many sends doesn't
        // re-allocate. The copy from scratch into the BytesMut is
        // unavoidable: minicbor's Write trait is foreign and
        // BytesMut isn't, so we can't write directly through the
        // typed property thunks (whose fn-pointer signatures fix the
        // writer to `&mut Vec<u8>`).
        self.scratch.clear();
        item.encode_into(&mut self.scratch);
        dst.reserve(self.scratch.len());
        dst.put_slice(&self.scratch);
        Ok(())
    }
}
