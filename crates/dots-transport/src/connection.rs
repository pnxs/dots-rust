//! Client-side connection state machine.
//!
//! [`Connection<S>`] wraps a [`Framed<S, TransmissionCodec>`] and drives
//! the DOTS handshake (Hello → Connect → ConnectResponse). Once
//! established, it exposes a `Stream`-like async API for receiving
//! transmissions and methods for sending typed or dynamic payloads.
//!
//! Generic over `S: AsyncRead + AsyncWrite + Unpin`, so it works over
//! TCP, Unix domain sockets, or any in-memory pipe like
//! [`tokio::io::duplex`] for testing.

use std::sync::Arc;

use bytes::BufMut;
use dots_core::{StructValue, decode_typed_from_slice};
use dots_model::{
    DotsConnectionState, DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgHello,
    Registry, Transmission, encode_typed_transmission_into,
};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::{TransmissionCodec, TransportError};

/// Errors produced while establishing or running a [`Connection`].
#[derive(Debug)]
pub enum ConnectionError {
    /// Underlying transport failure.
    Transport(TransportError),
    /// Peer closed the connection before the handshake completed.
    ConnectionClosed,
    /// Header carried no `type_name` so we can't dispatch.
    HeaderMissingTypeName,
    /// We expected one type and got another (e.g. waiting for Hello,
    /// got something else).
    UnexpectedMessage {
        expected: &'static str,
        got: String,
    },
    /// Server demanded auth and we don't yet support it.
    AuthenticationNotSupported,
    /// Server's `ConnectResponse.accepted` was false (or absent).
    ConnectionRejected {
        server_name: Option<String>,
    },
    /// Decoding a typed handshake payload from the dynamic transmission
    /// failed.
    DecodeFailure(String),
}

impl core::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::ConnectionClosed => f.write_str("peer closed the connection during handshake"),
            Self::HeaderMissingTypeName => f.write_str("incoming header missing type_name"),
            Self::UnexpectedMessage { expected, got } => {
                write!(f, "expected {expected}, got {got}")
            }
            Self::AuthenticationNotSupported => {
                f.write_str("server requires authentication, which this client does not yet support")
            }
            Self::ConnectionRejected { server_name } => write!(
                f,
                "server `{}` rejected the connection",
                server_name.as_deref().unwrap_or("?")
            ),
            Self::DecodeFailure(msg) => write!(f, "handshake decode error: {msg}"),
        }
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) => Some(e),
            _ => None,
        }
    }
}

impl From<TransportError> for ConnectionError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}

/// A DOTS client connection. Wraps a [`Framed`] and tracks handshake state.
#[derive(Debug)]
pub struct Connection<S> {
    framed: Framed<S, TransmissionCodec>,
    state: DotsConnectionState,
    server_name: Option<String>,
    client_id: Option<u32>,
    /// Reused encode buffer for typed sends — avoids per-message allocation.
    scratch: Vec<u8>,
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Connect over an established byte stream and run the DOTS handshake.
    ///
    /// `client_name` is what the broker will display / log this client
    /// as. `registry` must already contain the DOTS-internal types
    /// (use [`dots_model::registry_with_internal_types`] for the easy
    /// path). On success the returned [`Connection`] is in the
    /// [`DotsConnectionState::Connected`] state.
    ///
    /// Authentication is not supported yet; if the server's
    /// [`DotsMsgHello.authentication_required`] is `Some(true)`, this
    /// returns [`ConnectionError::AuthenticationNotSupported`].
    pub async fn establish(
        stream: S,
        client_name: &str,
        registry: Arc<Registry>,
    ) -> Result<Self, ConnectionError> {
        let codec = TransmissionCodec::new(registry);
        let framed = Framed::new(stream, codec);
        let mut conn = Self {
            framed,
            state: DotsConnectionState::Connecting,
            server_name: None,
            client_id: None,
            scratch: Vec::with_capacity(256),
        };
        conn.run_handshake(client_name).await?;
        Ok(conn)
    }

    async fn run_handshake(&mut self, client_name: &str) -> Result<(), ConnectionError> {
        // 1) Receive Hello.
        let txn = self.read_next().await?;
        let hello: DotsMsgHello = self.expect_typed(&txn, "DotsMsgHello")?;
        if hello.authentication_required == Some(true) {
            return Err(ConnectionError::AuthenticationNotSupported);
        }
        self.server_name = hello.server_name;

        // 2) Send Connect (no preload, no auth in this iteration).
        let connect = DotsMsgConnect {
            client_name: Some(client_name.into()),
            preload_cache: Some(false),
            ..Default::default()
        };
        self.send_typed("DotsMsgConnect", &connect).await?;

        // 3) Receive ConnectResponse.
        let txn = self.read_next().await?;
        let response: DotsMsgConnectResponse =
            self.expect_typed(&txn, "DotsMsgConnectResponse")?;
        if response.accepted != Some(true) {
            return Err(ConnectionError::ConnectionRejected {
                server_name: response.server_name,
            });
        }
        self.client_id = response.client_id;
        // No preload requested, so we're directly in the connected state.
        self.state = DotsConnectionState::Connected;
        Ok(())
    }

    async fn read_next(&mut self) -> Result<Transmission, ConnectionError> {
        match self.framed.next().await {
            Some(Ok(txn)) => Ok(txn),
            Some(Err(e)) => Err(ConnectionError::Transport(e)),
            None => Err(ConnectionError::ConnectionClosed),
        }
    }

    fn expect_typed<T>(
        &self,
        txn: &Transmission,
        expected: &'static str,
    ) -> Result<T, ConnectionError>
    where
        T: StructValue + Default,
    {
        let type_name = txn
            .header
            .type_name
            .as_deref()
            .ok_or(ConnectionError::HeaderMissingTypeName)?;
        if type_name != expected {
            return Err(ConnectionError::UnexpectedMessage {
                expected,
                got: type_name.into(),
            });
        }
        // Re-encode the dynamic payload to bytes, then decode as T.
        // Two passes through the codec — wasteful, but only happens
        // for handshake messages (twice per connection lifetime).
        let bytes = txn.payload.encode();
        decode_typed_from_slice(&bytes).map_err(|e| ConnectionError::DecodeFailure(e.to_string()))
    }

    /// Send a typed payload.
    ///
    /// Bypasses the codec's `Encoder<Transmission>` path — encodes the
    /// frame into the per-connection scratch buffer, copies it into the
    /// [`Framed`]'s internal write buffer, and flushes. Avoids the
    /// wire-bytes round-trip a `Transmission`-based send would require
    /// to build a [`DynamicStruct`] from the typed value.
    pub async fn send_typed<T>(
        &mut self,
        type_name: &str,
        payload: &T,
    ) -> Result<(), ConnectionError>
    where
        T: StructValue,
    {
        let header = DotsHeader {
            type_name: Some(type_name.into()),
            ..Default::default()
        };
        self.scratch.clear();
        encode_typed_transmission_into(&header, payload, &mut self.scratch);

        let buf = self.framed.write_buffer_mut();
        buf.reserve(self.scratch.len());
        buf.put_slice(&self.scratch);

        // Flush the framed sink to push the buffered bytes onto the
        // underlying byte stream.
        SinkExt::<Transmission>::flush(&mut self.framed)
            .await
            .map_err(ConnectionError::Transport)?;
        Ok(())
    }

    /// Send a dynamic transmission through the codec's normal path.
    pub async fn send_dynamic(&mut self, txn: Transmission) -> Result<(), ConnectionError> {
        self.framed
            .send(txn)
            .await
            .map_err(ConnectionError::Transport)?;
        Ok(())
    }

    /// Receive the next transmission, or `None` on stream close.
    pub async fn next(&mut self) -> Option<Result<Transmission, TransportError>> {
        self.framed.next().await
    }

    /// Current connection state. Becomes [`DotsConnectionState::Connected`]
    /// once `establish` returns successfully.
    pub fn state(&self) -> DotsConnectionState {
        self.state
    }

    /// Server name reported in the `DotsMsgHello`.
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    /// Client id assigned by the server in `DotsMsgConnectResponse`.
    pub fn client_id(&self) -> Option<u32> {
        self.client_id
    }

    /// Consume the connection, returning the wrapped stream. Useful
    /// when the caller wants to release the byte stream after the
    /// session ends.
    pub fn into_inner(self) -> S {
        self.framed.into_inner()
    }
}
