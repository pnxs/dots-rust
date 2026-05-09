//! Client-side connection state machine.
//!
//! [`Connection<S>`] wraps a [`Framed<S, TransmissionCodec>`] and drives
//! the DOTS handshake (Hello → Connect → ConnectResponse). Once
//! established, it exposes:
//!
//! - [`publish`](Connection::publish) — send a typed value
//! - [`subscribe`](Connection::subscribe) — register a typed subscription
//!   that yields [`Event<T>`] values via a `Stream`
//! - [`next`](Connection::next) — receive the next [`Transmission`] in
//!   raw form, while also dispatching to any matching subscriptions as
//!   a side effect
//!
//! Generic over `S: AsyncRead + AsyncWrite + Unpin`, so it works over
//! TCP, Unix domain sockets, or any in-memory pipe like
//! [`tokio::io::duplex`] for testing.

use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use bytes::BufMut;
use dots_core::{StructValue, decode_typed_from_slice};
use dots_model::{
    DotsConnectionState, DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgHello,
    Registry, Transmission, encode_typed_transmission_into,
};
use futures_util::{SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
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
    /// Type-erased dispatch table for subscriptions. Behind a `Mutex` so
    /// subscribers can be added/removed via `&self` (e.g. while the
    /// owner is in the middle of an `&mut self` `next()` call), and so
    /// the connection stays `Send` for multi-threaded runtimes.
    dispatch: Arc<Mutex<DispatchState>>,
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
            dispatch: Arc::new(Mutex::new(DispatchState::default())),
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

    /// Publish a typed value. The wire `type_name` comes from
    /// `T::DESCRIPTOR.name`, so this is the recommended high-level
    /// shortcut over [`send_typed`](Self::send_typed) when the value's
    /// own descriptor name is what should appear in the header.
    pub async fn publish<T>(&mut self, value: &T) -> Result<(), ConnectionError>
    where
        T: StructValue,
    {
        let type_name = value.descriptor().name;
        self.send_typed(type_name, value).await
    }

    /// Receive the next transmission, or `None` on stream close.
    ///
    /// As a side effect, dispatches the transmission to any matching
    /// subscriptions (typed `Event<T>` handlers registered via
    /// [`subscribe`](Self::subscribe)). The raw `Transmission` is also
    /// returned so callers can additionally inspect it.
    pub async fn next(&mut self) -> Option<Result<Transmission, TransportError>> {
        let result = self.framed.next().await;
        if let Some(Ok(ref txn)) = result {
            self.dispatch_to_subscribers(txn);
        }
        result
    }

    fn dispatch_to_subscribers(&self, txn: &Transmission) {
        let Some(type_name) = txn.header.type_name.as_deref() else {
            return;
        };
        let mut state = self.dispatch.lock().expect("dispatch mutex poisoned");
        if let Some(entries) = state.entries.get_mut(type_name) {
            // Decode failure is per-event — keep the subscription so a
            // malformed transmission doesn't kill an otherwise-healthy
            // subscriber.
            entries.retain_mut(|(_, entry)| entry.dispatch(txn).unwrap_or(true));
        }
    }

    /// Subscribe to typed events for `T`.
    ///
    /// Returns a [`Subscription<T>`] that implements `Stream<Item =
    /// Event<T>>`. Each transmission whose `header.type_name` matches
    /// `T::DESCRIPTOR.name` will be decoded and pushed to the
    /// subscription's channel as the connection drives reads via
    /// [`next`](Self::next).
    ///
    /// Dropping the subscription removes its dispatch entry on the next
    /// matching transmission (or sooner, when the subscription's
    /// `Drop` runs). Multiple subscriptions to the same type are
    /// supported; each gets its own copy of the event.
    ///
    /// Takes `&self` so subscriptions can be created from within
    /// `tokio::select!` arms that already hold `&mut self` for
    /// `next`/`publish`.
    pub fn subscribe<T>(&self) -> Subscription<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let entry: TypedDispatchEntry<T> = TypedDispatchEntry {
            sender: tx,
            _phantom: PhantomData,
        };
        let type_name = T::type_descriptor().name.to_string();
        let id = {
            let mut state = self.dispatch.lock().expect("dispatch mutex poisoned");
            state.next_id += 1;
            let id = state.next_id;
            state
                .entries
                .entry(type_name.clone())
                .or_default()
                .push((id, Box::new(entry)));
            id
        };
        Subscription {
            rx,
            type_name,
            id,
            dispatch: Arc::downgrade(&self.dispatch),
            _phantom: PhantomData,
        }
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

// ===== Pub/sub: Event, Subscription, dispatch =====

/// One typed observation: the original [`DotsHeader`] plus the decoded
/// payload value.
#[derive(Debug, Clone)]
pub struct Event<T> {
    pub header: DotsHeader,
    pub value: T,
}

/// RAII handle to a per-type subscription. Implements
/// `Stream<Item = Event<T>>`; dropping it removes the dispatch entry
/// (the connection notices on the next matching transmission, or the
/// `Drop` impl removes it eagerly if the connection is still live).
pub struct Subscription<T> {
    rx: mpsc::UnboundedReceiver<Event<T>>,
    type_name: String,
    id: u64,
    dispatch: Weak<Mutex<DispatchState>>,
    _phantom: PhantomData<T>,
}

impl<T> Subscription<T> {
    /// Receive the next event, or `None` if the connection has dropped
    /// the subscription (e.g. closed). Convenience over the
    /// [`Stream`] impl for callers not using `StreamExt`.
    pub async fn recv(&mut self) -> Option<Event<T>> {
        self.rx.recv().await
    }
}

impl<T: Unpin> Stream for Subscription<T> {
    type Item = Event<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl<T> Drop for Subscription<T> {
    fn drop(&mut self) {
        if let Some(dispatch) = self.dispatch.upgrade() {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            if let Some(entries) = state.entries.get_mut(&self.type_name) {
                entries.retain(|(id, _)| *id != self.id);
                if entries.is_empty() {
                    state.entries.remove(&self.type_name);
                }
            }
        }
    }
}

/// One subscriber entry: its id (used for removal) and the boxed
/// type-erased dispatch implementation.
type DispatchEntries = Vec<(u64, Box<dyn DispatchEntry>)>;

/// Type-erased dispatch table: map from wire `type_name` to the list of
/// active subscribers for that type.
#[derive(Default)]
struct DispatchState {
    next_id: u64,
    entries: HashMap<String, DispatchEntries>,
}

impl core::fmt::Debug for DispatchState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DispatchState")
            .field("subscriptions", &self.entries.len())
            .finish_non_exhaustive()
    }
}

/// Object-safe view of a single typed subscriber. The connection's
/// dispatch loop calls [`dispatch`](DispatchEntry::dispatch) for each
/// matching transmission; the impl decodes the payload as its `T` and
/// pushes the resulting [`Event<T>`] onto the subscriber's channel.
trait DispatchEntry: Send {
    /// Decode and forward the transmission. Returns `Ok(false)` if the
    /// subscriber's receiver has been dropped (so the entry should be
    /// removed); `Ok(true)` if the entry should be kept.
    fn dispatch(&mut self, txn: &Transmission) -> Result<bool, dots_core::DecodeError>;

    /// Type-erasure escape hatch (currently unused by the dispatch
    /// path itself; reserved for future introspection).
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn Any;
}

struct TypedDispatchEntry<T> {
    sender: mpsc::UnboundedSender<Event<T>>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> DispatchEntry for TypedDispatchEntry<T>
where
    T: StructValue + Default + Send + 'static,
{
    fn dispatch(&mut self, txn: &Transmission) -> Result<bool, dots_core::DecodeError> {
        if self.sender.is_closed() {
            return Ok(false);
        }
        let bytes = txn.payload.encode();
        let value: T = decode_typed_from_slice(&bytes)?;
        let event = Event {
            header: txn.header.clone(),
            value,
        };
        // Send failure means the receiver was dropped between the
        // is_closed check and the send — same outcome, drop the entry.
        Ok(self.sender.send(event).is_ok())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
