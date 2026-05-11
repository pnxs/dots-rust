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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use bytes::BufMut;
use dots_core::{
    EnumDescriptor, PropertySet, Publishable, StructDescriptor, StructValue, decode_typed_from_slice,
    key_set,
};
use dots_model::{
    DotsConnectionState, DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgHello,
    EnumDescriptorData, Registry, StructDescriptorData, Transmission,
    encode_typed_transmission_into, encode_typed_transmission_with_mask_into,
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
    /// A connection method was called from the wrong state — e.g.
    /// `finish_preload` while not in [`DotsConnectionState::EarlySubscribe`].
    InvalidState {
        expected: DotsConnectionState,
        actual: DotsConnectionState,
    },
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
            Self::InvalidState { expected, actual } => write!(
                f,
                "invalid connection state: expected {expected:?}, currently {actual:?}"
            ),
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
    /// Connect over an established byte stream and run the basic DOTS
    /// handshake **without** preload. Equivalent to:
    ///
    /// ```ignore
    /// ConnectionBuilder::new(stream, name, registry).preload(false).connect().await
    /// ```
    ///
    /// On success the returned [`Connection`] is in the
    /// [`DotsConnectionState::Connected`] state — type registration
    /// and cache preload are skipped. For typical clients use
    /// [`ConnectionBuilder`] instead, which handles the full lifecycle.
    ///
    /// If the broker requires authentication and no secret was
    /// configured (via [`ConnectionBuilder::with_auth`]), this returns
    /// [`ConnectionError::AuthenticationNotSupported`]. Use the
    /// builder to opt in to challenge-response.
    pub async fn establish(
        stream: S,
        client_name: &str,
        registry: Arc<Registry>,
    ) -> Result<Self, ConnectionError> {
        ConnectionBuilder::new(stream, client_name, registry)
            .preload(false)
            .connect()
            .await
    }

    /// Construct an empty connection wrapping the given framed stream.
    /// Used internally by [`ConnectionBuilder::connect`].
    fn from_framed(framed: Framed<S, TransmissionCodec>) -> Self {
        Self {
            framed,
            state: DotsConnectionState::Connecting,
            server_name: None,
            client_id: None,
            scratch: Vec::with_capacity(256),
            dispatch: Arc::new(Mutex::new(DispatchState::default())),
        }
    }

    /// Drive the initial Hello → Connect → ConnectResponse exchange.
    /// After this returns, `self.state` is `EarlySubscribe` if the
    /// server agreed to preload (`response.preload == Some(true)`),
    /// otherwise `Connected`.
    async fn run_initial_handshake(
        &mut self,
        client_name: &str,
        request_preload: bool,
        auth_secret: Option<&str>,
    ) -> Result<(), ConnectionError> {
        tracing::debug!(
            client_name,
            request_preload,
            "starting initial handshake"
        );
        let txn = self.read_next().await?;
        let hello: DotsMsgHello = self.expect_typed(&txn, "DotsMsgHello")?;
        tracing::info!(
            server_name = ?hello.server_name,
            auth_required = ?hello.authentication_required,
            "received DotsMsgHello"
        );
        let auth_required = hello.authentication_required == Some(true);
        let auth_challenge = hello.auth_challenge.unwrap_or(0);
        self.server_name = hello.server_name;

        let mut connect = DotsMsgConnect {
            client_name: Some(client_name.into()),
            preload_cache: Some(request_preload),
            ..Default::default()
        };
        if auth_required {
            let Some(secret) = auth_secret else {
                return Err(ConnectionError::AuthenticationNotSupported);
            };
            let cnonce = crate::auth::generate_cnonce();
            let response =
                crate::auth::compute_response(auth_challenge, &cnonce, client_name, secret);
            tracing::debug!("computed auth challenge response");
            connect.auth_challenge_response = Some(response);
            connect.cnonce = Some(cnonce);
        }
        self.send_typed("DotsMsgConnect", &connect).await?;
        tracing::debug!(request_preload, "sent DotsMsgConnect");

        let txn = self.read_next().await?;
        let response: DotsMsgConnectResponse =
            self.expect_typed(&txn, "DotsMsgConnectResponse")?;
        if response.accepted != Some(true) {
            tracing::warn!(
                server_name = ?response.server_name,
                "connection rejected by broker"
            );
            return Err(ConnectionError::ConnectionRejected {
                server_name: response.server_name,
            });
        }
        self.client_id = response.client_id;
        self.state = if response.preload == Some(true) {
            DotsConnectionState::EarlySubscribe
        } else {
            DotsConnectionState::Connected
        };
        tracing::info!(
            client_id = ?self.client_id,
            state = ?self.state,
            "handshake accepted"
        );
        Ok(())
    }

    /// Signal "I'm done publishing descriptors and subscribing", then
    /// drive the cache-preload phase: incoming cached transmissions
    /// flow through the normal subscription dispatch path. Returns
    /// when the server sends `DotsMsgConnectResponse` with
    /// `preload_finished = true`, transitioning to
    /// [`DotsConnectionState::Connected`].
    ///
    /// Errors with [`ConnectionError::InvalidState`] if not currently
    /// in [`DotsConnectionState::EarlySubscribe`] (e.g. preload was
    /// not requested, or this is being called twice).
    pub async fn finish_preload(&mut self) -> Result<(), ConnectionError> {
        if self.state != DotsConnectionState::EarlySubscribe {
            return Err(ConnectionError::InvalidState {
                expected: DotsConnectionState::EarlySubscribe,
                actual: self.state,
            });
        }
        tracing::debug!("signalling preload_client_finished and draining cache");

        let connect = DotsMsgConnect {
            preload_client_finished: Some(true),
            ..Default::default()
        };
        self.send_typed("DotsMsgConnect", &connect).await?;

        // Stream cache transmissions. Cache events have header.from_cache
        // set to a remaining count (0 for the last). The terminator is
        // a DotsMsgConnectResponse with preload_finished = true.
        loop {
            let txn = self.read_next().await?;
            let type_name = txn
                .header
                .type_name
                .as_deref()
                .ok_or(ConnectionError::HeaderMissingTypeName)?;
            if type_name == "DotsMsgConnectResponse" {
                let response: DotsMsgConnectResponse =
                    self.expect_typed(&txn, "DotsMsgConnectResponse")?;
                if response.preload_finished == Some(true) {
                    self.state = DotsConnectionState::Connected;
                    tracing::info!("preload finished, connection in Connected state");
                    return Ok(());
                }
                tracing::debug!("intermediate ConnectResponse during preload");
                continue;
            }
            // Cache event — fan out to subscriptions.
            tracing::trace!(
                type_name,
                from_cache = ?txn.header.from_cache,
                "preload cache event"
            );
            self.dispatch_to_subscribers(&txn);
        }
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
        // dotsd requires `attributes` on every published header — it's
        // the bitmask of payload properties that are valid. The CBOR
        // map is already sparse with the same information, but the
        // header field is mandatory at the protocol level.
        let header = DotsHeader {
            type_name: Some(type_name.into()),
            attributes: Some(payload.valid_set()),
            sender: self.client_id,
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
        T: StructValue + Publishable,
    {
        let type_name = value.descriptor().name;
        self.send_typed(type_name, value).await
    }

    /// Publish a typed value with a property mask. See
    /// [`GuestTransceiver::publish_with_mask`](crate::GuestTransceiver::publish_with_mask)
    /// for the masking semantics.
    pub async fn publish_with_mask<T>(
        &mut self,
        value: &T,
        included: PropertySet,
    ) -> Result<(), ConnectionError>
    where
        T: StructValue + Publishable,
    {
        let type_name = value.descriptor().name;
        let mask = (included | key_set(value)) & value.valid_set();
        let header = DotsHeader {
            type_name: Some(type_name.into()),
            attributes: Some(mask),
            sender: self.client_id,
            ..Default::default()
        };
        self.scratch.clear();
        encode_typed_transmission_with_mask_into(&header, value, mask, &mut self.scratch);

        let buf = self.framed.write_buffer_mut();
        buf.reserve(self.scratch.len());
        buf.put_slice(&self.scratch);

        SinkExt::<Transmission>::flush(&mut self.framed)
            .await
            .map_err(ConnectionError::Transport)?;
        Ok(())
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
        dispatch_external(&self.dispatch, txn);
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
        let id = self
            .dispatch
            .lock()
            .expect("dispatch mutex poisoned")
            .register(type_name.clone(), Box::new(entry));
        Subscription {
            rx,
            type_name,
            id,
            dispatch: Arc::downgrade(&self.dispatch),
            leaver: None,
            _phantom: PhantomData,
        }
    }

    /// Build a typed local cache mirror for `T`. The returned
    /// [`Container<T>`] receives the same dispatched transmissions as
    /// any [`Subscription<T>`] and updates its keyed map in place —
    /// `create` / `update` / `remove` semantics are derived from
    /// `header.remove_obj` and prior contents.
    ///
    /// Like [`subscribe`](Self::subscribe), takes `&self` so it can
    /// be called from within `select!` arms holding `&mut self`.
    pub fn container<T>(&self) -> crate::Container<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        crate::container::make_container(&self.dispatch)
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

    /// Crate-internal: a handle on the dispatch state, shared with
    /// any [`Subscription`] / [`Container`] / [`crate::App`]
    /// callbacks attached to this connection.
    pub(crate) fn dispatch_handle(&self) -> Arc<Mutex<DispatchState>> {
        self.dispatch.clone()
    }

    /// Crate-internal: consume the connection, returning the framed
    /// stream and the shared dispatch state. [`crate::App`] uses this
    /// to take over the read/write loop after the handshake.
    pub(crate) fn into_parts(
        self,
    ) -> (Framed<S, TransmissionCodec>, Arc<Mutex<DispatchState>>) {
        (self.framed, self.dispatch)
    }

    /// Crate-internal: dispatch a transmission to subscribers from
    /// outside the connection's own `next()` (e.g. the App's read
    /// loop after taking the framed via `into_parts`).
    pub(crate) fn dispatch_external(
        dispatch: &Arc<Mutex<DispatchState>>,
        txn: &Transmission,
    ) {
        dispatch_external(dispatch, txn);
    }
}

/// Dispatch a transmission to all subscribers of its `type_name`,
/// **without** holding the dispatch mutex while handlers run.
///
/// Why the gymnastics: handlers may register or drop subscriptions
/// (e.g. `subscribe_all_types` installs a `subscribe_dynamic` from
/// inside a `subscribe_new_struct_type` callback). Both operations
/// need the dispatch mutex; if we kept it locked across the handler
/// call we'd deadlock. The take-out-put-back pattern lets handlers
/// freely lock dispatch.
///
/// New subscribers registered during a dispatch don't see this
/// transmission (they're put back behind the entries we just
/// processed and only see future events). Subscribers that
/// `drop()` themselves from inside a handler run their unregister
/// while we hold no lock — but their type's entries vec is
/// momentarily missing from state, so the unregister silently
/// no-ops. The retain_mut return value is the canonical way to
/// drop self-from-dispatch and is unaffected.
fn dispatch_external(dispatch: &Arc<Mutex<DispatchState>>, txn: &Transmission) {
    let Some(type_name) = txn.header.type_name.as_deref() else {
        return;
    };

    let taken = {
        let mut state = dispatch.lock().expect("dispatch mutex poisoned");
        state.entries.remove(type_name)
    };
    let Some(mut entries) = taken else {
        return;
    };

    entries.retain_mut(|(_, entry)| entry.dispatch(txn).unwrap_or(true));

    let mut state = dispatch.lock().expect("dispatch mutex poisoned");
    let slot = state.entries.entry(type_name.to_string()).or_default();
    let added_during_dispatch = std::mem::take(slot);
    *slot = entries;
    slot.extend(added_during_dispatch);
}

// ===== Pub/sub: Event, Subscription, dispatch =====

/// One typed observation: the original [`DotsHeader`] plus the decoded
/// payload value.
#[derive(Debug, Clone)]
pub struct Event<T> {
    pub header: DotsHeader,
    pub value: T,
}

/// RAII guard run when a subscription handle is dropped, used by the
/// guest-side transceiver to decrement its per-type subscriber count
/// and publish `DotsMember(Leave)` when it goes to zero.
///
/// Carries a boxed `FnOnce` so that no module needs a direct
/// dependency on `GuestTransceiver` — the guest layer constructs the
/// leaver with a closure that captures a `Weak<GuestTransceiver>`.
pub struct GroupLeaver {
    on_drop: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
}

impl GroupLeaver {
    /// Build a leaver from a closure. The closure runs once, on drop.
    pub fn new(on_drop: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            on_drop: Some(Box::new(on_drop)),
        }
    }
}

impl Drop for GroupLeaver {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
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
    /// Set when this subscription was created via
    /// [`crate::GuestTransceiver`]; runs on drop to decrement the
    /// per-type subscriber count and publish `DotsMember(Leave)` if
    /// this was the last subscriber. Raw `Connection::subscribe` paths
    /// leave it `None` since they don't auto-join groups.
    leaver: Option<GroupLeaver>,
    _phantom: PhantomData<T>,
}

impl<T> Subscription<T> {
    /// Receive the next event, or `None` if the connection has dropped
    /// the subscription (e.g. closed). Convenience over the
    /// [`Stream`] impl for callers not using `StreamExt`.
    pub async fn recv(&mut self) -> Option<Event<T>> {
        self.rx.recv().await
    }

    pub(crate) fn from_parts(
        rx: mpsc::UnboundedReceiver<Event<T>>,
        type_name: String,
        id: u64,
        dispatch: Weak<Mutex<DispatchState>>,
    ) -> Self {
        Self {
            rx,
            type_name,
            id,
            dispatch,
            leaver: None,
            _phantom: PhantomData,
        }
    }

    /// Attach a leaver — called by `GuestTransceiver`'s
    /// subscription-creating methods after `from_parts` so that
    /// dropping this subscription publishes `DotsMember(Leave)` once
    /// the per-type subscriber count drops to zero.
    pub(crate) fn set_leaver(&mut self, leaver: GroupLeaver) {
        self.leaver = Some(leaver);
    }

    /// Try to receive a queued event without waiting. Returns
    /// `Err(_)` if the channel is empty (or disconnected) — useful
    /// for draining the cache events that arrived during
    /// [`Connection::finish_preload`].
    pub fn try_recv(&mut self) -> Result<Event<T>, mpsc::error::TryRecvError> {
        self.rx.try_recv()
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
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .unregister(&self.type_name, self.id);
        }
    }
}

/// One subscriber entry: its id (used for removal) and the boxed
/// type-erased dispatch implementation.
type DispatchEntries = Vec<(u64, Box<dyn DispatchEntry>)>;

/// Type-erased dispatch table: map from wire `type_name` to the list of
/// active subscribers for that type.
#[derive(Default)]
pub(crate) struct DispatchState {
    pub(crate) next_id: u64,
    pub(crate) entries: HashMap<String, DispatchEntries>,
}

impl DispatchState {
    /// Insert a new entry under `type_name`, allocating a fresh id.
    /// Returns the id so the caller can remove it later.
    pub(crate) fn register(
        &mut self,
        type_name: String,
        entry: Box<dyn DispatchEntry>,
    ) -> u64 {
        self.next_id += 1;
        let id = self.next_id;
        self.entries.entry(type_name).or_default().push((id, entry));
        id
    }

    /// Remove an entry by `(type_name, id)`. No-op if it isn't present.
    pub(crate) fn unregister(&mut self, type_name: &str, id: u64) {
        if let Some(entries) = self.entries.get_mut(type_name) {
            entries.retain(|(eid, _)| *eid != id);
            if entries.is_empty() {
                self.entries.remove(type_name);
            }
        }
    }
}

impl core::fmt::Debug for DispatchState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DispatchState")
            .field("subscriptions", &self.entries.len())
            .finish_non_exhaustive()
    }
}

/// Object-safe view of a single typed dispatch entry. The connection's
/// read loop calls [`dispatch`](DispatchEntry::dispatch) for each
/// matching transmission; impls decode the payload and route it to
/// either a subscriber's channel ([`TypedDispatchEntry`]) or a
/// container's local mirror.
pub(crate) trait DispatchEntry: Send {
    /// Decode and forward the transmission. Returns `Ok(false)` if the
    /// entry should be removed (e.g. the subscriber's receiver was
    /// dropped); `Ok(true)` if it should be kept.
    fn dispatch(&mut self, txn: &Transmission) -> Result<bool, dots_core::DecodeError>;
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
}

// ===== ConnectionBuilder =====

/// Build a [`Connection`] with type registration and optional cache preload.
///
/// Typical full lifecycle:
///
/// ```ignore
/// let mut conn = ConnectionBuilder::new(stream, "my-client", registry)
///     .publishes::<MyType1>()
///     .publishes::<MyType2>()
///     .preload(true)
///     .connect().await?;
/// // conn.state() == DotsConnectionState::EarlySubscribe
///
/// let sub = conn.subscribe::<MyType2>();
/// conn.finish_preload().await?;
/// // Cache events for MyType2 have been dispatched into `sub`.
/// // conn.state() == DotsConnectionState::Connected.
/// ```
///
/// `connect()` runs the initial handshake and publishes a
/// `StructDescriptorData` (or `EnumDescriptorData`) for every type
/// declared via the `publishes_*` methods, so the broker learns the
/// shape of each user-defined type before any value of it flows.
pub struct ConnectionBuilder<S> {
    stream: S,
    client_name: String,
    registry: Arc<Registry>,
    preload: bool,
    pending: Vec<PendingDescriptor>,
    /// Shared secret for SHA-256 challenge-response authentication.
    /// `None` means the client will reject any auth-required Hello.
    auth_secret: Option<String>,
}

enum PendingDescriptor {
    Struct(&'static StructDescriptor),
    Enum(&'static EnumDescriptor),
}

impl<S> ConnectionBuilder<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(stream: S, client_name: impl Into<String>, registry: Arc<Registry>) -> Self {
        Self {
            stream,
            client_name: client_name.into(),
            registry,
            preload: true,
            pending: Vec::new(),
            auth_secret: None,
        }
    }

    /// Configure a shared secret for SHA-256 challenge-response
    /// authentication. If the broker's `DotsMsgHello` indicates auth is
    /// required, the client computes the digest as
    /// `SHA256(SHA256(client_name || "::" || secret) || ":" ||
    ///  auth_challenge_le || ":" || cnonce)` and sends it in
    /// `DotsMsgConnect.auth_challenge_response`. Wire-compatible with
    /// dots-cpp's `LegacyAuthManager`.
    pub fn with_auth(mut self, secret: impl Into<String>) -> Self {
        self.auth_secret = Some(secret.into());
        self
    }

    /// Whether to request the broker's cache preload during connect.
    /// Default: `true`. Setting to `false` skips the
    /// [`DotsConnectionState::EarlySubscribe`] phase — `connect()`
    /// returns directly in [`DotsConnectionState::Connected`] and
    /// [`Connection::finish_preload`] must not be called.
    pub fn preload(mut self, on: bool) -> Self {
        self.preload = on;
        self
    }

    /// Publish a struct type's descriptor during connect, telling the
    /// broker about the shape of values it'll route on this client's
    /// behalf. Convenience over [`publishes_struct`](Self::publishes_struct)
    /// for types that implement [`StructValue`].
    pub fn publishes<T>(self) -> Self
    where
        T: StructValue,
    {
        self.publishes_struct(T::type_descriptor())
    }

    /// Publish a struct type's descriptor by descriptor reference.
    pub fn publishes_struct(mut self, descriptor: &'static StructDescriptor) -> Self {
        self.pending.push(PendingDescriptor::Struct(descriptor));
        self
    }

    /// Publish an enum type's descriptor by descriptor reference.
    pub fn publishes_enum(mut self, descriptor: &'static EnumDescriptor) -> Self {
        self.pending.push(PendingDescriptor::Enum(descriptor));
        self
    }

    /// Run the handshake, publish all queued type descriptors, and
    /// return a [`Connection`] in the appropriate state (see
    /// [`preload`](Self::preload)).
    pub async fn connect(self) -> Result<Connection<S>, ConnectionError> {
        let codec = TransmissionCodec::new(self.registry);
        let framed = Framed::new(self.stream, codec);
        let mut conn = Connection::from_framed(framed);

        conn.run_initial_handshake(
            &self.client_name,
            self.preload,
            self.auth_secret.as_deref(),
        )
        .await?;

        // After the initial Connect/ConnectResponse, publish each
        // declared type's descriptor data so the broker can route /
        // route subscriptions for them. We do this regardless of
        // preload — even non-preload clients still need to register
        // their types before publishing values.
        //
        // Send enums BEFORE structs: the broker's
        // `build_dynamic_struct` resolves nested type references via
        // its registry as it parses each struct descriptor, so any
        // enum referenced as a struct field must already be there.
        for pending in &self.pending {
            if let PendingDescriptor::Enum(d) = pending {
                let data = EnumDescriptorData::from_static(d);
                conn.send_typed("EnumDescriptorData", &data).await?;
            }
        }
        for pending in &self.pending {
            if let PendingDescriptor::Struct(d) = pending {
                let data = StructDescriptorData::from_static(d);
                conn.send_typed("StructDescriptorData", &data).await?;
            }
        }

        Ok(conn)
    }
}
