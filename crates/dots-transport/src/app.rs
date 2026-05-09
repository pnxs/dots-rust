//! High-level `App` API — callback-based subscriptions, automatic
//! type registration, and a single `run()` event loop. The shape
//! mirrors C++ DOTS's `dots::Application` so apps porting from C++
//! find familiar idioms.
//!
//! ```ignore
//! let app = App::connect("127.0.0.1:11235", "my-name").await?;
//!
//! let _sub = app.subscribe::<Pinger>(|event| {
//!     println!("got Pinger from {:?}", event.header.sender);
//! });
//!
//! let client = app.client();
//! tokio::spawn(async move {
//!     loop {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//!         client.publish(&Pinger { id: Some(1), ..Default::default() }).ok();
//!     }
//! });
//!
//! app.run_until_signal().await?;
//! ```
//!
//! `subscribe` / `publish` / `container` auto-register the type's
//! descriptor with the broker before preload finishes. Callbacks are
//! synchronous (called from the read loop) — for in-callback
//! publishes use the [`Client`] handle, which sends through an
//! internal channel rather than directly touching the framed sink.

use std::any::Any;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use dots_core::{
    EnumDescriptor, StructDescriptor, StructValue, Timepoint, decode_typed_from_slice,
};
use dots_model::{
    DotsHeader, DotsMember, DotsMemberEvent, EnumDescriptorData, Registry, StructDescriptorData,
    encode_typed_transmission_into,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::connection::{Connection, ConnectionBuilder, ConnectionError, DispatchEntry, DispatchState, Event};
use crate::container::Container;
use crate::TransportError;

/// Errors produced by the [`App`] lifecycle.
#[derive(Debug)]
pub enum AppError {
    Connection(ConnectionError),
    Transport(TransportError),
    Io(std::io::Error),
}

impl core::fmt::Display for AppError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Connection(e) => write!(f, "{e}"),
            Self::Transport(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connection(e) => Some(e),
            Self::Transport(e) => Some(e),
            Self::Io(e) => Some(e),
        }
    }
}

impl From<ConnectionError> for AppError {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}
impl From<TransportError> for AppError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}
impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Returned when [`Client::publish`] is called after the connection
/// has shut down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientClosed;

impl core::fmt::Display for ClientClosed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("App run loop has shut down — outbound channel closed")
    }
}

impl std::error::Error for ClientClosed {}

/// Shared state between [`App`] and any [`Client`] handles plus
/// callback dispatch entries.
struct AppState {
    /// Shared with the underlying `Connection`'s dispatch table —
    /// adding a callback here is observable from the same dispatch
    /// loop that drives the existing `Subscription` and `Container`
    /// types.
    dispatch: Arc<Mutex<DispatchState>>,
    /// Registry shared with the framed codec, used to wrap typed
    /// payloads in dynamic transmissions for the relay path.
    #[allow(dead_code)]
    registry: Arc<Registry>,
    /// Pending struct descriptors to publish before preload finishes.
    pending_structs: Mutex<HashSet<DescriptorPtr<StructDescriptor>>>,
    /// Pending enum descriptors to publish before preload finishes.
    pending_enums: Mutex<HashSet<DescriptorPtr<EnumDescriptor>>>,
    /// Group names for which we've already published a join — avoids
    /// duplicate `DotsMember(join)` publishes when subscribe<T> /
    /// container<T> is called multiple times for the same `T`.
    joined_groups: Mutex<HashSet<String>>,
    /// Outbound queue: pre-framed transmission bytes that the run
    /// loop drains into the framed sink.
    outbound_tx: mpsc::UnboundedSender<Vec<u8>>,
    /// Set by `App::exit()` / `Client::exit()` to break the loop.
    exit_flag: AtomicBool,
    /// Client id assigned by the broker; populated after handshake
    /// and used to fill `header.sender` on outbound publishes.
    client_id: Mutex<Option<u32>>,
}

/// Wraps a `&'static T` with pointer-equality semantics for HashSet.
struct DescriptorPtr<T: 'static>(&'static T);

impl<T> Clone for DescriptorPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for DescriptorPtr<T> {}
impl<T> PartialEq for DescriptorPtr<T> {
    fn eq(&self, other: &Self) -> bool {
        core::ptr::eq(self.0, other.0)
    }
}
impl<T> Eq for DescriptorPtr<T> {}
impl<T> std::hash::Hash for DescriptorPtr<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (self.0 as *const T as usize).hash(state);
    }
}

/// High-level DOTS client.
///
/// Owns the [`Connection`] and the read/write loop. Subscribers and
/// containers register through `&self`; publishes go through a
/// [`Client`] handle that's `Clone` + `Send`.
pub struct App {
    state: Arc<AppState>,
    /// Taken when `run()` is called.
    conn: Option<Connection<TcpStream>>,
    /// Drained inside `run()`.
    outbound_rx: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl App {
    /// Connect to a DOTS broker over TCP and run the handshake (with
    /// `preload = true`). Returns an `App` ready for the user to add
    /// subscriptions, then `run()`.
    pub async fn connect(addr: &str, client_name: &str) -> Result<App, AppError> {
        Self::connect_inner(addr, client_name, None).await
    }

    /// Same as [`connect`](Self::connect) but supplies a shared secret
    /// for SHA-256 challenge-response authentication. Use this for
    /// brokers that have `DotsAuthentication` rules requiring auth.
    pub async fn connect_with_auth(
        addr: &str,
        client_name: &str,
        secret: &str,
    ) -> Result<App, AppError> {
        Self::connect_inner(addr, client_name, Some(secret)).await
    }

    async fn connect_inner(
        addr: &str,
        client_name: &str,
        secret: Option<&str>,
    ) -> Result<App, AppError> {
        tracing::info!(
            addr,
            client_name,
            with_auth = secret.is_some(),
            "connecting to dotsd"
        );
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let registry = Arc::new(dots_model::registry_with_internal_types());
        let mut builder =
            ConnectionBuilder::new(stream, client_name, registry.clone()).preload(true);
        if let Some(s) = secret {
            builder = builder.with_auth(s);
        }
        let conn = builder.connect().await?;

        let dispatch = conn.dispatch_handle();
        let (tx, rx) = mpsc::unbounded_channel();
        let state = Arc::new(AppState {
            dispatch,
            registry,
            pending_structs: Mutex::new(HashSet::new()),
            pending_enums: Mutex::new(HashSet::new()),
            joined_groups: Mutex::new(HashSet::new()),
            outbound_tx: tx,
            exit_flag: AtomicBool::new(false),
            client_id: Mutex::new(conn.client_id()),
        });
        Ok(App {
            state,
            conn: Some(conn),
            outbound_rx: Some(rx),
        })
    }

    /// Create a [`Client`] handle for use inside callback handlers
    /// or in spawned tasks. Cheap; just an `Arc` clone.
    pub fn client(&self) -> Client {
        Client {
            state: self.state.clone(),
        }
    }

    /// Subscribe to typed events with a synchronous callback handler.
    /// The callback fires from the [`run`](Self::run) read loop on
    /// every transmission whose `header.type_name` matches `T`.
    ///
    /// Drop the returned [`SubscriptionHandle`] to detach the handler;
    /// call [`SubscriptionHandle::discard`] to keep it alive for the
    /// rest of the connection.
    pub fn subscribe<T>(
        &self,
        handler: impl FnMut(&Event<T>) + Send + 'static,
    ) -> SubscriptionHandle
    where
        T: StructValue + Default + Send + 'static,
    {
        self.state.register_struct::<T>();
        self.state.join_group(T::type_descriptor().name);
        register_callback::<T, _>(&self.state.dispatch, handler)
    }

    /// Subscribe to typed events as an async [`Subscription<T>`]
    /// stream. Useful when the consumer is itself an async state
    /// machine.
    pub fn subscribe_stream<T>(&self) -> crate::Subscription<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        self.state.register_struct::<T>();
        self.state.join_group(T::type_descriptor().name);
        // Defer to the connection's existing subscribe path — the
        // dispatch handle is the same `Arc` we hold here.
        connection_subscribe::<T>(&self.state.dispatch)
    }

    /// Build a typed [`Container<T>`] for `T`. Auto-registers `T`
    /// just like [`subscribe`](Self::subscribe).
    pub fn container<T>(&self) -> Container<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        self.state.register_struct::<T>();
        self.state.join_group(T::type_descriptor().name);
        crate::container::make_container(&self.state.dispatch)
    }

    /// Pre-register an enum type. Auto-registration is implicit for
    /// any DOTS struct used in `subscribe`/`publish`/`container`,
    /// but enums embedded in struct fields don't trigger that path.
    /// Call this once for any standalone enum the broker should know
    /// about.
    pub fn register_enum(&self, descriptor: &'static EnumDescriptor) {
        self.state
            .pending_enums
            .lock()
            .expect("pending mutex poisoned")
            .insert(DescriptorPtr(descriptor));
        self.state.registry.register_enum_static(descriptor);
    }

    /// Publish a typed value. Synchronous — bytes are pushed onto
    /// the outbound channel and sent by the `run()` loop.
    pub fn publish<T>(&self, value: &T) -> Result<(), ClientClosed>
    where
        T: StructValue,
    {
        self.state.register_struct_descriptor(T::type_descriptor());
        self.state.publish_typed(value)
    }

    /// Signal the `run()` loop to exit at the next iteration.
    pub fn exit(&self) {
        self.state.exit_flag.store(true, Ordering::Release);
    }

    /// Run the read/write event loop until [`exit`](Self::exit) is
    /// called or the connection closes.
    ///
    /// First publishes every queued type descriptor (via auto-
    /// registration tracking), then calls
    /// [`Connection::finish_preload`], then enters the main
    /// dispatch loop.
    pub async fn run(mut self) -> Result<(), AppError> {
        let mut conn = self.conn.take().expect("App::run called twice");
        let mut outbound_rx = self.outbound_rx.take().expect("App::run called twice");

        // Phase 1: publish all queued type descriptors.
        let pending_structs: Vec<&'static StructDescriptor> = self
            .state
            .pending_structs
            .lock()
            .expect("pending mutex poisoned")
            .iter()
            .map(|d| d.0)
            .collect();
        let pending_enums: Vec<&'static EnumDescriptor> = self
            .state
            .pending_enums
            .lock()
            .expect("pending mutex poisoned")
            .iter()
            .map(|d| d.0)
            .collect();
        tracing::debug!(
            structs = pending_structs.len(),
            enums = pending_enums.len(),
            "publishing pending type descriptors"
        );
        for d in pending_structs {
            let data = StructDescriptorData::from_static(d);
            conn.send_typed("StructDescriptorData", &data).await?;
        }
        for d in pending_enums {
            let data = EnumDescriptorData::from_static(d);
            conn.send_typed("EnumDescriptorData", &data).await?;
        }

        // Phase 2: finish preload (cache events flow through
        // dispatch into any installed subscriptions/containers).
        if conn.state() == dots_model::DotsConnectionState::EarlySubscribe {
            conn.finish_preload().await?;
        }

        // Phase 3: take the framed apart so we can interleave reads
        // and writes via futures::StreamExt::split.
        let (framed, dispatch) = conn.into_parts();
        let (mut sink, mut stream) = framed.split();

        tracing::debug!("entering App::run main dispatch loop");
        loop {
            if self.state.exit_flag.load(Ordering::Acquire) {
                tracing::info!("exit flag set, leaving run loop");
                break;
            }
            tokio::select! {
                biased;
                maybe_in = stream.next() => match maybe_in {
                    Some(Ok(txn)) => {
                        tracing::trace!(
                            type_name = ?txn.header.type_name,
                            sender = ?txn.header.sender,
                            "dispatching incoming transmission"
                        );
                        Connection::<TcpStream>::dispatch_external(&dispatch, &txn);
                    }
                    Some(Err(e)) => {
                        tracing::error!(error = %e, "transport error in run loop");
                        return Err(e.into());
                    }
                    None => {
                        tracing::info!("server closed connection");
                        break;
                    }
                },
                maybe_out = outbound_rx.recv() => match maybe_out {
                    Some(bytes) => {
                        SinkExt::<Vec<u8>>::send(&mut sink, bytes).await?;
                    }
                    None => {
                        tracing::debug!("outbound channel closed");
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Same as [`run`](Self::run) but also installs a Ctrl-C handler
    /// that calls [`exit`](Self::exit) on the first interrupt.
    pub async fn run_until_signal(self) -> Result<(), AppError> {
        let state = self.state.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                state.exit_flag.store(true, Ordering::Release);
            }
        });
        self.run().await
    }
}

/// Cheap, cloneable handle for use inside callback handlers and
/// spawned tasks. `publish` queues bytes onto the outbound channel
/// drained by the [`App::run`] loop.
#[derive(Clone)]
pub struct Client {
    state: Arc<AppState>,
}

impl Client {
    /// Publish a typed value. Synchronous (returns once bytes are
    /// queued — does not wait for the broker to acknowledge).
    pub fn publish<T>(&self, value: &T) -> Result<(), ClientClosed>
    where
        T: StructValue,
    {
        self.state.register_struct_descriptor(T::type_descriptor());
        self.state.publish_typed(value)
    }

    /// Subscribe with a callback. Same shape as [`App::subscribe`].
    pub fn subscribe<T>(
        &self,
        handler: impl FnMut(&Event<T>) + Send + 'static,
    ) -> SubscriptionHandle
    where
        T: StructValue + Default + Send + 'static,
    {
        self.state.register_struct::<T>();
        self.state.join_group(T::type_descriptor().name);
        register_callback::<T, _>(&self.state.dispatch, handler)
    }

    /// Subscribe as an async stream. Same shape as
    /// [`App::subscribe_stream`].
    pub fn subscribe_stream<T>(&self) -> crate::Subscription<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        self.state.register_struct::<T>();
        self.state.join_group(T::type_descriptor().name);
        connection_subscribe::<T>(&self.state.dispatch)
    }

    /// Get a typed local cache mirror. Same shape as [`App::container`].
    pub fn container<T>(&self) -> Container<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        self.state.register_struct::<T>();
        self.state.join_group(T::type_descriptor().name);
        crate::container::make_container(&self.state.dispatch)
    }

    /// Signal the App's run loop to exit.
    pub fn exit(&self) {
        self.state.exit_flag.store(true, Ordering::Release);
    }
}

impl AppState {
    fn register_struct<T: StructValue + 'static>(&self) {
        self.register_struct_descriptor(T::type_descriptor());
    }

    fn register_struct_descriptor(&self, descriptor: &'static StructDescriptor) {
        // Two registrations: (a) queue the descriptor for publishing
        // to the broker before preload finishes, and (b) tell the
        // codec's runtime registry about T so incoming transmissions
        // of this type can be decoded.
        self.pending_structs
            .lock()
            .expect("pending mutex poisoned")
            .insert(DescriptorPtr(descriptor));
        self.registry.register_struct_static(descriptor);
    }

    /// Mark the named group as one we want events from, and publish a
    /// `DotsMember { group_name, event: Join }` to dotsd if we haven't
    /// already. This is what makes the broker route transmissions of
    /// the type to us; just publishing the type's descriptor is not
    /// enough.
    fn join_group(&self, group_name: &str) {
        let already = !self
            .joined_groups
            .lock()
            .expect("joined_groups mutex poisoned")
            .insert(group_name.to_string());
        if already {
            return;
        }
        let member = DotsMember {
            group_name: Some(group_name.into()),
            event: Some(DotsMemberEvent::Join),
            client: *self.client_id.lock().expect("client_id mutex poisoned"),
        };
        // Best-effort: failure here means the connection is shutting
        // down. The publish will be observed via the next operation.
        let _ = self.publish_typed(&member);
    }

    fn publish_typed<T: StructValue>(&self, value: &T) -> Result<(), ClientClosed> {
        let type_name = value.descriptor().name;
        let header = DotsHeader {
            type_name: Some(type_name.into()),
            attributes: Some(value.valid_set().bits()),
            sender: *self.client_id.lock().expect("client_id mutex poisoned"),
            sent_time: Some(now_timepoint()),
            ..Default::default()
        };
        let mut bytes = Vec::with_capacity(64);
        encode_typed_transmission_into(&header, value, &mut bytes);
        self.outbound_tx.send(bytes).map_err(|_| ClientClosed)
    }
}

/// Wall-clock-now as a [`Timepoint`]. Lives here (not in dots-core)
/// because dots-core is `no_std`.
pub fn now_timepoint() -> Timepoint {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    Timepoint(secs)
}

// ===== Callback dispatch entry =====

/// RAII handle to a callback subscription. Drop = unsubscribe;
/// [`discard`](Self::discard) keeps it alive for the connection's
/// lifetime.
pub struct SubscriptionHandle {
    type_name: String,
    id: u64,
    dispatch: Weak<Mutex<DispatchState>>,
}

impl SubscriptionHandle {
    /// Detach this handle from its `Drop` cleanup, leaving the
    /// callback installed for the rest of the connection. Mirrors
    /// C++ DOTS's `Subscription::discard()`.
    pub fn discard(self) {
        core::mem::forget(self);
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        if let Some(dispatch) = self.dispatch.upgrade() {
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .unregister(&self.type_name, self.id);
        }
    }
}

fn register_callback<T, F>(
    dispatch: &Arc<Mutex<DispatchState>>,
    handler: F,
) -> SubscriptionHandle
where
    T: StructValue + Default + Send + 'static,
    F: FnMut(&Event<T>) + Send + 'static,
{
    let entry: CallbackDispatchEntry<T, F> = CallbackDispatchEntry {
        handler,
        _phantom: PhantomData,
    };
    let type_name = T::type_descriptor().name.to_string();
    let id = dispatch
        .lock()
        .expect("dispatch mutex poisoned")
        .register(type_name.clone(), Box::new(entry));
    SubscriptionHandle {
        type_name,
        id,
        dispatch: Arc::downgrade(dispatch),
    }
}

struct CallbackDispatchEntry<T, F> {
    handler: F,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, F> DispatchEntry for CallbackDispatchEntry<T, F>
where
    T: StructValue + Default + Send + 'static,
    F: FnMut(&Event<T>) + Send + 'static,
{
    fn dispatch(
        &mut self,
        txn: &dots_model::Transmission,
    ) -> Result<bool, dots_core::DecodeError> {
        let bytes = txn.payload.encode();
        let value: T = decode_typed_from_slice(&bytes)?;
        let event = Event {
            header: txn.header.clone(),
            value,
        };
        (self.handler)(&event);
        Ok(true)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Internal helper: register a stream subscription against a given
/// dispatch handle. Mirrors `Connection::subscribe<T>`'s body without
/// requiring a `Connection`-typed receiver.
fn connection_subscribe<T>(dispatch: &Arc<Mutex<DispatchState>>) -> crate::Subscription<T>
where
    T: StructValue + Default + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel();
    let entry = StreamDispatchEntry::<T> {
        sender: tx,
        _phantom: PhantomData,
    };
    let type_name = T::type_descriptor().name.to_string();
    let id = dispatch
        .lock()
        .expect("dispatch mutex poisoned")
        .register(type_name.clone(), Box::new(entry));
    crate::Subscription::<T>::from_parts(rx, type_name, id, Arc::downgrade(dispatch))
}

struct StreamDispatchEntry<T> {
    sender: mpsc::UnboundedSender<Event<T>>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> DispatchEntry for StreamDispatchEntry<T>
where
    T: StructValue + Default + Send + 'static,
{
    fn dispatch(
        &mut self,
        txn: &dots_model::Transmission,
    ) -> Result<bool, dots_core::DecodeError> {
        if self.sender.is_closed() {
            return Ok(false);
        }
        let bytes = txn.payload.encode();
        let value: T = decode_typed_from_slice(&bytes)?;
        let event = Event {
            header: txn.header.clone(),
            value,
        };
        Ok(self.sender.send(event).is_ok())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
