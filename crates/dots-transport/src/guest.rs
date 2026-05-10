//! Guest-side transceiver — the broker-facing equivalent of C++
//! `dots::GuestTransceiver`.
//!
//! A [`GuestTransceiver`] owns the shared subscription / dispatch /
//! pending-descriptor state for a guest. A [`GuestDriver<S>`] owns the
//! [`Connection<S>`] and pumps the read/write event loop. The pair is
//! created together by [`GuestTransceiver::from_connection`].
//!
//! This split lets non-TCP carriers (in-memory `tokio::io::duplex`,
//! Unix domain sockets, etc.) reuse the same publish/subscribe surface
//! as the high-level [`crate::App`] — and lets host-side tests run a
//! guest in the same process without networking.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use dots_core::{
    DynamicStruct, DynamicStructDescriptor, EnumDescriptor, PropertySet, Publishable,
    StructDescriptor, StructValue, Timepoint, decode_typed_from_slice, key_set,
};
use dots_model::{
    DotsHeader, DotsMember, DotsMemberEvent, EnumDescriptorData, Registry, StructDescriptorData,
    Transmission, encode_dynamic_transmission_into, encode_typed_transmission_into,
    encode_typed_transmission_with_mask_into,
};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Notify, mpsc};

use crate::connection::{Connection, DispatchEntry, DispatchState, Event, GroupLeaver};
use crate::container::Container;
use crate::error::TransportError;
use crate::ConnectionError;

/// Errors produced by the guest-side I/O loop.
#[derive(Debug)]
pub enum GuestError {
    Connection(ConnectionError),
    Transport(TransportError),
}

impl core::fmt::Display for GuestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Connection(e) => write!(f, "{e}"),
            Self::Transport(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for GuestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connection(e) => Some(e),
            Self::Transport(e) => Some(e),
        }
    }
}

impl From<ConnectionError> for GuestError {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}
impl From<TransportError> for GuestError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}

/// Returned when [`GuestTransceiver::publish`] is called after the
/// driver has shut down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientClosed;

impl core::fmt::Display for ClientClosed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("guest run loop has shut down — outbound channel closed")
    }
}

impl std::error::Error for ClientClosed {}

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

/// Guest-side shared state — subscription dispatch, pending
/// descriptors, group-join tracking, outbound publish queue. Cheap to
/// clone via `Arc<GuestTransceiver>`.
///
/// Created together with a [`GuestDriver`] by
/// [`GuestTransceiver::from_connection`].
pub struct GuestTransceiver {
    self_name: String,
    /// Shared with the [`GuestDriver`]'s `Connection` dispatch table —
    /// adding a callback here is observable from the same loop that
    /// drives [`crate::Subscription`] and [`Container`] entries.
    dispatch: Arc<Mutex<DispatchState>>,
    /// Type registry shared with the framed codec, used to wrap typed
    /// payloads in dynamic transmissions for the relay path.
    registry: Arc<Registry>,
    pending_structs: Mutex<HashSet<DescriptorPtr<StructDescriptor>>>,
    pending_enums: Mutex<HashSet<DescriptorPtr<EnumDescriptor>>>,
    /// Per-group active subscriber count. Incremented on every
    /// subscribe / container creation; decremented on the matching
    /// drop. `DotsMember(Join)` is published when a group's count
    /// transitions 0→1, `DotsMember(Leave)` on 1→0.
    joined_groups: Mutex<HashMap<String, u32>>,
    outbound_tx: mpsc::UnboundedSender<Vec<u8>>,
    exit_flag: AtomicBool,
    /// Notifies the [`GuestDriver`]'s select-loop that the exit flag
    /// has flipped. Without this, `exit()` only sets the flag and
    /// the driver wouldn't notice until the next message arrives on
    /// the wire — leaving callers blocked on `driver.await` for
    /// quiet connections. The driver's loop awaits this in parallel
    /// with stream/outbound so a flag flip wakes it immediately.
    exit_notify: Notify,
    /// Client id assigned by the broker; populated after handshake
    /// and used to fill `header.sender` on outbound publishes.
    client_id: Mutex<Option<u32>>,
}

impl GuestTransceiver {
    /// Build a guest from an established [`Connection`]. Returns the
    /// shared transceiver and a separable I/O driver.
    ///
    /// The driver must be polled (typically via
    /// [`GuestDriver::run`]) to actually exchange traffic with the
    /// broker — until then, publishes will queue and incoming
    /// transmissions will not be observed.
    pub fn from_connection<S>(
        self_name: impl Into<String>,
        registry: Arc<Registry>,
        conn: Connection<S>,
    ) -> (Arc<Self>, GuestDriver<S>)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let dispatch = conn.dispatch_handle();
        let client_id = conn.client_id();
        let (tx, rx) = mpsc::unbounded_channel();
        let transceiver = Arc::new(GuestTransceiver {
            self_name: self_name.into(),
            dispatch,
            registry,
            pending_structs: Mutex::new(HashSet::new()),
            pending_enums: Mutex::new(HashSet::new()),
            joined_groups: Mutex::new(HashMap::new()),
            outbound_tx: tx,
            exit_flag: AtomicBool::new(false),
            exit_notify: Notify::new(),
            client_id: Mutex::new(client_id),
        });
        let driver = GuestDriver {
            transceiver: transceiver.clone(),
            conn: Some(conn),
            outbound_rx: Some(rx),
        };
        (transceiver, driver)
    }

    /// Self-name supplied at construction.
    pub fn self_name(&self) -> &str {
        &self.self_name
    }

    /// Client id assigned by the broker in the handshake.
    pub fn client_id(&self) -> Option<u32> {
        *self.client_id.lock().expect("client_id mutex poisoned")
    }

    /// Type registry shared with the underlying codec.
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Subscribe to typed events with a synchronous callback handler.
    /// The callback fires from the [`GuestDriver`]'s read loop on
    /// every transmission whose `header.type_name` matches `T`.
    ///
    /// Drop the returned [`SubscriptionHandle`] to detach the handler;
    /// call [`SubscriptionHandle::discard`] to keep it installed for
    /// the rest of the connection. When the last subscriber of a
    /// type goes away, this also publishes `DotsMember(Leave)` so
    /// the broker stops routing.
    pub fn subscribe<T>(
        self: &Arc<Self>,
        handler: impl FnMut(&Event<T>) + Send + 'static,
    ) -> SubscriptionHandle
    where
        T: StructValue + Default + Send + 'static,
    {
        self.register_struct::<T>();
        let group = T::type_descriptor().name;
        self.join_group(group);
        let mut handle = register_callback::<T, _>(&self.dispatch, handler);
        handle.set_leaver(self.make_leaver(group));
        handle
    }

    /// Subscribe to typed events as an async [`crate::Subscription<T>`]
    /// stream.
    pub fn subscribe_stream<T>(self: &Arc<Self>) -> crate::Subscription<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        self.register_struct::<T>();
        let group = T::type_descriptor().name;
        self.join_group(group);
        let mut sub = connection_subscribe::<T>(&self.dispatch);
        sub.set_leaver(self.make_leaver(group));
        sub
    }

    /// Subscribe to *every* DOTS type — known now or learned later —
    /// with a single handler. Composes
    /// [`subscribe_new_struct_type`](Self::subscribe_new_struct_type) and
    /// [`subscribe_dynamic`](Self::subscribe_dynamic): for each
    /// descriptor in the registry (now or arriving via the wire), a
    /// dynamic subscription is installed that funnels its events into
    /// `handler`. Returns an [`AllTypesSubscription`] whose drop tears
    /// down the new-type observer plus every per-type dynamic sub it
    /// installed.
    ///
    /// Intended for tracing / inspection tools (mirrors dots-cli
    /// `trace`). Note: this also subscribes to internal DOTS types
    /// (DotsClient, DotsMember, DotsHeader, …) — that's deliberate
    /// for full visibility.
    pub fn subscribe_all_types<F>(self: &Arc<Self>, handler: F) -> AllTypesSubscription
    where
        F: FnMut(&Event<DynamicStruct>) + Send + 'static,
    {
        let handler = Arc::new(Mutex::new(handler));
        let dynamic_handles: Arc<Mutex<Vec<SubscriptionHandle>>> =
            Arc::new(Mutex::new(Vec::new()));

        let dynamic_handles_clone = dynamic_handles.clone();
        let handler_clone = handler.clone();
        let self_arc = self.clone();
        let new_type_handle = self.subscribe_new_struct_type(move |descriptor| {
            let h = handler_clone.clone();
            let sub = self_arc.subscribe_dynamic(descriptor.clone(), move |event| {
                if let Ok(mut h) = h.lock() {
                    h(event);
                }
            });
            if let Ok(mut handles) = dynamic_handles_clone.lock() {
                handles.push(sub);
            }
        });

        AllTypesSubscription {
            _new_type_handle: new_type_handle,
            _dynamic_handles: dynamic_handles,
        }
    }

    /// Subscribe to type-system events: every `StructDescriptorData`
    /// arriving on the wire is converted to a [`DynamicStructDescriptor`],
    /// registered with the codec registry, and passed to `handler`.
    /// Additionally, `handler` is invoked synchronously for each
    /// currently-registered struct descriptor before this returns
    /// (catch-up replay), so a fresh subscriber sees what's already
    /// known plus everything that arrives afterwards.
    ///
    /// Mirrors dots-cpp's `subscribe<StructDescriptor>` /
    /// `DynamicTypeReceiver` pattern. Combine with
    /// [`subscribe_dynamic`](Self::subscribe_dynamic) and
    /// [`publish_dynamic`](Self::publish_dynamic) for a fully
    /// dynamic client.
    pub fn subscribe_new_struct_type<F>(self: &Arc<Self>, handler: F) -> SubscriptionHandle
    where
        F: FnMut(&Arc<DynamicStructDescriptor>) + Send + 'static,
    {
        let handler = Arc::new(Mutex::new(handler));

        {
            let mut h = handler.lock().expect("handler mutex poisoned");
            for desc in self.registry.iter_structs() {
                h(&desc);
            }
        }

        let registry = self.registry.clone();
        let handler_for_wire = handler.clone();
        self.subscribe::<StructDescriptorData>(move |event| {
            match registry.build_dynamic_struct(&event.value) {
                Ok(desc) => {
                    let arc = Arc::new(desc);
                    registry.register_struct_dynamic(arc.clone());
                    if let Ok(mut h) = handler_for_wire.lock() {
                        h(&arc);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = ?e,
                        type_name = ?event.value.name,
                        "could not build dynamic descriptor from wire StructDescriptorData",
                    );
                }
            }
        })
    }

    /// Subscribe to a runtime-described type. The handler receives an
    /// [`Event<DynamicStruct>`] for every transmission whose
    /// `header.type_name` matches `descriptor.name`.
    ///
    /// Used by dynamic clients (no compiled-in type) — typically the
    /// descriptor was learned from the broker by subscribing to
    /// `StructDescriptorData` and converting via
    /// [`Registry::build_dynamic_struct`](dots_model::Registry::build_dynamic_struct).
    /// Registers the descriptor with the codec registry so the read
    /// loop can decode incoming wire bytes; joins the type group so
    /// the broker routes (and replays cache for) this type.
    pub fn subscribe_dynamic(
        self: &Arc<Self>,
        descriptor: Arc<DynamicStructDescriptor>,
        handler: impl FnMut(&Event<DynamicStruct>) + Send + 'static,
    ) -> SubscriptionHandle {
        let type_name = descriptor.name.clone();
        self.registry.register_struct_dynamic(descriptor);
        self.join_group(&type_name);
        let leaver = self.make_leaver(&type_name);
        let mut handle = register_dynamic_callback(&self.dispatch, type_name, handler);
        handle.set_leaver(leaver);
        handle
    }

    /// Build a typed [`Container<T>`] for `T`.
    pub fn container<T>(self: &Arc<Self>) -> Container<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        self.register_struct::<T>();
        let group = T::type_descriptor().name;
        self.join_group(group);
        let mut container = crate::container::make_container(&self.dispatch);
        container.set_leaver(self.make_leaver(group));
        container
    }

    /// Build a `GroupLeaver` whose drop publishes `DotsMember(Leave)`
    /// for `group` if the per-group subscriber count drops to zero.
    fn make_leaver(self: &Arc<Self>, group: &str) -> GroupLeaver {
        let weak = Arc::downgrade(self);
        let group = group.to_string();
        GroupLeaver::new(move || {
            if let Some(t) = weak.upgrade() {
                t.leave_group(&group);
            }
        })
    }

    /// Pre-register an enum type's descriptor so it gets shipped to
    /// the broker before preload finishes. Auto-registration covers
    /// any enum embedded in a subscribed/published struct's fields
    /// (recursively, including through nested structs and `Vec`),
    /// so this only needs to be called for standalone enums that
    /// never appear as a struct field.
    pub fn register_enum(&self, descriptor: &'static EnumDescriptor) {
        self.register_enum_descriptor_internal(descriptor);
    }

    /// Publish a typed value. Synchronous — bytes are pushed onto the
    /// outbound channel and sent by the [`GuestDriver::run`] loop.
    ///
    /// Substruct-only types (`#[dots(substruct_only)]`) intentionally
    /// don't implement [`Publishable`], so this fails to compile at
    /// the call site rather than producing a runtime error.
    pub fn publish<T>(&self, value: &T) -> Result<(), ClientClosed>
    where
        T: StructValue + Publishable,
    {
        self.register_struct_descriptor(T::type_descriptor());
        self.publish_typed(value)
    }

    /// Publish a typed value, restricting the wire payload to the
    /// properties named in `included` (plus the type's keys, which
    /// are always sent so receivers can identify the instance).
    ///
    /// Mirrors C++ `publish(instance, includedProperties, remove=false)`:
    /// only properties that are *both* set on `value` *and* included
    /// in the union of `included | key_set(value)` make it onto the
    /// wire. Useful for partial updates where some non-key fields
    /// are populated locally but should not be propagated yet.
    pub fn publish_with_mask<T>(
        &self,
        value: &T,
        included: PropertySet,
    ) -> Result<(), ClientClosed>
    where
        T: StructValue + Publishable,
    {
        self.register_struct_descriptor(T::type_descriptor());
        let mask = (included | key_set(value)) & value.valid_set();
        let header = DotsHeader {
            type_name: Some(value.descriptor().name.into()),
            attributes: Some(mask.bits()),
            sender: self.client_id(),
            sent_time: Some(now_timepoint()),
            ..Default::default()
        };
        let mut bytes = Vec::with_capacity(64);
        encode_typed_transmission_with_mask_into(&header, value, mask, &mut bytes);
        self.outbound_tx.send(bytes).map_err(|_| ClientClosed)
    }

    /// Publish a runtime-described value. The wire `type_name` and
    /// `attributes` come from `value.descriptor.name` and
    /// `value.valid` respectively. Used by dynamic clients that
    /// construct a [`DynamicStruct`] from a runtime descriptor (often
    /// learned from the broker via `StructDescriptorData`).
    ///
    /// Note: the descriptor is *not* re-published to the broker here.
    /// If the descriptor was learned from the broker, the broker
    /// already knows it; if the caller built it locally, they must
    /// publish a `StructDescriptorData` for it themselves before any
    /// peer can decode the payload.
    pub fn publish_dynamic(&self, value: &DynamicStruct) -> Result<(), ClientClosed> {
        let header = DotsHeader {
            type_name: Some(value.descriptor.name.clone().into()),
            attributes: Some(value.valid.bits()),
            sender: self.client_id(),
            sent_time: Some(now_timepoint()),
            ..Default::default()
        };
        let mut bytes = Vec::with_capacity(64);
        encode_dynamic_transmission_into(&header, value, &mut bytes);
        self.outbound_tx.send(bytes).map_err(|_| ClientClosed)
    }

    /// Publish a removal: tells the broker to drop the cached
    /// instance whose key matches `value`. The wire payload contains
    /// only the type's `#[dots(key)]` properties; `header.remove_obj
    /// = true` and `header.attributes` is the key-only bitmask.
    ///
    /// Mirrors C++ `transceiver.remove(instance)`.
    pub fn remove<T>(&self, value: &T) -> Result<(), ClientClosed>
    where
        T: StructValue + Publishable,
    {
        self.register_struct_descriptor(T::type_descriptor());
        let mask = key_set(value);
        let header = DotsHeader {
            type_name: Some(value.descriptor().name.into()),
            attributes: Some(mask.bits()),
            sender: self.client_id(),
            sent_time: Some(now_timepoint()),
            remove_obj: Some(true),
            ..Default::default()
        };
        let mut bytes = Vec::with_capacity(64);
        encode_typed_transmission_with_mask_into(&header, value, mask, &mut bytes);
        self.outbound_tx.send(bytes).map_err(|_| ClientClosed)
    }

    /// Signal the [`GuestDriver`]'s run loop to exit at the next
    /// iteration. Returns immediately; the actual loop break happens
    /// on the next pass through the select. The notify ensures that
    /// pass happens promptly — even on a quiet connection where no
    /// message is in flight.
    pub fn exit(&self) {
        self.exit_flag.store(true, Ordering::Release);
        self.exit_notify.notify_waiters();
    }

    fn register_struct<T: StructValue + 'static>(&self) {
        self.register_struct_descriptor(T::type_descriptor());
    }

    fn register_struct_descriptor(&self, descriptor: &'static StructDescriptor) {
        // (a) Queue the descriptor for publishing to the broker before
        // preload finishes. `pending_structs` is a HashSet so the
        // recursive walk below silently dedupes against types we've
        // already seen.
        let inserted = self
            .pending_structs
            .lock()
            .expect("pending mutex poisoned")
            .insert(DescriptorPtr(descriptor));
        // (b) Tell the codec's runtime registry about this type so
        // incoming transmissions of it can be decoded.
        self.registry.register_struct_static(descriptor);

        // (c) Walk the descriptor's properties for nested types: any
        // embedded struct or enum descriptor needs to travel to the
        // broker too, so peers reading those fields by name (e.g. a
        // C++ guest with no compiled-in copy of the user enum) can
        // resolve them. Skip if we've already registered this struct,
        // since its children will already have been walked and we
        // could otherwise loop on cyclic references.
        if !inserted {
            return;
        }
        for prop in descriptor.properties {
            self.register_field_kind_descriptors(&prop.kind);
        }
    }

    /// Recursively follow a [`FieldKind`] to register any nested
    /// struct or enum descriptors it transitively references. Vec
    /// types unwrap to their inner kind; primitives and strings have
    /// nothing to register.
    fn register_field_kind_descriptors(&self, kind: &dots_core::FieldKind) {
        use dots_core::FieldKind;
        match kind {
            FieldKind::Struct(d) => {
                // Recurse via register_struct_descriptor so nested
                // structs get their own properties walked too.
                self.register_struct_descriptor(d);
            }
            FieldKind::Enum(e) => {
                self.register_enum_descriptor_internal(e);
            }
            FieldKind::Vec(inner) => {
                self.register_field_kind_descriptors(inner);
            }
            _ => {}
        }
    }

    fn register_enum_descriptor_internal(&self, descriptor: &'static EnumDescriptor) {
        self.pending_enums
            .lock()
            .expect("pending mutex poisoned")
            .insert(DescriptorPtr(descriptor));
        self.registry.register_enum_static(descriptor);
    }

    /// Increment the per-group subscriber count, publishing
    /// `DotsMember(Join)` when the count transitions 0→1. Tells the
    /// broker to start routing transmissions of `group_name` to this
    /// guest.
    fn join_group(&self, group_name: &str) {
        let count = {
            let mut groups = self
                .joined_groups
                .lock()
                .expect("joined_groups mutex poisoned");
            let c = groups.entry(group_name.to_string()).or_insert(0);
            *c += 1;
            *c
        };
        if count == 1 {
            let member = DotsMember {
                group_name: Some(group_name.into()),
                event: Some(DotsMemberEvent::Join),
                client: self.client_id(),
            };
            let _ = self.publish_typed(&member);
        }
    }

    /// Decrement the per-group subscriber count, publishing
    /// `DotsMember(Leave)` when the count reaches 0. Removes the
    /// group entry entirely so a future re-subscribe re-publishes
    /// Join.
    pub(crate) fn leave_group(&self, group_name: &str) {
        let mut should_publish_leave = false;
        {
            let mut groups = self
                .joined_groups
                .lock()
                .expect("joined_groups mutex poisoned");
            if let Some(count) = groups.get_mut(group_name) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    groups.remove(group_name);
                    should_publish_leave = true;
                }
            }
        }
        if should_publish_leave {
            let member = DotsMember {
                group_name: Some(group_name.into()),
                event: Some(DotsMemberEvent::Leave),
                client: self.client_id(),
            };
            let _ = self.publish_typed(&member);
        }
    }

    fn publish_typed<T: StructValue>(&self, value: &T) -> Result<(), ClientClosed> {
        let type_name = value.descriptor().name;
        let header = DotsHeader {
            type_name: Some(type_name.into()),
            attributes: Some(value.valid_set().bits()),
            sender: self.client_id(),
            sent_time: Some(now_timepoint()),
            ..Default::default()
        };
        let mut bytes = Vec::with_capacity(64);
        encode_typed_transmission_into(&header, value, &mut bytes);
        self.outbound_tx.send(bytes).map_err(|_| ClientClosed)
    }
}

/// Owns the [`Connection`] and runs the read/write event loop.
/// Created together with a [`GuestTransceiver`] by
/// [`GuestTransceiver::from_connection`].
pub struct GuestDriver<S> {
    transceiver: Arc<GuestTransceiver>,
    conn: Option<Connection<S>>,
    outbound_rx: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl<S> GuestDriver<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Drive the connection forward:
    ///
    /// 1. Publish all queued type descriptors.
    /// 2. Finish preload if the connection is in `EarlySubscribe`.
    /// 3. Enter the main read/write select loop until exit, EOF, or
    ///    transport error.
    pub async fn run(mut self) -> Result<(), GuestError> {
        let mut conn = self.conn.take().expect("GuestDriver::run called twice");
        let mut outbound_rx = self
            .outbound_rx
            .take()
            .expect("GuestDriver::run called twice");

        // Phase 1: publish all queued type descriptors.
        let pending_structs: Vec<&'static StructDescriptor> = self
            .transceiver
            .pending_structs
            .lock()
            .expect("pending mutex poisoned")
            .iter()
            .map(|d| d.0)
            .collect();
        let pending_enums: Vec<&'static EnumDescriptor> = self
            .transceiver
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
        // Publish enums BEFORE structs. The broker's
        // `build_dynamic_struct` resolves nested type references
        // through its registry as it parses each `StructDescriptorData`,
        // so any enum referenced as a struct field must already be
        // registered there. Same constraint as dots-cpp's descriptor
        // exchange — declaration order is part of the contract.
        for d in pending_enums {
            let data = EnumDescriptorData::from_static(d);
            conn.send_typed("EnumDescriptorData", &data).await?;
        }
        for d in pending_structs {
            let data = StructDescriptorData::from_static(d);
            conn.send_typed("StructDescriptorData", &data).await?;
        }

        // Phase 2: finish preload (cache events flow through dispatch
        // into any installed subscriptions/containers).
        if conn.state() == dots_model::DotsConnectionState::EarlySubscribe {
            conn.finish_preload().await?;
        }

        // Phase 3: split the framed and run the main select loop.
        let (framed, dispatch) = conn.into_parts();
        let (mut sink, mut stream) = framed.split();

        tracing::debug!("entering GuestDriver run loop");
        loop {
            if self.transceiver.exit_flag.load(Ordering::Acquire) {
                tracing::info!("exit flag set, leaving guest run loop");
                break;
            }
            tokio::select! {
                biased;
                _ = self.transceiver.exit_notify.notified() => {
                    // Loop back to the top so the exit_flag check
                    // breaks us out cleanly.
                    continue;
                }
                maybe_in = stream.next() => match maybe_in {
                    Some(Ok(txn)) => {
                        tracing::trace!(
                            type_name = ?txn.header.type_name,
                            sender = ?txn.header.sender,
                            "dispatching incoming transmission"
                        );
                        Connection::<S>::dispatch_external(&dispatch, &txn);
                    }
                    Some(Err(e)) => {
                        tracing::error!(error = %e, "transport error in guest run loop");
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
    /// Set when this handle was created via
    /// [`GuestTransceiver::subscribe`]. Runs on drop to decrement
    /// the per-type subscriber count and publish `DotsMember(Leave)`
    /// if this was the last subscriber.
    leaver: Option<GroupLeaver>,
}

impl SubscriptionHandle {
    /// Detach this handle from its `Drop` cleanup, leaving the
    /// callback installed for the rest of the connection. Mirrors
    /// C++ DOTS's `Subscription::discard()`.
    ///
    /// Note that this also forgets the leaver, so `DotsMember(Leave)`
    /// is *not* published — the discarded subscription stays
    /// effective for the broker's routing as well.
    pub fn discard(self) {
        core::mem::forget(self);
    }

    fn set_leaver(&mut self, leaver: GroupLeaver) {
        self.leaver = Some(leaver);
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
        leaver: None,
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
        txn: &Transmission,
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

}

fn register_dynamic_callback<F>(
    dispatch: &Arc<Mutex<DispatchState>>,
    type_name: String,
    handler: F,
) -> SubscriptionHandle
where
    F: FnMut(&Event<DynamicStruct>) + Send + 'static,
{
    let entry = DynamicCallbackDispatchEntry { handler };
    let id = dispatch
        .lock()
        .expect("dispatch mutex poisoned")
        .register(type_name.clone(), Box::new(entry));
    SubscriptionHandle {
        type_name,
        id,
        dispatch: Arc::downgrade(dispatch),
        leaver: None,
    }
}

/// Composite handle returned by
/// [`GuestTransceiver::subscribe_all_types`]. Owns the
/// [`SubscriptionHandle`] for the type-system observer plus the
/// per-type dynamic subscriptions it installs; dropping it tears
/// every part down at once.
pub struct AllTypesSubscription {
    _new_type_handle: SubscriptionHandle,
    _dynamic_handles: Arc<Mutex<Vec<SubscriptionHandle>>>,
}

struct DynamicCallbackDispatchEntry<F> {
    handler: F,
}

impl<F> DispatchEntry for DynamicCallbackDispatchEntry<F>
where
    F: FnMut(&Event<DynamicStruct>) + Send + 'static,
{
    fn dispatch(&mut self, txn: &Transmission) -> Result<bool, dots_core::DecodeError> {
        // The codec already decoded the wire bytes into a
        // `DynamicStruct` against the registry's descriptor for this
        // type, so no further decode work is needed here.
        let event = Event {
            header: txn.header.clone(),
            value: txn.payload.clone(),
        };
        (self.handler)(&event);
        Ok(true)
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
        txn: &Transmission,
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
}
