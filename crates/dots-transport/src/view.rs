//! Client-side filtered subscription: [`View<T>`].
//!
//! A [`View<T>`] is the guest-side counterpart to the broker's
//! [`FilteredSub`](crate::host) — both share a `subscription_id`
//! allocated by the guest. The view holds a typed
//! [`Container<T>`](crate::Container) that mirrors only the rows
//! the predicate accepts (and only the properties the projection
//! mask includes); the broker's four-cases dispatch ensures the
//! container's local state stays consistent with the view's
//! definition.
//!
//! Construction is fallible (the connected broker may not support
//! filtered subscriptions, in which case [`ViewError::Unsupported`]
//! is returned synchronously). Drop unregisters the view and
//! publishes `DotsMember(Leave)` so the broker tears down its
//! [`FilteredSub`] state.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use dots_core::{StructValue, decode_typed_from_slice};
use dots_model::{Transmission, filter::DotsFilter};

use crate::connection::{Event, ViewDispatch};
use crate::container::{Container, ContainerEntry};
use crate::guest::GuestTransceiver;

/// Construction-time errors for [`View<T>`].
#[derive(Debug)]
pub enum ViewError {
    /// The broker didn't advertise `filtered_subscriptions = true`
    /// in its Hello capabilities (either it's a legacy server or
    /// the handshake hadn't completed when the view was opened).
    Unsupported,
    /// The transceiver is being torn down — its parent connection
    /// has been dropped.
    TransceiverGone,
}

impl core::fmt::Display for ViewError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unsupported => f.write_str(
                "peer doesn't advertise filtered_subscriptions capability",
            ),
            Self::TransceiverGone => f.write_str("transceiver dropped"),
        }
    }
}

impl std::error::Error for ViewError {}

/// Live filtered subscription. Holds a `subscription_id`, a typed
/// [`Container<T>`] mirroring the matching rows, and a handler list
/// that fires for each broker-delivered transition.
///
/// Drop the value to tear the subscription down. Drop is best-effort
/// — it publishes `DotsMember(Leave)` synchronously, but the actual
/// network I/O happens on the guest driver's loop.
pub struct View<T>
where
    T: StructValue + Default + Send + 'static + dots_core::GlobalRegistration,
{
    subscription_id: u32,
    type_name: String,
    state: Arc<ViewState<T>>,
    transceiver: Weak<GuestTransceiver>,
}

/// Shared inner state: the typed container, the handler list, and
/// counters. Held behind an `Arc` so the dispatch path (via
/// `Weak<dyn ViewDispatch>` in the dispatch demux) and the user's
/// `View<T>` handle share it.
pub(crate) struct ViewState<T> {
    container: Container<T>,
    handlers: Mutex<HashMap<u64, Box<dyn FnMut(&Event<T>) + Send>>>,
    next_handler_id: AtomicU64,
}

impl<T> ViewState<T>
where
    T: StructValue + Default + Send + 'static,
{
    fn new(container: Container<T>) -> Self {
        Self {
            container,
            handlers: Mutex::new(HashMap::new()),
            next_handler_id: AtomicU64::new(1),
        }
    }
}

impl<T> ViewDispatch for ViewState<T>
where
    T: StructValue + Default + Send + Clone + 'static,
{
    fn dispatch(&self, txn: &Transmission) {
        // Decode into the typed T. If the payload doesn't decode
        // cleanly we drop the event silently — this matches the
        // unfiltered subscription path's tolerance for partial /
        // unfamiliar payloads.
        let bytes = txn.payload.encode();
        let Ok(value) = decode_typed_from_slice::<T>(&bytes) else {
            return;
        };
        let event = Event {
            header: txn.header.clone(),
            value,
        };
        // Update the view's container (insert / remove via the same
        // dispatch entry shape used for unfiltered containers).
        crate::container::view_dispatch_update::<T>(&self.container, txn);
        // Fire user handlers. We collect ids to drop while
        // iterating so a handler whose receiver was closed doesn't
        // tie up subsequent calls. (Currently handlers are bare
        // FnMut closures with no close signal — kept simple; if
        // users want streaming semantics they can pipe through an
        // mpsc themselves.)
        let mut handlers = self
            .handlers
            .lock()
            .expect("view handlers mutex poisoned");
        for (_, handler) in handlers.iter_mut() {
            handler(&event);
        }
    }
}

/// RAII guard returned by [`View::subscribe`]. Dropping it removes
/// the handler from the view's handler list — the view itself stays
/// open until the parent [`View<T>`] is dropped.
pub struct ViewSubscription<T> {
    id: u64,
    state: Weak<ViewState<T>>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Drop for ViewSubscription<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let mut handlers = state
                .handlers
                .lock()
                .expect("view handlers mutex poisoned");
            handlers.remove(&self.id);
        }
    }
}

impl<T> View<T>
where
    T: StructValue + Default + Send + Clone + 'static + dots_core::GlobalRegistration,
{
    pub(crate) fn open(
        transceiver: &Arc<GuestTransceiver>,
        filter: DotsFilter,
    ) -> Result<Self, ViewError> {
        if !transceiver.peer_supports_filtered_subscriptions() {
            return Err(ViewError::Unsupported);
        }

        T::register_as_subscribed();
        transceiver.ensure_struct_descriptor::<T>();

        let type_name = T::type_descriptor().name.to_string();
        let subscription_id = transceiver.allocate_subscription_id();
        let container = crate::container::make_container::<T>(
            transceiver.dispatch_handle_ref(),
            None,
        );
        let state = Arc::new(ViewState::new(container));

        // Register first, so an in-flight transmission tagged with
        // our subscription_id (e.g. the preload stream) finds us.
        let weak: Weak<dyn ViewDispatch> = Arc::downgrade(&state) as _;
        transceiver.register_view(subscription_id, weak);

        // Publish the filtered join. Done after registration so we
        // can't lose the preload race.
        transceiver.publish_filtered_join(&type_name, subscription_id, filter.clone());

        Ok(Self {
            subscription_id,
            type_name,
            state,
            transceiver: Arc::downgrade(transceiver),
        })
    }

    /// `subscription_id` allocated for this view. Process-local;
    /// the broker uses `(client_id, subscription_id)` as the
    /// FilteredSub key.
    pub fn subscription_id(&self) -> u32 {
        self.subscription_id
    }

    /// The view's local cache mirror.
    pub fn container(&self) -> &Container<T> {
        &self.state.container
    }

    /// Install a sync callback that fires for every event the
    /// broker routes through this view. The handler is invoked
    /// synchronously from the connection's read loop, like the
    /// existing `subscribe` callbacks.
    ///
    /// Synchronous replay over the view's current container
    /// contents runs before this returns, so handlers see the
    /// "current state" snapshot followed by live events. Returns
    /// an RAII [`ViewSubscription`] guard — drop it to detach the
    /// handler.
    pub fn subscribe(
        &self,
        mut handler: impl FnMut(&Event<T>) + Send + 'static,
    ) -> ViewSubscription<T> {
        // Sync replay: call the handler over the current container
        // entries with synthetic Create-shaped events.
        let snapshot: Vec<ContainerEntry<T>> = self.state.container.snapshot();
        for entry in snapshot {
            let header = dots_model::DotsHeader {
                type_name: Some(self.type_name.clone()),
                attributes: Some(<T as dots_core::Transmittable>::valid_set(&entry.value)),
                sender: entry.clone_info.last_update_sender,
                sent_time: entry.clone_info.last_update_time,
                from_cache: Some(0),
                remove_obj: Some(false),
                is_from_myself: Some(false),
                subscription_id: Some(self.subscription_id),
                ..Default::default()
            };
            handler(&Event { header, value: entry.value });
        }

        let id = self.state.next_handler_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut handlers = self
                .state
                .handlers
                .lock()
                .expect("view handlers mutex poisoned");
            handlers.insert(id, Box::new(handler));
        }
        ViewSubscription {
            id,
            state: Arc::downgrade(&self.state),
            _phantom: PhantomData,
        }
    }
}

impl<T> Drop for View<T>
where
    T: StructValue + Default + Send + 'static + dots_core::GlobalRegistration,
{
    fn drop(&mut self) {
        if let Some(tx) = self.transceiver.upgrade() {
            tx.unregister_view(self.subscription_id);
            tx.publish_filtered_leave(&self.type_name, self.subscription_id);
        }
    }
}
