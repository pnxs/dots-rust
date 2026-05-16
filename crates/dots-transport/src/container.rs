//! Typed local cache mirror for cached DOTS types.
//!
//! A [`Container<T>`] is a per-key index of the latest known value of
//! every instance of `T` the connection has seen. It plugs into the
//! same dispatch table as [`Subscription`](crate::Subscription), so a
//! single transmission feeds both — letting users observe live events
//! and inspect aggregated state without setting up their own caching.
//!
//! Created via [`Connection::container`](crate::Connection::container).
//! Drop the returned handle to detach the container from the dispatch
//! loop.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, Weak};

use dots_core::{StructValue, Timepoint, decode_typed_from_slice, encode_key_bytes};
use dots_model::Transmission;

use crate::connection::{DispatchEntry, DispatchState};

/// Per-entry metadata: when the value was first seen, when it was
/// most recently updated, and what kind of operation produced it.
#[derive(Debug, Clone, PartialEq)]
pub struct CloneInfo {
    pub last_operation: Operation,
    pub last_update_time: Option<Timepoint>,
    pub last_update_sender: Option<u32>,
    pub created_time: Option<Timepoint>,
    pub created_sender: Option<u32>,
}

/// What kind of change produced an entry's current state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Entry was newly created (no previous entry for its key).
    Create,
    /// Entry already existed; this is a subsequent update.
    Update,
    /// Entry was removed. Stored entries never have this value;
    /// it's reserved for future events-stream usage.
    Remove,
}

/// One typed entry in a [`Container<T>`].
#[derive(Debug, Clone)]
pub struct ContainerEntry<T> {
    pub value: T,
    pub clone_info: CloneInfo,
}

type Entries<T> = BTreeMap<Vec<u8>, ContainerEntry<T>>;

/// A typed local mirror of all instances of `T` the connection has
/// observed. Updates as transmissions arrive (driven by
/// [`Connection::next`](crate::Connection::next)).
///
/// Cheaply `Clone`-able — clones share the same backing store and the
/// same RAII lifecycle, so dispatch-unregistration and any attached
/// group-leave only fire when the last `Container<T>` clone drops.
/// That makes it natural to hand a `Container<T>` into a callback or
/// spawned task without extra ceremony.
pub struct Container<T> {
    pub(crate) entries: Arc<Mutex<Entries<T>>>,
    /// Refcounted lifecycle bits — dispatch handle and optional
    /// group-leaver. The `Drop` impl on the inner struct fires once,
    /// when the last `Container<T>` clone goes out of scope.
    lifecycle: Arc<ContainerLifecycle>,
    /// `PhantomData<fn() -> T>` rather than `PhantomData<T>` so the
    /// container is unconditionally `Send + Sync` — `T`'s own
    /// auto-traits don't gate it (the actual `T` values live behind
    /// the `Arc<Mutex<…>>` in `entries`, whose `Sync` requirement is
    /// only `T: Send`).
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Clone for Container<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            lifecycle: self.lifecycle.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Lifecycle bits shared across all `Container<T>` clones. Held
/// behind an `Arc` in [`Container`] so dispatch-unregistration and
/// the optional [`GroupLeaver`] only run once — when the last clone
/// drops.
struct ContainerLifecycle {
    type_name: String,
    id: u64,
    dispatch: Weak<Mutex<DispatchState>>,
    /// `Some` when the container was created via
    /// [`GuestTransceiver::container`](crate::GuestTransceiver::container)
    /// (publishes `DotsMember(Leave)` on drop once nobody else holds
    /// a container for this type). `None` when created via raw
    /// [`Connection::container`](crate::Connection::container).
    _leaver: Option<crate::connection::GroupLeaver>,
}

impl<T> Container<T>
where
    T: StructValue + Default + Send + 'static,
{
    /// Number of entries currently stored.
    pub fn len(&self) -> usize {
        self.entries.lock().expect("container mutex poisoned").len()
    }

    /// `true` if no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.entries
            .lock()
            .expect("container mutex poisoned")
            .is_empty()
    }

    /// Run a closure over the current entries while holding the
    /// container's read lock. Returns whatever the closure returns —
    /// useful for in-place iteration or extracting a derived value
    /// without cloning the whole map.
    pub fn with_entries<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Entries<T>) -> R,
    {
        let entries = self.entries.lock().expect("container mutex poisoned");
        f(&entries)
    }
}

impl<T> Container<T>
where
    T: StructValue + Default + Send + Clone + 'static,
{
    /// Owned snapshot of all current entries. Bound to `T: Clone` —
    /// for non-clone types use [`with_entries`](Self::with_entries).
    pub fn snapshot(&self) -> Vec<ContainerEntry<T>> {
        self.entries
            .lock()
            .expect("container mutex poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Look up the entry whose key matches that of `query`. Only the
    /// `#[dots(key)]` properties of `query` are used; other fields are
    /// ignored. Returns a clone of the stored entry.
    pub fn get(&self, query: &T) -> Option<ContainerEntry<T>> {
        let key = encode_key_bytes(query);
        self.entries
            .lock()
            .expect("container mutex poisoned")
            .get(&key)
            .cloned()
    }
}

impl Drop for ContainerLifecycle {
    fn drop(&mut self) {
        if let Some(dispatch) = self.dispatch.upgrade() {
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .unregister(&self.type_name, self.id);
        }
        // `_leaver`'s own `Drop` (when `Some`) publishes the group
        // `Leave` — we don't need to do anything explicit here.
    }
}

// ===== Container construction (called from Connection::container) =====

/// Build a [`Container<T>`] and register its backing dispatch entry
/// with the connection's `DispatchState`.
///
/// `leaver` carries the optional RAII group-`Leave` guard — `Some`
/// when called from
/// [`GuestTransceiver::container`](crate::GuestTransceiver::container),
/// `None` from the raw `Connection::container` path.
pub(crate) fn make_container<T>(
    dispatch: &Arc<Mutex<DispatchState>>,
    leaver: Option<crate::connection::GroupLeaver>,
) -> Container<T>
where
    T: StructValue + Default + Send + 'static,
{
    let entries: Arc<Mutex<Entries<T>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let entry: ContainerDispatchEntry<T> = ContainerDispatchEntry {
        entries: entries.clone(),
        _phantom: PhantomData,
    };
    let type_name = T::type_descriptor().name.to_string();
    let id = dispatch
        .lock()
        .expect("dispatch mutex poisoned")
        .register(type_name.clone(), Box::new(entry));
    let lifecycle = Arc::new(ContainerLifecycle {
        type_name,
        id,
        dispatch: Arc::downgrade(dispatch),
        _leaver: leaver,
    });
    Container {
        entries,
        lifecycle,
        _phantom: PhantomData,
    }
}

/// The dispatch entry that updates a container in place.
struct ContainerDispatchEntry<T> {
    entries: Arc<Mutex<Entries<T>>>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> DispatchEntry for ContainerDispatchEntry<T>
where
    T: StructValue + Default + Send + 'static,
{
    fn dispatch(&mut self, txn: &Transmission) -> Result<bool, dots_core::DecodeError> {
        update_entries_from_txn::<T>(&self.entries, txn)?;
        Ok(true)
    }
}

/// Shared body of the container's per-event update — used both by
/// the unfiltered `ContainerDispatchEntry` path and by the
/// filtered-subscription `View<T>` dispatcher.
fn update_entries_from_txn<T>(
    entries: &Arc<Mutex<Entries<T>>>,
    txn: &Transmission,
) -> Result<(), dots_core::DecodeError>
where
    T: StructValue + Default + Send + 'static,
{
    let bytes = txn.payload.encode();
    let value: T = decode_typed_from_slice(&bytes)?;
    let key = encode_key_bytes(&value);

    let mut entries = entries.lock().expect("container mutex poisoned");
    if txn.header.remove_obj == Some(true) {
        entries.remove(&key);
        return Ok(());
    }

    let now_sender = txn.header.sender;
    let now_time = txn.header.sent_time;
    let (operation, created_time, created_sender) = match entries.get(&key) {
        Some(existing) => (
            Operation::Update,
            existing.clone_info.created_time,
            existing.clone_info.created_sender,
        ),
        None => (Operation::Create, now_time, now_sender),
    };
    entries.insert(
        key,
        ContainerEntry {
            value,
            clone_info: CloneInfo {
                last_operation: operation,
                last_update_time: now_time,
                last_update_sender: now_sender,
                created_time,
                created_sender,
            },
        },
    );
    Ok(())
}

/// Apply a transmission to a [`Container<T>`] from the
/// `View<T>` dispatch path. Decodes the payload, updates the
/// container, silently drops on decode failure (matches the
/// unfiltered subscription tolerance).
pub(crate) fn view_dispatch_update<T>(container: &Container<T>, txn: &Transmission)
where
    T: StructValue + Default + Send + 'static,
{
    let _ = update_entries_from_txn::<T>(&container.entries, txn);
}
