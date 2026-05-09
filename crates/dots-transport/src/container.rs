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
pub struct Container<T> {
    entries: Arc<Mutex<Entries<T>>>,
    type_name: String,
    id: u64,
    dispatch: Weak<Mutex<DispatchState>>,
    /// RAII guard that runs when this container is dropped, used by
    /// [`crate::GuestTransceiver`] to publish `DotsMember(Leave)`
    /// when the last subscriber for the type goes away. `None` when
    /// the container was created via raw `Connection::container`
    /// (which doesn't auto-join groups).
    leaver: Option<crate::connection::GroupLeaver>,
    _phantom: PhantomData<T>,
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

impl<T> Container<T> {
    /// A cheap, cloneable read-only handle on this container's
    /// data. Useful for sharing into callback handlers and tasks
    /// without giving up the [`Container`]'s RAII unregister.
    ///
    /// Dropping a handle does *not* unregister the underlying
    /// dispatch entry — that lifetime stays tied to the original
    /// [`Container`].
    pub fn handle(&self) -> ContainerHandle<T> {
        ContainerHandle {
            entries: self.entries.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Cheap, cloneable read-only handle on a [`Container`]'s state.
/// Yielded by [`Container::handle`].
pub struct ContainerHandle<T> {
    entries: Arc<Mutex<Entries<T>>>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for ContainerHandle<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> ContainerHandle<T>
where
    T: StructValue + Default + Send + 'static,
{
    pub fn len(&self) -> usize {
        self.entries.lock().expect("container mutex poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries
            .lock()
            .expect("container mutex poisoned")
            .is_empty()
    }

    pub fn with_entries<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Entries<T>) -> R,
    {
        let entries = self.entries.lock().expect("container mutex poisoned");
        f(&entries)
    }
}

impl<T> ContainerHandle<T>
where
    T: StructValue + Default + Send + Clone + 'static,
{
    pub fn snapshot(&self) -> Vec<ContainerEntry<T>> {
        self.entries
            .lock()
            .expect("container mutex poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn get(&self, query: &T) -> Option<ContainerEntry<T>> {
        let key = encode_key_bytes(query);
        self.entries
            .lock()
            .expect("container mutex poisoned")
            .get(&key)
            .cloned()
    }
}

impl<T> Drop for Container<T> {
    fn drop(&mut self) {
        if let Some(dispatch) = self.dispatch.upgrade() {
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .unregister(&self.type_name, self.id);
        }
    }
}

// ===== Container construction (called from Connection::container) =====

/// Build a [`Container<T>`] and register its backing dispatch entry
/// with the connection's `DispatchState`.
pub(crate) fn make_container<T>(dispatch: &Arc<Mutex<DispatchState>>) -> Container<T>
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
    Container {
        entries,
        type_name,
        id,
        dispatch: Arc::downgrade(dispatch),
        leaver: None,
        _phantom: PhantomData,
    }
}

impl<T> Container<T> {
    /// Attach a leaver — called by `GuestTransceiver::container` after
    /// `make_container` so that dropping this container publishes
    /// `DotsMember(Leave)` once the per-type subscriber count drops
    /// to zero.
    pub(crate) fn set_leaver(&mut self, leaver: crate::connection::GroupLeaver) {
        self.leaver = Some(leaver);
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
        let bytes = txn.payload.encode();
        let value: T = decode_typed_from_slice(&bytes)?;
        let key = encode_key_bytes(&value);

        let mut entries = self.entries.lock().expect("container mutex poisoned");
        if txn.header.remove_obj == Some(true) {
            entries.remove(&key);
            return Ok(true);
        }

        // Determine create vs. update by checking for an existing
        // entry. If it exists, preserve its created_* metadata.
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
        Ok(true)
    }
}
