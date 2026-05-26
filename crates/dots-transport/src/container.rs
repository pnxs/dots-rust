//! Type-erased + typed-view local cache mirrors for DOTS instances.
//!
//! Mirrors dots-cpp's `Container<type::Struct>` + `Container<T>` split:
//!
//! - [`DynContainer`] is the actual storage. It holds
//!   `BTreeMap<key_bytes, DynContainerEntry>` where each entry is a
//!   [`DynamicStruct`] (the runtime-described, type-erased payload)
//!   plus [`CloneInfo`]. The dispatcher inserts incoming transmissions
//!   here knowing only the descriptor — no compile-time `T`.
//! - [`Container<T>`] is a thin handle: `Arc<DynContainer>` +
//!   `PhantomData<T>`. Typed reads (`snapshot`, `get`, `with_entries`)
//!   decode the stored `DynamicStruct` into `T` on access.
//!
//! The split lets [`crate::GuestTransceiver`] pre-create empty
//! containers for every entry in `SUBSCRIBED_TYPES` during the
//! EarlySubscribe phase (it has the descriptors but not the typed
//! `T`). Cache replay events flow into those containers through the
//! normal dispatch path; a later `dots::container::<T>()` returns a
//! typed view of the already-populated container.
//!
//! Created via
//! [`GuestTransceiver::container`](crate::GuestTransceiver::container)
//! (pool-managed) or [`Connection::container`](crate::Connection::container)
//! (raw, no transceiver).

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, Weak};

use dots_core::{
    DynamicStruct, DynamicStructDescriptor, StructValue, Timepoint, decode_typed_from_slice,
    encode_key_bytes,
};
use dots_model::Transmission;

use crate::connection::{DispatchEntry, DispatchState, GroupLeaver};

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
    /// Entry was removed. Stored entries never carry this value;
    /// it's reserved for future events-stream usage.
    Remove,
}

/// Type-erased entry as held in [`DynContainer`]'s storage.
///
/// The value is a [`DynamicStruct`] (runtime-described). Typed views
/// decode it to `T` on access via
/// [`Container<T>::snapshot`] / [`get`](Container::get) /
/// [`with_entries`](Container::with_entries).
#[derive(Debug, Clone)]
pub struct DynContainerEntry {
    pub value: DynamicStruct,
    pub clone_info: CloneInfo,
}

/// One decoded entry from a [`Container<T>`].
///
/// Returned by [`Container<T>::snapshot`] / [`get`](Container::get)
/// and as the values in the map passed to
/// [`with_entries`](Container::with_entries). Each is produced by
/// decoding the corresponding [`DynContainerEntry::value`]
/// (a [`DynamicStruct`]) into `T`.
#[derive(Debug, Clone)]
pub struct ContainerEntry<T> {
    pub value: T,
    pub clone_info: CloneInfo,
}

type Entries = BTreeMap<Vec<u8>, DynContainerEntry>;

/// Type-erased local cache mirror. Owns the storage; a typed
/// [`Container<T>`] is a thin handle over `Arc<DynContainer>`.
///
/// One `DynContainer` exists per descriptor in the transceiver's
/// pool — matching dots-cpp's `Container<type::Struct>`. The
/// dispatcher inserts incoming transmissions by looking up the
/// type name in the pool and applying the update directly here.
pub struct DynContainer {
    descriptor: Arc<DynamicStructDescriptor>,
    entries: Mutex<Entries>,
    /// Optional RAII leaver — publishes `DotsMember(Leave)` when this
    /// container drops. `Some` only for the
    /// [`crate::GuestTransceiver::container`] path; `None` for raw
    /// containers built via
    /// [`crate::Connection::container`].
    _leaver: Option<GroupLeaver>,
}

impl DynContainer {
    /// Construct an empty container for the given descriptor.
    pub(crate) fn new(descriptor: Arc<DynamicStructDescriptor>, leaver: Option<GroupLeaver>) -> Self {
        Self {
            descriptor,
            entries: Mutex::new(BTreeMap::new()),
            _leaver: leaver,
        }
    }

    /// The descriptor this container holds instances of.
    pub fn descriptor(&self) -> &Arc<DynamicStructDescriptor> {
        &self.descriptor
    }

    /// Number of stored entries.
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

    /// Run a closure over the type-erased storage while holding the
    /// container's read lock. Useful for tools that don't have a
    /// compile-time `T` (e.g. tracing / inspection).
    pub fn with_entries_dyn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BTreeMap<Vec<u8>, DynContainerEntry>) -> R,
    {
        let entries = self.entries.lock().expect("container mutex poisoned");
        f(&entries)
    }

    /// Apply an incoming transmission to this container. The
    /// payload's `DynamicStruct` is stored verbatim; key bytes for
    /// indexing come from
    /// [`DynamicStruct::key_bytes`](dots_core::DynamicStruct::key_bytes).
    ///
    /// `remove_obj == Some(true)` headers extract the keyed entry;
    /// otherwise the entry is inserted-or-updated with refreshed
    /// `CloneInfo`. Matches the C++ `Container<>::insert` / `remove`
    /// semantics.
    pub(crate) fn apply(&self, txn: &Transmission) {
        let key = txn.payload.key_bytes();
        let mut entries = self.entries.lock().expect("container mutex poisoned");

        if txn.header.remove_obj == Some(true) {
            entries.remove(&key);
            return;
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
            DynContainerEntry {
                value: txn.payload.clone(),
                clone_info: CloneInfo {
                    last_operation: operation,
                    last_update_time: now_time,
                    last_update_sender: now_sender,
                    created_time,
                    created_sender,
                },
            },
        );
    }
}

/// Typed view over a [`DynContainer`].
///
/// Cheaply `Clone`-able — clones share the same underlying
/// `Arc<DynContainer>` (and therefore the same backing storage).
/// Dropping a `Container<T>` clone is a no-op for the dispatch table;
/// the [`crate::GuestTransceiver`]'s pool keeps the container alive
/// for the lifetime of the transceiver, matching dots-cpp.
pub struct Container<T> {
    pub(crate) inner: Arc<DynContainer>,
    /// `PhantomData<fn() -> T>` so the container is unconditionally
    /// `Send + Sync` regardless of `T`'s auto-traits.
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Clone for Container<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Container<T> {
    /// Wrap an existing [`Arc<DynContainer>`] in a typed view.
    /// Caller is responsible for ensuring the descriptor matches `T`.
    pub(crate) fn from_dyn(inner: Arc<DynContainer>) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Direct access to the underlying type-erased container.
    /// Useful for tools (like dots-tui) that walk the container pool
    /// without `T` at compile time.
    pub fn as_dyn(&self) -> &Arc<DynContainer> {
        &self.inner
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` if no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T> Container<T>
where
    T: StructValue + Default + Send + Clone + 'static,
{
    /// Owned snapshot of all current entries, each decoded to `T`.
    /// Entries whose payload fails to decode are silently skipped.
    pub fn snapshot(&self) -> Vec<ContainerEntry<T>> {
        let entries = self
            .inner
            .entries
            .lock()
            .expect("container mutex poisoned");
        entries
            .values()
            .filter_map(|entry| decode_entry::<T>(entry))
            .collect()
    }

    /// Look up the entry whose key matches that of `query`. Only the
    /// `#[dots(key)]` properties of `query` are used; other fields are
    /// ignored. Returns a clone of the stored entry with the value
    /// decoded to `T`.
    pub fn get(&self, query: &T) -> Option<ContainerEntry<T>> {
        let key = encode_key_bytes(query);
        let entries = self
            .inner
            .entries
            .lock()
            .expect("container mutex poisoned");
        let entry = entries.get(&key)?;
        decode_entry::<T>(entry)
    }

    /// Run a closure over a typed map of the current entries while
    /// holding the container's read lock. Decodes every stored
    /// [`DynamicStruct`] into `T` to build the temporary typed map
    /// — handy for adhoc inspection without materializing a full
    /// owned `Vec` snapshot, but O(n) decode per call.
    pub fn with_entries<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BTreeMap<Vec<u8>, ContainerEntry<T>>) -> R,
    {
        let entries = self
            .inner
            .entries
            .lock()
            .expect("container mutex poisoned");
        let typed: BTreeMap<Vec<u8>, ContainerEntry<T>> = entries
            .iter()
            .filter_map(|(k, entry)| decode_entry::<T>(entry).map(|e| (k.clone(), e)))
            .collect();
        f(&typed)
    }
}

/// Decode a stored type-erased entry into a typed view.
fn decode_entry<T>(entry: &DynContainerEntry) -> Option<ContainerEntry<T>>
where
    T: StructValue + Default + Send + 'static,
{
    let bytes = entry.value.encode();
    let value: T = decode_typed_from_slice(&bytes).ok()?;
    Some(ContainerEntry {
        value,
        clone_info: entry.clone_info.clone(),
    })
}

// ===== Container construction =====

/// Build an empty [`DynContainer`] for `descriptor` and register a
/// type-erased dispatch entry that funnels matching transmissions
/// into it. The entry holds a `Weak<DynContainer>` so dropping the
/// last `Arc<DynContainer>` triggers automatic dispatch cleanup on
/// the next matching event.
///
/// `leaver` carries the optional RAII group-`Leave` guard — `Some`
/// when called from
/// [`crate::GuestTransceiver::container`], `None` when raw containers
/// are constructed via [`crate::Connection::container`] or by
/// [`crate::View`].
pub(crate) fn make_dyn_container(
    descriptor: Arc<DynamicStructDescriptor>,
    dispatch: &Arc<Mutex<DispatchState>>,
    leaver: Option<GroupLeaver>,
) -> Arc<DynContainer> {
    let container = Arc::new(DynContainer::new(descriptor.clone(), leaver));
    let entry = DynContainerDispatchEntry {
        container: Arc::downgrade(&container),
    };
    dispatch
        .lock()
        .expect("dispatch mutex poisoned")
        .register(descriptor.name.clone(), Box::new(entry));
    container
}

/// Convenience: build a typed [`Container<T>`] from `T`'s static
/// descriptor. Internally creates a [`DynContainer`] via
/// [`make_dyn_container`] and wraps it in a typed view.
///
/// Used by [`crate::Connection::container`] (raw, no transceiver
/// pool) and by [`crate::View`] (filtered subscriptions own a
/// separate container per view).
pub(crate) fn make_container<T>(
    dispatch: &Arc<Mutex<DispatchState>>,
    leaver: Option<GroupLeaver>,
) -> Container<T>
where
    T: StructValue + Default + Send + 'static,
{
    let descriptor =
        Arc::new(DynamicStructDescriptor::from_static(T::type_descriptor()));
    let inner = make_dyn_container(descriptor, dispatch, leaver);
    Container::from_dyn(inner)
}

/// Apply a transmission directly to a [`Container<T>`]'s underlying
/// [`DynContainer`] — used by [`crate::View`] for filtered
/// subscriptions, where dispatch routes by `subscription_id` rather
/// than by type name.
pub(crate) fn view_dispatch_update<T>(container: &Container<T>, txn: &Transmission)
where
    T: StructValue + Default + Send + 'static,
{
    container.inner.apply(txn);
}

/// Dispatch entry that updates a [`DynContainer`] in place. Holds a
/// `Weak<DynContainer>` so the entry self-removes (returns
/// `Ok(false)` from `dispatch`) once the container's last
/// `Arc<DynContainer>` is dropped.
struct DynContainerDispatchEntry {
    container: Weak<DynContainer>,
}

impl DispatchEntry for DynContainerDispatchEntry {
    fn dispatch(&mut self, txn: &Transmission) -> Result<bool, dots_core::DecodeError> {
        let Some(container) = self.container.upgrade() else {
            return Ok(false);
        };
        container.apply(txn);
        Ok(true)
    }
}
