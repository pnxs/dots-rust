//! Type-erased + typed-view local cache mirrors for DOTS instances.
//!
//! Mirrors dots-cpp's `Container<type::Struct>` + `Container<T>` split:
//!
//! - [`DynContainer`] is the actual storage. It holds
//!   `BTreeMap<key_bytes, DynContainerEntry>` where each entry is an
//!   [`AnyStruct`] (a heap allocation laid out exactly like the typed
//!   `T` would be) plus [`CloneInfo`]. The dispatcher inserts incoming
//!   transmissions here knowing only the descriptor — no compile-time
//!   `T`.
//! - [`Container<T>`] is a thin handle: `Arc<DynContainer>` +
//!   `PhantomData<T>`. Typed reads borrow `&T` directly out of the
//!   stored `AnyStruct`s via a descriptor-identity-checked pointer
//!   cast — no CBOR roundtrip. The borrowed accessors are
//!   [`get`](Container::get) (single entry, returns a guard-backed
//!   [`ContainerRef`]), [`for_each`](Container::for_each) (iterate
//!   while the lock is held), and [`lock`](Container::lock) (hold the
//!   read lock across several lookups via a [`ContainerReadGuard`]).
//!   [`snapshot`](Container::snapshot) stays as the explicit
//!   owned-`Vec` path.
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

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, Mutex, Weak};

use dots_core::{
    AnyStruct, StructDescriptor, StructValue, Timepoint, encode_key_bytes, encode_key_into,
};
use dots_model::{DotsHeader, Transmission};
use parking_lot::{RwLock, RwLockReadGuard};

use crate::connection::{DispatchEntry, DispatchState, GroupLeaver};

thread_local! {
    /// Reusable buffer for encoding lookup keys on the read path so
    /// point reads don't heap-allocate a fresh `Vec` per call. The key
    /// bytes are only *borrowed* for the `BTreeMap` lookup (which
    /// accepts `&[u8]` via `Borrow<[u8]>`), so a scratch buffer that
    /// lives only for the duration of the lookup is sufficient.
    static KEY_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Encode `query`'s `#[dots(key)]` properties into the thread-local
/// scratch buffer and run `f` with the borrowed key bytes. Avoids the
/// per-lookup allocation that [`encode_key_bytes`] would incur. Used
/// by the read paths ([`Container::get`], [`ContainerReadGuard::get`])
/// where the key is consumed immediately by a `BTreeMap` lookup and
/// never stored.
fn with_key_bytes<R>(query: &dyn StructValue, f: impl FnOnce(&[u8]) -> R) -> R {
    KEY_SCRATCH.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        encode_key_into(query, &mut buf);
        f(&buf)
    })
}

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
    /// Entry was removed. Stored entries never carry this value, but
    /// it is the operation reported on the [`crate::Event`] delivered to
    /// subscribers for a `remove_obj` transmission.
    Remove,
}

/// The classified outcome of an incoming transmission, as seen by a
/// subscriber. Mirrors what dots-cpp's `Dispatcher::dispatchEvent`
/// hands to an `Event<T>`: the effective [`Operation`], the post-merge
/// `updated` value (the "local clone"), and its [`CloneInfo`].
pub(crate) struct EventClassification<T> {
    pub operation: Operation,
    pub updated: T,
    pub clone_info: CloneInfo,
}

/// Classify an incoming transmission for an **uncached** subscriber —
/// one with no local container, matching dots-cpp's handling of types
/// that have no cache: the transmitted instance is also the `updated`
/// instance, and the operation is create (or remove when the header
/// flags a deletion).
///
/// Cached types are classified by the container instead, in
/// [`DynContainer::apply_and_classify`], which updates the cache first
/// and reads the dispatched instance back from it (the faithful port of
/// dots-cpp's `Dispatcher::dispatchEvent`).
pub(crate) fn classify_event<T>(header: &DotsHeader, value: &T) -> EventClassification<T>
where
    T: StructValue + Clone + Send + 'static,
{
    let operation = if header.remove_obj == Some(true) {
        Operation::Remove
    } else {
        Operation::Create
    };
    EventClassification {
        operation,
        updated: value.clone(),
        clone_info: CloneInfo {
            last_operation: operation,
            last_update_time: header.sent_time,
            last_update_sender: header.sender,
            created_time: header.sent_time,
            created_sender: header.sender,
        },
    }
}

/// Type-erased entry as held in [`DynContainer`]'s storage.
///
/// The value is an [`AnyStruct`] — a heap allocation whose layout
/// matches the typed `T` exactly. Typed views borrow `&T` directly
/// via [`AnyStruct::as_typed`].
#[derive(Clone)]
pub struct DynContainerEntry {
    pub value: AnyStruct,
    pub clone_info: CloneInfo,
}

impl core::fmt::Debug for DynContainerEntry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DynContainerEntry")
            .field("type", &self.value.descriptor().name)
            .field("clone_info", &self.clone_info)
            .finish_non_exhaustive()
    }
}

/// One owned entry cloned out of a [`Container<T>`].
///
/// Returned by [`Container::snapshot`], the explicit owned-`Vec` path.
/// The borrowed reads ([`get`](Container::get),
/// [`for_each`](Container::for_each), [`lock`](Container::lock)) hand
/// back `&T` + `&CloneInfo` directly instead, avoiding the clone.
#[derive(Debug, Clone)]
pub struct ContainerEntry<T> {
    pub value: T,
    pub clone_info: CloneInfo,
}

/// The instance a cached container produced for an incoming
/// transmission, to be delivered to event subscribers. Mirrors what
/// dots-cpp's `Container::insert` / `Container::remove` hand back to
/// `Dispatcher::dispatchEvent`: the container is updated *first*, then
/// the stored (create / update) or extracted (remove) instance is what
/// the event carries.
pub(crate) struct DispatchOutcome {
    pub operation: Operation,
    /// The post-update container instance (create / update) or the
    /// just-removed instance (remove), type-erased.
    pub updated: AnyStruct,
    pub clone_info: CloneInfo,
    /// Lazily-materialised typed `Arc<T>` of [`updated`](Self::updated),
    /// shared by every subscriber of this transmission. Type-erased so the
    /// (type-erased) container can build the outcome; the first subscriber
    /// fills it, the rest clone the `Arc`. `None` until first use.
    shared_typed: Option<Arc<dyn Any + Send + Sync>>,
}

impl DispatchOutcome {
    /// The typed `updated` instance, shared across subscribers: the first
    /// caller deep-clones it out of the type-erased [`updated`](Self::updated)
    /// and caches it; subsequent callers get a cheap `Arc` clone.
    ///
    /// `fallback` covers the impossible case of a descriptor-identity
    /// mismatch (the transmission was routed here by type name, so the
    /// stored value is a `T`); it is never expected to run.
    pub(crate) fn shared_updated<T>(&mut self, fallback: &T) -> Arc<T>
    where
        T: StructValue + Clone + Send + Sync + 'static,
    {
        if let Some(existing) = &self.shared_typed {
            if let Ok(typed) = Arc::clone(existing).downcast::<T>() {
                return typed;
            }
        }
        let typed = Arc::new(
            self.updated
                .as_typed::<T>()
                .cloned()
                .unwrap_or_else(|| fallback.clone()),
        );
        self.shared_typed = Some(Arc::clone(&typed) as Arc<dyn Any + Send + Sync>);
        typed
    }
}

/// Per-transmission hand-off from a cached container (which runs first)
/// to the event subscribers (which run after). Lets the partial-update
/// merge and create/update/remove classification happen exactly once per
/// transmission instead of once per subscriber.
pub(crate) enum DispatchShared {
    /// No cached container ran for this transmission; subscribers fall
    /// back to uncached semantics (`updated == transmitted`, create).
    Uncached,
    /// A cached container applied the update and produced the instance to
    /// dispatch.
    Cached(DispatchOutcome),
    /// A cached container applied the update but nothing should be
    /// dispatched — e.g. a remove of a key that wasn't present, which
    /// dots-cpp drops without emitting an event.
    Suppressed,
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
    descriptor: &'static StructDescriptor,
    /// Read-mostly storage: dispatch takes the write lock on each
    /// incoming transmission (bursty), while application reads
    /// (`get` / `for_each` / `snapshot` / `len`) take the read lock and
    /// run concurrently. `parking_lot::RwLock` also means the lock
    /// can't be poisoned, so a panic inside a user closure passed to
    /// [`Container::for_each`] won't brick every later access.
    entries: RwLock<Entries>,
    /// Optional RAII leaver — publishes `DotsMember(Leave)` when this
    /// container drops. `Some` only for the
    /// [`crate::GuestTransceiver::container`] path; `None` for raw
    /// containers built via
    /// [`crate::Connection::container`].
    _leaver: Option<GroupLeaver>,
}

impl DynContainer {
    /// Construct an empty container for the given descriptor.
    pub(crate) fn new(descriptor: &'static StructDescriptor, leaver: Option<GroupLeaver>) -> Self {
        Self {
            descriptor,
            entries: RwLock::new(BTreeMap::new()),
            _leaver: leaver,
        }
    }

    /// The descriptor this container holds instances of.
    pub fn descriptor(&self) -> &'static StructDescriptor {
        self.descriptor
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// `true` if no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Run a closure over the type-erased storage while holding the
    /// container's read lock. Useful for tools that don't have a
    /// compile-time `T` (e.g. tracing / inspection).
    pub fn with_entries_dyn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BTreeMap<Vec<u8>, DynContainerEntry>) -> R,
    {
        let entries = self.entries.read();
        f(&entries)
    }

    /// Apply an incoming transmission to this container. The
    /// payload's [`AnyStruct`] is stored verbatim (cloned out of the
    /// transmission); key bytes for indexing come from
    /// [`encode_key_bytes`] over the same buffer.
    ///
    /// `remove_obj == Some(true)` headers remove the keyed entry;
    /// otherwise the entry is inserted-or-updated with refreshed
    /// `CloneInfo`. Matches the C++ `Container<>::insert` / `remove`
    /// semantics.
    ///
    /// Wire-only payloads ([`dots_model::Payload::Wire`]) are silently
    /// dropped: typed containers exist only for types whose static
    /// descriptor is known, so a Wire payload here would mean the
    /// receiver subscribed to a dynamic type whose container was
    /// somehow opened — not a supported path.
    pub(crate) fn apply(&self, txn: &Transmission) {
        let dots_model::Payload::Typed(value) = &txn.payload else {
            tracing::warn!(
                type_name = txn.header.type_name.as_deref().unwrap_or("?"),
                "DynContainer dropped a Wire payload (no static descriptor)"
            );
            return;
        };
        if !core::ptr::eq(value.descriptor(), self.descriptor) {
            tracing::warn!(
                container = self.descriptor.name,
                payload = value.descriptor().name,
                "DynContainer received payload for unexpected type"
            );
            return;
        }
        let key = encode_key_bytes(value);
        let mut entries = self.entries.write();

        if txn.header.remove_obj == Some(true) {
            entries.remove(&key);
            return;
        }

        let now_sender = txn.header.sender;
        let now_time = txn.header.sent_time;
        // Single BTree traversal: the entry API locates the slot once,
        // and we read the prior `created_*` (on update) or seed it (on
        // create) from that same slot rather than a separate `get`.
        match entries.entry(key) {
            Entry::Occupied(mut slot) => {
                // Partial-update merge, matching dots-cpp
                // `Container::updateWithoutKeys`: overlay only the
                // properties the update addresses onto the existing
                // entry, in place, leaving unmentioned properties (and
                // the keys) intact. The mask is the header's
                // `attributes` — the authoritative set of properties
                // the publisher touched — falling back to the payload's
                // own valid set for peers that omit it.
                //
                // Merging in place (rather than cloning the whole
                // incoming value and replacing) reuses the existing
                // buffer and deep-clones only the changed properties;
                // `created_*` survive because we mutate the entry
                // instead of rebuilding it.
                let mask = txn.header.attributes.unwrap_or_else(|| value.valid_set());
                let entry = slot.get_mut();
                entry.value.merge_from(value, mask);
                entry.clone_info.last_operation = Operation::Update;
                entry.clone_info.last_update_time = now_time;
                entry.clone_info.last_update_sender = now_sender;
            }
            Entry::Vacant(slot) => {
                slot.insert(DynContainerEntry {
                    value: value.clone(),
                    clone_info: CloneInfo {
                        last_operation: Operation::Create,
                        last_update_time: now_time,
                        last_update_sender: now_sender,
                        created_time: now_time,
                        created_sender: now_sender,
                    },
                });
            }
        }
    }

    /// Owned-payload variant of [`apply`](Self::apply), used when the
    /// dispatcher hands this container the only copy of an incoming
    /// transmission (the read loop owns it and drops it afterward).
    ///
    /// It moves the decoded value rather than cloning it:
    /// - **create** stores the [`AnyStruct`] directly (zero deep clone);
    /// - **update** moves the changed properties out of the incoming
    ///   value into the existing entry via
    ///   [`AnyStruct::merge_take`](dots_core::AnyStruct::merge_take)
    ///   (no deep clone of `String`/`Vec` payloads).
    ///
    /// Behaviour is otherwise identical to [`apply`](Self::apply); only
    /// `Payload::Typed` reaches here (the dispatcher routes `Wire`
    /// payloads through the borrowing path).
    pub(crate) fn apply_owned(&self, txn: Transmission) {
        let dots_model::Transmission { header, payload } = txn;
        let dots_model::Payload::Typed(value) = payload else {
            tracing::warn!(
                type_name = header.type_name.as_deref().unwrap_or("?"),
                "DynContainer::apply_owned dropped a non-Typed payload"
            );
            return;
        };
        if !core::ptr::eq(value.descriptor(), self.descriptor) {
            tracing::warn!(
                container = self.descriptor.name,
                payload = value.descriptor().name,
                "DynContainer received payload for unexpected type"
            );
            return;
        }
        let key = encode_key_bytes(&value);
        let mut entries = self.entries.write();

        if header.remove_obj == Some(true) {
            entries.remove(&key);
            return;
        }

        let now_sender = header.sender;
        let now_time = header.sent_time;
        match entries.entry(key) {
            Entry::Occupied(mut slot) => {
                let mask = header.attributes.unwrap_or_else(|| value.valid_set());
                // Move the changed properties out of the owned incoming
                // value; whatever isn't moved drops with `value`.
                let mut value = value;
                let entry = slot.get_mut();
                entry.value.merge_take(&mut value, mask);
                entry.clone_info.last_operation = Operation::Update;
                entry.clone_info.last_update_time = now_time;
                entry.clone_info.last_update_sender = now_sender;
            }
            Entry::Vacant(slot) => {
                // Move the whole decoded value in — no clone.
                slot.insert(DynContainerEntry {
                    value,
                    clone_info: CloneInfo {
                        last_operation: Operation::Create,
                        last_update_time: now_time,
                        last_update_sender: now_sender,
                        created_time: now_time,
                        created_sender: now_sender,
                    },
                });
            }
        }
    }

    /// Apply `txn` to the container and return the instance to dispatch to
    /// event subscribers — the faithful port of dots-cpp's
    /// `Container::insert` / `Container::remove` feeding
    /// `Dispatcher::dispatchEvent`: the container is updated *first*, then
    /// the stored (create / update) or just-extracted (remove) instance is
    /// what the event carries, with the operation taken from the entry's
    /// refreshed [`CloneInfo`].
    ///
    /// Returns `None` when nothing should be dispatched: a remove of a key
    /// that isn't present (dots-cpp emits no event in that case) or a
    /// payload that doesn't belong to this container.
    pub(crate) fn apply_and_classify(&self, txn: &Transmission) -> Option<DispatchOutcome> {
        let dots_model::Payload::Typed(value) = &txn.payload else {
            return None;
        };
        if !core::ptr::eq(value.descriptor(), self.descriptor) {
            return None;
        }
        let key = encode_key_bytes(value);
        let now_sender = txn.header.sender;
        let now_time = txn.header.sent_time;
        let mut entries = self.entries.write();

        if txn.header.remove_obj == Some(true) {
            // Extract the entry so the dispatched instance is the
            // last-known value merged with the (key-only) remove payload;
            // an absent key emits no event, matching dots-cpp.
            let mut removed = entries.remove(&key)?;
            let mask = txn.header.attributes.unwrap_or_else(|| value.valid_set());
            removed.value.merge_from(value, mask);
            removed.clone_info.last_operation = Operation::Remove;
            removed.clone_info.last_update_time = now_time;
            removed.clone_info.last_update_sender = now_sender;
            return Some(DispatchOutcome {
                operation: Operation::Remove,
                updated: removed.value,
                clone_info: removed.clone_info,
                shared_typed: None,
            });
        }

        match entries.entry(key) {
            Entry::Occupied(mut slot) => {
                let mask = txn.header.attributes.unwrap_or_else(|| value.valid_set());
                let entry = slot.get_mut();
                entry.value.merge_from(value, mask);
                entry.clone_info.last_operation = Operation::Update;
                entry.clone_info.last_update_time = now_time;
                entry.clone_info.last_update_sender = now_sender;
                Some(DispatchOutcome {
                    operation: Operation::Update,
                    updated: entry.value.clone(),
                    clone_info: entry.clone_info.clone(),
                    shared_typed: None,
                })
            }
            Entry::Vacant(slot) => {
                let stored = slot.insert(DynContainerEntry {
                    value: value.clone(),
                    clone_info: CloneInfo {
                        last_operation: Operation::Create,
                        last_update_time: now_time,
                        last_update_sender: now_sender,
                        created_time: now_time,
                        created_sender: now_sender,
                    },
                });
                Some(DispatchOutcome {
                    operation: Operation::Create,
                    updated: stored.value.clone(),
                    clone_info: stored.clone_info.clone(),
                    shared_typed: None,
                })
            }
        }
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
    T: StructValue + Send + Sync + Clone + 'static,
{
    /// Lock the container for batched read access. Returns a
    /// [`ContainerReadGuard`] whose drop releases the lock — iterate
    /// or look up via the guard's methods, then drop it.
    ///
    /// Prefer this over [`for_each`](Self::for_each) when you want
    /// `break` / `?` semantics, or over [`get`](Self::get) when you
    /// need several lookups under the same lock.
    pub fn lock(&self) -> ContainerReadGuard<'_, T> {
        ContainerReadGuard {
            guard: self.inner.entries.read(),
            _phantom: PhantomData,
        }
    }

    /// Look up the entry whose key matches that of `query`. Only the
    /// `#[dots(key)]` properties of `query` are used; other fields are
    /// ignored.
    ///
    /// Returns a [`ContainerRef`] holding the container's read lock
    /// for as long as the borrow is alive — the caller observes `&T`
    /// directly out of the stored buffer (no clone). Clone the
    /// specific fields you need, or `(*entry).clone()` for the whole
    /// value, then drop the ref to release the lock.
    pub fn get<'a>(&'a self, query: &T) -> Option<ContainerRef<'a, T>> {
        let entries = self.inner.entries.read();
        // `with_key_bytes` borrows a thread-local scratch buffer only
        // for the lookup; the returned `&DynContainerEntry` borrows
        // `entries`, not the scratch, so it outlives the closure.
        let entry = with_key_bytes(query, |key| entries.get(key))?;
        // SAFETY: while `entries` (the read guard) is held, the
        // BTreeMap's entries (and therefore the AnyStruct buffer at
        // this key) are stable. The `&T` reflects the buffer's
        // contents; the pointer remains valid for the guard's lifetime.
        let value: &T = entry
            .value
            .as_typed::<T>()
            .expect("container stored value descriptor must match T");
        let value_ptr: *const T = value;
        let clone_info_ptr: *const CloneInfo = &entry.clone_info;
        Some(ContainerRef {
            value: value_ptr,
            clone_info: clone_info_ptr,
            _guard: entries,
            _phantom: PhantomData,
        })
    }

    /// Iterate every stored entry while holding the container's read
    /// lock. The closure receives `(key_bytes, &T, &CloneInfo)` — all
    /// borrowed, no clones. Returning from the closure releases the
    /// lock.
    ///
    /// Use [`snapshot`](Self::snapshot) instead if you need to drop
    /// the lock before processing the entries.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &T, &CloneInfo),
    {
        let entries = self.inner.entries.read();
        for (k, entry) in entries.iter() {
            let value: &T = entry
                .value
                .as_typed::<T>()
                .expect("container stored value descriptor must match T");
            f(k.as_slice(), value, &entry.clone_info);
        }
    }

    /// Owned snapshot of all current entries, each cloned out of the
    /// stored `AnyStruct` via a pointer cast. Drops the lock before
    /// returning — useful when the caller wants to process entries
    /// without blocking the dispatch path.
    pub fn snapshot(&self) -> Vec<ContainerEntry<T>> {
        let entries = self.inner.entries.read();
        entries
            .values()
            .map(|entry| {
                let value: &T = entry
                    .value
                    .as_typed::<T>()
                    .expect("container stored value descriptor must match T");
                ContainerEntry {
                    value: value.clone(),
                    clone_info: entry.clone_info.clone(),
                }
            })
            .collect()
    }
}

/// Borrowed view into a single container entry.
///
/// Holds the container's read-lock guard so the underlying buffer
/// can't be mutated or dropped while the borrow is alive. Drops the
/// lock when it goes out of scope. Implements [`Deref`] to `&T` so
/// field access reads like `entry.field` — clone the value (or only
/// the fields you need) before dropping the ref if you want to own
/// anything past the borrow.
///
/// **Hold it briefly.** While a `ContainerRef` is alive it pins the
/// container's read lock, which blocks the dispatch path (a writer)
/// from applying further updates to *this* type. Never hold one
/// across an `.await` or any long-running work — read what you need,
/// clone it out, and drop the ref. (Other readers can proceed
/// concurrently; only writers block.)
pub struct ContainerRef<'a, T> {
    /// Pointer into the BTreeMap entry's `AnyStruct` buffer. Valid
    /// for the lifetime of `_guard` (the read lock pins the entry).
    value: *const T,
    /// Pointer to the `CloneInfo` sitting next to `value` in the
    /// same `DynContainerEntry`.
    clone_info: *const CloneInfo,
    /// Guard listed last so it drops *after* the raw-pointer fields
    /// (the fields themselves have no Drop, but order encodes intent
    /// — pointers should never outlive the guard).
    _guard: RwLockReadGuard<'a, Entries>,
    /// Encodes the `'a` lifetime + `T` covariance so the borrow
    /// checker treats `&self -> &T` correctly.
    _phantom: PhantomData<&'a T>,
}

impl<T> ContainerRef<'_, T> {
    /// Metadata recorded by the container when this entry was last
    /// inserted or updated.
    pub fn clone_info(&self) -> &CloneInfo {
        // SAFETY: pointer was taken from a `&CloneInfo` borrowed
        // through the still-held read guard, so the pointee is
        // valid for `&self`'s lifetime.
        #[allow(unsafe_code)]
        unsafe {
            &*self.clone_info
        }
    }
}

impl<T> Deref for ContainerRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: same reasoning as `clone_info` — pointer is into
        // an `AnyStruct` buffer that the read guard pins.
        #[allow(unsafe_code)]
        unsafe {
            &*self.value
        }
    }
}

/// Read guard returned by [`Container::lock`].
///
/// Holds the container's read lock for the guard's lifetime — iterate or
/// look up via its methods, then drop the guard to release the lock.
/// Dispatch updates to this container block while the guard is alive.
pub struct ContainerReadGuard<'a, T> {
    guard: RwLockReadGuard<'a, Entries>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> ContainerReadGuard<'_, T>
where
    T: StructValue + 'static,
{
    /// Borrowed iterator over `(key_bytes, &T, &CloneInfo)` tuples.
    /// No allocation, no clone — values are read directly from the
    /// stored `AnyStruct` buffers.
    pub fn iter(&self) -> ContainerIter<'_, T> {
        ContainerIter {
            inner: self.guard.iter(),
            _phantom: PhantomData,
        }
    }

    /// Number of entries currently held.
    pub fn len(&self) -> usize {
        self.guard.len()
    }

    /// `true` if no entries are held.
    pub fn is_empty(&self) -> bool {
        self.guard.is_empty()
    }

    /// Look up the entry whose key matches `query` (only `#[dots(key)]`
    /// properties matter). Cheaper than [`Container::get`] when you
    /// already hold the guard — no second lock acquisition.
    pub fn get(&self, query: &T) -> Option<(&T, &CloneInfo)> {
        let entry = with_key_bytes(query, |key| self.guard.get(key))?;
        let value: &T = entry
            .value
            .as_typed::<T>()
            .expect("container stored value descriptor must match T");
        Some((value, &entry.clone_info))
    }
}

impl<'iter, 'guard, T> IntoIterator for &'iter ContainerReadGuard<'guard, T>
where
    T: StructValue + 'static,
{
    type Item = (&'iter [u8], &'iter T, &'iter CloneInfo);
    type IntoIter = ContainerIter<'iter, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Borrowed iterator produced by [`ContainerReadGuard::iter`].
pub struct ContainerIter<'a, T> {
    inner: std::collections::btree_map::Iter<'a, Vec<u8>, DynContainerEntry>,
    _phantom: PhantomData<fn() -> T>,
}

impl<'a, T> Iterator for ContainerIter<'a, T>
where
    T: StructValue + 'static,
{
    type Item = (&'a [u8], &'a T, &'a CloneInfo);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, entry) = self.inner.next()?;
        let value: &T = entry
            .value
            .as_typed::<T>()
            .expect("container stored value descriptor must match T");
        Some((k.as_slice(), value, &entry.clone_info))
    }
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
    descriptor: &'static StructDescriptor,
    dispatch: &Arc<Mutex<DispatchState>>,
    leaver: Option<GroupLeaver>,
) -> Arc<DynContainer> {
    let container = Arc::new(DynContainer::new(descriptor, leaver));
    let entry = DynContainerDispatchEntry {
        container: Arc::downgrade(&container),
    };
    dispatch
        .lock()
        .expect("dispatch mutex poisoned")
        .register(descriptor.name.into(), Box::new(entry));
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
    T: StructValue + Send + Sync + 'static,
{
    let inner = make_dyn_container(T::type_descriptor(), dispatch, leaver);
    Container::from_dyn(inner)
}

/// Apply a transmission directly to a [`Container<T>`]'s underlying
/// [`DynContainer`] — used by [`crate::View`] for filtered
/// subscriptions, where dispatch routes by `subscription_id` rather
/// than by type name.
pub(crate) fn view_dispatch_update<T>(container: &Container<T>, txn: &Transmission)
where
    T: StructValue + Send + Sync + 'static,
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
    fn dispatch(
        &mut self,
        txn: &Transmission,
        has_subscribers: bool,
        shared: &mut DispatchShared,
    ) -> Result<bool, dots_core::DecodeError> {
        let Some(container) = self.container.upgrade() else {
            return Ok(false);
        };
        if has_subscribers {
            // Update the cache, then publish the instance to dispatch so
            // the subscribers (which run after) emit it — dots-cpp's
            // update-then-dispatch order.
            *shared = match container.apply_and_classify(txn) {
                Some(outcome) => DispatchShared::Cached(outcome),
                None => DispatchShared::Suppressed,
            };
        } else {
            // Pure cache mirror, no event subscribers: just apply.
            container.apply(txn);
        }
        Ok(true)
    }

    /// A container stores the value long-term, so it's the natural
    /// owner of an incoming transmission on the move-capable dispatch
    /// path.
    fn wants_owned(&self) -> bool {
        true
    }

    fn dispatch_owned(
        &mut self,
        txn: Transmission,
        has_subscribers: bool,
        shared: &mut DispatchShared,
    ) -> Result<bool, dots_core::DecodeError> {
        let Some(container) = self.container.upgrade() else {
            return Ok(false);
        };
        if has_subscribers {
            // Subscribers need the transmission for `transmitted`, so the
            // payload can't be moved into the container here — classify by
            // borrow. (The owned-move fast path below only runs when the
            // container is the sole consumer.)
            *shared = match container.apply_and_classify(&txn) {
                Some(outcome) => DispatchShared::Cached(outcome),
                None => DispatchShared::Suppressed,
            };
        } else if matches!(txn.payload, dots_model::Payload::Typed(_)) {
            // `apply_owned` only handles `Typed`; route `Wire` (no owned
            // `AnyStruct` to move) through the borrowing path.
            container.apply_owned(txn);
        } else {
            container.apply(&txn);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod classify_tests {
    use super::*;
    use crate::connection::Event;
    use dots_core::{AnyStruct, Timepoint};
    use dots_derive::DotsStruct;
    use dots_model::Payload;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "ClassifyPinger", cached)]
    struct Pinger {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        message: Option<String>,
        #[dots(tag = 3)]
        sequence: Option<u64>,
    }

    fn header(value: &Pinger, remove: bool) -> DotsHeader {
        DotsHeader {
            attributes: Some(value.valid_set()),
            remove_obj: if remove { Some(true) } else { None },
            sender: Some(7),
            sent_time: Some(Timepoint(1.0)),
            ..Default::default()
        }
    }

    fn txn(value: &Pinger, h: &DotsHeader) -> Transmission {
        Transmission {
            header: h.clone(),
            payload: Payload::Typed(AnyStruct::from_struct_value(value)),
        }
    }

    /// `apply_and_classify` updates the container *and* hands back the
    /// instance to dispatch, taken from the (post-update) container —
    /// dots-cpp's `Container::insert` / `remove` + `dispatchEvent`.
    fn classify(c: &Arc<DynContainer>, value: &Pinger, h: &DotsHeader) -> Option<DispatchOutcome> {
        c.apply_and_classify(&txn(value, h))
    }

    fn updated(o: &DispatchOutcome) -> Pinger {
        o.updated.as_typed::<Pinger>().cloned().expect("outcome is a Pinger")
    }

    #[test]
    fn classifies_create_update_remove_against_container() {
        let c = Arc::new(DynContainer::new(Pinger::DESCRIPTOR, None));

        // Create: empty container, full value.
        let v1 = Pinger {
            id: Some(1),
            message: Some("hi".into()),
            sequence: Some(10),
        };
        let r = classify(&c, &v1, &header(&v1, false)).expect("create emits an event");
        assert_eq!(r.operation, Operation::Create);
        assert_eq!(updated(&r), v1);
        assert_eq!(r.clone_info.last_operation, Operation::Create);
        assert_eq!(r.clone_info.created_sender, Some(7));

        // Update: partial transmission (only `message`). The `updated`
        // value must reflect the merge — `sequence` survives from the
        // prior create.
        let v2 = Pinger {
            id: Some(1),
            message: Some("yo".into()),
            sequence: None,
        };
        let r = classify(&c, &v2, &header(&v2, false)).expect("update emits an event");
        assert_eq!(r.operation, Operation::Update);
        let u = updated(&r);
        assert_eq!(u.id, Some(1));
        assert_eq!(u.message.as_deref(), Some("yo"));
        assert_eq!(u.sequence, Some(10), "unmentioned property survives the merge");
        assert_eq!(r.clone_info.last_operation, Operation::Update);

        // Remove: key-only transmission. `updated` is the last-known
        // merged state so a handler can inspect what disappeared.
        let v3 = Pinger {
            id: Some(1),
            ..Default::default()
        };
        let r = classify(&c, &v3, &header(&v3, true)).expect("removing a present key emits an event");
        assert_eq!(r.operation, Operation::Remove);
        let u = updated(&r);
        assert_eq!(u.message.as_deref(), Some("yo"));
        assert_eq!(u.sequence, Some(10));
        assert_eq!(r.clone_info.last_operation, Operation::Remove);
        assert!(c.is_empty(), "the entry is gone after remove");
    }

    /// Removing a key that was never present emits no event, matching
    /// dots-cpp's `dispatchEvent` (which skips on an empty extracted node).
    #[test]
    fn remove_of_absent_key_emits_no_event() {
        let c = Arc::new(DynContainer::new(Pinger::DESCRIPTOR, None));
        let v = Pinger {
            id: Some(99),
            ..Default::default()
        };
        assert!(classify(&c, &v, &header(&v, true)).is_none());
    }

    #[test]
    fn no_container_is_always_create() {
        let v = Pinger {
            id: Some(1),
            message: Some("hi".into()),
            sequence: Some(10),
        };
        let h = header(&v, false);
        let r = classify_event::<Pinger>(&h, &v);
        assert_eq!(r.operation, Operation::Create);
        assert_eq!(r.updated, v);
    }

    /// On a partial update, `Event::transmitted()` is the (sparse) wire
    /// instance the publisher sent, while `Event::updated()` is the
    /// merged local clone — so the two genuinely differ.
    #[test]
    fn event_transmitted_differs_from_updated_on_partial_update() {
        let c = Arc::new(DynContainer::new(Pinger::DESCRIPTOR, None));

        // Seed the container with a full instance.
        let create = Pinger {
            id: Some(1),
            message: Some("hi".into()),
            sequence: Some(10),
        };
        c.apply(&txn(&create, &header(&create, false)));

        // A partial update touching only `message` (no `sequence`).
        let partial = Pinger {
            id: Some(1),
            message: Some("bye".into()),
            sequence: None,
        };
        let outcome = classify(&c, &partial, &header(&partial, false)).expect("update event");
        let mut shared = DispatchShared::Cached(outcome);
        let ev = Event::from_shared(header(&partial, false), partial.clone(), true, &mut shared)
            .expect("update is dispatched");

        assert!(ev.is_update());
        // `transmitted()` is exactly what arrived on the wire — sparse.
        assert_eq!(ev.transmitted(), &partial);
        assert_eq!(ev.transmitted().sequence, None);
        // `updated()` is the merged local clone — it carries the
        // surviving `sequence` and the new `message`.
        assert_eq!(ev.updated().message.as_deref(), Some("bye"));
        assert_eq!(ev.updated().sequence, Some(10));
        // The whole point: the two views are not the same.
        assert_ne!(ev.transmitted(), ev.updated());
    }

    /// On a remove, `Event::updated()` carries the container's content as
    /// it was *prior to* the remove (the last-known state), not the
    /// key-only wire payload that triggered the deletion.
    #[test]
    fn event_updated_holds_pre_remove_container_content() {
        let c = Arc::new(DynContainer::new(Pinger::DESCRIPTOR, None));

        // Create, then update, so the container holds the latest state.
        let create = Pinger {
            id: Some(1),
            message: Some("hi".into()),
            sequence: Some(10),
        };
        c.apply(&txn(&create, &header(&create, false)));
        let update = Pinger {
            id: Some(1),
            message: Some("latest".into()),
            sequence: Some(20),
        };
        c.apply(&txn(&update, &header(&update, false)));

        // Capture the exact container content right before the remove.
        let pre_remove = c.with_entries_dyn(|entries| {
            entries
                .values()
                .next()
                .expect("one entry present")
                .value
                .as_typed::<Pinger>()
                .cloned()
                .expect("entry is a Pinger")
        });

        // Remove via a key-only transmission, as a real broker sends.
        let key_only = Pinger {
            id: Some(1),
            ..Default::default()
        };
        let outcome = classify(&c, &key_only, &header(&key_only, true)).expect("remove event");
        let mut shared = DispatchShared::Cached(outcome);
        let ev = Event::from_shared(header(&key_only, true), key_only.clone(), true, &mut shared)
            .expect("remove is dispatched");

        assert!(ev.is_remove());
        // The transmitted payload is key-only — the non-key fields are
        // absent, so it is NOT the pre-remove content.
        assert_eq!(ev.transmitted().message, None);
        assert_eq!(ev.transmitted().sequence, None);
        // `updated()` reproduces the container content prior to remove.
        assert_eq!(ev.updated(), &pre_remove);
        assert_eq!(ev.updated().message.as_deref(), Some("latest"));
        assert_eq!(ev.updated().sequence, Some(20));
    }
}
