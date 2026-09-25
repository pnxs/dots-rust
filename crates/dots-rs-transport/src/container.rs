//! Type-erased + typed-view local cache mirrors for DOTS instances.
//!
//! Mirrors dots-cpp's `Container<type::Struct>` + `Container<T>` split:
//!
//! - [`DynContainer`] is the actual storage. It holds
//!   `BTreeMap<key_bytes, DynContainerEntry>` where each entry is a
//!   [`ContainerValue`] plus [`CloneInfo`]. Built over either
//!   descriptor kind (see [`ContainerDescriptor`]): a static
//!   descriptor stores [`AnyStruct`] entries (heap allocations laid
//!   out exactly like the typed `T`), a runtime-described one stores
//!   [`DynamicStruct`] entries — the cache behind `subscribe_dynamic`
//!   on wire-learned cached types. The dispatcher inserts incoming
//!   transmissions here knowing only the descriptor — no compile-time
//!   `T`.
//! - [`Container<T>`] is a thin handle: `Arc<DynContainer>` +
//!   `PhantomData<T>`. Typed reads borrow `&T` directly out of the
//!   stored `AnyStruct`s via a descriptor-identity-checked pointer
//!   cast — no CBOR roundtrip. The borrowed accessors are
//!   [`get`](Container::get) (single entry, returns a guard-backed
//!   [`ContainerRef`]), [`for_each`](Container::for_each) (iterate
//!   while the lock is held), [`with_iter`](Container::with_iter)
//!   (run iterator operations under the lock), and [`lock`](Container::lock) (hold the
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

use dots_rs_core::{
    AnyStruct, DynamicStruct, DynamicStructDescriptor, PropertySet, StructDescriptor, StructValue,
    Timepoint, encode_key_bytes, encode_key_into, encode_to_vec,
};
use dots_rs_model::{DotsHeader, Payload, Transmission};
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
/// Panic message for typed reads over a mismatching stored value. The
/// monotone promotion invariant (a container reachable through a typed
/// `Container<T>` view is in the static representation and stays there)
/// makes hitting this a library bug, not a user error — hence panic
/// rather than `Option` on every read path.
const TYPED_VIEW_EXPECT: &str = "typed Container over a dynamic-representation container — \
     obtain typed views via GuestTransceiver::container so promotion runs";

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

/// The descriptor a [`DynContainer`] is built over: either the
/// compile-time `&'static` descriptor a `#[derive(DotsStruct)]` type
/// emits, or a runtime-described descriptor learned from the wire
/// (descriptor exchange). The kind determines the container's storage
/// representation — see [`ContainerValue`].
#[derive(Clone)]
pub enum ContainerDescriptor {
    /// Compile-time descriptor; entries are stored as [`AnyStruct`]
    /// (layout-compatible with the typed `T`, zero-copy typed reads).
    Static(&'static StructDescriptor),
    /// Runtime-described descriptor; entries are stored as
    /// [`DynamicStruct`] (the wire-only tagged-union representation).
    Dynamic(Arc<DynamicStructDescriptor>),
}

impl ContainerDescriptor {
    /// The described type's wire name.
    pub fn name(&self) -> &str {
        match self {
            Self::Static(d) => d.name,
            Self::Dynamic(d) => &d.name,
        }
    }

    /// Whether the described type is cached (broker keeps state;
    /// containers classify create/update/remove against it).
    pub fn is_cached(&self) -> bool {
        match self {
            Self::Static(d) => d.flags.is_cached(),
            Self::Dynamic(d) => d.flags.is_cached(),
        }
    }
}

impl From<&'static StructDescriptor> for ContainerDescriptor {
    fn from(d: &'static StructDescriptor) -> Self {
        Self::Static(d)
    }
}

impl From<Arc<DynamicStructDescriptor>> for ContainerDescriptor {
    fn from(d: Arc<DynamicStructDescriptor>) -> Self {
        Self::Dynamic(d)
    }
}

impl core::fmt::Debug for ContainerDescriptor {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Static(d) => f.debug_tuple("Static").field(&d.name).finish(),
            Self::Dynamic(d) => f.debug_tuple("Dynamic").field(&d.name).finish(),
        }
    }
}

/// A stored instance, in the representation the owning container's
/// [`ContainerDescriptor`] kind mandates: `Static` containers hold
/// only `Typed` values, `Dynamic` containers only `Dynamic` values.
/// Incoming payloads of the other representation are converted on the
/// way in (see `convert_payload`), so the variants never mix within
/// one container.
#[derive(Clone, Debug)]
pub enum ContainerValue {
    /// Layout-compatible heap buffer; typed views borrow `&T` from it.
    Typed(AnyStruct),
    /// Wire-only tagged-union representation.
    Dynamic(DynamicStruct),
}

impl ContainerValue {
    /// Borrow the stored value as `&T`. `None` for the `Dynamic`
    /// representation (no typed layout to cast to).
    pub fn as_typed<T: StructValue>(&self) -> Option<&T> {
        match self {
            Self::Typed(a) => a.as_typed::<T>(),
            Self::Dynamic(_) => None,
        }
    }

    /// Borrow the stored value as a [`DynamicStruct`]. `None` for the
    /// `Typed` representation (project via CBOR instead if needed).
    pub fn as_dynamic(&self) -> Option<&DynamicStruct> {
        match self {
            Self::Typed(_) => None,
            Self::Dynamic(d) => Some(d),
        }
    }

    /// The stored type's wire name.
    pub fn type_name(&self) -> &str {
        match self {
            Self::Typed(a) => a.descriptor().name,
            Self::Dynamic(d) => &d.descriptor.name,
        }
    }

    /// Masked partial-update copy, cloning the addressed properties
    /// in. Both sides are same-variant by the container's
    /// representation invariant; a cross-variant call is a library bug
    /// and degrades to a no-op.
    fn copy_from(&mut self, src: &ContainerValue, mask: PropertySet) {
        match (self, src) {
            (Self::Typed(dst), Self::Typed(s)) => dst.copy_from(s, mask),
            (Self::Dynamic(dst), Self::Dynamic(s)) => dst.copy_from(s, mask),
            _ => debug_assert!(false, "container representation invariant violated in copy_from"),
        }
    }

    /// Masked partial-update copy that **moves** the addressed
    /// properties out of `src`. Same-variant invariant as
    /// [`copy_from`](Self::copy_from).
    fn copy_take(&mut self, src: &mut ContainerValue, mask: PropertySet) {
        match (self, src) {
            (Self::Typed(dst), Self::Typed(s)) => dst.copy_take(s, mask),
            (Self::Dynamic(dst), Self::Dynamic(s)) => dst.copy_take(s, mask),
            _ => debug_assert!(false, "container representation invariant violated in copy_take"),
        }
    }
}

/// A payload admitted into a container, borrowed when its
/// representation already matches the container's and owned when it
/// had to be converted. Keeps the hot path (matching representation)
/// clone-free: updates merge straight from the borrow, and only a
/// create clones the value in.
enum Converted<'a> {
    BorrowedTyped(&'a AnyStruct),
    BorrowedDynamic(&'a DynamicStruct),
    Owned(ContainerValue),
}

impl Converted<'_> {
    /// Merge this payload's masked properties into an existing entry.
    fn merge_into(&self, dst: &mut ContainerValue, mask: PropertySet) {
        match (dst, self) {
            (ContainerValue::Typed(d), Self::BorrowedTyped(s)) => d.copy_from(*s, mask),
            (ContainerValue::Dynamic(d), Self::BorrowedDynamic(s)) => d.copy_from(s, mask),
            (d, Self::Owned(s)) => d.copy_from(s, mask),
            _ => debug_assert!(false, "container representation invariant violated in merge_into"),
        }
    }

    /// Take ownership for a create insert (clones the borrowed cases).
    fn into_value(self) -> ContainerValue {
        match self {
            Self::BorrowedTyped(a) => ContainerValue::Typed(a.clone()),
            Self::BorrowedDynamic(d) => ContainerValue::Dynamic(d.clone()),
            Self::Owned(v) => v,
        }
    }
}

/// Admit `payload` into a container described by `descriptor`:
/// identity-check it, and convert it to the container's representation
/// when it arrived in the other one. Cross-representation conversion
/// goes through a descriptor-driven CBOR roundtrip **against the
/// container's own descriptor** — for the `Dynamic` side this keeps
/// every stored entry sharing the container's `Arc` (never
/// [`DynamicStruct::from_struct_value`], which mints a fresh `Arc` per
/// call and breaks pointer-identity equality of stored values).
///
/// Returns `None` (with a warning) for foreign types and conversion
/// failures; the transmission is dropped for this container.
fn convert_payload<'a>(
    payload: &'a Payload,
    descriptor: &ContainerDescriptor,
) -> Option<Converted<'a>> {
    let payload_name = match payload {
        Payload::Typed(a) => a.descriptor().name,
        Payload::Wire(w) => &w.descriptor.name,
    };
    if payload_name != descriptor.name() {
        tracing::warn!(
            container = descriptor.name(),
            payload = payload_name,
            "DynContainer received payload for unexpected type"
        );
        return None;
    }
    match (descriptor, payload) {
        (ContainerDescriptor::Static(d), Payload::Typed(a)) => {
            if core::ptr::eq(a.descriptor(), *d) {
                Some(Converted::BorrowedTyped(a))
            } else {
                // Same name, different static descriptor — cannot
                // happen (one 'static descriptor per derived type).
                tracing::warn!(
                    container = d.name,
                    "DynContainer payload static descriptor identity mismatch"
                );
                None
            }
        }
        (ContainerDescriptor::Dynamic(d), Payload::Wire(w)) => {
            if Arc::ptr_eq(&w.descriptor, d) {
                Some(Converted::BorrowedDynamic(w))
            } else {
                // Structurally the same type but a different descriptor
                // instance (registry re-registration) — normalise onto
                // the container's Arc via re-decode.
                decode_dynamic(&w.encode(), d).map(Converted::Owned)
            }
        }
        (ContainerDescriptor::Static(d), Payload::Wire(w)) => {
            decode_typed(&w.encode(), d).map(Converted::Owned)
        }
        (ContainerDescriptor::Dynamic(d), Payload::Typed(a)) => {
            decode_dynamic(&encode_to_vec(a), d).map(Converted::Owned)
        }
    }
}

/// Owned-payload variant of [`convert_payload`]: moves the value
/// through when the representation matches, converts otherwise.
fn convert_payload_owned(
    payload: Payload,
    descriptor: &ContainerDescriptor,
) -> Option<ContainerValue> {
    match (descriptor, payload) {
        (ContainerDescriptor::Static(d), Payload::Typed(a))
            if core::ptr::eq(a.descriptor(), *d) =>
        {
            Some(ContainerValue::Typed(a))
        }
        (ContainerDescriptor::Dynamic(d), Payload::Wire(w)) if Arc::ptr_eq(&w.descriptor, d) => {
            Some(ContainerValue::Dynamic(w))
        }
        (_, payload) => Some(convert_payload(&payload, descriptor)?.into_value()),
    }
}

fn decode_dynamic(bytes: &[u8], descriptor: &Arc<DynamicStructDescriptor>) -> Option<ContainerValue> {
    match DynamicStruct::decode(descriptor.clone(), bytes) {
        Ok(d) => Some(ContainerValue::Dynamic(d)),
        Err(e) => {
            tracing::warn!(
                container = %descriptor.name,
                error = %e,
                "DynContainer dropped payload that failed dynamic conversion"
            );
            None
        }
    }
}

fn decode_typed(bytes: &[u8], descriptor: &'static StructDescriptor) -> Option<ContainerValue> {
    match AnyStruct::decode_from_slice(descriptor, bytes) {
        Ok(a) => Some(ContainerValue::Typed(a)),
        Err(e) => {
            tracing::warn!(
                container = descriptor.name,
                error = %e,
                "DynContainer dropped payload that failed typed conversion"
            );
            None
        }
    }
}

/// Type-erased entry as held in [`DynContainer`]'s storage.
///
/// The value is a [`ContainerValue`] in the representation the
/// container's [`ContainerDescriptor`] kind mandates — [`AnyStruct`]
/// under a static descriptor (typed views borrow `&T` directly via
/// [`AnyStruct::as_typed`]), [`DynamicStruct`] under a runtime one.
#[derive(Clone)]
pub struct DynContainerEntry {
    pub value: ContainerValue,
    pub clone_info: CloneInfo,
}

impl core::fmt::Debug for DynContainerEntry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DynContainerEntry")
            .field("type", &self.value.type_name())
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
    /// just-removed instance (remove), type-erased, in the container's
    /// representation.
    pub updated: ContainerValue,
    pub clone_info: CloneInfo,
    /// Lazily-materialised typed `Arc<T>` of [`updated`](Self::updated),
    /// shared by every typed subscriber of this transmission. Type-erased
    /// so the (type-erased) container can build the outcome; the first
    /// subscriber fills it, the rest clone the `Arc`. `None` until first
    /// use.
    shared_typed: Option<Arc<dyn Any + Send + Sync>>,
    /// Lazily-materialised `Arc<DynamicStruct>` of `updated`, shared by
    /// every *dynamic* subscriber of this transmission. A separate slot
    /// from `shared_typed` so mixed typed + dynamic subscriber sets don't
    /// evict each other's projection.
    shared_dynamic: Option<Arc<DynamicStruct>>,
}

impl DispatchOutcome {
    /// The typed `updated` instance, shared across subscribers: the first
    /// caller deep-clones it out of the type-erased [`updated`](Self::updated)
    /// and caches it; subsequent callers get a cheap `Arc` clone.
    ///
    /// `fallback` covers the impossible cases — a descriptor-identity
    /// mismatch (the transmission was routed here by type name, so the
    /// stored value is a `T`) or a dynamic-representation container (a
    /// typed subscriber's existence implies the typed pool path ran and
    /// promoted the container to static). Never expected to run.
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

    /// The `updated` instance as a [`DynamicStruct`], shared across the
    /// transmission's dynamic subscribers: the first caller materialises
    /// it (an `Arc`-bump for dynamic-representation containers, one CBOR
    /// projection for static ones) and caches it; the rest clone the
    /// `Arc`.
    ///
    /// `descriptor` is the subscriber's runtime descriptor — projections
    /// from a `Typed` value decode against it so successive events of one
    /// subscription share a single descriptor `Arc` (pointer-identity
    /// equality between event values works).
    pub(crate) fn shared_dynamic(
        &mut self,
        descriptor: &Arc<DynamicStructDescriptor>,
    ) -> Arc<DynamicStruct> {
        if let Some(existing) = &self.shared_dynamic {
            return Arc::clone(existing);
        }
        let dynamic = Arc::new(match &self.updated {
            ContainerValue::Dynamic(d) => d.clone(),
            ContainerValue::Typed(a) => DynamicStruct::decode(descriptor.clone(), &encode_to_vec(a))
                .unwrap_or_else(|_| DynamicStruct::from_struct_value(a)),
        });
        self.shared_dynamic = Some(Arc::clone(&dynamic));
        dynamic
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

/// A [`DynContainer`]'s lock-guarded interior: the descriptor and the
/// entries live behind ONE `RwLock` so [`DynContainer::promote_to_static`]
/// can swap both atomically — no reader or dispatcher ever observes a
/// mixed representation.
pub(crate) struct ContainerState {
    descriptor: ContainerDescriptor,
    entries: Entries,
}

/// Type-erased local cache mirror. Owns the storage; a typed
/// [`Container<T>`] is a thin handle over `Arc<DynContainer>`.
///
/// One `DynContainer` exists per descriptor in the transceiver's
/// pool — matching dots-cpp's `Container<type::Struct>`. The
/// dispatcher inserts incoming transmissions by looking up the
/// type name in the pool and applying the update directly here.
/// Built over either a compile-time or a runtime-described descriptor
/// (see [`ContainerDescriptor`]); the descriptor kind fixes the entry
/// representation.
pub struct DynContainer {
    /// The described type's wire name — stable and lock-free (a
    /// container never changes its type, only its representation).
    name: String,
    /// Read-mostly storage: dispatch takes the write lock on each
    /// incoming transmission (bursty), while application reads
    /// (`get` / `for_each` / `snapshot` / `len`) take the read lock and
    /// run concurrently. `parking_lot::RwLock` also means the lock
    /// can't be poisoned, so a panic inside a user closure passed to
    /// [`Container::for_each`] won't brick every later access.
    state: RwLock<ContainerState>,
    /// Optional RAII leaver — publishes `DotsMember(Leave)` when this
    /// container drops. `Some` only for the
    /// [`crate::GuestTransceiver::container`] path; `None` for raw
    /// containers built via
    /// [`crate::Connection::container`].
    _leaver: Option<GroupLeaver>,
}

impl DynContainer {
    /// Construct an empty container for the given descriptor.
    pub(crate) fn new(
        descriptor: impl Into<ContainerDescriptor>,
        leaver: Option<GroupLeaver>,
    ) -> Self {
        let descriptor = descriptor.into();
        Self {
            name: descriptor.name().to_string(),
            state: RwLock::new(ContainerState {
                descriptor,
                entries: BTreeMap::new(),
            }),
            _leaver: leaver,
        }
    }

    /// The described type's wire name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The descriptor this container holds instances of. Cheap clone
    /// (`&'static` ref or `Arc` bump) taken under the read lock — the
    /// kind can change once via `promote_to_static` (crate-internal).
    pub fn descriptor(&self) -> ContainerDescriptor {
        self.state.read().descriptor.clone()
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.state.read().entries.len()
    }

    /// `true` if no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.state.read().entries.is_empty()
    }

    /// Run a closure over the type-erased storage while holding the
    /// container's read lock. Useful for tools that don't have a
    /// compile-time `T` (e.g. tracing / inspection).
    pub fn with_entries_dyn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BTreeMap<Vec<u8>, DynContainerEntry>) -> R,
    {
        let state = self.state.read();
        f(&state.entries)
    }

    /// Run `f` over borrowed `(key_bytes, &ContainerValue, &CloneInfo)`
    /// entries under one read lock, without cloning or converting values.
    /// Works with both dynamic and static storage, including promoted
    /// containers: inspect [`ContainerValue`] to access the representation.
    ///
    /// The lock is released before returning. Borrowed entries cannot escape
    /// the closure; return owned results instead. Keep the closure brief,
    /// avoid re-entering this container, and do I/O after it returns.
    ///
    /// ```compile_fail
    /// use dots_rs_transport::{ContainerValue, DynContainer};
    /// fn escape(container: &DynContainer) -> Option<&ContainerValue> {
    ///     container.with_iter(|mut iter| iter.next().map(|(_, value, _)| value))
    /// }
    /// ```
    pub fn with_iter<F, R>(&self, f: F) -> R
    where
        F: for<'entry> FnOnce(DynContainerIter<'entry>) -> R,
    {
        let state = self.state.read();
        f(DynContainerIter {
            inner: state.entries.iter(),
        })
    }

    /// Convert a dynamic-representation container to the static
    /// representation **in place**: every stored [`DynamicStruct`] is
    /// re-decoded into an [`AnyStruct`] and the descriptor swaps to
    /// `Static`, all under one write lock. No-op if already static.
    ///
    /// Called by the typed pool path
    /// ([`crate::GuestTransceiver::container`]) when it finds a
    /// container that `subscribe_dynamic` created first from a
    /// wire-learned descriptor. Promotion is monotone — a container
    /// never goes back to the dynamic representation — which is what
    /// lets typed read paths `expect` the `Typed` variant. Callers
    /// must not hold the pool mutex.
    pub(crate) fn promote_to_static(&self, descriptor: &'static StructDescriptor) {
        let mut state = self.state.write();
        if matches!(state.descriptor, ContainerDescriptor::Static(_)) {
            return;
        }
        debug_assert_eq!(state.descriptor.name(), descriptor.name);
        state.entries.retain(|key, entry| {
            let ContainerValue::Dynamic(d) = &entry.value else {
                return true;
            };
            match AnyStruct::decode_from_slice(descriptor, &d.encode()) {
                Ok(a) => {
                    debug_assert_eq!(
                        *key,
                        encode_key_bytes(&a),
                        "key bytes must be representation-independent"
                    );
                    entry.value = ContainerValue::Typed(a);
                    true
                }
                Err(e) => {
                    tracing::warn!(
                        container = %self.name,
                        error = %e,
                        "dropping entry that failed static promotion"
                    );
                    false
                }
            }
        });
        state.descriptor = ContainerDescriptor::Static(descriptor);
    }

    /// Apply an incoming transmission to this container. Payloads in
    /// the container's representation are stored verbatim (cloned out
    /// of the transmission); the other representation is converted on
    /// the way in (see [`convert_payload`]). Key bytes for indexing
    /// come from [`Payload::key_bytes`], which is
    /// representation-independent.
    ///
    /// `remove_obj == Some(true)` headers remove the keyed entry;
    /// otherwise the entry is inserted-or-updated with refreshed
    /// `CloneInfo`. Matches the C++ `Container<>::insert` / `remove`
    /// semantics.
    pub(crate) fn apply(&self, txn: &Transmission) {
        // Write lock FIRST: conversion consults the descriptor, which
        // may only change (promotion) under this same lock.
        let mut state = self.state.write();
        let Some(value) = convert_payload(&txn.payload, &state.descriptor) else {
            return;
        };
        let key = txn.payload.key_bytes();

        if txn.header.remove_obj == Some(true) {
            state.entries.remove(&key);
            return;
        }

        let now_sender = txn.header.sender;
        let now_time = txn.header.sent_time;
        // Single BTree traversal: the entry API locates the slot once,
        // and we read the prior `created_*` (on update) or seed it (on
        // create) from that same slot rather than a separate `get`.
        match state.entries.entry(key) {
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
                let mask = txn.header.attributes.unwrap_or_else(|| txn.payload.valid_set());
                let entry = slot.get_mut();
                value.merge_into(&mut entry.value, mask);
                entry.clone_info.last_operation = Operation::Update;
                entry.clone_info.last_update_time = now_time;
                entry.clone_info.last_update_sender = now_sender;
            }
            Entry::Vacant(slot) => {
                slot.insert(DynContainerEntry {
                    value: value.into_value(),
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
    /// It moves the decoded value rather than cloning it (when the
    /// payload is already in the container's representation):
    /// - **create** stores the value directly (zero deep clone);
    /// - **update** moves the changed properties out of the incoming
    ///   value into the existing entry via `merge_take` (no deep clone
    ///   of `String`/`Vec` payloads).
    ///
    /// Cross-representation payloads are converted first (see
    /// [`convert_payload_owned`]); behaviour is otherwise identical to
    /// [`apply`](Self::apply).
    pub(crate) fn apply_owned(&self, txn: Transmission) {
        let dots_rs_model::Transmission { header, payload } = txn;
        let key = payload.key_bytes();
        let mask_fallback = payload.valid_set();
        let mut state = self.state.write();
        let Some(mut value) = convert_payload_owned(payload, &state.descriptor) else {
            return;
        };

        if header.remove_obj == Some(true) {
            state.entries.remove(&key);
            return;
        }

        let now_sender = header.sender;
        let now_time = header.sent_time;
        match state.entries.entry(key) {
            Entry::Occupied(mut slot) => {
                let mask = header.attributes.unwrap_or(mask_fallback);
                // Move the changed properties out of the owned incoming
                // value; whatever isn't moved drops with `value`.
                let entry = slot.get_mut();
                entry.value.copy_take(&mut value, mask);
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
        let mut state = self.state.write();
        let value = convert_payload(&txn.payload, &state.descriptor)?;
        let key = txn.payload.key_bytes();
        let now_sender = txn.header.sender;
        let now_time = txn.header.sent_time;

        if txn.header.remove_obj == Some(true) {
            // Extract the entry so the dispatched instance is the
            // last-known value merged with the (key-only) remove payload;
            // an absent key emits no event, matching dots-cpp.
            let mut removed = state.entries.remove(&key)?;
            let mask = txn.header.attributes.unwrap_or_else(|| txn.payload.valid_set());
            value.merge_into(&mut removed.value, mask);
            removed.clone_info.last_operation = Operation::Remove;
            removed.clone_info.last_update_time = now_time;
            removed.clone_info.last_update_sender = now_sender;
            return Some(DispatchOutcome {
                operation: Operation::Remove,
                updated: removed.value,
                clone_info: removed.clone_info,
                shared_typed: None,
                shared_dynamic: None,
            });
        }

        match state.entries.entry(key) {
            Entry::Occupied(mut slot) => {
                let mask = txn.header.attributes.unwrap_or_else(|| txn.payload.valid_set());
                let entry = slot.get_mut();
                value.merge_into(&mut entry.value, mask);
                entry.clone_info.last_operation = Operation::Update;
                entry.clone_info.last_update_time = now_time;
                entry.clone_info.last_update_sender = now_sender;
                Some(DispatchOutcome {
                    operation: Operation::Update,
                    updated: entry.value.clone(),
                    clone_info: entry.clone_info.clone(),
                    shared_typed: None,
                    shared_dynamic: None,
                })
            }
            Entry::Vacant(slot) => {
                let stored = slot.insert(DynContainerEntry {
                    value: value.into_value(),
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
                    shared_dynamic: None,
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
            guard: self.inner.state.read(),
            _phantom: PhantomData,
        }
    }

    /// Run `f` over borrowed `(key_bytes, &T, &CloneInfo)` entries under
    /// one read lock. Standard iterator operations can select and transform
    /// entries without cloning whole instances.
    ///
    /// The lock is released before returning. Borrowed entries cannot escape
    /// the closure; return owned results instead. Keep the closure brief,
    /// avoid re-entering this container, and do I/O after it returns.
    ///
    /// ```
    /// use dots_rs_core::StructValue;
    /// use dots_rs_transport::Container;
    /// fn count<T: StructValue + Send + Sync + Clone + 'static>(c: &Container<T>) -> usize {
    ///     c.with_iter(|iter| iter.filter(|(_, _, info)| info.created_sender.is_some()).count())
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// use dots_rs_core::StructValue;
    /// use dots_rs_transport::Container;
    /// fn escape<T: StructValue + Send + Sync + Clone + 'static>(c: &Container<T>) -> Option<&T> {
    ///     c.with_iter(|mut iter| iter.next().map(|(_, value, _)| value))
    /// }
    /// ```
    pub fn with_iter<F, R>(&self, f: F) -> R
    where
        F: for<'entry> FnOnce(ContainerIter<'entry, T>) -> R,
    {
        let guard = self.lock();
        f(guard.iter())
    }

    /// Borrow the entry whose key matches `query`.
    ///
    /// Compatibility alias for [`get_ref`](Self::get_ref). This method
    /// continues to return a borrowed value; prefer `get_ref` in new code
    /// to make the borrowing semantics explicit.
    pub fn get<'a>(&'a self, query: &T) -> Option<ContainerRef<'a, T>> {
        self.get_ref(query)
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
    pub fn get_ref<'a>(&'a self, query: &T) -> Option<ContainerRef<'a, T>> {
        let state = self.inner.state.read();
        // `with_key_bytes` borrows a thread-local scratch buffer only
        // for the lookup; the returned `&DynContainerEntry` borrows
        // `state`, not the scratch, so it outlives the closure.
        let entry = with_key_bytes(query, |key| state.entries.get(key))?;
        // SAFETY: while `state` (the read guard) is held, the
        // BTreeMap's entries (and therefore the AnyStruct buffer at
        // this key) are stable. The `&T` reflects the buffer's
        // contents; the pointer remains valid for the guard's lifetime.
        let value: &T = entry.value.as_typed::<T>().expect(TYPED_VIEW_EXPECT);
        let value_ptr: *const T = value;
        let clone_info_ptr: *const CloneInfo = &entry.clone_info;
        Some(ContainerRef {
            value: value_ptr,
            clone_info: clone_info_ptr,
            _guard: state,
            _phantom: PhantomData,
        })
    }

    /// Return an owned copy of the first instance matching `predicate`.
    ///
    /// Scans borrowed instances in container iteration order and stops at
    /// the first match. Only that instance is cloned. Returns `None` if
    /// nothing matches. For lookup by a complete key, prefer [`get`](Self::get).
    ///
    /// The predicate runs under the container's read lock: keep it brief
    /// and do not re-enter this container. The lock is released before
    /// returning; the result can be retained across updates or `.await`.
    pub fn find<F>(&self, mut predicate: F) -> Option<T>
    where
        F: FnMut(&T) -> bool,
    {
        let guard = self.lock();
        guard
            .iter()
            .find(|(_, value, _)| predicate(value))
            .map(|(_, value, _)| value.clone())
    }

    /// Return owned copies of all instances matching `predicate`.
    ///
    /// Scans borrowed instances once in container iteration order and
    /// clones only matches. Returns an empty vector if nothing matches.
    /// Unlike [`snapshot`](Self::snapshot), results contain only the values,
    /// without clone metadata.
    ///
    /// The predicate runs under one read lock, so all matches reflect a
    /// consistent container state. Keep it brief and do not re-enter this
    /// container. The lock is released before returning.
    pub fn filter<F>(&self, mut predicate: F) -> Vec<T>
    where
        F: FnMut(&T) -> bool,
    {
        let guard = self.lock();
        guard
            .iter()
            .filter(|(_, value, _)| predicate(value))
            .map(|(_, value, _)| value.clone())
            .collect()
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
        let state = self.inner.state.read();
        for (k, entry) in state.entries.iter() {
            let value: &T = entry.value.as_typed::<T>().expect(TYPED_VIEW_EXPECT);
            f(k.as_slice(), value, &entry.clone_info);
        }
    }

    /// Owned snapshot of all current entries, each cloned out of the
    /// stored `AnyStruct` via a pointer cast. Drops the lock before
    /// returning — useful when the caller wants to process entries
    /// without blocking the dispatch path.
    pub fn snapshot(&self) -> Vec<ContainerEntry<T>> {
        let state = self.inner.state.read();
        state
            .entries
            .values()
            .map(|entry| {
                let value: &T = entry.value.as_typed::<T>().expect(TYPED_VIEW_EXPECT);
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
    _guard: RwLockReadGuard<'a, ContainerState>,
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
    guard: RwLockReadGuard<'a, ContainerState>,
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
            inner: self.guard.entries.iter(),
            _phantom: PhantomData,
        }
    }

    /// Number of entries currently held.
    pub fn len(&self) -> usize {
        self.guard.entries.len()
    }

    /// `true` if no entries are held.
    pub fn is_empty(&self) -> bool {
        self.guard.entries.is_empty()
    }

    /// Look up the entry whose key matches `query` (only `#[dots(key)]`
    /// properties matter). Cheaper than [`Container::get`] when you
    /// already hold the guard — no second lock acquisition.
    pub fn get(&self, query: &T) -> Option<(&T, &CloneInfo)> {
        let entry = with_key_bytes(query, |key| self.guard.entries.get(key))?;
        let value: &T = entry.value.as_typed::<T>().expect(TYPED_VIEW_EXPECT);
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
        let value: &T = entry.value.as_typed::<T>().expect(TYPED_VIEW_EXPECT);
        Some((k.as_slice(), value, &entry.clone_info))
    }
}

/// Borrowed iterator supplied to [`DynContainer::with_iter`].
/// Values retain their stored representation; iteration does not convert them.
pub struct DynContainerIter<'a> {
    inner: std::collections::btree_map::Iter<'a, Vec<u8>, DynContainerEntry>,
}

impl<'a> Iterator for DynContainerIter<'a> {
    type Item = (&'a [u8], &'a ContainerValue, &'a CloneInfo);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, entry) = self.inner.next()?;
        Some((key.as_slice(), &entry.value, &entry.clone_info))
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
    descriptor: impl Into<ContainerDescriptor>,
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
        .register(container.name().to_string(), Box::new(entry));
    container
}

/// Convenience: build a typed [`Container<T>`] from `T`'s static
/// descriptor. Internally creates a [`DynContainer`] via
/// [`make_dyn_container`] and wraps it in a typed view.
///
/// Used by [`crate::Connection::container`] (raw, no transceiver
/// pool).
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

/// Build a typed [`Container<T>`] that is NOT registered for
/// type-name dispatch: only its owner feeds it, via
/// [`view_dispatch_update`]. Used by [`crate::View`], whose broker
/// routes matching transmissions by `subscription_id` — the same
/// guest may also hold an unfiltered subscription on `T`, and those
/// byte-verbatim transmissions must never reach the view's container
/// (they carry unfiltered, unprojected state).
pub(crate) fn make_view_container<T>() -> Container<T>
where
    T: StructValue + Send + Sync + 'static,
{
    Container::from_dyn(Arc::new(DynContainer::new(T::type_descriptor(), None)))
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
    ) -> Result<bool, dots_rs_core::DecodeError> {
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
    ) -> Result<bool, dots_rs_core::DecodeError> {
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
        } else {
            container.apply_owned(txn);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod classify_tests {
    use super::*;
    use crate::connection::Event;
    use dots_rs_core::{AnyStruct, Timepoint};
    use dots_rs_derive::DotsStruct;
    use dots_rs_model::Payload;

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

/// The dynamic-representation counterpart of `classify_tests`: a
/// container built over a runtime descriptor (`ContainerDescriptor::
/// Dynamic`) storing `DynamicStruct` entries — the `subscribe_dynamic`
/// / wire-learned-type cache.
#[cfg(test)]
mod dynamic_classify_tests {
    use super::*;
    use dots_rs_core::Timepoint;
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "DynClassifyDynPinger", cached)]
    struct DynPinger {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        message: Option<String>,
        #[dots(tag = 3)]
        sequence: Option<u64>,
    }

    fn dyn_descriptor() -> Arc<DynamicStructDescriptor> {
        Arc::new(DynamicStructDescriptor::from_static(DynPinger::DESCRIPTOR))
    }

    fn dyn_container(desc: &Arc<DynamicStructDescriptor>) -> Arc<DynContainer> {
        Arc::new(DynContainer::new(desc.clone(), None))
    }

    fn header(value: &DynPinger, remove: bool) -> DotsHeader {
        DotsHeader {
            attributes: Some(value.valid_set()),
            remove_obj: if remove { Some(true) } else { None },
            sender: Some(7),
            sent_time: Some(Timepoint(1.0)),
            ..Default::default()
        }
    }

    /// A `Payload::Wire` transmission, decoded against `desc` — what a
    /// wire-learned type's transmission looks like after the codec ran.
    fn wire_txn(desc: &Arc<DynamicStructDescriptor>, value: &DynPinger, h: &DotsHeader) -> Transmission {
        let d = DynamicStruct::decode(desc.clone(), &encode_to_vec(value))
            .expect("descriptor-driven roundtrip succeeds");
        Transmission {
            header: h.clone(),
            payload: Payload::Wire(d),
        }
    }

    fn typed_txn(value: &DynPinger, h: &DotsHeader) -> Transmission {
        Transmission {
            header: h.clone(),
            payload: Payload::Typed(AnyStruct::from_struct_value(value)),
        }
    }

    /// Full-value comparison through the deterministic encoding —
    /// avoids descriptor-identity concerns of `DynamicStruct`'s
    /// `PartialEq`.
    fn assert_dynamic_eq(d: &DynamicStruct, expected: &DynPinger) {
        assert_eq!(d.encode(), encode_to_vec(expected));
    }

    #[test]
    fn dynamic_with_iter_borrows_before_and_after_promotion() {
        let desc = dyn_descriptor();
        let c = dyn_container(&desc);
        assert_eq!(c.with_iter(|iter| iter.count()), 0);
        let value = DynPinger {
            id: Some(1),
            message: Some("stored".into()),
            sequence: Some(10),
        };
        c.apply(&wire_txn(&desc, &value, &header(&value, false)));
        c.with_iter(|mut iter| {
            assert!(c.state.try_write().is_none());
            let (key, stored, info) = iter.next().unwrap();
            assert_eq!(key, encode_key_bytes(&value));
            assert_eq!(info.created_sender, Some(7));
            let dynamic = stored.as_dynamic().unwrap();
            assert!(Arc::ptr_eq(&dynamic.descriptor, &desc));
            assert_dynamic_eq(dynamic, &value);
            assert!(iter.next().is_none());
        });
        assert!(c.state.try_write().is_some());
        c.promote_to_static(DynPinger::DESCRIPTOR);
        c.with_iter(|mut iter| {
            let (_, stored, _) = iter.next().unwrap();
            assert_eq!(stored.as_typed::<DynPinger>(), Some(&value));
        });
        assert!(c.state.try_write().is_some());
    }

    #[test]
    fn dynamic_container_classifies_create_update_remove() {
        let desc = dyn_descriptor();
        let c = dyn_container(&desc);

        let v1 = DynPinger {
            id: Some(1),
            message: Some("hi".into()),
            sequence: Some(10),
        };
        let r = c
            .apply_and_classify(&wire_txn(&desc, &v1, &header(&v1, false)))
            .expect("create emits an event");
        assert_eq!(r.operation, Operation::Create);
        assert_dynamic_eq(r.updated.as_dynamic().expect("dynamic representation"), &v1);
        assert_eq!(r.clone_info.created_sender, Some(7));

        // Partial update touching only `message`: `sequence` survives.
        let v2 = DynPinger {
            id: Some(1),
            message: Some("yo".into()),
            sequence: None,
        };
        let r = c
            .apply_and_classify(&wire_txn(&desc, &v2, &header(&v2, false)))
            .expect("update emits an event");
        assert_eq!(r.operation, Operation::Update);
        let merged = DynPinger {
            id: Some(1),
            message: Some("yo".into()),
            sequence: Some(10),
        };
        assert_dynamic_eq(r.updated.as_dynamic().unwrap(), &merged);
        assert_eq!(r.clone_info.last_operation, Operation::Update);

        // Remove: key-only payload; `updated` is the pre-remove state.
        let v3 = DynPinger {
            id: Some(1),
            ..Default::default()
        };
        let r = c
            .apply_and_classify(&wire_txn(&desc, &v3, &header(&v3, true)))
            .expect("removing a present key emits an event");
        assert_eq!(r.operation, Operation::Remove);
        assert_dynamic_eq(r.updated.as_dynamic().unwrap(), &merged);
        assert!(c.is_empty(), "the entry is gone after remove");
    }

    #[test]
    fn dynamic_container_suppresses_remove_of_absent_key() {
        let desc = dyn_descriptor();
        let c = dyn_container(&desc);
        let v = DynPinger {
            id: Some(99),
            ..Default::default()
        };
        assert!(
            c.apply_and_classify(&wire_txn(&desc, &v, &header(&v, true)))
                .is_none()
        );
        assert!(c.is_empty());
    }

    /// Stored entries share the container's descriptor `Arc` even when
    /// the payload was decoded against a *different* descriptor
    /// instance (registry re-registration) — the conversion normalises
    /// onto the container's `Arc`.
    #[test]
    fn foreign_descriptor_arc_normalises_onto_container_arc() {
        let container_desc = dyn_descriptor();
        let c = dyn_container(&container_desc);
        let other_desc = dyn_descriptor(); // structurally equal, different Arc
        assert!(!Arc::ptr_eq(&container_desc, &other_desc));

        let v = DynPinger {
            id: Some(1),
            message: Some("hi".into()),
            sequence: None,
        };
        c.apply(&wire_txn(&other_desc, &v, &header(&v, false)));
        c.with_entries_dyn(|entries| {
            let entry = entries.values().next().expect("one entry");
            let stored = entry.value.as_dynamic().expect("dynamic representation");
            assert!(Arc::ptr_eq(&stored.descriptor, &container_desc));
        });
    }

    /// Cross-representation admission, both directions: a `Typed`
    /// payload converts into a dynamic-representation container, and a
    /// `Wire` payload converts into a static-representation container
    /// (which used to warn-drop it).
    #[test]
    fn cross_representation_payloads_convert_on_the_way_in() {
        // Typed payload -> dynamic container.
        let desc = dyn_descriptor();
        let c = dyn_container(&desc);
        let v = DynPinger {
            id: Some(1),
            message: Some("typed".into()),
            sequence: Some(5),
        };
        c.apply(&typed_txn(&v, &header(&v, false)));
        c.with_entries_dyn(|entries| {
            let entry = entries.values().next().expect("one entry");
            let stored = entry.value.as_dynamic().expect("converted to dynamic");
            assert!(Arc::ptr_eq(&stored.descriptor, &desc));
            assert_dynamic_eq(stored, &v);
        });

        // Wire payload -> static container.
        let c = Arc::new(DynContainer::new(DynPinger::DESCRIPTOR, None));
        c.apply(&wire_txn(&desc, &v, &header(&v, false)));
        let typed_view = Container::<DynPinger>::from_dyn(c);
        let entry = typed_view.get(&v).expect("converted to typed");
        assert_eq!(&*entry, &v);
    }

    #[test]
    fn promotion_migrates_entries_and_is_idempotent() {
        let desc = dyn_descriptor();
        let c = dyn_container(&desc);
        let v1 = DynPinger {
            id: Some(1),
            message: Some("one".into()),
            sequence: Some(10),
        };
        let v2 = DynPinger {
            id: Some(2),
            message: Some("two".into()),
            sequence: Some(20),
        };
        c.apply(&wire_txn(&desc, &v1, &header(&v1, false)));
        c.apply(&wire_txn(&desc, &v2, &header(&v2, false)));
        let keys_before =
            c.with_entries_dyn(|entries| entries.keys().cloned().collect::<Vec<_>>());

        c.promote_to_static(DynPinger::DESCRIPTOR);
        c.promote_to_static(DynPinger::DESCRIPTOR); // idempotent

        assert!(matches!(c.descriptor(), ContainerDescriptor::Static(_)));
        let keys_after = c.with_entries_dyn(|entries| entries.keys().cloned().collect::<Vec<_>>());
        assert_eq!(keys_before, keys_after, "keys are representation-independent");

        let typed_view = Container::<DynPinger>::from_dyn(c.clone());
        let snapshot = typed_view.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].value, v1);
        assert_eq!(snapshot[1].value, v2);
        assert_eq!(
            snapshot[0].clone_info.created_sender,
            Some(7),
            "CloneInfo survives promotion"
        );

        // The promoted container keeps classifying (now converting
        // incoming Wire payloads to the static representation).
        let v1b = DynPinger {
            id: Some(1),
            message: Some("one-updated".into()),
            sequence: None,
        };
        let r = c
            .apply_and_classify(&wire_txn(&desc, &v1b, &header(&v1b, false)))
            .expect("update after promotion");
        assert_eq!(r.operation, Operation::Update);
        let u = r.updated.as_typed::<DynPinger>().cloned().expect("typed representation");
        assert_eq!(u.message.as_deref(), Some("one-updated"));
        assert_eq!(u.sequence, Some(10));
    }

    #[test]
    fn shared_dynamic_projects_once_and_keeps_separate_slot_from_typed() {
        // Dynamic-representation outcome: Arc is shared across calls.
        let desc = dyn_descriptor();
        let c = dyn_container(&desc);
        let v = DynPinger {
            id: Some(1),
            message: Some("hi".into()),
            sequence: Some(10),
        };
        let mut outcome = c
            .apply_and_classify(&wire_txn(&desc, &v, &header(&v, false)))
            .expect("create");
        let a = outcome.shared_dynamic(&desc);
        let b = outcome.shared_dynamic(&desc);
        assert!(Arc::ptr_eq(&a, &b), "one projection, shared by Arc");
        assert!(Arc::ptr_eq(&a.descriptor, &desc));

        // Typed-representation outcome: `shared_updated::<T>` and
        // `shared_dynamic` materialise independently (separate slots),
        // and repeated calls of each share their own Arc.
        let c = Arc::new(DynContainer::new(DynPinger::DESCRIPTOR, None));
        let mut outcome = c
            .apply_and_classify(&typed_txn(&v, &header(&v, false)))
            .expect("create");
        let t1 = outcome.shared_updated::<DynPinger>(&v);
        let d1 = outcome.shared_dynamic(&desc);
        let t2 = outcome.shared_updated::<DynPinger>(&v);
        let d2 = outcome.shared_dynamic(&desc);
        assert!(Arc::ptr_eq(&t1, &t2));
        assert!(Arc::ptr_eq(&d1, &d2));
        assert_eq!(&*t1, &v);
        assert_dynamic_eq(&d1, &v);
    }
}

#[cfg(test)]
mod search_tests {
    use super::*;
    use dots_rs_core::AnyStruct;
    use dots_rs_derive::DotsStruct;
    use dots_rs_model::Payload;
    use std::cell::Cell;

    thread_local! {
        static CLONES: Cell<usize> = const { Cell::new(0) };
    }

    #[derive(DotsStruct, Default, Debug, PartialEq)]
    #[dots(name = "ContainerSearchItem", cached)]
    struct Item {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        message: Option<String>,
    }

    impl Clone for Item {
        fn clone(&self) -> Self {
            CLONES.with(|count| count.set(count.get() + 1));
            Self {
                id: self.id,
                message: self.message.clone(),
            }
        }
    }

    fn insert(container: &Container<Item>, id: u32, message: &str) {
        let value = Item {
            id: Some(id),
            message: Some(message.into()),
        };
        container.inner.apply(&Transmission {
            header: DotsHeader {
                type_name: Some("ContainerSearchItem".into()),
                ..Default::default()
            },
            payload: Payload::Typed(AnyStruct::from_struct_value(&value)),
        });
    }

    #[test]
    fn with_iter_borrows_and_projects_without_cloning_instances() {
        let container = make_view_container::<Item>();
        assert_eq!(container.with_iter(|iter| iter.count()), 0);
        insert(&container, 1, "skip");
        insert(&container, 2, "match");
        CLONES.with(|count| count.set(0));
        let mut visited = 0;
        let ids = container.with_iter(|iter| {
            assert!(container.inner.state.try_write().is_none());
            iter.filter(|(_, item, _)| {
                visited += 1;
                item.message.as_deref() == Some("match")
            })
            .map(|(key, item, info)| {
                assert_eq!(key, encode_key_bytes(item));
                assert_eq!(info.last_operation, Operation::Create);
                item.id.unwrap()
            })
            .collect::<Vec<_>>()
        });
        assert_eq!(ids, vec![2]);
        assert_eq!(visited, 2);
        assert_eq!(CLONES.with(Cell::get), 0);
        assert!(container.inner.state.try_write().is_some());
        assert_eq!(
            container.as_dyn().with_iter(|iter| {
                iter.map(|(_, value, _)| value.as_typed::<Item>().unwrap().id.unwrap())
                    .collect::<Vec<_>>()
            }),
            vec![1, 2]
        );
        assert_eq!(CLONES.with(Cell::get), 0);
        assert!(container.inner.state.try_write().is_some());
    }

    #[test]
    fn searches_clone_only_matches_and_release_the_lock() {
        let container = make_view_container::<Item>();
        assert!(container.find(|_| panic!("empty container")).is_none());
        assert!(container.filter(|_| panic!("empty container")).is_empty());
        insert(&container, 1, "skip");
        insert(&container, 2, "match");
        insert(&container, 3, "match");
        CLONES.with(|count| count.set(0));

        let mut visited = Vec::new();
        let found = container
            .find(|value| {
                visited.push(value.id.unwrap());
                value.message.as_deref() == Some("match")
            })
            .unwrap();
        assert_eq!(found.id, Some(2));
        assert_eq!(visited, vec![1, 2], "find stops at the first match");
        assert_eq!(CLONES.with(Cell::get), 1);
        assert!(container.inner.state.try_write().is_some());

        visited.clear();
        let filtered = container.filter(|value| {
            visited.push(value.id.unwrap());
            value.message.as_deref() == Some("match")
        });
        assert_eq!(visited, vec![1, 2, 3]);
        assert_eq!(
            filtered.iter().map(|v| v.id.unwrap()).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            CLONES.with(Cell::get),
            3,
            "only matching instances are cloned"
        );
        assert!(container.inner.state.try_write().is_some());

        assert!(container.find(|_| false).is_none());
        assert!(container.filter(|_| false).is_empty());
        assert_eq!(CLONES.with(Cell::get), 3, "misses do not clone");
        assert_eq!(container.filter(|_| true).len(), 3);
        assert_eq!(CLONES.with(Cell::get), 6);

        insert(&container, 2, "changed");
        assert_eq!(found.message.as_deref(), Some("match"));
        assert_eq!(filtered[0].message.as_deref(), Some("match"));
        assert_eq!(
            container.get(&found).unwrap().message.as_deref(),
            Some("changed")
        );
    }
}
