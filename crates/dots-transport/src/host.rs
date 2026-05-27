//! Host-side transceiver — the broker-facing equivalent of C++
//! `dots::HostTransceiver`.
//!
//! A [`HostTransceiver`] accepts guest connections through
//! [`accept`](HostTransceiver::accept), drives the broker-side
//! handshake (`Hello` → `Connect` → `ConnectResponse`), routes
//! `DotsMember(Join/Leave)` to maintain per-type subscription groups,
//! fans out incoming transmissions to subscribed peers, and replays
//! the cached pool to late subscribers on `Join`.
//!
//! Listening accepts both TCP ([`accept_tcp`](HostTransceiver::accept_tcp))
//! and Unix-domain sockets ([`accept_unix`](HostTransceiver::accept_unix));
//! [`serve_endpoint`](HostTransceiver::serve_endpoint) parses URI strings
//! produced by [`crate::parse_endpoint`] and binds the appropriate
//! listener. Tests can also feed an in-memory duplex stream into
//! [`accept`](HostTransceiver::accept) directly.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::io::IoSlice;
#[cfg(feature = "stats")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::Mutex as AsyncMutex;

use dots_core::{
    AnyStruct, DynamicFieldKind, DynamicStruct, DynamicStructDescriptor, PropertySet,
    Publishable, StructValue, Transmittable, decode_typed_from_slice, dots,
};
use dots_model::{
    DotsCacheInfo, DotsConnectionState, DotsHeader, DotsMember, DotsMemberEvent, DotsMsgConnect,
    DotsMsgConnectResponse, DotsMsgHello, DotsServerCapabilities, EnumDescriptorData,
    RawTransmission, Registry, StructDescriptorData, Transmission,
    encode_frame_with_header, encode_transmission_into, encode_transmission_with_mask_into,
    filter::DotsFilter,
};

use crate::filter::CompiledPredicate;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio_util::codec::FramedRead;

use crate::codec::RawTransmissionCodec;
use crate::guest::now_timepoint;

/// Per-guest writer with an internal output buffer and a dedicated
/// drainer task.
///
/// Producers ([`GuestWriter::enqueue`]) are entirely **synchronous**:
/// take the sync state mutex briefly, append bytes to the queue,
/// release the mutex, notify the drainer if the queue had been
/// empty. No `.await` happens on the producer side, so the fan-out
/// hot path has no async state machine — closures stay tiny and
/// `drop_in_place` cost vanishes.
///
/// The drainer task ([`writer_loop`]) is spawned once per guest at
/// `accept_*` time. It awaits on [`Notify`]; when woken, it drains
/// the queue (`BytesMut::split` keeps capacity) and writes each
/// chunk via [`WriteHandle::write_buf`]. Coalesces back-to-back
/// frames produced by concurrent fan-outs into a single `try_write`
/// syscall.
///
/// Backpressure: if a producer would push the queue past
/// [`WRITE_OVERFLOW_THRESHOLD`] it sets `overflow + closed` and
/// notifies the drainer to exit. The reader task checks
/// `is_overflowed` each loop iteration and tears the connection
/// down — matches the C++ `dotsd` "drop slow consumers" policy.
struct GuestWriter {
    handle: WriteHandle,
    state: Mutex<WriteState>,
    /// Drainer wakeup. Producers fire `notify_one` when the queue
    /// transitions empty → non-empty (or to signal `closed`).
    notify: tokio::sync::Notify,
    /// Read-side counters. Bumped from the per-guest read loop in
    /// [`run_guest`]; read from `snapshot_stats`. Lives outside the
    /// write mutex so reader and writer don't contend on each
    /// other's hot paths.
    #[cfg(feature = "stats")]
    read_stats: ReadStats,
}

#[cfg(feature = "stats")]
#[derive(Default)]
struct ReadStats {
    bytes: AtomicU64,
    frames: AtomicU64,
}

struct WriteState {
    /// Pending frames to write. Each entry is a refcounted
    /// `Bytes`, so the fan-out path can share one allocation
    /// across N unfiltered subscribers via `clone()` (Arc bump,
    /// no copy). The drainer turns the deque into a borrowed
    /// `IoSlice` array and emits the lot with one
    /// `try_write_vectored` syscall.
    queue: VecDeque<Bytes>,
    /// Sum of `queue.iter().map(|b| b.len()).sum()` kept
    /// incrementally so the overflow check is O(1) and the
    /// drainer doesn't have to recompute.
    queued_bytes: usize,
    /// Sticky bit set when a producer would push past
    /// [`WRITE_OVERFLOW_THRESHOLD`] or when the drainer hits a
    /// fatal I/O error. Reader task checks this and exits.
    overflow: bool,
    /// Sticky bit telling the drainer to exit at the next idle
    /// point. Set on overflow, on socket failure, and on
    /// `remove_guest`.
    closed: bool,
    /// Monotonic counters and high-water marks read by
    /// [`HostTransceiver::guest_stats`]. Lives under the same
    /// mutex as `queue` — every producer and the drainer already
    /// hold it, so per-bump cost is one add.
    #[cfg(feature = "stats")]
    stats: WriteStatsRaw,
}

#[cfg(feature = "stats")]
#[derive(Default)]
struct WriteStatsRaw {
    bytes_sent: u64,
    frames_sent: u64,
    drainer_wakeups: u64,
    peak_queued_bytes: u64,
    peak_queued_frames: u32,
}

/// Per-guest I/O statistics, snapshotted via
/// [`HostTransceiver::guest_stats`] / [`HostTransceiver::guest_stats_all`].
///
/// Counters are split into three camps:
/// - **Cumulative write** (`bytes_sent`, `frames_sent`,
///   `drainer_wakeups`) and **cumulative read** (`bytes_received`,
///   `frames_received`) — monotonic for the lifetime of the guest,
///   never reset by snapshotting. Consumers compute deltas across
///   snapshots if they want rates.
/// - **High-water marks** (`peak_queued_bytes`, `peak_queued_frames`)
///   — reset on every snapshot to the current queued values, so each
///   reading reports the peak observed *since the previous snapshot*.
///
/// `current_queued_bytes` is the instantaneous backlog at the moment
/// the snapshot was taken; useful for spotting a chronic non-zero
/// floor that the high-water marks would never reset below.
///
/// Byte counters on both directions include the full wire frame (5
/// byte size prefix + encoded header + encoded payload), so
/// `bytes_sent` and `bytes_received` are directly comparable.
///
/// Derived ratios worth tracking on the consumer side:
/// - `frames_sent / drainer_wakeups` — average vectored batch size.
///   Closer to 1 means the producer never gets ahead of the drainer;
///   large values mean fan-out bursts are amortized over one syscall.
/// - `bytes_sent / drainer_wakeups` — average write size per syscall.
#[derive(Debug, Default, Clone)]
pub struct GuestStats {
    /// Total bytes handed to the socket since the guest connected
    /// (full wire frame size).
    pub bytes_sent: u64,
    /// Total frames handed to the socket since the guest connected.
    pub frames_sent: u64,
    /// Total bytes pulled off the socket since the guest connected
    /// (full wire frame size).
    pub bytes_received: u64,
    /// Total frames decoded from the socket since the guest
    /// connected.
    pub frames_received: u64,
    /// Number of times the drainer task woke up from `notified()`.
    pub drainer_wakeups: u64,
    /// Peak `queued_bytes` observed between the previous snapshot
    /// (or guest connect) and this one. Reset on read.
    pub peak_queued_bytes: u64,
    /// Peak pending-frame count observed since the previous
    /// snapshot. Reset on read.
    pub peak_queued_frames: u32,
    /// Currently-queued bytes at snapshot time. Not reset.
    pub current_queued_bytes: u64,
    /// `true` once the per-guest writer has been marked for
    /// disconnect — either via [`WRITE_OVERFLOW_THRESHOLD`] tripping
    /// or a fatal socket error during drain.
    pub overflow_disconnected: bool,
}

/// Drop bytes destined for a guest whose buffered queue would grow
/// past this threshold. Mirrors the C++ broker's "disconnect slow
/// consumers" policy. 1 MiB matches the default size that the C++
/// `dotsd` uses for its per-connection write buffer.
const WRITE_OVERFLOW_THRESHOLD: usize = 1024 * 1024;

/// Underlying write half. The TCP/Unix variants expose `&self`
/// methods (`try_write`, `writable`) so the drive loop can use them
/// without further locking. The `Generic` variant is for tests
/// using `tokio::io::DuplexStream` and falls back to an async lock
/// around the type-erased writer.
enum WriteHandle {
    Tcp(tokio::net::tcp::OwnedWriteHalf),
    #[cfg(unix)]
    Unix(tokio::net::unix::OwnedWriteHalf),
    Generic(AsyncMutex<Box<dyn AsyncWrite + Send + Unpin>>),
}

/// `Arc`-wrapped `GuestWriter`. Producers clone the `Arc` to fan
/// out (single atomic op per subscriber); a dedicated drainer task
/// per guest ([`writer_loop`]) owns the actual socket writes.
type SharedWriter = Arc<GuestWriter>;

impl GuestWriter {
    fn new(handle: WriteHandle) -> Arc<Self> {
        Arc::new(Self {
            handle,
            state: Mutex::new(WriteState {
                queue: VecDeque::with_capacity(64),
                queued_bytes: 0,
                overflow: false,
                closed: false,
                #[cfg(feature = "stats")]
                stats: WriteStatsRaw::default(),
            }),
            notify: tokio::sync::Notify::new(),
            #[cfg(feature = "stats")]
            read_stats: ReadStats::default(),
        })
    }

    /// Credit one received frame of `frame_bytes` to this guest's
    /// read counters. Called by the per-guest read loop after a
    /// successful frame decode. Atomic-only — does not touch the
    /// write mutex. No-op when the `stats` feature is off; the
    /// call site stays unchanged.
    #[inline]
    fn record_received(&self, _frame_bytes: usize) {
        #[cfg(feature = "stats")]
        {
            self.read_stats
                .bytes
                .fetch_add(_frame_bytes as u64, Ordering::Relaxed);
            self.read_stats.frames.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Append `frame` to the output queue. **Synchronous** — no
    /// `.await`, no async state machine on the producer side. The
    /// per-guest drainer task picks the frame up; if the queue was
    /// empty we wake it via `notify_one`.
    ///
    /// `frame` is moved into the queue without a copy. The fan-out
    /// path passes the same `Bytes` (clone is a refcount bump) to
    /// every unfiltered subscriber, so one encoded transmission
    /// turns into one allocation regardless of subscriber count.
    ///
    /// Frames are dropped (and `overflow + closed` set) if the
    /// queue would grow past [`WRITE_OVERFLOW_THRESHOLD`] — the
    /// reader task observes the overflow flag on its next iteration
    /// and disconnects the slow consumer.
    fn enqueue(&self, frame: Bytes) {
        let should_notify;
        {
            let mut state = self.state.lock().expect("guest write state poisoned");
            if state.closed {
                return;
            }
            if state.queued_bytes.saturating_add(frame.len()) > WRITE_OVERFLOW_THRESHOLD {
                state.overflow = true;
                state.closed = true;
                tracing::warn!(
                    queued = state.queued_bytes,
                    incoming = frame.len(),
                    threshold = WRITE_OVERFLOW_THRESHOLD,
                    "guest write buffer overflow; marking for disconnect",
                );
                drop(state);
                self.notify.notify_one();
                return;
            }
            let was_empty = state.queue.is_empty();
            state.queued_bytes += frame.len();
            state.queue.push_back(frame);
            #[cfg(feature = "stats")]
            {
                if state.queued_bytes as u64 > state.stats.peak_queued_bytes {
                    state.stats.peak_queued_bytes = state.queued_bytes as u64;
                }
                let frames = state.queue.len() as u32;
                if frames > state.stats.peak_queued_frames {
                    state.stats.peak_queued_frames = frames;
                }
            }
            should_notify = was_empty;
        }
        if should_notify {
            self.notify.notify_one();
        }
    }

    /// Signal the drainer task to exit at the next idle point.
    /// Used during `remove_guest` to clean up the per-guest writer
    /// task on disconnect.
    fn close(&self) {
        {
            let mut state = self.state.lock().expect("guest write state poisoned");
            state.closed = true;
        }
        self.notify.notify_one();
    }

    /// Cheap snapshot of the overflow bit — read by the per-guest
    /// reader loop to detect "this guest fell behind, tear it down".
    fn is_overflowed(&self) -> bool {
        self.state.lock().expect("guest write state poisoned").overflow
    }

    /// Build a [`GuestStats`] snapshot. Read counters come from
    /// atomics (no lock); write counters and the high-water marks
    /// come from `WriteState`. Resets the high-water marks to the
    /// *current* queued values so the next snapshot reports peaks
    /// observed strictly between the two calls (and not below the
    /// current sustained backlog).
    ///
    /// Returns `None` when the `stats` feature is disabled —
    /// no counters were maintained, so there's nothing meaningful
    /// to report.
    fn snapshot_stats(&self) -> Option<GuestStats> {
        #[cfg(feature = "stats")]
        {
            let bytes_received = self.read_stats.bytes.load(Ordering::Relaxed);
            let frames_received = self.read_stats.frames.load(Ordering::Relaxed);
            let mut state = self.state.lock().expect("guest write state poisoned");
            let current_bytes = state.queued_bytes as u64;
            let current_frames = state.queue.len() as u32;
            let snap = GuestStats {
                bytes_sent: state.stats.bytes_sent,
                frames_sent: state.stats.frames_sent,
                bytes_received,
                frames_received,
                drainer_wakeups: state.stats.drainer_wakeups,
                peak_queued_bytes: state.stats.peak_queued_bytes,
                peak_queued_frames: state.stats.peak_queued_frames,
                current_queued_bytes: current_bytes,
                overflow_disconnected: state.overflow,
            };
            state.stats.peak_queued_bytes = current_bytes;
            state.stats.peak_queued_frames = current_frames;
            Some(snap)
        }
        #[cfg(not(feature = "stats"))]
        {
            None
        }
    }
}

/// Drainer loop: one task per guest, owns the WriteHandle through
/// the shared `GuestWriter`. Awaits notifications; on each wake,
/// drains the queue to the socket until empty, then awaits again.
/// Exits when `closed` is set.
///
/// The TCP/Unix paths submit the whole queue with a single
/// `try_write_vectored` per attempt (`writev(2)`), so N frames
/// enqueued between drainer wakeups become one syscall instead of
/// N. Partial writes (kernel buffer fills mid-call) trim the
/// front of `chunks` and re-arm via `writable()`.
async fn writer_loop(writer: Arc<GuestWriter>) {
    let mut chunks: VecDeque<Bytes> = VecDeque::new();
    loop {
        // Note: `notified()` returns a future that registers the
        // waker eagerly, so a `notify_one` issued *after* we
        // construct the future but *before* we `.await` still
        // wakes us — no missed-wakeup race.
        let notified = writer.notify.notified();
        // Count one wakeup per outer iteration (i.e. per notified
        // arrival). The inner loop may run several drain cycles
        // before the queue stays empty; those are still one
        // logical wake.
        #[cfg(feature = "stats")]
        {
            let mut state = writer.state.lock().expect("guest write state poisoned");
            state.stats.drainer_wakeups = state.stats.drainer_wakeups.saturating_add(1);
        }
        loop {
            {
                let mut state = writer.state.lock().expect("guest write state poisoned");
                if state.queue.is_empty() {
                    if state.closed {
                        return;
                    }
                    break;
                }
                // Credit the batch under the same lock that drains
                // the queue. A subsequent `write_chunks` failure
                // counts as "attempted to send" — that's both
                // useful (the final stats snapshot reflects what
                // the drainer was carrying when it died) and lets
                // the stats path stay a single conditional block
                // here.
                #[cfg(feature = "stats")]
                {
                    state.stats.bytes_sent = state
                        .stats
                        .bytes_sent
                        .saturating_add(state.queued_bytes as u64);
                    state.stats.frames_sent = state
                        .stats
                        .frames_sent
                        .saturating_add(state.queue.len() as u64);
                }
                chunks.append(&mut state.queue);
                state.queued_bytes = 0;
            }
            if let Err(e) = writer.handle.write_chunks(&mut chunks).await {
                tracing::debug!(error = %e, "drainer write failed; closing guest writer");
                let mut state = writer.state.lock().expect("guest write state poisoned");
                state.overflow = true;
                state.closed = true;
                return;
            }
        }
        notified.await;
    }
}

impl WriteHandle {
    /// Drain `chunks` to the socket, leaving `chunks` empty on
    /// success. On error, `chunks` may still hold unwritten
    /// frames — the caller is closing the writer so they get
    /// dropped.
    async fn write_chunks(&self, chunks: &mut VecDeque<Bytes>) -> std::io::Result<()> {
        match self {
            Self::Tcp(half) => drain_vectored(half, chunks).await,
            #[cfg(unix)]
            Self::Unix(half) => drain_vectored(half, chunks).await,
            Self::Generic(m) => {
                use tokio::io::AsyncWriteExt;
                let mut g = m.lock().await;
                while let Some(chunk) = chunks.pop_front() {
                    g.write_all(&chunk).await?;
                }
                Ok(())
            }
        }
    }
}

/// Linux's documented `IOV_MAX` floor. tokio caps `try_write_vectored`
/// at this internally; capping here too keeps the slice allocation
/// from growing unbounded under heavy fan-out.
const MAX_IOV: usize = 1024;

/// Advance the front of `chunks` by `n` written bytes. Whole frames
/// at the front pop_front; the remaining first frame is sliced via
/// `Bytes::advance` (refcount stays shared, no copy).
fn advance_chunks(chunks: &mut VecDeque<Bytes>, mut n: usize) {
    while n > 0 {
        let Some(front) = chunks.front_mut() else {
            return;
        };
        if front.len() <= n {
            n -= front.len();
            chunks.pop_front();
        } else {
            front.advance(n);
            return;
        }
    }
}

/// Build a `[IoSlice]` view over the first `MAX_IOV` chunks. Reused
/// each iteration of a partial-write loop. Returned vector borrows
/// from `chunks` for its lifetime.
fn ioslices_for<'a>(chunks: &'a VecDeque<Bytes>) -> Vec<IoSlice<'a>> {
    chunks
        .iter()
        .take(MAX_IOV)
        .map(|b| IoSlice::new(b.as_ref()))
        .collect()
}

/// The two methods `drain_vectored` needs from a tokio write half.
/// Implemented for `tcp::OwnedWriteHalf` and (on unix)
/// `unix::OwnedWriteHalf` — both expose identical inherent
/// `try_write_vectored` / `writable` signatures, but tokio doesn't
/// give us a trait that covers them, so we declare our own. The
/// `+ Send` bound on `writable`'s future is what lets
/// `writer_loop`'s `tokio::spawn` keep its `Send` requirement.
trait DrainHalf {
    fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize>;
    fn writable(&self) -> impl std::future::Future<Output = std::io::Result<()>> + Send;
}

impl DrainHalf for tokio::net::tcp::OwnedWriteHalf {
    fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        Self::try_write_vectored(self, bufs)
    }
    fn writable(&self) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        Self::writable(self)
    }
}

#[cfg(unix)]
impl DrainHalf for tokio::net::unix::OwnedWriteHalf {
    fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        Self::try_write_vectored(self, bufs)
    }
    fn writable(&self) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        Self::writable(self)
    }
}

async fn drain_vectored<H: DrainHalf>(
    half: &H,
    chunks: &mut VecDeque<Bytes>,
) -> std::io::Result<()> {
    while !chunks.is_empty() {
        let slices = ioslices_for(chunks);
        match half.try_write_vectored(&slices) {
            Ok(n) => {
                drop(slices);
                advance_chunks(chunks, n);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                drop(slices);
                half.writable().await?;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Sentinel sender id used for transmissions originating from the
/// host itself (matches dots-cpp `Connection::HostId`).
pub const HOST_ID: u32 = 1;

/// In-memory broker. Accepts guest streams, runs the broker-side
/// handshake, and routes published transmissions to subscribed guests
/// based on `DotsMember(Join/Leave)`.
pub struct HostTransceiver {
    self_name: String,
    registry: Arc<Registry>,
    inner: Arc<Mutex<HostInner>>,
}

struct HostInner {
    /// Per-type-name → membership state for the group.
    groups: FxHashMap<String, Group>,
    /// Per-guest state, keyed by client id.
    guests: FxHashMap<u32, GuestRecord>,
    /// Monotonic id allocator for new guests. Starts at 2 since 1 is
    /// reserved as `HOST_ID`.
    next_client_id: u32,
    /// Container pool: per-cached-type, key-bytes → cached entry.
    /// Updated on every incoming transmission of a cached type;
    /// replayed on `DotsMember(Join)`.
    pool: FxHashMap<String, BTreeMap<Vec<u8>, CachedEntry>>,
    /// Optional application-supplied callback fired whenever a
    /// guest's connection state changes
    /// (`EarlySubscribe` / `Connected` / `Closed`). Daemons (e.g.
    /// `dotsd`) install one to publish `DotsClient` and any
    /// related per-client records. The host stays free of
    /// daemon-level type knowledge — see [`set_transition_handler`].
    transition_handler: Option<Arc<dyn Fn(&ConnectionTransition) + Send + Sync>>,
}

/// Connection-state transition delivered to the host's
/// [`HostTransceiver::set_transition_handler`] callback.
///
/// Mirrors the role of dots-cpp's `transition_handler_t` — the host
/// reports raw lifecycle events; the daemon decides what (if any)
/// DOTS records to publish in response (`DotsClient`,
/// `DotsClientStatistics`, …).
///
/// `final_stats` is populated only for `state == Closed`, snapshotted
/// before the guest's writer is dropped. Lets the daemon publish a
/// terminal `DotsClientStatistics` so short-lived guests aren't
/// invisible to periodic polling.
#[derive(Debug, Clone)]
pub struct ConnectionTransition {
    pub client_id: u32,
    pub client_name: Option<String>,
    pub state: DotsConnectionState,
    pub final_stats: Option<GuestStats>,
}

/// Per-type membership state — kept as a struct rather than a bare
/// set so unfiltered subscribers can take the byte-fan-out hot path
/// and filtered subscribers can apply the four-cases dispatch in the
/// same critical section.
///
/// The two halves are independent: a single guest may hold both an
/// unfiltered subscription on `T` (a regular `subscribe::<T>`) and
/// any number of filtered subscriptions on `T` (multiple
/// `view::<T>(filter)` opens with distinct `subscription_id`s).
#[derive(Default)]
struct Group {
    /// Guest ids that have unfiltered membership in this group.
    /// Receives transmissions byte-verbatim from the publish path.
    unfiltered_subs: FxHashSet<u32>,
    /// Filtered subscriptions keyed by `(client_id, subscription_id)`.
    /// One guest may hold multiple filtered subs distinguished by id.
    filtered_subs: FxHashMap<(u32, u32), FilteredSub>,
}

impl Group {
    fn is_empty(&self) -> bool {
        self.unfiltered_subs.is_empty() && self.filtered_subs.is_empty()
    }

    fn subscriber_count(&self) -> usize {
        self.unfiltered_subs.len() + self.filtered_subs.len()
    }
}

/// One filtered subscription. Holds the original wire [`DotsFilter`]
/// (echoed in fan-out for debugging / tracing), the
/// [`CompiledPredicate`] for fast per-event evaluation, and the
/// "visible shadow" — the set of cache key-bytes currently in this
/// view.
///
/// The shadow is what makes the four-cases dispatch fire correctly:
/// comparing pre-merge membership against the post-merge predicate
/// outcome classifies each publish as `enter / in-view-update /
/// leave / silent-drop`.
struct FilteredSub {
    filter: DotsFilter,
    compiled: CompiledPredicate,
    /// Cache key-bytes of instances currently in this view. Mapped
    /// from the C++ side's `unordered_set<const Struct*>` to Rust's
    /// stable identity for cached instances (since BTreeMap nodes
    /// may relocate on insert, pointer-equality isn't safe — but the
    /// CBOR-encoded key bytes are deterministic and stable).
    visible: HashSet<Vec<u8>>,
}

/// One cached instance held in the pool. The payload is stored in
/// whichever decode form it arrived as ([`Payload::Typed`] when the
/// broker has the static descriptor, [`Payload::Wire`] otherwise);
/// on replay we re-encode it with a fresh `header.from_cache`
/// countdown.
#[derive(Clone)]
struct CachedEntry {
    payload: dots_model::Payload,
    /// Sender on the *first* publish for this key. Preserved across
    /// updates from other clients. Mirrors dots-cpp's
    /// `CloneInformation::createdFrom` — daemons rely on this to
    /// detect whether a disconnected client's data is still
    /// referenced anywhere in the pool.
    created_from: Option<u32>,
    /// Last-update sender (the publisher's `client_id`, or `HOST_ID`
    /// for host-originated publishes). Overwritten on every update.
    last_update_sender: Option<u32>,
    /// Header `sent_time` from the most recent update.
    last_update_time: Option<dots_core::Timepoint>,
    /// Property bitmask of the most recent update (for reproducing
    /// `header.attributes` on replay).
    attributes: PropertySet,
}

struct GuestRecord {
    client_name: Option<String>,
    /// Shared write end of this guest's connection. Producers
    /// (handshake replies, direct replies, fan-out from peer tasks)
    /// `enqueue` bytes synchronously; the per-guest drainer task
    /// awaits notifications and performs the actual socket I/O.
    write_half: SharedWriter,
    /// Handle to the per-guest read task. Kept so the host can
    /// abort it during shutdown.
    task: JoinHandle<()>,
}

impl HostTransceiver {
    /// Build a new host with a fresh registry pre-populated with the
    /// DOTS-internal types.
    pub fn new(self_name: impl Into<String>) -> Arc<Self> {
        let registry = Arc::new(dots_model::registry_with_internal_types());
        Self::with_registry(self_name, registry)
    }

    /// Build a new host with a caller-supplied registry. The registry
    /// must already include the DOTS-internal types — e.g. via
    /// [`dots_model::registry_with_internal_types`].
    pub fn with_registry(self_name: impl Into<String>, registry: Arc<Registry>) -> Arc<Self> {
        Arc::new(HostTransceiver {
            self_name: self_name.into(),
            registry,
            inner: Arc::new(Mutex::new(HostInner {
                groups: FxHashMap::default(),
                guests: FxHashMap::default(),
                next_client_id: HOST_ID + 1,
                pool: FxHashMap::default(),
                transition_handler: None,
            })),
        })
    }

    /// Install a callback invoked on every guest connection-state
    /// transition. Replaces any previously-installed handler. Pass a
    /// no-op closure to clear (the host has no dedicated `clear`
    /// method since transition handlers should typically be set once
    /// at startup).
    ///
    /// The callback fires synchronously **after** the host's inner
    /// lock is released, so it's free to call back into
    /// [`publish`](Self::publish) / [`remove`](Self::remove) /
    /// [`guest_stats`](Self::guest_stats) etc. without deadlock.
    /// Keep the callback work short — long-running callbacks block
    /// the read loop of the transitioning guest (and, for `Closed`,
    /// the `remove_guest` cleanup of every disconnecting peer).
    pub fn set_transition_handler<F>(&self, handler: F)
    where
        F: Fn(&ConnectionTransition) + Send + Sync + 'static,
    {
        self.inner
            .lock()
            .expect("host mutex poisoned")
            .transition_handler = Some(Arc::new(handler));
    }

    /// Snapshot the transition handler under the inner lock and
    /// fire it with `transition` after releasing the lock. No-op if
    /// nothing is installed.
    fn fire_transition(&self, transition: ConnectionTransition) {
        let handler = self
            .inner
            .lock()
            .expect("host mutex poisoned")
            .transition_handler
            .clone();
        if let Some(handler) = handler {
            handler(&transition);
        }
    }

    pub fn self_name(&self) -> &str {
        &self.self_name
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Currently-connected guest count.
    pub fn guest_count(&self) -> usize {
        self.inner.lock().expect("host mutex poisoned").guests.len()
    }

    /// Snapshot statistics for `client_id`. Returns `None` if the
    /// guest is no longer connected **or** if the `stats` feature is
    /// disabled at compile time — callers don't need to coordinate
    /// with `remove_guest` themselves and can treat "stats off" the
    /// same as "guest not found".
    ///
    /// Each call resets the high-water marks on the underlying
    /// writer, so a periodic poller naturally gets the peak
    /// observed between consecutive polls. See [`GuestStats`]
    /// for field semantics.
    pub fn guest_stats(&self, client_id: u32) -> Option<GuestStats> {
        self.writer_for(client_id).and_then(|w| w.snapshot_stats())
    }

    /// Snapshot stats for every currently-connected guest. One pass
    /// under the host mutex collects the `SharedWriter` handles;
    /// each writer's per-state lock is then taken individually, so
    /// the host mutex is held only briefly. As with
    /// [`guest_stats`](Self::guest_stats), each entry's high-water
    /// marks are reset by this call. Returns an empty vector when
    /// the `stats` feature is disabled.
    pub fn guest_stats_all(&self) -> Vec<(u32, GuestStats)> {
        let writers: Vec<(u32, SharedWriter)> = {
            let inner = self.inner.lock().expect("host mutex poisoned");
            inner
                .guests
                .iter()
                .map(|(id, rec)| (*id, rec.write_half.clone()))
                .collect()
        };
        writers
            .into_iter()
            .filter_map(|(id, w)| w.snapshot_stats().map(|s| (id, s)))
            .collect()
    }

    /// Number of subscriptions on a given type group. Counts each
    /// filtered subscription separately (a single guest holding two
    /// filtered subs on `T` contributes 2 to the count) plus each
    /// unfiltered subscriber once.
    pub fn group_size(&self, type_name: &str) -> usize {
        self.inner
            .lock()
            .expect("host mutex poisoned")
            .groups
            .get(type_name)
            .map(Group::subscriber_count)
            .unwrap_or(0)
    }

    /// Abort all per-guest tasks and clear internal state. Call this
    /// before dropping the last `Arc<HostTransceiver>` reference if
    /// the host is embedded in a longer-running app — otherwise the
    /// per-guest tokio tasks (which each hold an `Arc<Self>`) will
    /// keep the host alive until each connection naturally ends.
    ///
    /// After `shutdown` returns the host stops accepting traffic and
    /// fan-out becomes a no-op. Intended for graceful in-process
    /// teardown; the `dotsd` binary doesn't need this because the
    /// tokio runtime's own shutdown aborts all spawned tasks.
    pub fn shutdown(&self) {
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        let guest_count = inner.guests.len();
        for (_, record) in inner.guests.drain() {
            record.task.abort();
        }
        inner.groups.clear();
        inner.pool.clear();
        tracing::debug!(guest_count, "host shutdown — guest tasks aborted");
    }

    /// Names of all groups that have at least one subscriber.
    pub fn group_names(&self) -> Vec<String> {
        self.inner
            .lock()
            .expect("host mutex poisoned")
            .groups
            .keys()
            .cloned()
            .collect()
    }

    /// Number of cached instances currently held in the pool for a
    /// given type.
    pub fn cache_size(&self, type_name: &str) -> usize {
        self.inner
            .lock()
            .expect("host mutex poisoned")
            .pool
            .get(type_name)
            .map(BTreeMap::len)
            .unwrap_or(0)
    }

    /// Decode every cached entry of `T` and return them as owned
    /// typed values. Intended for daemon-level sweeps (e.g.
    /// `cleanUpClients`) that want to inspect cached state without
    /// installing a subscription. Round-trip cost: one CBOR
    /// re-encode + decode per entry, so don't call inside hot paths.
    ///
    /// Returns an empty vector if no entries exist or if a decode
    /// fails (which shouldn't happen for entries the host accepted
    /// — this is a belt-and-braces guard).
    pub fn cached_values<T>(&self) -> Vec<T>
    where
        T: StructValue + Default + Clone,
    {
        let type_name = T::type_descriptor().name;
        let payloads: Vec<dots_model::Payload> = {
            let inner = self.inner.lock().expect("host mutex poisoned");
            inner
                .pool
                .get(type_name)
                .map(|map| map.values().map(|e| e.payload.clone()).collect())
                .unwrap_or_default()
        };
        payloads
            .into_iter()
            .filter_map(|p| match p {
                // Typed: zero-cost cast through descriptor identity.
                dots_model::Payload::Typed(a) => a.as_typed::<T>().cloned(),
                // Wire: fall back to the CBOR roundtrip (rare — only
                // for types the host has the dynamic descriptor for
                // but no compiled-in `T`).
                dots_model::Payload::Wire(d) => decode_typed_from_slice::<T>(&d.encode()).ok(),
            })
            .collect()
    }

    /// True if **any** cache entry, across all types, was either
    /// created by or last-updated by `client_id`. Used by
    /// `cleanUpClients`-style sweeps to decide whether a Closed
    /// client's metadata is still referenced anywhere — only when
    /// nothing references it is the `DotsClient` record safe to
    /// drop. Mirrors dots-cpp's
    /// `cloneInformation.createdFrom == id || lastUpdateFrom == id`
    /// check.
    pub fn client_id_referenced(&self, client_id: u32) -> bool {
        let inner = self.inner.lock().expect("host mutex poisoned");
        for map in inner.pool.values() {
            for entry in map.values() {
                if entry.created_from == Some(client_id)
                    || entry.last_update_sender == Some(client_id)
                {
                    return true;
                }
            }
        }
        false
    }

    /// Accept an incoming guest stream. Spawns a tokio task that runs
    /// the broker-side handshake and inbound-dispatch loop. Returns
    /// the allocated client id.
    ///
    /// All outbound traffic flows through the per-guest
    /// [`GuestWriter`]: producers append bytes to its internal queue
    /// under a brief sync mutex; a dedicated drainer task awaits a
    /// notification and writes the queue to the socket. The fan-out
    /// hot path therefore stays sync and `.await`-free per subscriber.
    pub fn accept<S>(self: &Arc<Self>, stream: S) -> u32
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(stream);
        let writer = GuestWriter::new(WriteHandle::Generic(AsyncMutex::new(
            Box::new(write_half) as Box<dyn AsyncWrite + Send + Unpin>,
        )));
        self.spawn_guest_task(read_half, writer)
    }

    /// Specialised accept for `tokio::net::TcpStream`. Uses
    /// `into_split` so the write half is an `OwnedWriteHalf` whose
    /// `try_write`/`writable` methods take `&self` — no internal
    /// `tokio::io::split` mutex.
    pub fn accept_tcp(self: &Arc<Self>, stream: TcpStream) -> u32 {
        let (read_half, write_half) = stream.into_split();
        let writer = GuestWriter::new(WriteHandle::Tcp(write_half));
        self.spawn_guest_task(read_half, writer)
    }

    /// Specialised accept for `tokio::net::UnixStream`. Same shape as
    /// [`accept_tcp`].
    #[cfg(unix)]
    pub fn accept_unix(self: &Arc<Self>, stream: UnixStream) -> u32 {
        let (read_half, write_half) = stream.into_split();
        let writer = GuestWriter::new(WriteHandle::Unix(write_half));
        self.spawn_guest_task(read_half, writer)
    }

    /// Common tail of the `accept_*` methods: allocate a client id,
    /// register the guest, and spawn the read task.
    fn spawn_guest_task<R>(self: &Arc<Self>, read_half: R, writer: SharedWriter) -> u32
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let host = self.clone();
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        let client_id = inner.next_client_id;
        inner.next_client_id += 1;
        // Per-guest drainer task: owns the write half via the
        // shared `GuestWriter`, awaits `notify`, drains the queue
        // until empty, repeats. Exits when `closed` is set.
        let writer_for_drainer = writer.clone();
        let drainer = tokio::spawn(writer_loop(writer_for_drainer));
        let writer_for_record = writer.clone();
        let task = {
            let host = host.clone();
            tokio::spawn(async move {
                let writer_for_close = writer.clone();
                if let Err(e) = run_guest(host.clone(), client_id, read_half, writer).await {
                    tracing::warn!(client_id, error = %e, "guest task ended with error");
                }
                // Tell the drainer to exit, then await it so the
                // socket has flushed any final bytes before the
                // `Closed` transition fires for downstream
                // observers.
                writer_for_close.close();
                let _ = drainer.await;
                host.remove_guest(client_id);
            })
        };
        inner.guests.insert(
            client_id,
            GuestRecord {
                client_name: None,
                write_half: writer_for_record,
                task,
            },
        );
        client_id
    }

    /// Bind and serve an [`Endpoint`] (parsed from a `tcp://` or
    /// `uds://` URI). For UDS endpoints, stale socket files from a
    /// previous run are cleaned up before binding, and the returned
    /// [`EndpointHandle`] removes the socket file on drop.
    pub async fn serve_endpoint(
        self: &Arc<Self>,
        endpoint: crate::Endpoint,
    ) -> std::io::Result<EndpointHandle> {
        match endpoint {
            crate::Endpoint::Tcp(addr) => {
                let listener = tokio::net::TcpListener::bind(&addr).await?;
                let local = listener.local_addr()?;
                tracing::debug!(listen = %local, "TCP endpoint ready");
                Ok(EndpointHandle {
                    join: self.serve_tcp(listener),
                    _uds_guard: None,
                })
            }
            #[cfg(unix)]
            crate::Endpoint::Uds(path) => {
                // Best-effort cleanup of a stale socket file from a
                // previous run. `bind` would otherwise fail with
                // EADDRINUSE.
                if path.exists() {
                    let _ = std::fs::remove_file(&path);
                }
                let listener = tokio::net::UnixListener::bind(&path)?;
                tracing::debug!(listen = %path.display(), "UDS endpoint ready");
                Ok(EndpointHandle {
                    join: self.serve_unix(listener),
                    _uds_guard: Some(UdsSocketGuard { path }),
                })
            }
            #[cfg(not(unix))]
            crate::Endpoint::Uds(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix domain sockets are only supported on Unix platforms",
            )),
        }
    }

    /// Spawn an accept loop that pulls TCP connections off `listener`
    /// and feeds each one into [`accept`](Self::accept). The returned
    /// [`JoinHandle`] yields `Ok(())` only on graceful end-of-stream;
    /// the loop runs until the listener errors (e.g. socket closed,
    /// resource exhausted).
    pub fn serve_tcp(
        self: &Arc<Self>,
        listener: tokio::net::TcpListener,
    ) -> JoinHandle<std::io::Result<()>> {
        let host = self.clone();
        tokio::spawn(async move {
            loop {
                let (stream, peer) = listener.accept().await?;
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::warn!(?peer, error = %e, "set_nodelay failed on accepted TCP stream");
                }
                let id = host.accept_tcp(stream);
                tracing::debug!(?peer, client_id = id, "TCP guest accepted");
            }
        })
    }

    /// Spawn an accept loop on a Unix domain socket listener. Same
    /// semantics as [`serve_tcp`](Self::serve_tcp). Linux/macOS only.
    #[cfg(unix)]
    pub fn serve_unix(
        self: &Arc<Self>,
        listener: tokio::net::UnixListener,
    ) -> JoinHandle<std::io::Result<()>> {
        let host = self.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await?;
                let peer = stream
                    .peer_addr()
                    .ok()
                    .and_then(|a| a.as_pathname().map(|p| p.display().to_string()));
                let id = host.accept_unix(stream);
                tracing::debug!(peer = ?peer, client_id = id, "UDS guest accepted");
            }
        })
    }

    /// Publish a typed value from the host itself. Routes to every
    /// guest currently subscribed to `T`'s type-name group, and folds
    /// the value into the cache pool if `T` is `cached`.
    ///
    /// Synchronous: enqueues bytes to each subscriber's drainer
    /// task. The actual socket I/O happens asynchronously in those
    /// drainer tasks.
    pub fn publish<P: Publishable + StructValue>(&self, value: &P) {
        let type_name = Transmittable::type_name(value).to_string();
        let mask = StructValue::valid_set(value);
        let header = dots!(DotsHeader {
            type_name: type_name.clone(),
            attributes: mask,
            sender: HOST_ID,
            sent_time: now_timepoint(),
            server_sent_time: now_timepoint(),
        });
        let dyn_payload = self.payload_as_dynamic(&type_name, value, mask);
        let mut bytes = Vec::with_capacity(64);
        encode_transmission_into(&header, value, &mut bytes);
        self.cache_and_fan_out(&type_name, &header, Bytes::from(bytes), dyn_payload.as_ref(), HOST_ID);
    }

    /// Publish a partial update from the host. Same masking rules as
    /// [`GuestTransceiver::publish_with_mask`](crate::GuestTransceiver::publish_with_mask):
    /// only properties that are both set on `value` and present in
    /// `included | key_set(value)` are emitted.
    pub fn publish_with_mask<P: Publishable + StructValue>(&self, value: &P, included: PropertySet) {
        let type_name = Transmittable::type_name(value).to_string();
        let mask = (included | Transmittable::key_set(value)) & StructValue::valid_set(value);
        let header = dots!(DotsHeader {
            type_name: type_name.clone(),
            attributes: mask,
            sender: HOST_ID,
            sent_time: now_timepoint(),
            server_sent_time: now_timepoint(),
        });
        let dyn_payload = self.payload_as_dynamic(&type_name, value, mask);
        let mut bytes = Vec::with_capacity(64);
        encode_transmission_with_mask_into(&header, value, mask, &mut bytes);
        self.cache_and_fan_out(&type_name, &header, Bytes::from(bytes), dyn_payload.as_ref(), HOST_ID);
    }

    /// Publish a removal from the host. Routes to every guest
    /// subscribed to the value's type-name group, with
    /// `header.remove_obj = true` and only key fields in the payload.
    /// Drops the entry from the host's cache pool.
    pub fn remove<P: Publishable + StructValue>(&self, value: &P) {
        let type_name = Transmittable::type_name(value).to_string();
        let mask = Transmittable::key_set(value);
        let header = dots!(DotsHeader {
            type_name: type_name.clone(),
            attributes: mask,
            sender: HOST_ID,
            sent_time: now_timepoint(),
            server_sent_time: now_timepoint(),
            remove_obj: true,
        });
        let dyn_payload = self.payload_as_dynamic(&type_name, value, mask);
        let mut bytes = Vec::with_capacity(64);
        encode_transmission_with_mask_into(&header, value, mask, &mut bytes);
        self.cache_and_fan_out(&type_name, &header, Bytes::from(bytes), dyn_payload.as_ref(), HOST_ID);
    }

    /// Build the cache-side representation of a typed publish.
    ///
    /// Uses the registry's static descriptor (always present here —
    /// the host's `publish<P: Publishable>` is bound by `P:
    /// StructValue`, which means the descriptor was registered
    /// statically). Cloning into an `AnyStruct` with the desired mask
    /// avoids the CBOR encode-then-decode roundtrip the previous
    /// `DynamicStruct`-only implementation used.
    fn payload_as_dynamic<T: StructValue>(
        &self,
        type_name: &str,
        value: &T,
        mask: PropertySet,
    ) -> Option<dots_model::Payload> {
        let stat = self.registry.lookup_static_struct(type_name)?;
        // Sanity check: the value's own descriptor must match.
        if !core::ptr::eq(stat, value.descriptor()) {
            return None;
        }
        Some(dots_model::Payload::Typed(AnyStruct::from_struct_value_with_mask(
            value, mask,
        )))
    }

    /// Enqueue `bytes` to every subscriber of `type_name` (excluding
    /// `exclude_client_id`). **Synchronous**: holds the host inner
    /// mutex while iterating subscribers and calling each writer's
    /// `enqueue` (which takes its own short-held mutex). Avoids the
    /// async state machine the previous design needed for awaiting
    /// per-subscriber writes — actual socket I/O happens later in
    /// each guest's drainer task.
    /// Atomic cache mutation + fan-out, including four-cases
    /// dispatch to filtered subscribers.
    ///
    /// Holds [`Self::inner`] for a single critical section so the
    /// pre-merge snapshot, cache mutation, and filtered fan-out all
    /// see the same view of the world. The unfiltered hot path is
    /// byte-identical to the legacy `fan_out_bytes` flow when no
    /// filtered subs exist for `type_name` — the per-fan-out
    /// overhead is one branch on `group.filtered_subs.is_empty()`.
    ///
    /// `payload` is the post-merge `DynamicStruct` form of the
    /// publish (`None` for non-cached types whose group has no
    /// filtered subs, since no decode is needed in that case). The
    /// host's own publish helpers pre-decode via CBOR round-trip;
    /// the guest-fan-out path decodes from the raw payload bytes
    /// when it sees a cached type or filtered subs.
    fn cache_and_fan_out(
        &self,
        type_name: &str,
        header: &DotsHeader,
        raw_frame: Bytes,
        payload: Option<&dots_model::Payload>,
        exclude_client_id: u32,
    ) {
        let mut inner = self.inner.lock().expect("host mutex poisoned");

        let descriptor = match self.registry.lookup(type_name) {
            Some(dots_model::DescriptorEntry::Struct(d)) => Some(d.clone()),
            _ => None,
        };
        let is_cached = descriptor.as_ref().is_some_and(|d| d.flags.is_cached());
        let is_remove = header.remove_obj == Some(true);
        let has_filtered_subs = inner
            .groups
            .get(type_name)
            .is_some_and(|g| !g.filtered_subs.is_empty());

        // Pre-merge snapshot — only needed when at least one filtered
        // sub exists. For the all-unfiltered case the four-cases
        // branch is skipped entirely and we pay zero clone cost.
        let pre_merge_payload: Option<dots_model::Payload> = if is_cached && has_filtered_subs {
            payload.and_then(|p| {
                let key = p.key_bytes();
                inner.pool.get(type_name).and_then(|m| m.get(&key)).map(|e| e.payload.clone())
            })
        } else {
            None
        };

        // Mutate the cache.
        if is_cached {
            if let Some(p) = payload {
                let key = p.key_bytes();
                let map = inner.pool.entry(type_name.to_string()).or_default();
                if is_remove {
                    map.remove(&key);
                    if map.is_empty() {
                        inner.pool.remove(type_name);
                    }
                } else {
                    // Preserve `created_from` across updates — only
                    // the first publish for a key sets it. Mirrors
                    // dots-cpp `CloneInformation::createdFrom`.
                    let created_from = map.get(&key).and_then(|e| e.created_from).or(header.sender);
                    map.insert(
                        key,
                        CachedEntry {
                            payload: p.clone(),
                            created_from,
                            last_update_sender: header.sender,
                            last_update_time: header.sent_time,
                            attributes: header.attributes.unwrap_or_default(),
                        },
                    );
                }
            }
        }

        // Filtered fan-out: build per-sub outbound bytes while
        // holding a mutable borrow on the group (we mutate
        // `visible`). Borrow-check splits `inner.groups` mutably
        // from `inner.guests` immutably via separate field access
        // — collect (client_id, bytes) pairs and enqueue after.
        let mut filtered_outbound: Vec<(u32, Bytes)> = Vec::new();
        let key_set = descriptor
            .as_ref()
            .map(|d| {
                let mut s = PropertySet::EMPTY;
                for p in &d.properties {
                    if p.is_key {
                        s = s.with_tag(p.tag);
                    }
                }
                s
            })
            .unwrap_or(PropertySet::EMPTY);
        let key_bytes_post = payload.map(|p| p.key_bytes());
        let key_bytes_pre = pre_merge_payload.as_ref().map(|p| p.key_bytes());
        // No group entry → no subscribers → cache already mutated
        // above; nothing more to do.
        let HostInner { groups, guests, .. } = &mut *inner;
        let Some(group) = groups.get_mut(type_name) else {
            return;
        };

        if !group.filtered_subs.is_empty() && payload.is_some() {
            for ((cid, sub_id), sub) in &mut group.filtered_subs {
                if *cid == exclude_client_id {
                    continue;
                }
                let post = payload.expect("checked above");
                let was_visible = key_bytes_pre
                    .as_ref()
                    .is_some_and(|k| sub.visible.contains(k));
                let now_matches = !is_remove && matches_payload(&sub.compiled, post);

                match (now_matches, was_visible) {
                    (true, _) => {
                        if !was_visible {
                            if let Some(k) = &key_bytes_post {
                                sub.visible.insert(k.clone());
                            }
                        }
                        let mask_proj = sub.filter.property_mask.unwrap_or(post.valid_set());
                        let attrs = header.attributes.unwrap_or(post.valid_set());
                        let effective = (mask_proj | key_set) & attrs;
                        let mut h = header.clone();
                        h.attributes = Some(effective);
                        h.subscription_id = Some(*sub_id);
                        // For "enter view" we want the full
                        // post-merge state projected; we send only
                        // the bits in `effective`, which already
                        // excludes anything outside `attrs`. The
                        // payload encode below honours that mask.
                        let mut bytes = Vec::with_capacity(64);
                        encode_transmission_with_mask_into(&h, post, effective, &mut bytes);
                        filtered_outbound.push((*cid, Bytes::from(bytes)));
                    }
                    (false, true) => {
                        if let Some(k) = &key_bytes_post {
                            sub.visible.remove(k);
                        }
                        // Leave-view: synthesize a key-only remove
                        // using the post-merge keys (which equal
                        // pre-merge keys under the DOTS contract).
                        // Use the post-merge payload if available;
                        // otherwise fall back to pre-merge.
                        let p = payload.or(pre_merge_payload.as_ref()).expect("checked above");
                        let mut h = header.clone();
                        h.attributes = Some(key_set);
                        h.remove_obj = Some(true);
                        h.subscription_id = Some(*sub_id);
                        let mut bytes = Vec::with_capacity(64);
                        encode_transmission_with_mask_into(&h, p, key_set, &mut bytes);
                        filtered_outbound.push((*cid, Bytes::from(bytes)));
                    }
                    (false, false) => {}
                }
            }
        }

        // Unfiltered fan-out (byte-verbatim). All subscribers share
        // the same refcounted `Bytes`: `clone()` is an Arc bump, no
        // memcpy, so one published frame is one allocation regardless
        // of subscriber count.
        for &id in &group.unfiltered_subs {
            if id == exclude_client_id {
                continue;
            }
            if let Some(record) = guests.get(&id) {
                record.write_half.enqueue(raw_frame.clone());
            }
        }

        for (cid, bytes) in filtered_outbound {
            if let Some(record) = guests.get(&cid) {
                record.write_half.enqueue(bytes);
            }
        }
    }

    /// Enqueue `bytes` to every unfiltered subscriber of `type_name`
    /// (excluding `exclude_client_id`). **Synchronous**: holds the
    /// host inner mutex while iterating subscribers and calling
    /// each writer's `enqueue` (which takes its own short-held
    /// mutex). Avoids the async state machine the previous design
    /// needed for awaiting per-subscriber writes — actual socket
    /// I/O happens later in each guest's drainer task.
    ///
    /// Filtered subscribers are NOT touched here. They are addressed
    /// by [`Self::cache_and_fan_out`] which evaluates the
    /// four-cases transition logic and re-encodes the payload with
    /// each subscription's projection mask.
    #[allow(dead_code)] // retained for tests / callers that don't need filtering
    fn fan_out_bytes(&self, type_name: &str, bytes: Bytes, exclude_client_id: u32) {
        let inner = self.inner.lock().expect("host mutex poisoned");
        let Some(group) = inner.groups.get(type_name) else {
            return;
        };
        for &id in &group.unfiltered_subs {
            if id == exclude_client_id {
                continue;
            }
            if let Some(record) = inner.guests.get(&id) {
                record.write_half.enqueue(bytes.clone());
            }
        }
    }

    fn set_client_name(&self, client_id: u32, name: Option<String>) {
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        if let Some(record) = inner.guests.get_mut(&client_id) {
            record.client_name = name;
        }
    }

    fn join_group(&self, client_id: u32, group_name: &str) {
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        inner
            .groups
            .entry(group_name.to_string())
            .or_default()
            .unfiltered_subs
            .insert(client_id);
    }

    /// Replay the cached entries for `type_name` to the guest with
    /// `client_id`, then send `DotsCacheInfo{end_transmission}`. No-op
    /// for non-cached types.
    fn replay_cache_to(&self, client_id: u32, type_name: &str) {
        // Snapshot the cache contents and the guest's writer handle
        // under the std::Mutex, then drop the lock before any await.
        let writer: Option<SharedWriter>;
        let entries: Vec<(DotsHeader, dots_model::Payload)>;
        let cached_type: bool;
        {
            let inner = self.inner.lock().expect("host mutex poisoned");
            writer = inner.guests.get(&client_id).map(|r| r.write_half.clone());
            cached_type = matches!(
                self.registry.lookup(type_name),
                Some(dots_model::DescriptorEntry::Struct(d)) if d.flags.is_cached()
            );
            entries = match inner.pool.get(type_name) {
                None => Vec::new(),
                Some(map) => {
                    // `header.from_cache` counts down from `len-1` to
                    // `0` so the receiver knows when the last entry
                    // has arrived.
                    let total = map.len();
                    map.values()
                        .enumerate()
                        .map(|(i, e)| {
                            let from_cache = (total - 1 - i) as u32;
                            let header = dots!(DotsHeader {
                                type_name: type_name.to_string(),
                                sent_time: e.last_update_time,
                                server_sent_time: now_timepoint(),
                                attributes: e.attributes,
                                sender: e.last_update_sender,
                                from_cache: from_cache,
                                remove_obj: false,
                                is_from_myself: false,
                            });
                            (header, e.payload.clone())
                        })
                        .collect()
                }
            };
        }

        let Some(writer) = writer else {
            return;
        };
        // No entries and not a cached type: don't even send the end
        // marker — the guest's preload sequencer doesn't expect one
        // for non-cached groups.
        if entries.is_empty() && !cached_type {
            return;
        }

        // Build one contiguous buffer with every replayed entry's
        // frame back-to-back, then a final `end_transmission`
        // marker. A single `write_all` call sends them as one
        // syscall (or one barrier-locked retry loop), keeping the
        // whole replay atomic against concurrent fan-outs without
        // having to expose a multi-write lock-guard from
        // `SharedWriter`.
        let mut buf = Vec::with_capacity(128 + entries.len() * 96);
        for (header, payload) in &entries {
            Transmission {
                header: header.clone(),
                payload: payload.clone(),
            }
            .encode_into(&mut buf);
        }
        let info = dots!(DotsCacheInfo {
            type_name: type_name,
            end_transmission: true,
        });
        let header = dots!(DotsHeader {
            type_name: "DotsCacheInfo",
            attributes: Transmittable::valid_set(&info),
            sender: HOST_ID,
        });
        encode_transmission_into(&header, &info, &mut buf);
        writer.enqueue(Bytes::from(buf));
    }

    fn leave_group(&self, client_id: u32, group_name: &str) {
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        if let Some(g) = inner.groups.get_mut(group_name) {
            g.unfiltered_subs.remove(&client_id);
            if g.is_empty() {
                inner.groups.remove(group_name);
            }
        }
    }

    /// Remove a single filtered subscription identified by
    /// `(client_id, subscription_id)`. Prunes the enclosing group
    /// entry when no unfiltered or filtered subscribers remain.
    fn leave_filtered_sub(&self, client_id: u32, group_name: &str, subscription_id: u32) {
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        if let Some(g) = inner.groups.get_mut(group_name) {
            g.filtered_subs.remove(&(client_id, subscription_id));
            if g.is_empty() {
                inner.groups.remove(group_name);
            }
        }
    }

    /// Compile the predicate for a filtered-join request, install
    /// the [`FilteredSub`], and preload matching cache entries to
    /// the requesting guest.
    ///
    /// On validation failure (unknown property, type mismatch, etc.)
    /// the join is logged and dropped — the guest's View<T> never
    /// sees any traffic and will time out / detect the failure via
    /// its own preload-completion expectations.
    fn handle_filtered_join(
        &self,
        client_id: u32,
        group_name: &str,
        subscription_id: u32,
        filter: DotsFilter,
    ) {
        let descriptor = match self.registry.lookup(group_name) {
            Some(dots_model::DescriptorEntry::Struct(d)) => d.clone(),
            _ => {
                tracing::warn!(
                    client_id,
                    group_name,
                    subscription_id,
                    "filtered join for unknown type; dropping"
                );
                return;
            }
        };
        let empty_predicate = dots_model::DotsPredicate::default();
        let predicate = filter.predicate.as_ref().unwrap_or(&empty_predicate);
        let compiled = match CompiledPredicate::compile(predicate, &descriptor) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    client_id,
                    group_name,
                    subscription_id,
                    error = %e,
                    "filtered join failed predicate compilation; dropping"
                );
                return;
            }
        };

        // Build the FilteredSub, populate `visible` during preload.
        let mut sub = FilteredSub {
            filter: filter.clone(),
            compiled,
            visible: HashSet::new(),
        };

        // Compute the key-set + projection mask once.
        let mut key_set = PropertySet::EMPTY;
        for p in &descriptor.properties {
            if p.is_key {
                key_set = key_set.with_tag(p.tag);
            }
        }
        let project = filter.property_mask.unwrap_or(PropertySet::from_bits(u32::MAX));

        // Snapshot matching cache entries (two-pass so the
        // `from_cache` countdown is accurate). Also grab the
        // guest's writer.
        let (writer, matches): (Option<SharedWriter>, Vec<(DotsHeader, dots_model::Payload, Vec<u8>)>) = {
            let inner = self.inner.lock().expect("host mutex poisoned");
            let writer = inner.guests.get(&client_id).map(|r| r.write_half.clone());
            let mut out = Vec::new();
            if let Some(map) = inner.pool.get(group_name) {
                for (k, e) in map.iter() {
                    if matches_payload(&sub.compiled, &e.payload) {
                        let attrs = e.attributes;
                        let effective = (project | key_set) & attrs;
                        let header = dots!(DotsHeader {
                            type_name: group_name.to_string(),
                            sent_time: e.last_update_time,
                            server_sent_time: now_timepoint(),
                            attributes: effective,
                            sender: e.last_update_sender,
                            from_cache: 0_u32,
                            remove_obj: false,
                            is_from_myself: false,
                            subscription_id: subscription_id,
                        });
                        out.push((header, e.payload.clone(), k.clone()));
                    }
                }
            }
            (writer, out)
        };

        // Populate `visible` to reflect what we just preloaded.
        for (_, _, k) in &matches {
            sub.visible.insert(k.clone());
        }

        // Insert the sub now (so any concurrent publish on this
        // type is dispatched to it). Insertion happens AFTER the
        // preload snapshot so we don't double-deliver in the
        // unlikely race where a publish lands between snapshot and
        // insert — those are picked up by the publish path's
        // four-cases logic instead.
        {
            let mut inner = self.inner.lock().expect("host mutex poisoned");
            inner
                .groups
                .entry(group_name.to_string())
                .or_default()
                .filtered_subs
                .insert((client_id, subscription_id), sub);
        }

        let Some(writer) = writer else {
            return;
        };

        // Stream matched entries with descending `from_cache`
        // countdown, terminated by `DotsCacheInfo{end_transmission}`.
        let total = matches.len();
        let mut buf = Vec::with_capacity(128 + total * 96);
        for (i, (mut header, payload, _k)) in matches.into_iter().enumerate() {
            header.from_cache = Some((total - 1 - i) as u32);
            let mask = header.attributes.unwrap_or(payload.valid_set());
            encode_transmission_with_mask_into(&header, &payload, mask, &mut buf);
        }
        let info = dots!(DotsCacheInfo {
            type_name: group_name,
            end_transmission: true,
        });
        // Terminator carries no subscription_id — matches dots-cpp
        // wire (the receiver routes it through the global
        // dispatcher, not through the View). See HostTransceiver.cpp
        // in dots-cpp branch server-side-filtering.
        let header = dots!(DotsHeader {
            type_name: "DotsCacheInfo",
            attributes: Transmittable::valid_set(&info),
            sender: HOST_ID,
        });
        encode_transmission_into(&header, &info, &mut buf);
        writer.enqueue(Bytes::from(buf));
    }

    fn remove_guest(&self, client_id: u32) {
        // 1. Honor `[cleanup]` flags: any cached entry whose
        //    `last_update_sender` is this guest must be removed and
        //    its removal fanned out to subscribers. Mirrors C++
        //    HostTransceiver::handleTransitionImpl.
        self.cleanup_entries_for_guest(client_id);

        // 2. Snapshot final write-stats while the writer is still
        //    mapped — the transition handler receives them inside
        //    `final_stats` so daemons can publish a terminal
        //    `DotsClientStatistics` for short-lived guests.
        let final_stats = self.writer_for(client_id).and_then(|w| w.snapshot_stats());

        // 3. Drop the guest from the registry and prune empty
        //    groups. Snapshot the name so we can deliver it in the
        //    transition event after the lock is released.
        let name = {
            let mut inner = self.inner.lock().expect("host mutex poisoned");
            let name = inner
                .guests
                .get(&client_id)
                .and_then(|r| r.client_name.clone());
            inner.guests.remove(&client_id);
            for g in inner.groups.values_mut() {
                g.unfiltered_subs.remove(&client_id);
                g.filtered_subs.retain(|(cid, _), _| *cid != client_id);
            }
            inner.groups.retain(|_, g| !g.is_empty());
            name
        };
        tracing::debug!(client_id, "guest removed");

        // 4. Fire the transition. Daemon-level types
        //    (DotsClient, DotsClientStatistics, …) are published by
        //    the application's handler — the host owns no daemon
        //    type knowledge.
        self.fire_transition(ConnectionTransition {
            client_id,
            client_name: name,
            state: DotsConnectionState::Closed,
            final_stats,
        });
    }

    /// Walk the cache pool for entries owned by `client_id` whose
    /// type carries the `[cleanup]` flag, build synthetic removal
    /// transmissions for each, and route them through the standard
    /// `cache_and_fan_out` path so the pool mutation, unfiltered
    /// fan-out, and filtered four-cases all happen atomically per
    /// entry. Mirrors dots-cpp's "auto-remove instances of cleanup-
    /// flagged types when their publisher disconnects" semantics.
    fn cleanup_entries_for_guest(&self, client_id: u32) {
        // Pass 1: identify the cleanup-eligible entries without
        // mutating the pool. We need the payload (for filter
        // evaluation) snapshot before `cache_and_fan_out` removes
        // them on our behalf.
        let to_remove: Vec<(String, CachedEntry)> = {
            let inner = self.inner.lock().expect("host mutex poisoned");
            let cleanup_types: Vec<String> = inner
                .pool
                .keys()
                .filter(|name| {
                    matches!(
                        self.registry.lookup(name),
                        Some(dots_model::DescriptorEntry::Struct(d))
                            if d.flags.is_cleanup()
                    )
                })
                .cloned()
                .collect();
            let mut out: Vec<(String, CachedEntry)> = Vec::new();
            for type_name in cleanup_types {
                if let Some(map) = inner.pool.get(&type_name) {
                    for (_k, entry) in map.iter() {
                        if entry.last_update_sender == Some(client_id) {
                            out.push((type_name.clone(), entry.clone()));
                        }
                    }
                }
            }
            out
        };

        if !to_remove.is_empty() {
            tracing::debug!(
                client_id,
                count = to_remove.len(),
                "publishing cleanup removals for departing guest"
            );
        }
        for (type_name, entry) in to_remove {
            let txn = removal_txn(&type_name, entry);
            self.route_synthetic_removal(&type_name, txn);
        }
    }

    /// Encode a synthetic removal transmission and route it through
    /// [`Self::cache_and_fan_out`] — does the pool removal,
    /// unfiltered byte fanout, and filtered four-cases dispatch in
    /// one critical section per entry.
    fn route_synthetic_removal(&self, type_name: &str, txn: Transmission) {
        let mut buf = Vec::with_capacity(64);
        txn.encode_into(&mut buf);
        self.cache_and_fan_out(type_name, &txn.header, Bytes::from(buf), Some(&txn.payload), HOST_ID);
    }

    /// Snapshot the per-guest writer for `client_id`. Returns `None`
    /// if the guest is no longer connected. Acquires and releases the
    /// inner lock on each call — cheap (one std::Mutex round-trip),
    /// so callers don't need to share the snapshot across an await.
    fn writer_for(&self, client_id: u32) -> Option<SharedWriter> {
        self.inner
            .lock()
            .expect("host mutex poisoned")
            .guests
            .get(&client_id)
            .map(|r| r.write_half.clone())
    }

    /// Encode each removal transmission and route it through
    /// `cache_and_fan_out` so both unfiltered and filtered subs see
    /// the leave. Shared by `cleanup_entries_for_guest` and
    /// `handle_clear_cache`.
    fn fan_out_removals(&self, txns: Vec<Transmission>) {
        for txn in txns {
            let type_name = txn.header.type_name.clone().unwrap_or_default();
            self.route_synthetic_removal(&type_name, txn);
        }
    }

}

// ===== Per-guest task =====

async fn run_guest<R>(
    host: Arc<HostTransceiver>,
    client_id: u32,
    read_half: R,
    writer: SharedWriter,
) -> Result<(), HostError>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    // Reader-only loop: outbound bytes (handshake replies, fan-out
    // arrivals from peer tasks) are enqueued into `writer` by
    // whichever task produces them; the per-guest drainer task does
    // the socket I/O.
    let mut stream_in =
        FramedRead::new(read_half, RawTransmissionCodec::new(host.registry.clone()));

    // ----- Phase 1: Hello -----
    let hello = dots!(DotsMsgHello {
        server_name: host.self_name.clone(),
        auth_challenge: 0_u64,
        authentication_required: false,
        capabilities: DotsServerCapabilities {
            filtered_subscriptions: true,
        },
    });
    write_typed(&writer, "DotsMsgHello", &hello);
    tracing::debug!(client_id, "sent Hello");

    // ----- Phase 2: Connect / ConnectResponse -----
    let connect_raw = next_txn(&mut stream_in).await?;
    let connect: DotsMsgConnect = decode_handshake(&connect_raw, "DotsMsgConnect")?;
    let preload_requested = connect.preload_cache == Some(true);
    host.set_client_name(client_id, connect.client_name.clone());
    tracing::debug!(
        client_id,
        client_name = ?connect.client_name,
        preload = preload_requested,
        "Connect received"
    );

    let resp = dots!(DotsMsgConnectResponse {
        server_name: host.self_name.clone(),
        client_id: client_id,
        accepted: true,
        preload: preload_requested,
    });
    write_typed(&writer, "DotsMsgConnectResponse", &resp);
    let initial_state = if preload_requested {
        DotsConnectionState::EarlySubscribe
    } else {
        DotsConnectionState::Connected
    };
    host.fire_transition(ConnectionTransition {
        client_id,
        client_name: connect.client_name.clone(),
        state: initial_state,
        final_stats: None,
    });

    // ----- Phase 3: EarlySubscribe (if preload requested) -----
    if preload_requested {
        loop {
            match stream_in.next().await {
                Some(Ok(raw)) => {
                    writer.record_received(raw.frame_bytes);
                    if handle_preload_message(&host, client_id, &raw, &writer)? {
                        break;
                    }
                }
                Some(Err(e)) => return Err(HostError::Transport(e.to_string())),
                None => return Ok(()),
            }
        }
        tracing::debug!(client_id, "guest preload phase complete");
        host.fire_transition(ConnectionTransition {
            client_id,
            client_name: connect.client_name.clone(),
            state: DotsConnectionState::Connected,
            final_stats: None,
        });
    }

    // ----- Phase 4: Connected — fan-out happens inline in
    //                 `handle_connected_message` via per-guest
    //                 drainer tasks; this loop only reads.
    loop {
        match stream_in.next().await {
            Some(Ok(raw)) => {
                writer.record_received(raw.frame_bytes);
                handle_connected_message(&host, client_id, &raw);
            }
            Some(Err(e)) => return Err(HostError::Transport(e.to_string())),
            None => return Ok(()),
        }
        // Slow-consumer disconnect: if our own queue overflowed
        // because peers wrote faster than we read, give up.
        // `remove_guest` cleans up the registry and fires the
        // `Closed` transition for us.
        if writer.is_overflowed() {
            tracing::warn!(client_id, "guest write buffer overflowed; disconnecting");
            return Err(HostError::Transport("write buffer overflow".into()));
        }
    }
}

/// Handle a transmission received during the EarlySubscribe phase.
/// Returns `Ok(true)` when the guest sent `preload_client_finished`,
/// at which point the broker should send `ConnectResponse(preload_finished)`.
fn handle_preload_message(
    host: &Arc<HostTransceiver>,
    client_id: u32,
    raw: &RawTransmission,
    writer: &SharedWriter,
) -> Result<bool, HostError> {
    let type_name = raw.header.type_name.as_deref().unwrap_or("");
    if type_name == "DotsMsgConnect" {
        let connect: DotsMsgConnect = decode_handshake(raw, "DotsMsgConnect")?;
        if connect.preload_client_finished == Some(true) {
            let resp = dots!(DotsMsgConnectResponse {
                server_name: host.self_name.clone(),
                client_id: client_id,
                accepted: true,
                preload_finished: true,
            });
            write_typed(writer, "DotsMsgConnectResponse", &resp);
            return Ok(true);
        }
        return Ok(false);
    }
    // Everything else during preload — including descriptor data and
    // DotsMember — flows through the same dispatch the connected
    // phase uses. That ensures descriptors received during one
    // guest's preload are also fanned out to other already-connected
    // guests subscribed to `StructDescriptorData`.
    handle_connected_message(host, client_id, raw);
    Ok(false)
}

/// Handle a transmission received in the Connected phase: route
/// `DotsMember`, fan everything else out to the type group.
///
/// Synchronous: enqueues outbound bytes to the relevant guests'
/// per-guest drainer tasks. The actual socket I/O happens later in
/// those tasks — keeps this hot path closure-free.
fn handle_connected_message(
    host: &Arc<HostTransceiver>,
    client_id: u32,
    raw: &RawTransmission,
) {
    let Some(type_name) = raw.header.type_name.as_deref() else {
        return;
    };
    if type_name == "DotsMember" {
        // Group membership is for the broker; not fanned out.
        handle_member(host, client_id, raw);
        return;
    }
    if type_name == "DotsEcho" {
        // Direct reply only; no fan-out.
        handle_echo(host, client_id, raw);
        return;
    }
    if type_name == "DotsDescriptorRequest" {
        // Direct reply with all matching descriptors; no fan-out.
        handle_descriptor_request(host, client_id, raw);
        return;
    }
    if type_name == "DotsClearCache" {
        // Operates on the broker's pool; the resulting removals are
        // fanned out via the standard publish-with-remove_obj path.
        handle_clear_cache(host, raw);
        return;
    }
    if type_name == "StructDescriptorData" {
        // Register locally so we can decode subsequent payloads of
        // the new type, then fall through to fan-out — other guests
        // subscribed to `StructDescriptorData` (e.g. dots-cpp guests
        // populating their own type registry) need to see it too.
        register_incoming_struct(host, raw);
    } else if type_name == "EnumDescriptorData" {
        register_incoming_enum(host, raw);
    } else if type_name == "DotsMsgError" {
        // dots-cpp sends `DotsMsgError{ .errorCode = 0 }` from its
        // Connection destructor as part of graceful shutdown
        // (`Connection.cpp:60`), so code 0 is *not* an error — it's a
        // clean-close marker. Non-zero codes are real protocol errors;
        // either way the peer will close the socket and our reader
        // loop sees EOF.
        match decode_typed_from_slice::<dots_model::DotsMsgError>(&raw.payload) {
            Ok(err) if err.error_code == Some(0) => tracing::debug!(
                client_id,
                "guest signalled graceful close via DotsMsgError(0)",
            ),
            Ok(err) => tracing::warn!(
                client_id,
                error_code = ?err.error_code,
                error_text = ?err.error_text,
                "guest reported protocol error",
            ),
            Err(e) => tracing::warn!(
                client_id, error = %e, "received malformed DotsMsgError",
            ),
        }
        return;
    } else if type_name == "DotsMsgConnect"
        || type_name == "DotsMsgConnectResponse"
        || type_name == "DotsMsgHello"
    {
        // Handshake messages outside the handshake — log and drop.
        tracing::warn!(client_id, type_name, "stray handshake message after preload");
        return;
    }

    // Build the outbound header: preserve `sender` if the guest set
    // it, else stamp with the broker's view; refresh server_sent_time;
    // default `is_from_myself` to false (the receiving guest flips it
    // to true on loopback when comparing sender to its own client_id).
    let mut header = raw.header.clone();
    if header.sender.is_none() {
        header.sender = Some(client_id);
    }
    header.server_sent_time = Some(now_timepoint());
    if header.is_from_myself.is_none() {
        header.is_from_myself = Some(false);
    }

    // Decode the payload to `DynamicStruct` only if needed — i.e.
    // the type is cached (cache merge needs the dynamic form) or
    // any filtered subscription exists (filter evaluation needs
    // it). Non-cached types with no filtered subs skip the decode
    // entirely; their fan-out is purely byte-verbatim.
    let descriptor = match host.registry.lookup(type_name) {
        Some(dots_model::DescriptorEntry::Struct(d)) => Some(d.clone()),
        _ => None,
    };
    let is_cached = descriptor.as_ref().is_some_and(|d| d.flags.is_cached());
    let needs_dynamic = is_cached || {
        let inner = host.inner.lock().expect("host mutex poisoned");
        inner
            .groups
            .get(type_name)
            .is_some_and(|g| !g.filtered_subs.is_empty())
    };
    let dyn_payload: Option<dots_model::Payload> = if needs_dynamic {
        match (&descriptor, DynamicStruct::decode(descriptor.clone().unwrap_or_else(|| panic!("descriptor present")), &raw.payload)) {
            (Some(_), Ok(p)) => Some(dots_model::Payload::Wire(p)),
            (Some(_), Err(e)) => {
                tracing::warn!(
                    client_id,
                    type_name,
                    error = %e,
                    "failed to decode payload for cache/filter dispatch; falling back to unfiltered fan-out",
                );
                None
            }
            _ => None,
        }
    } else {
        None
    };

    // Encode the framed bytes for the unfiltered fast path: re-encode
    // the (small) header, splice in the payload bytes verbatim. No
    // DynamicStruct allocation needed, no CBOR walk over the payload.
    let mut buf = Vec::with_capacity(FRAME_OVERHEAD_HINT + raw.payload.len());
    encode_frame_with_header(&header, &raw.payload, &mut buf);

    host.cache_and_fan_out(type_name, &header, Bytes::from(buf), dyn_payload.as_ref(), 0);
}

/// Capacity hint for the per-frame overhead (4-byte size prefix +
/// CBOR-encoded `DotsHeader`) so the fan-out scratch buffer doesn't
/// need to grow on the first append. Hot path; constant.
const FRAME_OVERHEAD_HINT: usize = 64;

/// Reply to a `DotsEcho` request: copy the payload, set `request =
/// false`, and send it back only to the originating guest. No fan-out.
fn handle_echo(host: &Arc<HostTransceiver>, client_id: u32, raw: &RawTransmission) {
    let echo: dots_model::DotsEcho = match decode_typed_from_slice(&raw.payload) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(client_id, error = %e, "failed to decode DotsEcho");
            return;
        }
    };
    if echo.request != Some(true) {
        // Only requests get replies; ignore replies from misbehaving
        // peers (we don't currently send our own echo requests).
        return;
    }
    let reply = dots!(dots_model::DotsEcho {
        request: false,
        ..echo
    });
    let header = dots!(DotsHeader {
        type_name: "DotsEcho",
        attributes: Transmittable::valid_set(&reply),
        sender: HOST_ID,
        sent_time: now_timepoint(),
        server_sent_time: now_timepoint(),
    });
    let mut buf = Vec::with_capacity(64);
    encode_transmission_into(&header, &reply, &mut buf);
    if let Some(writer) = host.writer_for(client_id) {
        writer.enqueue(Bytes::from(buf));
    }
}

/// Reply to a `DotsDescriptorRequest`: stream all known non-internal
/// struct descriptors (filtered by whitelist/blacklist) directly to
/// the requesting guest, terminated by
/// `DotsCacheInfo{end_descriptor_request: true}`. Mirrors C++
/// `HostTransceiver::handleDescriptorRequest`.
/// Reorder `descriptors` so any nested struct dependency appears
/// before the type that references it. The receiver's
/// `Registry::build_dynamic_struct` resolves property `type_name`s
/// against types already registered, so emitting a dependent first
/// would leave the client unable to decode payloads that reference
/// the missing leaf.
///
/// Plain DFS post-order over the dependency graph. Dependencies that
/// fall outside the input set (e.g. `internal` types the client
/// statically registers via [`dots_model::register_dots_internal_types`])
/// are silently skipped — they're assumed already present.
fn sort_descriptors_by_dependency(
    descriptors: Vec<Arc<DynamicStructDescriptor>>,
) -> Vec<Arc<DynamicStructDescriptor>> {
    let by_name: BTreeMap<&str, &Arc<DynamicStructDescriptor>> = descriptors
        .iter()
        .map(|d| (d.name.as_str(), d))
        .collect();
    let mut visited: HashSet<String> = HashSet::with_capacity(descriptors.len());
    let mut out: Vec<Arc<DynamicStructDescriptor>> = Vec::with_capacity(descriptors.len());
    for d in &descriptors {
        visit_descriptor(d, &by_name, &mut visited, &mut out);
    }
    out
}

fn visit_descriptor(
    node: &Arc<DynamicStructDescriptor>,
    by_name: &BTreeMap<&str, &Arc<DynamicStructDescriptor>>,
    visited: &mut HashSet<String>,
    out: &mut Vec<Arc<DynamicStructDescriptor>>,
) {
    if !visited.insert(node.name.clone()) {
        return;
    }
    for p in &node.properties {
        walk_field_for_struct_deps(&p.kind, by_name, visited, out);
    }
    out.push(node.clone());
}

fn walk_field_for_struct_deps(
    kind: &DynamicFieldKind,
    by_name: &BTreeMap<&str, &Arc<DynamicStructDescriptor>>,
    visited: &mut HashSet<String>,
    out: &mut Vec<Arc<DynamicStructDescriptor>>,
) {
    match kind {
        DynamicFieldKind::Vec(inner) => {
            walk_field_for_struct_deps(inner, by_name, visited, out)
        }
        DynamicFieldKind::Struct(child) => {
            if let Some(node) = by_name.get(child.name.as_str()) {
                visit_descriptor(node, by_name, visited, out);
            }
            // Else: leaf isn't in the visible set — presumed pre-
            // registered on the client (internal type) or otherwise
            // out of scope. Not a cycle, so skip silently.
        }
        _ => {}
    }
}

fn handle_descriptor_request(
    host: &Arc<HostTransceiver>,
    client_id: u32,
    raw: &RawTransmission,
) {
    let req: dots_model::DotsDescriptorRequest = match decode_typed_from_slice(&raw.payload) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(client_id, error = %e, "failed to decode DotsDescriptorRequest");
            return;
        }
    };
    let whitelist = req.whitelist.as_deref().unwrap_or(&[]);
    let blacklist = req.blacklist.as_deref().unwrap_or(&[]);

    // Snapshot of registered struct descriptors. The registry's
    // entries map already has the dynamic form for each.
    let visible: Vec<Arc<DynamicStructDescriptor>> = host
        .registry
        .iter_structs()
        .into_iter()
        .filter(|d| !d.flags.is_internal())
        .filter(|d| whitelist.is_empty() || whitelist.iter().any(|w| w == &d.name))
        .filter(|d| !blacklist.iter().any(|b| b == &d.name))
        .collect();
    // The client's `Registry::build_dynamic_struct` resolves nested
    // struct references against types it already has — so a
    // dependent type emitted before its leaf would be undecodable.
    // Topo-sort by dependency before serialising.
    let descriptors = sort_descriptors_by_dependency(visible);

    tracing::debug!(
        client_id,
        count = descriptors.len(),
        "replying to DotsDescriptorRequest"
    );

    // Snapshot the guest's writer once, then concatenate every
    // descriptor + terminator into a single buffer that's written as
    // one atomic enqueue — keeps the descriptor stream from
    // interleaving with anything else this guest might be sent.
    let Some(writer) = host.writer_for(client_id) else {
        return;
    };

    // Concatenate every descriptor frame plus the terminating
    // `DotsCacheInfo{end_descriptor_request}` into one buffer and
    // write it as a single atomic call — same trick as
    // `replay_cache_to`. Keeps ordering across concurrent fan-outs
    // without needing to expose a multi-write guard on SharedWriter.
    let mut buf = Vec::with_capacity(128 + descriptors.len() * 96);
    for d in &descriptors {
        let data = StructDescriptorData::from_dynamic(d);
        let header = dots!(DotsHeader {
            type_name: "StructDescriptorData",
            attributes: Transmittable::valid_set(&data),
            sender: HOST_ID,
            sent_time: now_timepoint(),
            server_sent_time: now_timepoint(),
        });
        encode_transmission_into(&header, &data, &mut buf);
    }

    let info = dots!(DotsCacheInfo {
        end_descriptor_request: true,
    });
    let header = dots!(DotsHeader {
        type_name: "DotsCacheInfo",
        attributes: Transmittable::valid_set(&info),
        sender: HOST_ID,
        sent_time: now_timepoint(),
        server_sent_time: now_timepoint(),
    });
    encode_transmission_into(&header, &info, &mut buf);
    writer.enqueue(Bytes::from(buf));
}

/// Handle `DotsClearCache`: drop the named types' entries from the
/// host's pool and fan out removal transmissions for each cleared
/// instance. Mirrors C++ `HostTransceiver::handleClearCache`.
fn handle_clear_cache(host: &Arc<HostTransceiver>, raw: &RawTransmission) {
    let req: dots_model::DotsClearCache = match decode_typed_from_slice(&raw.payload) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "failed to decode DotsClearCache");
            return;
        }
    };
    let Some(type_names) = req.type_names else {
        tracing::warn!("DotsClearCache missing type_names");
        return;
    };

    // Snapshot the entries to remove (without mutating the pool) —
    // `route_synthetic_removal` will perform the actual pool
    // removal + four-cases fan-out atomically per entry.
    let to_publish: Vec<Transmission> = {
        let inner = host.inner.lock().expect("host mutex poisoned");
        let mut out = Vec::new();
        for type_name in &type_names {
            if let Some(map) = inner.pool.get(type_name) {
                for (_, entry) in map.iter() {
                    out.push(removal_txn(type_name, entry.clone()));
                }
            }
        }
        out
    };

    if !to_publish.is_empty() {
        tracing::debug!(
            count = to_publish.len(),
            ?type_names,
            "publishing DotsClearCache removals"
        );
    }
    host.fan_out_removals(to_publish);
}

/// Build a host-originated removal transmission for a cached entry.
/// Used when the broker drops an entry on its own initiative — either
/// because the publisher disconnected (`cleanup` flag) or a guest
/// asked to clear the type via `DotsClearCache`.
fn removal_txn(type_name: &str, entry: CachedEntry) -> Transmission {
    let header = dots!(DotsHeader {
        type_name: type_name,
        sent_time: entry.last_update_time,
        server_sent_time: now_timepoint(),
        attributes: entry.attributes,
        sender: HOST_ID,
        remove_obj: true,
        is_from_myself: false,
    });
    Transmission {
        header,
        payload: entry.payload,
    }
}

fn register_incoming_struct(host: &Arc<HostTransceiver>, raw: &RawTransmission) {
    let data: StructDescriptorData = match decode_typed_from_slice(&raw.payload) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "failed to decode StructDescriptorData");
            return;
        }
    };
    let name = data.name.clone();
    if let Some(name) = name.as_deref() {
        if host.registry.lookup(name).is_some() {
            return; // already registered (likely from a previous guest).
        }
    }
    match host.registry.build_dynamic_struct(&data) {
        Ok(d) => {
            tracing::debug!(type_name = ?d.name, "registered guest struct descriptor");
            host.registry.register_struct_dynamic(Arc::new(d));
        }
        Err(e) => tracing::warn!(error = %e, "failed to build dynamic struct from descriptor"),
    }
}

/// Evaluate a compiled predicate against a payload.
///
/// The filter's predicate engine walks `DynamicStruct::properties`
/// (its tagged-union value list) directly, so wire-only payloads
/// match natively. Typed payloads land here only in test or
/// future-typed-broker setups — for the current broker route, the
/// receive path always builds [`dots_model::Payload::Wire`] before
/// calling the filter. Typed payloads conservatively return `false`
/// so an accidental Typed entry doesn't silently bypass filtering.
fn matches_payload(
    pred: &crate::filter::CompiledPredicate,
    payload: &dots_model::Payload,
) -> bool {
    match payload {
        dots_model::Payload::Wire(d) => pred.matches(d),
        dots_model::Payload::Typed(_) => false,
    }
}

fn register_incoming_enum(host: &Arc<HostTransceiver>, raw: &RawTransmission) {
    let data: EnumDescriptorData = match decode_typed_from_slice(&raw.payload) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "failed to decode EnumDescriptorData");
            return;
        }
    };
    let name = data.name.clone();
    if let Some(name) = name.as_deref() {
        if host.registry.lookup(name).is_some() {
            return;
        }
    }
    match host.registry.build_dynamic_enum(&data) {
        Ok(d) => {
            tracing::debug!(type_name = ?d.name, "registered guest enum descriptor");
            host.registry.register_enum_dynamic(Arc::new(d));
        }
        Err(e) => tracing::warn!(error = %e, "failed to build dynamic enum from descriptor"),
    }
}

fn handle_member(host: &Arc<HostTransceiver>, client_id: u32, raw: &RawTransmission) {
    let member: DotsMember = match decode_typed_from_slice(&raw.payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(client_id, error = %e, "failed to decode DotsMember");
            return;
        }
    };
    let Some(group_name) = member.group_name.clone() else {
        tracing::warn!(client_id, "DotsMember missing group_name");
        return;
    };
    let group_name = group_name.as_str();
    let has_filter = member.filter.is_some();
    let has_sub_id = member.subscription_id.is_some();
    match member.event {
        Some(DotsMemberEvent::Join) => {
            match (member.filter, member.subscription_id) {
                (Some(filter), Some(sub_id)) => {
                    host.handle_filtered_join(client_id, group_name, sub_id, filter);
                }
                (None, None) => {
                    host.join_group(client_id, group_name);
                    tracing::debug!(client_id, group_name, "guest joined group");
                    host.replay_cache_to(client_id, group_name);
                }
                _ => {
                    tracing::warn!(
                        client_id,
                        group_name,
                        has_filter,
                        has_sub_id,
                        "DotsMember(Join) with inconsistent filter/subscription_id; dropped"
                    );
                }
            }
        }
        Some(DotsMemberEvent::Leave) => {
            match member.subscription_id {
                Some(sub_id) => {
                    host.leave_filtered_sub(client_id, group_name, sub_id);
                    tracing::debug!(
                        client_id,
                        group_name,
                        sub_id,
                        "guest left filtered subscription"
                    );
                }
                None => {
                    host.leave_group(client_id, group_name);
                    tracing::debug!(client_id, group_name, "guest left group");
                }
            }
        }
        Some(DotsMemberEvent::Kill) | None => {
            tracing::warn!(client_id, ?member.event, "ignored DotsMember event");
        }
    }
}

// ===== Helpers =====

#[derive(Debug)]
enum HostError {
    Transport(String),
    Decode(String),
}

impl core::fmt::Display for HostError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(s) => write!(f, "transport: {s}"),
            Self::Decode(s) => write!(f, "decode: {s}"),
        }
    }
}

impl std::error::Error for HostError {}

fn write_typed<T>(writer: &SharedWriter, type_name: &str, value: &T)
where
    T: StructValue,
{
    let header = dots!(DotsHeader {
        type_name: type_name,
        attributes: <T as Transmittable>::valid_set(value),
        sender: HOST_ID,
    });
    let mut buf = Vec::with_capacity(64);
    encode_transmission_into(&header, value, &mut buf);
    writer.enqueue(Bytes::from(buf));
}

async fn next_txn<R>(stream: &mut R) -> Result<RawTransmission, HostError>
where
    R: futures_util::Stream<Item = Result<RawTransmission, crate::TransportError>> + Unpin,
{
    match stream.next().await {
        Some(Ok(txn)) => Ok(txn),
        Some(Err(e)) => Err(HostError::Transport(e.to_string())),
        None => Err(HostError::Transport("stream closed".into())),
    }
}

fn decode_handshake<T>(raw: &RawTransmission, expected: &str) -> Result<T, HostError>
where
    T: StructValue + Default,
{
    let actual = raw.header.type_name.as_deref().unwrap_or("");
    if actual != expected {
        return Err(HostError::Decode(format!(
            "expected {expected}, got {actual}"
        )));
    }
    decode_typed_from_slice(&raw.payload).map_err(|e| HostError::Decode(e.to_string()))
}

// ===== Public endpoint handle =====

/// Owns a live listener task plus, for UDS endpoints, an RAII guard
/// that removes the socket file on drop. Returned by
/// [`HostTransceiver::serve_endpoint`].
///
/// Hold this for the daemon's lifetime; the listener task continues
/// running as long as the host is alive (or until [`abort`](Self::abort)
/// is called). Dropping the handle drops the guard (cleaning the
/// socket file) but does *not* by itself stop the accept loop —
/// tokio aborts the task only when the runtime shuts down or
/// [`HostTransceiver::shutdown`] is called.
pub struct EndpointHandle {
    join: JoinHandle<std::io::Result<()>>,
    /// `Some` for UDS endpoints — its `Drop` removes the bound socket
    /// file. `None` for TCP. Underscore-prefixed because we only
    /// rely on its `Drop` side effect; never read directly.
    _uds_guard: Option<UdsSocketGuard>,
}

impl EndpointHandle {
    /// Abort the underlying accept-loop task. The listener stops
    /// accepting new connections immediately; existing per-guest
    /// tasks keep running until they end naturally or the host is
    /// shut down.
    pub fn abort(&self) {
        self.join.abort();
    }

    /// Await the accept loop. Yields `Ok(())` on a clean stream end
    /// and `Err(io::Error)` if the listener errored. Cancel-safe.
    pub async fn join(self) -> std::io::Result<()> {
        match self.join.await {
            Ok(r) => r,
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(std::io::Error::other(e)),
        }
    }
}

/// Removes a UDS socket file when dropped. Created internally by
/// [`HostTransceiver::serve_endpoint`] for `uds://` endpoints.
struct UdsSocketGuard {
    path: std::path::PathBuf,
}

impl Drop for UdsSocketGuard {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %e,
                    "failed to remove UDS socket file on shutdown"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dots_core::{DynamicPropertyDescriptor, StructFlags};

    fn leaf(name: &str) -> Arc<DynamicStructDescriptor> {
        Arc::new(DynamicStructDescriptor {
            name: name.to_string(),
            flags: StructFlags::NONE,
            properties: vec![],
        })
    }

    fn struct_with_nested(
        name: &str,
        nested: &Arc<DynamicStructDescriptor>,
    ) -> Arc<DynamicStructDescriptor> {
        Arc::new(DynamicStructDescriptor {
            name: name.to_string(),
            flags: StructFlags::NONE,
            properties: vec![DynamicPropertyDescriptor {
                name: "nested".to_string(),
                tag: 1,
                is_key: false,
                kind: DynamicFieldKind::Struct(nested.clone()),
            }],
        })
    }

    fn position_of(out: &[Arc<DynamicStructDescriptor>], name: &str) -> usize {
        out.iter()
            .position(|d| d.name == name)
            .unwrap_or_else(|| panic!("{name} missing from sorted output"))
    }

    #[test]
    fn sort_emits_leaf_before_dependent() {
        let leaf = leaf("DotsStatistics");
        let dependent = struct_with_nested("DotsClientStatistics", &leaf);
        // Input in "wrong" order — dependent first.
        let sorted = sort_descriptors_by_dependency(vec![dependent.clone(), leaf.clone()]);
        assert!(
            position_of(&sorted, "DotsStatistics")
                < position_of(&sorted, "DotsClientStatistics"),
            "leaf must precede dependent in sorted output: {:?}",
            sorted.iter().map(|d| d.name.as_str()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn sort_handles_vector_of_nested_struct() {
        let leaf = leaf("Inner");
        let outer = Arc::new(DynamicStructDescriptor {
            name: "Outer".to_string(),
            flags: StructFlags::NONE,
            properties: vec![DynamicPropertyDescriptor {
                name: "items".to_string(),
                tag: 1,
                is_key: false,
                kind: DynamicFieldKind::Vec(Box::new(DynamicFieldKind::Struct(leaf.clone()))),
            }],
        });
        let sorted = sort_descriptors_by_dependency(vec![outer, leaf]);
        assert!(position_of(&sorted, "Inner") < position_of(&sorted, "Outer"));
    }

    #[test]
    fn sort_skips_dependencies_outside_visible_set() {
        // Dependent references a leaf that's NOT in the input — the
        // client is presumed to already have it (internal type).
        // Sort should still succeed and emit the dependent.
        let external_leaf = leaf("DotsHeader");
        let dependent = struct_with_nested("Wrapper", &external_leaf);
        let sorted = sort_descriptors_by_dependency(vec![dependent]);
        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0].name, "Wrapper");
    }
}
