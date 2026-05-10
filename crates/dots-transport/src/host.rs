//! Host-side transceiver — the broker-facing equivalent of C++
//! `dots::HostTransceiver`.
//!
//! A [`HostTransceiver`] accepts guest connections through
//! [`accept`](HostTransceiver::accept), drives the broker-side
//! handshake (`Hello` → `Connect` → `ConnectResponse`), routes
//! `DotsMember(Join/Leave)` to maintain per-type subscription groups,
//! and fans out incoming transmissions to subscribed peers.
//!
//! This iteration is in-memory only: tests connect a guest via
//! [`tokio::io::duplex`] and use the existing [`crate::App`] /
//! [`crate::GuestTransceiver`] on the other end. A `Listener` trait
//! (TCP / UDS) will be added in a later step.
//!
//! Cache replay on `Join` is **not** done here; that lives in step 3
//! of the host implementation slice (the [`ContainerPool`]).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;

use dots_core::{DynamicStruct, StructValue, decode_typed_from_slice, key_set};
use dots_model::{
    DotsCacheInfo, DotsConnectionState, DotsHeader, DotsMember, DotsMemberEvent, DotsMsgConnect,
    DotsMsgConnectResponse, DotsMsgHello, EnumDescriptorData, RawTransmission, Registry,
    StructDescriptorData, Transmission, daemon::DotsClient, encode_frame_with_header,
    encode_typed_transmission_into, encode_typed_transmission_with_mask_into,
};
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;
use tokio_util::codec::FramedRead;

use crate::codec::RawTransmissionCodec;
use crate::guest::now_timepoint;

/// Type-erased writer half shared between the per-guest read task
/// (which writes its own handshake responses) and any other guest's
/// task that fans out to it. The `AsyncMutex` serialises concurrent
/// writers; `Box<dyn ...>` keeps `GuestRecord` non-generic so the
/// guest registry can hold heterogeneous stream types side-by-side.
type SharedWriter = Arc<AsyncMutex<Box<dyn AsyncWrite + Send + Unpin>>>;

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
    /// Per-type-name → set of guest ids that have joined the group.
    groups: FxHashMap<String, FxHashSet<u32>>,
    /// Per-guest state, keyed by client id.
    guests: FxHashMap<u32, GuestRecord>,
    /// Monotonic id allocator for new guests. Starts at 2 since 1 is
    /// reserved as `HOST_ID`.
    next_client_id: u32,
    /// Container pool: per-cached-type, key-bytes → cached entry.
    /// Updated on every incoming transmission of a cached type;
    /// replayed on `DotsMember(Join)`.
    pool: FxHashMap<String, BTreeMap<Vec<u8>, CachedEntry>>,
}

/// One cached instance held in the pool. The payload is stored as the
/// dynamic struct that arrived on the wire; on replay we re-encode it
/// with a fresh `header.from_cache` countdown.
struct CachedEntry {
    payload: DynamicStruct,
    /// Last-update sender (the publisher's `client_id`, or `HOST_ID`
    /// for host-originated publishes).
    last_update_sender: Option<u32>,
    /// Header `sent_time` from the most recent update.
    last_update_time: Option<dots_core::Timepoint>,
    /// Property bitmask of the most recent update (for reproducing
    /// `header.attributes` on replay).
    attributes: u64,
}

struct GuestRecord {
    client_name: Option<String>,
    /// Shared write end of this guest's connection. The per-guest
    /// read task uses it for handshake responses and direct replies
    /// (echo, descriptor-request); peer tasks use it during fan-out
    /// to write transmissions destined for this guest. `AsyncMutex`
    /// serialises any concurrent writers — uncontended on a typical
    /// fan-out since each subscriber's lock fires once per message.
    write_half: SharedWriter,
    /// Handle to the per-guest read task. Kept so the host can
    /// abort it during shutdown.
    #[allow(dead_code)]
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
            })),
        })
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

    /// Number of guests subscribed to a given type group.
    pub fn group_size(&self, type_name: &str) -> usize {
        self.inner
            .lock()
            .expect("host mutex poisoned")
            .groups
            .get(type_name)
            .map(FxHashSet::len)
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
        tracing::info!(guest_count, "host shutdown — guest tasks aborted");
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

    /// Accept an incoming guest stream. Spawns a tokio task that runs
    /// the broker-side handshake and inbound-dispatch loop. Returns
    /// the allocated client id.
    ///
    /// The stream is split into independent halves: the read half is
    /// owned by the spawned task; the write half is wrapped in an
    /// `AsyncMutex` so any task — including this guest's own read
    /// task and any peer task fanning out a transmission — can write
    /// to it after acquiring the lock. Replaces the older mpsc-based
    /// design where a per-guest writer task drained an outbound
    /// channel; we now write directly from whichever task produced
    /// the outbound bytes.
    pub fn accept<S>(self: &Arc<Self>, stream: S) -> u32
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let host = self.clone();
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        let client_id = inner.next_client_id;
        inner.next_client_id += 1;
        let (read_half, write_half) = tokio::io::split(stream);
        let writer: SharedWriter = Arc::new(AsyncMutex::new(
            Box::new(write_half) as Box<dyn AsyncWrite + Send + Unpin>,
        ));
        let writer_for_record = writer.clone();
        let task = {
            let host = host.clone();
            tokio::spawn(async move {
                if let Err(e) = run_guest(host.clone(), client_id, read_half, writer).await {
                    tracing::warn!(client_id, error = %e, "guest task ended with error");
                }
                host.remove_guest(client_id).await;
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
                tracing::info!(listen = %local, "TCP endpoint ready");
                Ok(EndpointHandle {
                    join: self.serve_tcp(listener),
                    _uds_guard: None,
                })
            }
            crate::Endpoint::Uds(path) => {
                // Best-effort cleanup of a stale socket file from a
                // previous run. `bind` would otherwise fail with
                // EADDRINUSE.
                if path.exists() {
                    let _ = std::fs::remove_file(&path);
                }
                let listener = tokio::net::UnixListener::bind(&path)?;
                tracing::info!(listen = %path.display(), "UDS endpoint ready");
                Ok(EndpointHandle {
                    join: self.serve_unix(listener),
                    _uds_guard: Some(UdsSocketGuard { path }),
                })
            }
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
                let id = host.accept(stream);
                tracing::info!(?peer, client_id = id, "TCP guest accepted");
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
                let id = host.accept(stream);
                tracing::info!(peer = ?peer, client_id = id, "UDS guest accepted");
            }
        })
    }

    /// Publish a typed value from the host itself. Routes to every
    /// guest currently subscribed to `T`'s type-name group, and folds
    /// the value into the cache pool if `T` is `cached`.
    ///
    /// Async because fan-out now writes directly into each
    /// subscriber's socket (no mpsc hand-off).
    pub async fn publish<T>(&self, value: &T)
    where
        T: StructValue,
    {
        let type_name = value.descriptor().name;
        let header = DotsHeader {
            type_name: Some(type_name.into()),
            attributes: Some(value.valid_set().bits()),
            sender: Some(HOST_ID),
            sent_time: Some(now_timepoint()),
            server_sent_time: Some(now_timepoint()),
            ..Default::default()
        };

        // Cache update: re-decode through the registry to get a
        // DynamicStruct (matches the shape we'd see if the value had
        // arrived from a guest).
        if let Some(dots_model::DescriptorEntry::Struct(d)) = self.registry.lookup(type_name) {
            if d.flags.is_cached() {
                let mut payload_bytes = Vec::with_capacity(64);
                let mut enc = dots_core::minicbor::Encoder::new(&mut payload_bytes);
                dots_core::encode_into_encoder(value, &mut enc).expect("encode infallible");
                if let Ok(payload) = DynamicStruct::decode(d.clone(), &payload_bytes) {
                    self.update_cache(type_name, &header, &payload);
                }
            }
        }

        let mut bytes = Vec::with_capacity(64);
        encode_typed_transmission_into(&header, value, &mut bytes);
        self.fan_out_bytes(type_name, Bytes::from(bytes), /*exclude*/ HOST_ID)
            .await;
    }

    /// Publish a removal from the host. Routes to every guest
    /// subscribed to `T`'s type-name group, with `header.remove_obj
    /// = true` and only key fields in the payload. Drops the entry
    /// from the host's cache pool.
    pub async fn remove<T>(&self, value: &T)
    where
        T: StructValue,
    {
        let type_name = value.descriptor().name;
        let mask = key_set(value);
        let header = DotsHeader {
            type_name: Some(type_name.into()),
            attributes: Some(mask.bits()),
            sender: Some(HOST_ID),
            sent_time: Some(now_timepoint()),
            server_sent_time: Some(now_timepoint()),
            remove_obj: Some(true),
            ..Default::default()
        };

        // Update cache: round-trip via DynamicStruct so `update_cache`
        // can use payload.key_bytes(). Same path as host.publish.
        if let Some(dots_model::DescriptorEntry::Struct(d)) = self.registry.lookup(type_name) {
            if d.flags.is_cached() {
                let mut payload_bytes = Vec::with_capacity(64);
                let mut enc = dots_core::minicbor::Encoder::new(&mut payload_bytes);
                dots_core::encode_into_encoder_with_mask(value, mask, &mut enc)
                    .expect("encode infallible");
                if let Ok(payload) = DynamicStruct::decode(d.clone(), &payload_bytes) {
                    self.update_cache(type_name, &header, &payload);
                }
            }
        }

        let mut bytes = Vec::with_capacity(64);
        encode_typed_transmission_with_mask_into(&header, value, mask, &mut bytes);
        self.fan_out_bytes(type_name, Bytes::from(bytes), /*exclude*/ HOST_ID)
            .await;
    }

    /// Snapshot the writer handles of every subscriber to `type_name`
    /// (excluding `exclude_client_id`), then write `bytes` to each
    /// directly. The lookup is brief — we hold the host's inner
    /// mutex only long enough to clone the `Arc<AsyncMutex<...>>`
    /// handles. Per-subscriber writes happen sequentially under
    /// each subscriber's own write lock.
    ///
    /// `SmallVec<[_; 8]>` keeps the snapshot on the stack for typical
    /// fan-outs (≤8 subscribers) — the common case for ping/pong-shaped
    /// workloads. Larger groups still work; the SmallVec spills to
    /// the heap automatically.
    async fn fan_out_bytes(&self, type_name: &str, bytes: Bytes, exclude_client_id: u32) {
        let writers: SmallVec<[SharedWriter; 8]> = {
            let inner = self.inner.lock().expect("host mutex poisoned");
            let Some(targets) = inner.groups.get(type_name) else {
                return;
            };
            targets
                .iter()
                .copied()
                .filter(|&id| id != exclude_client_id)
                .filter_map(|id| inner.guests.get(&id).map(|r| r.write_half.clone()))
                .collect()
        };
        for writer in writers {
            let mut w = writer.lock().await;
            // Subscriber-side errors are fatal for that one connection,
            // not the broker. The reader task will notice the broken
            // socket on its next poll and trigger remove_guest. Drop
            // the error here; logging it would spam on benign
            // disconnects.
            let _ = w.write_all(&bytes).await;
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
            .insert(client_id);
    }

    /// Update the cache pool for a `cached` type. Called after the
    /// host has decided to fan out a transmission. Insert/update on
    /// normal publish, remove on `header.remove_obj == Some(true)`.
    fn update_cache(&self, type_name: &str, header: &DotsHeader, payload: &DynamicStruct) {
        if !payload.descriptor.flags.is_cached() {
            return;
        }
        let key = payload.key_bytes();
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        let map = inner.pool.entry(type_name.to_string()).or_default();
        if header.remove_obj == Some(true) {
            map.remove(&key);
            if map.is_empty() {
                inner.pool.remove(type_name);
            }
            return;
        }
        map.insert(
            key,
            CachedEntry {
                payload: payload.clone(),
                last_update_sender: header.sender,
                last_update_time: header.sent_time,
                attributes: header.attributes.unwrap_or(0),
            },
        );
    }

    /// Replay the cached entries for `type_name` to the guest with
    /// `client_id`, then send `DotsCacheInfo{end_transmission}`. No-op
    /// for non-cached types.
    async fn replay_cache_to(&self, client_id: u32, type_name: &str) {
        // Snapshot the cache contents and the guest's writer handle
        // under the std::Mutex, then drop the lock before any await.
        let writer: Option<SharedWriter>;
        let entries: Vec<(DotsHeader, DynamicStruct)>;
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
                            let header = DotsHeader {
                                type_name: Some(type_name.to_string()),
                                sent_time: e.last_update_time,
                                server_sent_time: Some(now_timepoint()),
                                attributes: Some(e.attributes),
                                sender: e.last_update_sender,
                                from_cache: Some(from_cache),
                                remove_obj: Some(false),
                                is_from_myself: Some(false),
                            };
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

        // Hold the writer lock for the whole replay so other peers'
        // fan-outs don't interleave between replayed entries.
        let mut w = writer.lock().await;
        for (header, payload) in &entries {
            let txn = Transmission {
                header: header.clone(),
                payload: payload.clone(),
            };
            let mut buf = Vec::with_capacity(64);
            txn.encode_into(&mut buf);
            if w.write_all(&buf).await.is_err() {
                tracing::debug!(
                    client_id,
                    type_name,
                    "write to guest failed during cache replay; aborting"
                );
                return;
            }
        }
        // Append the end-transmission marker on the same locked
        // writer to keep ordering against any in-flight fan-outs.
        let info = DotsCacheInfo {
            type_name: Some(type_name.into()),
            end_transmission: Some(true),
            ..Default::default()
        };
        let header = DotsHeader {
            type_name: Some("DotsCacheInfo".into()),
            attributes: Some(info.valid_set().bits()),
            sender: Some(HOST_ID),
            ..Default::default()
        };
        let mut buf = Vec::with_capacity(64);
        encode_typed_transmission_into(&header, &info, &mut buf);
        let _ = w.write_all(&buf).await;
    }

    fn leave_group(&self, client_id: u32, group_name: &str) {
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        if let Some(g) = inner.groups.get_mut(group_name) {
            g.remove(&client_id);
            if g.is_empty() {
                inner.groups.remove(group_name);
            }
        }
    }

    async fn remove_guest(&self, client_id: u32) {
        // 1. Honor `[cleanup]` flags: any cached entry whose
        //    `last_update_sender` is this guest must be removed and
        //    its removal fanned out to subscribers. Mirrors C++
        //    HostTransceiver::handleTransitionImpl.
        self.cleanup_entries_for_guest(client_id).await;

        // 2. Drop the guest from the registry and prune empty
        //    groups. Snapshot the name so we can publish DotsClient
        //    after the lock is released.
        let name = {
            let mut inner = self.inner.lock().expect("host mutex poisoned");
            let name = inner
                .guests
                .get(&client_id)
                .and_then(|r| r.client_name.clone());
            inner.guests.remove(&client_id);
            for g in inner.groups.values_mut() {
                g.remove(&client_id);
            }
            inner.groups.retain(|_, g| !g.is_empty());
            name
        };
        tracing::debug!(client_id, "guest removed");

        // 3. Publish the final DotsClient state.
        self.publish_dots_client(client_id, name, DotsConnectionState::Closed, false)
            .await;
    }

    /// Walk the cache pool for entries owned by `client_id` whose
    /// type carries the `[cleanup]` flag, drop them from the pool,
    /// and fan a removal transmission out to existing subscribers.
    /// Mirrors dots-cpp's "auto-remove instances of cleanup-flagged
    /// types when their publisher disconnects" semantics.
    async fn cleanup_entries_for_guest(&self, client_id: u32) {
        let to_publish: Vec<Transmission> = {
            let mut inner = self.inner.lock().expect("host mutex poisoned");

            // Find which type-names are `[cleanup]` flagged. Collect
            // names first (avoid holding the registry lock while we
            // hold inner).
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

            let mut out = Vec::new();
            for type_name in cleanup_types {
                let Some(map) = inner.pool.get_mut(&type_name) else {
                    continue;
                };
                let keys_to_remove: Vec<Vec<u8>> = map
                    .iter()
                    .filter(|(_, e)| e.last_update_sender == Some(client_id))
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in keys_to_remove {
                    if let Some(entry) = map.remove(&key) {
                        let header = DotsHeader {
                            type_name: Some(type_name.clone()),
                            sent_time: entry.last_update_time,
                            server_sent_time: Some(now_timepoint()),
                            attributes: Some(entry.attributes),
                            sender: Some(HOST_ID),
                            remove_obj: Some(true),
                            is_from_myself: Some(false),
                            ..Default::default()
                        };
                        out.push(Transmission {
                            header,
                            payload: entry.payload,
                        });
                    }
                }
                if map.is_empty() {
                    inner.pool.remove(&type_name);
                }
            }
            out
        };

        if !to_publish.is_empty() {
            tracing::debug!(
                client_id,
                count = to_publish.len(),
                "publishing cleanup removals for departing guest"
            );
        }
        for txn in to_publish {
            let type_name = txn.header.type_name.clone().unwrap_or_default();
            let mut buf = Vec::with_capacity(64);
            txn.encode_into(&mut buf);
            self.fan_out_bytes(&type_name, Bytes::from(buf), HOST_ID)
                .await;
        }
    }

    /// Publish a [`DotsClient`] record for this guest's current state.
    /// Routes through the normal publish path so the cache pool stays
    /// in sync. C++ dotsd publishes on every connection-state
    /// transition; we mirror that.
    async fn publish_dots_client(
        &self,
        client_id: u32,
        name: Option<String>,
        state: DotsConnectionState,
        running: bool,
    ) {
        let record = DotsClient {
            id: Some(client_id),
            name,
            running: Some(running),
            connection_state: Some(state),
            ..Default::default()
        };
        self.publish(&record).await;
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
    // arrivals from peer tasks) are written directly into `writer`
    // by whichever task produced them, so we no longer need a
    // `select!` between an inbound stream and an outbound channel.
    let mut stream_in =
        FramedRead::new(read_half, RawTransmissionCodec::new(host.registry.clone()));

    // ----- Phase 1: Hello -----
    let hello = DotsMsgHello {
        server_name: Some(host.self_name.clone()),
        auth_challenge: Some(0),
        authentication_required: Some(false),
    };
    write_typed(&writer, "DotsMsgHello", &hello).await?;
    tracing::debug!(client_id, "sent Hello");

    // ----- Phase 2: Connect / ConnectResponse -----
    let connect_raw = next_txn(&mut stream_in).await?;
    let connect: DotsMsgConnect = decode_handshake(&connect_raw, "DotsMsgConnect")?;
    let preload_requested = connect.preload_cache == Some(true);
    host.set_client_name(client_id, connect.client_name.clone());
    tracing::info!(
        client_id,
        client_name = ?connect.client_name,
        preload = preload_requested,
        "Connect received"
    );

    let resp = DotsMsgConnectResponse {
        server_name: Some(host.self_name.clone()),
        client_id: Some(client_id),
        accepted: Some(true),
        preload: Some(preload_requested),
        ..Default::default()
    };
    write_typed(&writer, "DotsMsgConnectResponse", &resp).await?;
    let initial_state = if preload_requested {
        DotsConnectionState::EarlySubscribe
    } else {
        DotsConnectionState::Connected
    };
    host.publish_dots_client(
        client_id,
        connect.client_name.clone(),
        initial_state,
        true,
    )
    .await;

    // ----- Phase 3: EarlySubscribe (if preload requested) -----
    if preload_requested {
        loop {
            match stream_in.next().await {
                Some(Ok(raw)) => {
                    if handle_preload_message(&host, client_id, &raw, &writer).await? {
                        break;
                    }
                }
                Some(Err(e)) => return Err(HostError::Transport(e.to_string())),
                None => return Ok(()),
            }
        }
        tracing::debug!(client_id, "guest preload phase complete");
        host.publish_dots_client(
            client_id,
            connect.client_name.clone(),
            DotsConnectionState::Connected,
            true,
        )
        .await;
    }

    // ----- Phase 4: Connected — fan-out happens inline in
    //                 `handle_connected_message` (writes to peers'
    //                 shared writers directly).
    loop {
        match stream_in.next().await {
            Some(Ok(raw)) => {
                handle_connected_message(&host, client_id, &raw).await;
            }
            Some(Err(e)) => return Err(HostError::Transport(e.to_string())),
            None => return Ok(()),
        }
    }
}

/// Handle a transmission received during the EarlySubscribe phase.
/// Returns `Ok(true)` when the guest sent `preload_client_finished`,
/// at which point the broker should send `ConnectResponse(preload_finished)`.
async fn handle_preload_message(
    host: &Arc<HostTransceiver>,
    client_id: u32,
    raw: &RawTransmission,
    writer: &SharedWriter,
) -> Result<bool, HostError> {
    let type_name = raw.header.type_name.as_deref().unwrap_or("");
    if type_name == "DotsMsgConnect" {
        let connect: DotsMsgConnect = decode_handshake(raw, "DotsMsgConnect")?;
        if connect.preload_client_finished == Some(true) {
            let resp = DotsMsgConnectResponse {
                server_name: Some(host.self_name.clone()),
                client_id: Some(client_id),
                accepted: Some(true),
                preload_finished: Some(true),
                ..Default::default()
            };
            write_typed(writer, "DotsMsgConnectResponse", &resp).await?;
            return Ok(true);
        }
        return Ok(false);
    }
    // Everything else during preload — including descriptor data and
    // DotsMember — flows through the same dispatch the connected
    // phase uses. That ensures descriptors received during one
    // guest's preload are also fanned out to other already-connected
    // guests subscribed to `StructDescriptorData`.
    handle_connected_message(host, client_id, raw).await;
    Ok(false)
}

/// Handle a transmission received in the Connected phase: route
/// `DotsMember`, fan everything else out to the type group.
///
/// Hot-path principle: the payload travels as raw `Bytes` through the
/// broker. Internal types and cached types decode it on demand; pure
/// pass-through types (the bulk of fan-out traffic) never touch the
/// CBOR decoder beyond the header. Outbound bytes are produced once
/// per inbound transmission and refcount-shared across all subscribers
/// via [`HostTransceiver::fan_out_bytes`].
async fn handle_connected_message(
    host: &Arc<HostTransceiver>,
    client_id: u32,
    raw: &RawTransmission,
) {
    let Some(type_name) = raw.header.type_name.as_deref() else {
        return;
    };
    if type_name == "DotsMember" {
        // Group membership is for the broker; not fanned out.
        handle_member(host, client_id, raw).await;
        return;
    }
    if type_name == "DotsEcho" {
        // Direct reply only; no fan-out.
        handle_echo(host, client_id, raw).await;
        return;
    }
    if type_name == "DotsDescriptorRequest" {
        // Direct reply with all matching descriptors; no fan-out.
        handle_descriptor_request(host, client_id, raw).await;
        return;
    }
    if type_name == "DotsClearCache" {
        // Operates on the broker's pool; the resulting removals are
        // fanned out via the standard publish-with-remove_obj path.
        handle_clear_cache(host, raw).await;
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
    } else if type_name == "DotsMsgConnect"
        || type_name == "DotsMsgConnectResponse"
        || type_name == "DotsMsgHello"
        || type_name == "DotsMsgError"
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

    // Cache merge — only cached types pay the payload-decode cost.
    // Non-cached types skip the dynamic round-trip entirely.
    if let Some(dots_model::DescriptorEntry::Struct(d)) = host.registry.lookup(type_name) {
        if d.flags.is_cached() {
            match DynamicStruct::decode(d.clone(), &raw.payload) {
                Ok(payload) => host.update_cache(type_name, &header, &payload),
                Err(e) => {
                    tracing::warn!(
                        client_id,
                        type_name,
                        error = %e,
                        "failed to decode payload for cache update; skipping cache merge",
                    );
                }
            }
        }
    }

    // Fan-out: re-encode the (small) header, splice in the payload
    // bytes verbatim. No DynamicStruct allocation, no CBOR walk over
    // the payload.
    let mut buf = Vec::with_capacity(SIZE_PREFIX_LEN_HINT + raw.payload.len());
    encode_frame_with_header(&header, &raw.payload, &mut buf);
    host.fan_out_bytes(type_name, Bytes::from(buf), 0).await;
}

/// Capacity hint for the size prefix + a typical re-encoded header,
/// used so the fan-out scratch buffer doesn't need to grow on the
/// first append. Hot path; constant.
const SIZE_PREFIX_LEN_HINT: usize = 64;

/// Reply to a `DotsEcho` request: copy the payload, set `request =
/// false`, and send it back only to the originating guest. No fan-out.
async fn handle_echo(host: &Arc<HostTransceiver>, client_id: u32, raw: &RawTransmission) {
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
    let reply = dots_model::DotsEcho {
        request: Some(false),
        ..echo
    };
    let header = DotsHeader {
        type_name: Some("DotsEcho".into()),
        attributes: Some(reply.valid_set().bits()),
        sender: Some(HOST_ID),
        sent_time: Some(now_timepoint()),
        server_sent_time: Some(now_timepoint()),
        ..Default::default()
    };
    let mut buf = Vec::with_capacity(64);
    encode_typed_transmission_into(&header, &reply, &mut buf);
    send_to_guest(host, client_id, &buf).await;
}

/// Reply to a `DotsDescriptorRequest`: stream all known non-internal
/// struct descriptors (filtered by whitelist/blacklist) directly to
/// the requesting guest, terminated by
/// `DotsCacheInfo{end_descriptor_request: true}`. Mirrors C++
/// `HostTransceiver::handleDescriptorRequest`.
async fn handle_descriptor_request(
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
    let descriptors: Vec<Arc<dots_core::DynamicStructDescriptor>> = host
        .registry
        .iter_structs()
        .into_iter()
        .filter(|d| !d.flags.is_internal())
        .filter(|d| whitelist.is_empty() || whitelist.iter().any(|w| w == &d.name))
        .filter(|d| !blacklist.iter().any(|b| b == &d.name))
        .collect();

    tracing::debug!(
        client_id,
        count = descriptors.len(),
        "replying to DotsDescriptorRequest"
    );

    // Snapshot the guest's writer once, then write every descriptor
    // and the terminator under a single lock-acquire — keeps the
    // descriptor stream from interleaving with anything else this
    // guest might be sent.
    let writer = host
        .inner
        .lock()
        .expect("host mutex poisoned")
        .guests
        .get(&client_id)
        .map(|r| r.write_half.clone());
    let Some(writer) = writer else {
        return;
    };
    let mut w = writer.lock().await;

    for d in &descriptors {
        let data = StructDescriptorData::from_dynamic(d);
        let header = DotsHeader {
            type_name: Some("StructDescriptorData".into()),
            attributes: Some(data.valid_set().bits()),
            sender: Some(HOST_ID),
            sent_time: Some(now_timepoint()),
            server_sent_time: Some(now_timepoint()),
            ..Default::default()
        };
        let mut buf = Vec::with_capacity(64);
        encode_typed_transmission_into(&header, &data, &mut buf);
        if w.write_all(&buf).await.is_err() {
            return;
        }
    }

    // Terminate with DotsCacheInfo{end_descriptor_request: true}.
    let info = DotsCacheInfo {
        end_descriptor_request: Some(true),
        ..Default::default()
    };
    let header = DotsHeader {
        type_name: Some("DotsCacheInfo".into()),
        attributes: Some(info.valid_set().bits()),
        sender: Some(HOST_ID),
        sent_time: Some(now_timepoint()),
        server_sent_time: Some(now_timepoint()),
        ..Default::default()
    };
    let mut buf = Vec::with_capacity(32);
    encode_typed_transmission_into(&header, &info, &mut buf);
    let _ = w.write_all(&buf).await;
}

/// Send pre-encoded bytes to a single guest by client_id. The lookup
/// + write happens under that guest's writer lock; if the guest has
/// already been removed, the call is a no-op.
async fn send_to_guest(host: &Arc<HostTransceiver>, client_id: u32, bytes: &[u8]) {
    let writer = host
        .inner
        .lock()
        .expect("host mutex poisoned")
        .guests
        .get(&client_id)
        .map(|r| r.write_half.clone());
    if let Some(writer) = writer {
        let mut w = writer.lock().await;
        if w.write_all(bytes).await.is_err() {
            tracing::debug!(client_id, "write to guest failed; subscriber will be removed by its read task");
        }
    }
}

/// Handle `DotsClearCache`: drop the named types' entries from the
/// host's pool and fan out removal transmissions for each cleared
/// instance. Mirrors C++ `HostTransceiver::handleClearCache`.
async fn handle_clear_cache(host: &Arc<HostTransceiver>, raw: &RawTransmission) {
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

    let to_publish: Vec<Transmission> = {
        let mut inner = host.inner.lock().expect("host mutex poisoned");
        let mut out = Vec::new();
        for type_name in &type_names {
            let Some(map) = inner.pool.get_mut(type_name) else {
                continue;
            };
            let drained: Vec<(Vec<u8>, CachedEntry)> = std::mem::take(map).into_iter().collect();
            inner.pool.remove(type_name);
            for (_, entry) in drained {
                let header = DotsHeader {
                    type_name: Some(type_name.clone()),
                    sent_time: entry.last_update_time,
                    server_sent_time: Some(now_timepoint()),
                    attributes: Some(entry.attributes),
                    sender: Some(HOST_ID),
                    remove_obj: Some(true),
                    is_from_myself: Some(false),
                    ..Default::default()
                };
                out.push(Transmission {
                    header,
                    payload: entry.payload,
                });
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
    for txn in to_publish {
        let type_name = txn.header.type_name.clone().unwrap_or_default();
        let mut buf = Vec::with_capacity(64);
        txn.encode_into(&mut buf);
        host.fan_out_bytes(&type_name, Bytes::from(buf), HOST_ID)
            .await;
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

async fn handle_member(host: &Arc<HostTransceiver>, client_id: u32, raw: &RawTransmission) {
    let member: DotsMember = match decode_typed_from_slice(&raw.payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(client_id, error = %e, "failed to decode DotsMember");
            return;
        }
    };
    let Some(group_name) = member.group_name.as_deref() else {
        tracing::warn!(client_id, "DotsMember missing group_name");
        return;
    };
    match member.event {
        Some(DotsMemberEvent::Join) => {
            host.join_group(client_id, group_name);
            tracing::debug!(client_id, group_name, "guest joined group");
            host.replay_cache_to(client_id, group_name).await;
        }
        Some(DotsMemberEvent::Leave) => {
            host.leave_group(client_id, group_name);
            tracing::debug!(client_id, group_name, "guest left group");
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

async fn write_typed<T>(
    writer: &SharedWriter,
    type_name: &str,
    value: &T,
) -> Result<(), HostError>
where
    T: StructValue,
{
    let header = DotsHeader {
        type_name: Some(type_name.into()),
        attributes: Some(value.valid_set().bits()),
        sender: Some(HOST_ID),
        ..Default::default()
    };
    let mut buf = Vec::with_capacity(64);
    encode_typed_transmission_into(&header, value, &mut buf);
    let mut w = writer.lock().await;
    w.write_all(&buf)
        .await
        .map_err(|e| HostError::Transport(e.to_string()))
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

