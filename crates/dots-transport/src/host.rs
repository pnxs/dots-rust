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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use dots_core::{DynamicStruct, StructValue, decode_typed_from_slice, key_set};
use dots_model::{
    DotsCacheInfo, DotsConnectionState, DotsHeader, DotsMember, DotsMemberEvent, DotsMsgConnect,
    DotsMsgConnectResponse, DotsMsgHello, EnumDescriptorData, Registry, StructDescriptorData,
    Transmission, daemon::DotsClient, encode_typed_transmission_into,
    encode_typed_transmission_with_mask_into,
};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;

use crate::codec::TransmissionCodec;
use crate::guest::now_timepoint;

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
    groups: HashMap<String, HashSet<u32>>,
    /// Per-guest state, keyed by client id.
    guests: HashMap<u32, GuestRecord>,
    /// Monotonic id allocator for new guests. Starts at 2 since 1 is
    /// reserved as `HOST_ID`.
    next_client_id: u32,
    /// Container pool: per-cached-type, key-bytes → cached entry.
    /// Updated on every incoming transmission of a cached type;
    /// replayed on `DotsMember(Join)`.
    pool: HashMap<String, BTreeMap<Vec<u8>, CachedEntry>>,
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
    /// Pre-encoded transmissions queued to be written to this guest.
    outbound_tx: mpsc::UnboundedSender<Vec<u8>>,
    /// Handle to the per-guest task. Kept so the host can join/abort
    /// during shutdown if needed.
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
                groups: HashMap::new(),
                guests: HashMap::new(),
                next_client_id: HOST_ID + 1,
                pool: HashMap::new(),
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
            .map(HashSet::len)
            .unwrap_or(0)
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
    /// the broker-side handshake and dispatch loop. Returns the
    /// allocated client id.
    pub fn accept<S>(self: &Arc<Self>, stream: S) -> u32
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let host = self.clone();
        let mut inner = self.inner.lock().expect("host mutex poisoned");
        let client_id = inner.next_client_id;
        inner.next_client_id += 1;
        let task = {
            let host = host.clone();
            tokio::spawn(async move {
                if let Err(e) = run_guest(host.clone(), client_id, stream, out_rx).await {
                    tracing::warn!(client_id, error = %e, "guest task ended with error");
                }
                host.remove_guest(client_id);
            })
        };
        inner.guests.insert(
            client_id,
            GuestRecord {
                client_name: None,
                outbound_tx: out_tx,
                task,
            },
        );
        client_id
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
    pub fn publish<T>(&self, value: &T)
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
        self.fan_out_bytes(type_name, &bytes, /*exclude*/ HOST_ID);
    }

    /// Publish a removal from the host. Routes to every guest
    /// subscribed to `T`'s type-name group, with `header.remove_obj
    /// = true` and only key fields in the payload. Drops the entry
    /// from the host's cache pool.
    pub fn remove<T>(&self, value: &T)
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
        self.fan_out_bytes(type_name, &bytes, /*exclude*/ HOST_ID);
    }

    fn fan_out_bytes(&self, type_name: &str, bytes: &[u8], exclude_client_id: u32) {
        let inner = self.inner.lock().expect("host mutex poisoned");
        let Some(targets) = inner.groups.get(type_name) else {
            return;
        };
        for &client_id in targets {
            if client_id == exclude_client_id {
                continue;
            }
            if let Some(record) = inner.guests.get(&client_id) {
                let _ = record.outbound_tx.send(bytes.to_vec());
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
    fn replay_cache_to(&self, client_id: u32, type_name: &str) {
        let inner = self.inner.lock().expect("host mutex poisoned");
        let Some(map) = inner.pool.get(type_name) else {
            // Not cached or no entries — still send end-transmission
            // so the guest's preload sequencer terminates cleanly,
            // but only if a cached descriptor exists.
            let cached = matches!(
                self.registry.lookup(type_name),
                Some(dots_model::DescriptorEntry::Struct(d)) if d.flags.is_cached()
            );
            if cached {
                drop(inner);
                self.send_cache_info_end(client_id, type_name);
            }
            return;
        };

        let total = map.len() as u32;
        let mut remaining = total;
        // Snapshot — clone the entries so we can drop the lock before
        // the (lock-free) send loop.
        let snapshot: Vec<(DotsHeader, DynamicStruct)> = map
            .values()
            .map(|e| {
                remaining = remaining.saturating_sub(1);
                let header = DotsHeader {
                    type_name: Some(type_name.to_string()),
                    sent_time: e.last_update_time,
                    server_sent_time: Some(now_timepoint()),
                    attributes: Some(e.attributes),
                    sender: e.last_update_sender,
                    from_cache: Some(remaining),
                    remove_obj: Some(false),
                    is_from_myself: Some(false),
                };
                (header, e.payload.clone())
            })
            .collect();
        drop(inner);

        let inner = self.inner.lock().expect("host mutex poisoned");
        let Some(record) = inner.guests.get(&client_id) else {
            return;
        };
        for (header, payload) in &snapshot {
            let txn = Transmission {
                header: header.clone(),
                payload: payload.clone(),
            };
            let mut buf = Vec::with_capacity(64);
            txn.encode_into(&mut buf);
            let _ = record.outbound_tx.send(buf);
        }
        drop(inner);
        self.send_cache_info_end(client_id, type_name);
    }

    fn send_cache_info_end(&self, client_id: u32, type_name: &str) {
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
        let inner = self.inner.lock().expect("host mutex poisoned");
        if let Some(record) = inner.guests.get(&client_id) {
            let _ = record.outbound_tx.send(buf);
        }
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

    fn remove_guest(&self, client_id: u32) {
        // Snapshot the guest's name before we drop the record, so we
        // can publish a final DotsClient(state=Closed) below.
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
        self.publish_dots_client(client_id, name, DotsConnectionState::Closed, false);
    }

    /// Publish a [`DotsClient`] record for this guest's current state.
    /// Routes through the normal publish path so the cache pool stays
    /// in sync. C++ dotsd publishes on every connection-state
    /// transition; we mirror that.
    fn publish_dots_client(
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
        self.publish(&record);
    }
}

// ===== Per-guest task =====

async fn run_guest<S>(
    host: Arc<HostTransceiver>,
    client_id: u32,
    stream: S,
    mut out_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let codec = TransmissionCodec::new(host.registry.clone());
    let framed = Framed::new(stream, codec);
    let (mut sink, mut stream_in) = framed.split();

    // ----- Phase 1: Hello -----
    let hello = DotsMsgHello {
        server_name: Some(host.self_name.clone()),
        auth_challenge: Some(0),
        authentication_required: Some(false),
    };
    send_typed(&mut sink, "DotsMsgHello", &hello).await?;
    tracing::debug!(client_id, "sent Hello");

    // ----- Phase 2: Connect / ConnectResponse -----
    let connect_txn = next_txn(&mut stream_in).await?;
    let connect: DotsMsgConnect = decode_handshake(&connect_txn, "DotsMsgConnect")?;
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
    send_typed(&mut sink, "DotsMsgConnectResponse", &resp).await?;
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
    );

    // ----- Phase 3: EarlySubscribe (if preload requested) -----
    if preload_requested {
        loop {
            tokio::select! {
                biased;
                inbound = stream_in.next() => match inbound {
                    Some(Ok(txn)) => {
                        if handle_preload_message(&host, client_id, &txn, &mut sink).await? {
                            // preload_client_finished — break and enter Connected state.
                            break;
                        }
                    }
                    Some(Err(e)) => return Err(HostError::Transport(e.to_string())),
                    None => return Ok(()),
                },
                outbound = out_rx.recv() => match outbound {
                    Some(bytes) => sink.send(bytes).await
                        .map_err(|e| HostError::Transport(e.to_string()))?,
                    None => return Ok(()),
                }
            }
        }
        tracing::debug!(client_id, "guest preload phase complete");
        host.publish_dots_client(
            client_id,
            connect.client_name.clone(),
            DotsConnectionState::Connected,
            true,
        );
    }

    // ----- Phase 4: Connected — fan-out main loop -----
    loop {
        tokio::select! {
            biased;
            inbound = stream_in.next() => match inbound {
                Some(Ok(txn)) => {
                    handle_connected_message(&host, client_id, &txn);
                }
                Some(Err(e)) => return Err(HostError::Transport(e.to_string())),
                None => return Ok(()),
            },
            outbound = out_rx.recv() => match outbound {
                Some(bytes) => sink.send(bytes).await
                    .map_err(|e| HostError::Transport(e.to_string()))?,
                None => return Ok(()),
            }
        }
    }
}

/// Handle a transmission received during the EarlySubscribe phase.
/// Returns `Ok(true)` when the guest sent `preload_client_finished`,
/// at which point the broker should send `ConnectResponse(preload_finished)`.
async fn handle_preload_message<W>(
    host: &Arc<HostTransceiver>,
    client_id: u32,
    txn: &Transmission,
    sink: &mut W,
) -> Result<bool, HostError>
where
    W: futures_util::Sink<Vec<u8>, Error = crate::TransportError> + Unpin,
{
    let type_name = txn.header.type_name.as_deref().unwrap_or("");
    if type_name == "DotsMsgConnect" {
        let connect: DotsMsgConnect = decode_handshake(txn, "DotsMsgConnect")?;
        if connect.preload_client_finished == Some(true) {
            let resp = DotsMsgConnectResponse {
                server_name: Some(host.self_name.clone()),
                client_id: Some(client_id),
                accepted: Some(true),
                preload_finished: Some(true),
                ..Default::default()
            };
            send_typed(sink, "DotsMsgConnectResponse", &resp).await?;
            return Ok(true);
        }
        return Ok(false);
    }
    // Everything else during preload — including descriptor data and
    // DotsMember — flows through the same dispatch the connected
    // phase uses. That ensures descriptors received during one
    // guest's preload are also fanned out to other already-connected
    // guests subscribed to `StructDescriptorData`.
    handle_connected_message(host, client_id, txn);
    Ok(false)
}

/// Handle a transmission received in the Connected phase: route
/// `DotsMember`, fan everything else out to the type group.
fn handle_connected_message(
    host: &Arc<HostTransceiver>,
    client_id: u32,
    txn: &Transmission,
) {
    let Some(type_name) = txn.header.type_name.as_deref() else {
        return;
    };
    if type_name == "DotsMember" {
        // Group membership is for the broker; not fanned out.
        handle_member(host, client_id, txn);
        return;
    }
    if type_name == "DotsEcho" {
        // Direct reply only; no fan-out.
        handle_echo(host, client_id, txn);
        return;
    }
    if type_name == "StructDescriptorData" {
        // Register locally so we can decode subsequent payloads of
        // the new type, then fall through to fan-out — other guests
        // subscribed to `StructDescriptorData` (e.g. dots-cpp guests
        // populating their own type registry) need to see it too.
        register_incoming_struct(host, txn);
    } else if type_name == "EnumDescriptorData" {
        register_incoming_enum(host, txn);
    } else if type_name == "DotsMsgConnect"
        || type_name == "DotsMsgConnectResponse"
        || type_name == "DotsMsgHello"
        || type_name == "DotsMsgError"
    {
        // Handshake messages outside the handshake — log and drop.
        tracing::warn!(client_id, type_name, "stray handshake message after preload");
        return;
    }

    // Re-encode with the original sender preserved (or stamped if
    // missing) and a fresh server_sent_time.
    let mut header = txn.header.clone();
    if header.sender.is_none() {
        header.sender = Some(client_id);
    }
    header.server_sent_time = Some(now_timepoint());
    if header.is_from_myself.is_none() {
        header.is_from_myself = Some(false);
    }

    // Update the cache pool if this type is cached.
    host.update_cache(type_name, &header, &txn.payload);

    let mut buf = Vec::with_capacity(64);
    let outgoing = Transmission {
        header,
        payload: txn.payload.clone(),
    };
    outgoing.encode_into(&mut buf);
    host.fan_out_bytes(type_name, &buf, client_id);
}

/// Reply to a `DotsEcho` request: copy the payload, set `request =
/// false`, and send it back only to the originating guest. No fan-out.
fn handle_echo(host: &Arc<HostTransceiver>, client_id: u32, txn: &Transmission) {
    let bytes = txn.payload.encode();
    let echo: dots_model::DotsEcho = match decode_typed_from_slice(&bytes) {
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

    let inner = host.inner.lock().expect("host mutex poisoned");
    if let Some(record) = inner.guests.get(&client_id) {
        let _ = record.outbound_tx.send(buf);
    }
}

fn register_incoming_struct(host: &Arc<HostTransceiver>, txn: &Transmission) {
    let bytes = txn.payload.encode();
    let data: StructDescriptorData = match decode_typed_from_slice(&bytes) {
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

fn register_incoming_enum(host: &Arc<HostTransceiver>, txn: &Transmission) {
    let bytes = txn.payload.encode();
    let data: EnumDescriptorData = match decode_typed_from_slice(&bytes) {
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

fn handle_member(host: &Arc<HostTransceiver>, client_id: u32, txn: &Transmission) {
    let bytes = txn.payload.encode();
    let member: DotsMember = match decode_typed_from_slice(&bytes) {
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
            host.replay_cache_to(client_id, group_name);
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

async fn send_typed<W, T>(sink: &mut W, type_name: &str, value: &T) -> Result<(), HostError>
where
    W: futures_util::Sink<Vec<u8>, Error = crate::TransportError> + Unpin,
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
    sink.send(buf)
        .await
        .map_err(|e| HostError::Transport(e.to_string()))
}

async fn next_txn<R>(stream: &mut R) -> Result<Transmission, HostError>
where
    R: futures_util::Stream<Item = Result<Transmission, crate::TransportError>> + Unpin,
{
    match stream.next().await {
        Some(Ok(txn)) => Ok(txn),
        Some(Err(e)) => Err(HostError::Transport(e.to_string())),
        None => Err(HostError::Transport("stream closed".into())),
    }
}

fn decode_handshake<T>(txn: &Transmission, expected: &str) -> Result<T, HostError>
where
    T: StructValue + Default,
{
    let actual = txn.header.type_name.as_deref().unwrap_or("");
    if actual != expected {
        return Err(HostError::Decode(format!(
            "expected {expected}, got {actual}"
        )));
    }
    let bytes = txn.payload.encode();
    decode_typed_from_slice(&bytes).map_err(|e| HostError::Decode(e.to_string()))
}

