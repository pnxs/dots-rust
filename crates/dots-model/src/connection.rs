//! DOTS connection-layer types: per-transmission [`DotsHeader`] and the
//! handshake messages.
//!
//! Mirrors `external/dots/model/connection.dots` from dots-cpp. The
//! deprecated `DotsTransportHeader` (v1 framing with `nameSpace` and
//! `destinationGroup`) is intentionally not ported — v2 framing ships
//! the bare `DotsHeader` and v1 isn't a target.
//!
//! ## A note on `timepoint` and `property_set`
//!
//! The `.dots` model uses `timepoint` and `property_set` — semantic
//! types backed by `f64` (seconds since Unix epoch) and `u64` on the
//! wire respectively. This iteration represents them as `Option<f64>`
//! and `Option<u64>` directly, with the wire encoding matching DOTS.
//! Typed wrappers (a `Timepoint` newtype, reusing
//! [`dots_core::PropertySet`]) can come later without any wire-format
//! change.

use dots_derive::{DotsEnum, DotsStruct};

/// Per-transmission metadata envelope.
///
/// Every published value is preceded by a `DotsHeader` carrying the
/// type name, timestamps, validity bitmask, sender id, and various
/// flags. The header travels on the wire alongside the payload —
/// either both inline in v2 framing (CBOR tag 300 wrapping a 2-element
/// array of header + payload), or with the payload separately framed
/// in legacy v1.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsHeader [internal,cached=false] {
///     1: string typeName;
///     2: timepoint sentTime;
///     7: timepoint serverSentTime;
///     3: property_set attributes;
///     5: uint32 sender;
///     8: uint32 fromCache;
///     4: bool removeObj;
///     6: bool isFromMyself;
/// }
/// ```
///
/// Tags are non-contiguous; that's intentional — they're the .dots-source
/// numbering and are part of the wire contract.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsHeader", internal)]
pub struct DotsHeader {
    /// Name of the payload's type.
    #[dots(tag = 1)]
    pub type_name: Option<String>,
    /// Originating client's send timestamp — fractional seconds since
    /// the Unix epoch.
    #[dots(tag = 2)]
    pub sent_time: Option<f64>,
    /// Server's forward timestamp — same encoding as `sent_time`.
    #[dots(tag = 7)]
    pub server_sent_time: Option<f64>,
    /// Bitmask of which payload properties are valid. Redundant with
    /// the payload's CBOR map (sparse already), but explicit for
    /// peers that prefer to consult a single field.
    #[dots(tag = 3)]
    pub attributes: Option<u64>,
    /// Originating client id.
    #[dots(tag = 5)]
    pub sender: Option<u32>,
    /// During cache preload, the count of remaining objects after this
    /// one. `None` (absent on the wire) means "not from cache".
    #[dots(tag = 8)]
    pub from_cache: Option<u32>,
    /// True if the payload represents a deletion of the object.
    #[dots(tag = 4)]
    pub remove_obj: Option<bool>,
    /// Set true on the receiving client when the sender id matches
    /// this client (i.e. the publication is a loopback of one's own).
    #[dots(tag = 6)]
    pub is_from_myself: Option<bool>,
}

/// Server → guest, opening message of the connection handshake.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsMsgHello [internal,cached=false] {
///     1: string serverName;
///     2: uint64 authChallenge;
///     3: bool authenticationRequired;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsMsgHello", internal)]
pub struct DotsMsgHello {
    #[dots(tag = 1)]
    pub server_name: Option<String>,
    /// 64-bit nonce; the guest hashes this with its secret to produce
    /// `DotsMsgConnect.auth_challenge_response`.
    #[dots(tag = 2)]
    pub auth_challenge: Option<u64>,
    #[dots(tag = 3)]
    pub authentication_required: Option<bool>,
}

/// Guest → server, sent twice during the handshake:
///
/// 1. Right after `DotsMsgHello` with `client_name` and possibly
///    `preload_cache = Some(true)` to request the server-side cache.
/// 2. After all preload subscriptions have been issued, with
///    `preload_client_finished = Some(true)` to signal "I'm ready,
///    start streaming the cache".
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsMsgConnect [internal,cached=false] {
///     1: string clientName;
///     2: bool preloadCache;
///     3: bool preloadClientFinished;
///     4: string authChallengeResponse;
///     5: string cnonce;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsMsgConnect", internal)]
pub struct DotsMsgConnect {
    #[dots(tag = 1)]
    pub client_name: Option<String>,
    #[dots(tag = 2)]
    pub preload_cache: Option<bool>,
    #[dots(tag = 3)]
    pub preload_client_finished: Option<bool>,
    #[dots(tag = 4)]
    pub auth_challenge_response: Option<String>,
    #[dots(tag = 5)]
    pub cnonce: Option<String>,
}

/// Server → guest, response after authentication.
///
/// `accepted` indicates whether the guest may proceed; if true, the
/// server then transitions into preload streaming (when requested) or
/// straight into the `connected` state.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsMsgConnectResponse [internal,cached=false] {
///     1: string serverName;
///     5: uint32 clientId;
///     2: bool accepted;
///     3: bool preload;
///     4: bool preloadFinished;
/// }
/// ```
///
/// Note the non-contiguous tag layout — `clientId` is at tag 5, between
/// `accepted` (2) and `preload` (3) numerically. That's the source ordering.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsMsgConnectResponse", internal)]
pub struct DotsMsgConnectResponse {
    #[dots(tag = 1)]
    pub server_name: Option<String>,
    #[dots(tag = 5)]
    pub client_id: Option<u32>,
    #[dots(tag = 2)]
    pub accepted: Option<bool>,
    #[dots(tag = 3)]
    pub preload: Option<bool>,
    #[dots(tag = 4)]
    pub preload_finished: Option<bool>,
}

/// Either party → other, signalling a fatal protocol error.
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsMsgError", internal)]
pub struct DotsMsgError {
    #[dots(tag = 1)]
    pub error_code: Option<i32>,
    #[dots(tag = 2)]
    pub error_text: Option<String>,
}

/// Connection-level state machine.
///
/// Tracked locally by both peers. Not transmitted as a field of any
/// message in the standard handshake; provided here so that runtime
/// state can be represented in the same Rust types as the wire ones.
///
/// Mirrors `.dots`:
/// ```text
/// enum DotsConnectionState {
///     1: connecting,
///     2: early_subscribe,
///     3: connected,
///     4: suspended,
///     5: closed
/// }
/// ```
#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(name = "DotsConnectionState")]
pub enum DotsConnectionState {
    #[default]
    #[dots(tag = 1)]
    Connecting,
    #[dots(tag = 2)]
    EarlySubscribe,
    #[dots(tag = 3)]
    Connected,
    #[dots(tag = 4)]
    Suspended,
    #[dots(tag = 5)]
    Closed,
}

// ===== Group membership =====

/// A client's join/leave/kill action against a routing group. dotsd
/// uses one group per type-name as the basis for subscription
/// routing — to be sent events for type `T`, a client publishes
/// `DotsMember { groupName: T, event: join }` once.
///
/// Mirrors `.dots`:
/// ```text
/// enum DotsMemberEvent {
///     1: join,
///     2: leave,
///     3: kill
/// }
/// ```
#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(name = "DotsMemberEvent")]
pub enum DotsMemberEvent {
    #[default]
    #[dots(tag = 1)]
    Join,
    #[dots(tag = 2)]
    Leave,
    #[dots(tag = 3)]
    Kill,
}

/// A group membership event. Publishing this is how a client tells
/// the broker to start (or stop) routing transmissions of a given
/// type to it.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsMember [internal,cached=false] {
///     1: string groupName;
///     2: DotsMemberEvent event;
///     3: uint32 client;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsMember", internal)]
pub struct DotsMember {
    #[dots(tag = 1)]
    pub group_name: Option<String>,
    #[dots(tag = 2)]
    pub event: Option<DotsMemberEvent>,
    #[dots(tag = 3)]
    pub client: Option<u32>,
}

/// Operation kind for a value-cache event — what kind of change just
/// happened to an instance of a cached type.
///
/// Mirrors `.dots`:
/// ```text
/// enum DotsMt {
///     1: create,
///     2: update,
///     3: remove
/// }
/// ```
#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(name = "DotsMt")]
pub enum DotsMt {
    #[default]
    #[dots(tag = 1)]
    Create,
    #[dots(tag = 2)]
    Update,
    #[dots(tag = 3)]
    Remove,
}

/// Per-instance cache metadata held alongside each container entry on
/// the broker side; clients receive it for cache replay events.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsCloneInformation [internal] {
///     1: DotsMt lastOperation;
///     2: uint32 lastUpdateFrom;
///     3: timepoint created;
///     4: uint32 createdFrom;
///     5: timepoint modified;
///     6: timepoint localUpdateTime;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsCloneInformation", internal)]
pub struct DotsCloneInformation {
    #[dots(tag = 1)]
    pub last_operation: Option<DotsMt>,
    #[dots(tag = 2)]
    pub last_update_from: Option<u32>,
    #[dots(tag = 3)]
    pub created: Option<f64>,
    #[dots(tag = 4)]
    pub created_from: Option<u32>,
    #[dots(tag = 5)]
    pub modified: Option<f64>,
    #[dots(tag = 6)]
    pub local_update_time: Option<f64>,
}

// ===== System events from dotsd (user.dots) =====

/// Synchronization signal from dotsd. Sent in two situations:
///
/// 1. **Per-type cache end:** after a guest joins a group via
///    `DotsMember(join, T)`, the broker streams the cached objects of
///    `T` and then transmits `DotsCacheInfo { typeName: T,
///    endTransmission: true }` to mark "cache for T fully delivered".
///    Used by clients that want to wait for the initial state of a
///    type before doing further work.
///
/// 2. **Descriptor request end:** after a `DotsDescriptorRequest`,
///    the broker sends one `StructDescriptor` per matching type
///    followed by `DotsCacheInfo { endDescriptorRequest: true }`.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsCacheInfo [internal,cached=false] {
///     1: string typeName;
///     2: bool startTransmission;
///     3: bool endTransmission;
///     4: bool endDescriptorRequest;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsCacheInfo", internal)]
pub struct DotsCacheInfo {
    #[dots(tag = 1)]
    pub type_name: Option<String>,
    #[dots(tag = 2)]
    pub start_transmission: Option<bool>,
    #[dots(tag = 3)]
    pub end_transmission: Option<bool>,
    #[dots(tag = 4)]
    pub end_descriptor_request: Option<bool>,
}

/// Tells the broker (or a client) to clear cached instances of one or
/// more types.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsClearCache [internal,cached=false] {
///     1: vector<string> typeNames;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsClearCache", internal)]
pub struct DotsClearCache {
    #[dots(tag = 1)]
    pub type_names: Option<Vec<String>>,
}

/// Asks the broker to (re-)publish the descriptors of all known types,
/// optionally filtered by white/blacklist.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsDescriptorRequest [internal,cached=false] {
///     1: vector<string> whitelist;
///     2: vector<string> blacklist;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsDescriptorRequest", internal)]
pub struct DotsDescriptorRequest {
    #[dots(tag = 1)]
    pub whitelist: Option<Vec<String>>,
    #[dots(tag = 2)]
    pub blacklist: Option<Vec<String>>,
}

/// Echo / keep-alive / RTT-measurement primitive. Guests may send
/// `DotsEcho { request: true, ... }` and the broker replies with the
/// same payload but `request: false`.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsEcho [internal,cached=false] {
///     1: bool request;
///     2: uint32 identifier;
///     3: uint32 sequenceNumber;
///     4: string data;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsEcho", internal)]
pub struct DotsEcho {
    #[dots(tag = 1)]
    pub request: Option<bool>,
    #[dots(tag = 2)]
    pub identifier: Option<u32>,
    #[dots(tag = 3)]
    pub sequence_number: Option<u32>,
    #[dots(tag = 4)]
    pub data: Option<String>,
}
