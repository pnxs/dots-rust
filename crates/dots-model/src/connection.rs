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
