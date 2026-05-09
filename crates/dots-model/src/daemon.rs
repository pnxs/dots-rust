//! Daemon-side DOTS types — the system records dotsd publishes about
//! connected clients, broker statistics, and process health.
//!
//! Mirrors `lib/src/model/daemon.dots` in dots-cpp. Clients that want
//! to introspect the broker (subscribe to `DotsClient`, read
//! `DotsDaemonStatus`, etc.) need these types registered.

use dots_core::{Duration, Timepoint};
use dots_derive::DotsStruct;

use crate::connection::DotsConnectionState;

/// Per-client record published by dotsd on every connect/disconnect.
/// The cache lets new subscribers see the full set of currently-
/// connected clients on join.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsClient [internal] {
///     1: [key] uint32 id;
///     2: string name;
///     3: bool running;
///     4: vector<string> publishedTypes;
///     5: vector<string> subscribedTypes;
///     6: DotsConnectionState connectionState;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsClient", internal, cached)]
pub struct DotsClient {
    #[dots(tag = 1, key)]
    pub id: Option<u32>,
    #[dots(tag = 2)]
    pub name: Option<String>,
    #[dots(tag = 3)]
    pub running: Option<bool>,
    #[dots(tag = 4)]
    pub published_types: Option<Vec<String>>,
    #[dots(tag = 5)]
    pub subscribed_types: Option<Vec<String>>,
    #[dots(tag = 6)]
    pub connection_state: Option<DotsConnectionState>,
}

/// Counters for bytes / packages on a transmission direction. Used as
/// a sub-record of [`DotsDaemonStatus`].
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsStatistics [internal] {
///     1: uint64 bytes;
///     2: uint64 packages;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsStatistics", internal)]
pub struct DotsStatistics {
    #[dots(tag = 1)]
    pub bytes: Option<u64>,
    #[dots(tag = 2)]
    pub packages: Option<u64>,
}

/// Aggregate cache size on the broker. Sub-record of
/// [`DotsDaemonStatus`].
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsCacheStatus [internal] {
///     1: uint32 nrTypes;
///     2: uint64 size;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsCacheStatus", internal)]
pub struct DotsCacheStatus {
    #[dots(tag = 1)]
    pub nr_types: Option<u32>,
    #[dots(tag = 2)]
    pub size: Option<u64>,
}

/// Process-level resource usage on the broker. Mirrors fields from
/// `getrusage(2)` plus CPU times. Sub-record of [`DotsDaemonStatus`].
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsResourceUsage [internal] {
///     1: int32 minorFaults;
///     2: int32 majorFaults;
///     3: int32 inBlock;
///     4: int32 outBlock;
///     5: int32 nrSignals;
///     6: int32 nrSwaps;
///     7: int32 nrVoluntaryContextSwitches;
///     8: int32 nrInvoluntaryContextSwitches;
///     9: int32 maxRss;
///     10: duration userCpuTime;
///     11: duration systemCpuTime;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsResourceUsage", internal)]
pub struct DotsResourceUsage {
    #[dots(tag = 1)]
    pub minor_faults: Option<i32>,
    #[dots(tag = 2)]
    pub major_faults: Option<i32>,
    #[dots(tag = 3)]
    pub in_block: Option<i32>,
    #[dots(tag = 4)]
    pub out_block: Option<i32>,
    #[dots(tag = 5)]
    pub nr_signals: Option<i32>,
    #[dots(tag = 6)]
    pub nr_swaps: Option<i32>,
    #[dots(tag = 7)]
    pub nr_voluntary_context_switches: Option<i32>,
    #[dots(tag = 8)]
    pub nr_involuntary_context_switches: Option<i32>,
    #[dots(tag = 9)]
    pub max_rss: Option<i32>,
    #[dots(tag = 10)]
    pub user_cpu_time: Option<Duration>,
    #[dots(tag = 11)]
    pub system_cpu_time: Option<Duration>,
}

/// Aggregate daemon state — published periodically by dotsd. Keyed by
/// `serverName` so consumers can filter to a specific broker in a
/// federation.
///
/// Mirrors `.dots`:
/// ```text
/// struct DotsDaemonStatus [internal] {
///     1: [key] string serverName;
///     2: timepoint startTime;
///     3: DotsStatistics received;
///     4: DotsStatistics sent;
///     5: DotsCacheStatus cache;
///     6: DotsResourceUsage resourceUsage;
/// }
/// ```
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "DotsDaemonStatus", internal, cached)]
pub struct DotsDaemonStatus {
    #[dots(tag = 1, key)]
    pub server_name: Option<String>,
    #[dots(tag = 2)]
    pub start_time: Option<Timepoint>,
    #[dots(tag = 3)]
    pub received: Option<DotsStatistics>,
    #[dots(tag = 4)]
    pub sent: Option<DotsStatistics>,
    #[dots(tag = 5)]
    pub cache: Option<DotsCacheStatus>,
    #[dots(tag = 6)]
    pub resource_usage: Option<DotsResourceUsage>,
}
