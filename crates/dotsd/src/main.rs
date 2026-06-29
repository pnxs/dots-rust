//! `dotsd` — DOTS broker daemon.
//!
//! Listens on one or more endpoints, accepts guest connections, drives
//! the broker-side handshake, and routes pub/sub via the
//! [`HostTransceiver`]. The host itself is daemon-agnostic — it only
//! emits raw connection-state transitions via
//! [`HostTransceiver::set_transition_handler`]; everything
//! daemon-flavoured (publishing `DotsClient`, `DotsClientStatistics`,
//! …) lives here. Mirrors the dots-cpp split between `HostTransceiver`
//! and `DotsDaemon`.
//!
//! Usage:
//!
//! ```text
//! dotsd                                              # default tcp://0.0.0.0:11235
//! dotsd tcp://127.0.0.1:11236                        # custom TCP address
//! dotsd uds:///tmp/dotsd.sock                        # UDS only
//! dotsd tcp://0.0.0.0:11235 uds:///tmp/dotsd.sock    # both at once
//! dotsd --name my-host tcp://0.0.0.0:11235           # custom daemon name
//! dotsd --stats-interval 10                          # publish stats every 10s
//! dotsd --stats-interval 0                           # disable stats publishing
//! dotsd --cleanup-interval 30                        # reap unreferenced Closed clients every 30s
//! dotsd --cleanup-interval 0                         # disable cleanup sweep
//! ```
//!
//! Endpoint URI parsing + binding lives in `dots-transport` so any
//! embedded broker can accept the same syntax. Logging is via the
//! `tracing` crate plus `tracing-subscriber` (override the default
//! `info` level with the `RUST_LOG` env var).

use std::sync::Arc;
use std::time::Duration;

use dots_rs_core::dots;
use dots_rs_model::*;
use dots_rs_transport::{
    ConnectionTransition, Endpoint, EndpointHandle, GuestStats, HostTransceiver, parse_endpoint,
};

const DEFAULT_ENDPOINT: &str = "tcp://0.0.0.0:11235";
const DEFAULT_NAME: &str = "dotsd";
/// How often [`publish_stats`] snapshots per-guest write stats and
/// publishes a [`DotsClientStatistics`] for each connected guest.
const DEFAULT_STATS_INTERVAL: Duration = Duration::from_secs(5);
/// How often [`cleanup_clients`] sweeps the cache for unreferenced
/// Closed clients. Matches dots-cpp `DotsDaemon`'s 10-second cadence.
const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dots_rs_transport::init_tracing("");

    let Args {
        name: daemon_name,
        endpoints,
        stats_interval,
        cleanup_interval,
    } = parse_args()?;

    let host = HostTransceiver::new(daemon_name.clone());

    // Install the transition handler before endpoints start
    // accepting, so the very first guest's `Connect` already fires
    // a publish. Mirrors dots-cpp `DotsDaemon::handleTransition`
    // — the host stays oblivious to `DotsClient`/`DotsClientStatistics`,
    // we own both here.
    let handler_host = host.clone();
    host.set_transition_handler(move |t| publish_on_transition(&handler_host, t));

    // EndpointHandle owns each accept loop's JoinHandle plus the
    // UDS socket guard (for uds:// endpoints). Dropping it cleans
    // up the socket file; we keep them all alive for the daemon's
    // lifetime.
    let mut handles: Vec<EndpointHandle> = Vec::new();
    for ep in endpoints {
        handles.push(host.serve_endpoint(ep).await?);
    }
    if handles.is_empty() {
        return Err("no endpoints configured".into());
    }
    tracing::info!(name = daemon_name, "dotsd ready — accepting guests");

    let stats_task = stats_interval.map(|interval| {
        tracing::info!(?interval, "stats publisher enabled");
        tokio::spawn(publish_stats(host.clone(), interval))
    });
    let cleanup_task = cleanup_interval.map(|interval| {
        tracing::info!(?interval, "cleanup sweep enabled");
        tokio::spawn(cleanup_clients(host.clone(), interval))
    });

    tokio::signal::ctrl_c().await?;
    tracing::info!("Ctrl-C received — shutting down");
    if let Some(task) = stats_task {
        task.abort();
    }
    if let Some(task) = cleanup_task {
        task.abort();
    }
    Ok(())
}

/// Transition handler installed on [`HostTransceiver`].
///
/// On every state change:
/// - Publish a fresh `DotsClient` record for the transitioning guest
///   (mirrors dots-cpp `DotsDaemon::handleTransition`). `running` is
///   true for `EarlySubscribe` / `Connected`, false for `Closed`.
/// - For `Closed` transitions, also publish a terminal
///   `DotsClientStatistics` carrying the final write-stats snapshot
///   the host captured before tearing the writer down. This is what
///   makes short-lived guests visible to subscribers — periodic
///   polling on its own would miss them.
///
/// Removal of cached records (DotsClient and DotsClientStatistics
/// alike) is deferred to a future `cleanUpClients`-style sweep so
/// both records' lifecycles stay bound together — see dots-cpp
/// `DotsDaemon::cleanUpClients`.
fn publish_on_transition(host: &Arc<HostTransceiver>, t: &ConnectionTransition) {
    let is_closed = t.state == DotsConnectionState::Closed;
    host.publish(&dots!(DotsClient {
        id: t.client_id,
        name: t.client_name.clone(),
        running: !is_closed,
        connection_state: t.state,
    }));
    if is_closed {
        if let Some(stats) = t.final_stats.as_ref() {
            host.publish(&stats_record(t.client_id, stats));
        }
    }
}

/// Build the `DotsClientStatistics` projection of one
/// `GuestStats` snapshot. Shared between the periodic publisher
/// and the terminal-on-Close path so the wire shape is identical.
fn stats_record(client_id: u32, s: &GuestStats) -> DotsClientStatistics {
    dots!(DotsClientStatistics {
        client_id: client_id,
        sent: dots!(DotsStatistics {
            bytes: s.bytes_sent,
            packages: s.frames_sent,
        }),
        received: dots!(DotsStatistics {
            bytes: s.bytes_received,
            packages: s.frames_received,
        }),
        drainer_wakeups: s.drainer_wakeups,
        peak_queued_bytes: s.peak_queued_bytes,
        peak_queued_frames: s.peak_queued_frames,
        current_queued_bytes: s.current_queued_bytes,
        overflow_disconnected: s.overflow_disconnected,
    })
}

/// Periodic publisher of [`DotsClientStatistics`] for **live**
/// guests.
///
/// One record per currently-connected guest per tick. Terminal stats
/// for disconnected guests are emitted by [`publish_on_transition`]
/// on `Closed`, so this loop has no removal logic — the two records
/// (`DotsClient` and `DotsClientStatistics`) share a lifecycle and
/// will eventually be reaped together by the (still-to-be-written)
/// `cleanUpClients` sweep.
async fn publish_stats(host: Arc<HostTransceiver>, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    // If the runtime is busy and we skip a tick, fire once and
    // realign — don't burst-fire to catch up.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let snapshot = host.guest_stats_all();
        let published_ids: Vec<u32> = snapshot.iter().map(|(id, _)| *id).collect();
        for (id, s) in &snapshot {
            host.publish(&stats_record(*id, s));
        }
        tracing::debug!(
            published = ?published_ids,
            cached_entries = host.cache_size("DotsClientStatistics"),
            subscribers = host.group_size("DotsClientStatistics"),
            "stats tick",
        );
    }
}

/// Periodic sweep that removes Closed `DotsClient` records (and the
/// paired `DotsClientStatistics`) once nothing in the cache references
/// the client's id any more. Mirrors dots-cpp `DotsDaemon::cleanUpClients`:
///
/// 1. Snapshot all cached `DotsClient` records.
/// 2. For each whose `connectionState == Closed`, ask the host
///    whether *any* cached entry across any type still has
///    `createdFrom == id` or `lastUpdateFrom == id`.
/// 3. Remove the paired `(DotsClient, DotsClientStatistics)` records
///    for the unreferenced ones — both share a lifecycle, so the
///    sweep keeps them in lockstep.
async fn cleanup_clients(host: Arc<HostTransceiver>, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // First tick fires immediately; skip it so we don't reap anything
    // before guests have had a chance to connect.
    tick.tick().await;
    loop {
        tick.tick().await;
        let expired: Vec<u32> = host
            .cached_values::<DotsClient>()
            .into_iter()
            .filter(|c| c.connection_state == Some(DotsConnectionState::Closed))
            .filter_map(|c| {
                let id = c.id?;
                (!host.client_id_referenced(id)).then_some(id)
            })
            .collect();
        for id in &expired {
            host.remove(&dots!(DotsClient { id: *id }));
            host.remove(&dots!(DotsClientStatistics {
                client_id: *id,
            }));
        }
        tracing::debug!(reaped = ?expired, "cleanup sweep");
    }
}

struct Args {
    name: String,
    endpoints: Vec<Endpoint>,
    /// `None` disables the stats publisher; `Some(d)` runs it every
    /// `d`.
    stats_interval: Option<Duration>,
    /// `None` disables the cleanup sweep; `Some(d)` runs it every
    /// `d`.
    cleanup_interval: Option<Duration>,
}

fn parse_args() -> Result<Args, Box<dyn std::error::Error>> {
    let mut name = DEFAULT_NAME.to_string();
    let mut endpoints = Vec::new();
    let mut stats_interval = Some(DEFAULT_STATS_INTERVAL);
    let mut cleanup_interval = Some(DEFAULT_CLEANUP_INTERVAL);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--name" => {
                name = args.next().ok_or("--name requires an argument")?;
            }
            s if s.starts_with("--name=") => {
                name = s[7..].to_string();
            }
            "--stats-interval" => {
                let v = args.next().ok_or("--stats-interval requires an argument")?;
                stats_interval = parse_interval_seconds(&v, "--stats-interval")?;
            }
            s if s.starts_with("--stats-interval=") => {
                stats_interval = parse_interval_seconds(&s[17..], "--stats-interval")?;
            }
            "--cleanup-interval" => {
                let v = args.next().ok_or("--cleanup-interval requires an argument")?;
                cleanup_interval = parse_interval_seconds(&v, "--cleanup-interval")?;
            }
            s if s.starts_with("--cleanup-interval=") => {
                cleanup_interval = parse_interval_seconds(&s[19..], "--cleanup-interval")?;
            }
            other => endpoints.push(parse_endpoint(other)?),
        }
    }

    if endpoints.is_empty() {
        endpoints.push(parse_endpoint(DEFAULT_ENDPOINT)?);
    }
    Ok(Args {
        name,
        endpoints,
        stats_interval,
        cleanup_interval,
    })
}

fn parse_interval_seconds(
    s: &str,
    flag: &str,
) -> Result<Option<Duration>, Box<dyn std::error::Error>> {
    let secs: u64 = s
        .parse()
        .map_err(|_| format!("{flag} expects an integer seconds value, got {s:?}"))?;
    Ok(if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    })
}

fn print_help() {
    eprintln!(
        "Usage: dotsd [OPTIONS] [ENDPOINTS...]\n\
         \n\
         OPTIONS:\n    \
           --name <NAME>                Daemon name [default: dotsd]\n    \
           --stats-interval <SECONDS>   Publish DotsClientStatistics every N seconds\n    \
                                        (0 disables) [default: {stats_secs}]\n    \
           --cleanup-interval <SECONDS> Reap unreferenced Closed DotsClient entries\n    \
                                        every N seconds (0 disables) [default: {cleanup_secs}]\n    \
           -h, --help                   Show this help\n\
         \n\
         ENDPOINTS:\n    \
           tcp://<addr>:<port>    Listen on TCP\n    \
           uds:///<path>          Listen on Unix domain socket\n    \
           (default: {DEFAULT_ENDPOINT})\n",
        stats_secs = DEFAULT_STATS_INTERVAL.as_secs(),
        cleanup_secs = DEFAULT_CLEANUP_INTERVAL.as_secs(),
    );
}
