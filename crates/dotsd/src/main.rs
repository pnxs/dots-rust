//! `dotsd` — DOTS broker daemon.
//!
//! Listens on one or more endpoints, accepts guest connections, drives
//! the broker-side handshake, and routes pub/sub via the
//! [`HostTransceiver`].
//!
//! Usage:
//!
//! ```text
//! dotsd                                              # default tcp://0.0.0.0:11235
//! dotsd tcp://127.0.0.1:11236                        # custom TCP address
//! dotsd uds:///tmp/dotsd.sock                        # UDS only
//! dotsd tcp://0.0.0.0:11235 uds:///tmp/dotsd.sock    # both at once
//! dotsd --name my-host tcp://0.0.0.0:11235           # custom daemon name
//! ```
//!
//! Logging via `tracing` + `tracing-subscriber` (override the default
//! `info` level with the `RUST_LOG` env var).

use std::path::PathBuf;

use dots_transport::HostTransceiver;
use tokio::net::{TcpListener, UnixListener};

const DEFAULT_ENDPOINT: &str = "tcp://0.0.0.0:11235";
const DEFAULT_NAME: &str = "dotsd";

enum Endpoint {
    Tcp(String),
    Uds(PathBuf),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .compact()
        .init();

    let (daemon_name, endpoints) = parse_args()?;

    let host = HostTransceiver::new(daemon_name.clone());
    let mut serve_handles = Vec::new();
    for ep in endpoints {
        match ep {
            Endpoint::Tcp(addr) => {
                let listener = TcpListener::bind(&addr).await?;
                let local = listener.local_addr()?;
                tracing::info!(name = daemon_name, listen = %local, "TCP endpoint ready");
                serve_handles.push(host.serve_tcp(listener));
            }
            Endpoint::Uds(path) => {
                // Best-effort cleanup of a stale socket file from a
                // previous run. UnixListener::bind fails with EADDRINUSE
                // otherwise.
                if path.exists() {
                    if let Err(e) = std::fs::remove_file(&path) {
                        tracing::warn!(path = %path.display(), error = %e,
                            "failed to clean up stale UDS file");
                    }
                }
                let listener = UnixListener::bind(&path)?;
                tracing::info!(name = daemon_name, listen = %path.display(),
                    "UDS endpoint ready");
                serve_handles.push(host.serve_unix(listener));
            }
        }
    }

    if serve_handles.is_empty() {
        return Err("no endpoints configured".into());
    }

    // Wait for Ctrl-C OR for any serve handle to terminate (which
    // signals a fatal accept-loop error on that listener).
    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received — shutting down");
        }
        (joined, _idx, _rest) = futures_util::future::select_all(serve_handles) => {
            match joined {
                Ok(Ok(())) => tracing::info!("an accept loop ended cleanly"),
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "accept loop terminated");
                    return Err(e.into());
                }
                Err(e) => {
                    tracing::error!(error = %e, "accept loop task panicked");
                    return Err(e.into());
                }
            }
        }
    }

    Ok(())
}

fn parse_args() -> Result<(String, Vec<Endpoint>), Box<dyn std::error::Error>> {
    let mut name = DEFAULT_NAME.to_string();
    let mut endpoints = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--name" => {
                name = args
                    .next()
                    .ok_or("--name requires an argument")?;
            }
            s if s.starts_with("--name=") => {
                name = s[7..].to_string();
            }
            s if s.starts_with("tcp://") => {
                endpoints.push(Endpoint::Tcp(s[6..].to_string()));
            }
            s if s.starts_with("uds://") => {
                // uds:///path/to/socket → strip the scheme; the
                // remainder includes the leading slash for absolute
                // paths.
                endpoints.push(Endpoint::Uds(PathBuf::from(&s[6..])));
            }
            other => return Err(format!("unrecognized argument: {other}").into()),
        }
    }

    if endpoints.is_empty() {
        // Default to a TCP endpoint matching the legacy behavior.
        endpoints.push(Endpoint::Tcp(DEFAULT_ENDPOINT[6..].to_string()));
    }
    Ok((name, endpoints))
}

fn print_help() {
    eprintln!(
        "Usage: dotsd [OPTIONS] [ENDPOINTS...]\n\
         \n\
         OPTIONS:\n    \
           --name <NAME>          Daemon name [default: dotsd]\n    \
           -h, --help             Show this help\n\
         \n\
         ENDPOINTS:\n    \
           tcp://<addr>:<port>    Listen on TCP\n    \
           uds:///<path>          Listen on Unix domain socket\n    \
           (default: {DEFAULT_ENDPOINT})\n"
    );
}
