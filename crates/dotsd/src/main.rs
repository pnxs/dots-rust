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
//! Endpoint URI parsing + binding lives in `dots-transport` so any
//! embedded broker can accept the same syntax. Logging is via the
//! `tracing` crate plus `tracing-subscriber` (override the default
//! `info` level with the `RUST_LOG` env var).

use dots_transport::{Endpoint, EndpointHandle, HostTransceiver, parse_endpoint};

const DEFAULT_ENDPOINT: &str = "tcp://0.0.0.0:11235";
const DEFAULT_NAME: &str = "dotsd";

#[tokio::main(flavor = "current_thread")]
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

    tokio::signal::ctrl_c().await?;
    tracing::info!("Ctrl-C received — shutting down");
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
                name = args.next().ok_or("--name requires an argument")?;
            }
            s if s.starts_with("--name=") => {
                name = s[7..].to_string();
            }
            other => endpoints.push(parse_endpoint(other)?),
        }
    }

    if endpoints.is_empty() {
        endpoints.push(parse_endpoint(DEFAULT_ENDPOINT)?);
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
