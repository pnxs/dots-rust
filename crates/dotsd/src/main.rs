//! `dotsd` — DOTS broker daemon.
//!
//! Mirrors the role of dots-cpp's `dotsd`: listens on a TCP socket,
//! accepts guest connections, drives the broker-side handshake, and
//! routes publish/subscribe traffic between guests via the
//! [`HostTransceiver`].
//!
//! Usage:
//!
//! ```text
//! dotsd                        # listen on 0.0.0.0:11235, name "dotsd"
//! dotsd 127.0.0.1:11235        # custom listen address
//! dotsd 0.0.0.0:11235 my-host  # custom name too
//! ```
//!
//! Logging via `tracing` + `tracing-subscriber` (override the default
//! `info` level with the `RUST_LOG` env var).

use std::sync::Arc;

use dots_transport::HostTransceiver;
use tokio::net::TcpListener;

const DEFAULT_LISTEN: &str = "0.0.0.0:11235";
const DEFAULT_NAME: &str = "dotsd";

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

    let mut args = std::env::args().skip(1);
    let listen_addr = args.next().unwrap_or_else(|| DEFAULT_LISTEN.into());
    let daemon_name = args.next().unwrap_or_else(|| DEFAULT_NAME.into());

    let host = HostTransceiver::new(daemon_name.clone());
    let listener = TcpListener::bind(&listen_addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(
        name = daemon_name,
        listen = %local_addr,
        "dotsd ready — accepting guests"
    );

    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received — shutting down");
        }
        result = accept_loop(host.clone(), listener) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "accept loop terminated");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

async fn accept_loop(
    host: Arc<HostTransceiver>,
    listener: TcpListener,
) -> std::io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        if let Err(e) = stream.set_nodelay(true) {
            tracing::warn!(?peer, error = %e, "failed to set TCP_NODELAY on accepted stream");
        }
        let client_id = host.accept(stream);
        tracing::info!(?peer, client_id, "guest accepted");
    }
}
