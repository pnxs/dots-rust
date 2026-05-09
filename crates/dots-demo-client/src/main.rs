//! DOTS demo client — App-based.
//!
//! Connects to a `dotsd` broker over TCP, publishes a `Pinger` once
//! per second, and prints every incoming `Pinger` event from the
//! callback subscription. Container alongside the callback so the
//! local cache stays in sync; we print its size on each event.
//!
//! Run two instances against the same broker to see them route
//! Pingers to each other.
//!
//! ```text
//! ./dotsd                                                  # in one terminal
//! cargo run --bin dots-demo-client                         # default 127.0.0.1:11235
//! cargo run --bin dots-demo-client -- 127.0.0.1:11235 bob  # second client
//! ```
//!
//! Logging is via `tracing` + `tracing-subscriber`. Override the default
//! `info` level with `RUST_LOG`, e.g.
//!
//! ```text
//! RUST_LOG=dots_transport=debug cargo run --bin dots-demo-client
//! ```

use std::sync::Arc;
use std::time::Duration;

use dots_derive::DotsStruct;
use dots_model::DotsCacheInfo;
use dots_transport::App;

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "Pinger", cached)]
struct Pinger {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    message: Option<String>,
    #[dots(tag = 3)]
    sequence: Option<u64>,
}

const DEFAULT_ADDR: &str = "127.0.0.1:11235";
const DEFAULT_NAME: &str = "dots-demo-client";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default log level: info. Override via RUST_LOG env var, e.g.
    //   RUST_LOG=dots_transport=debug,dots_demo_client=info cargo run ...
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .compact()
        .init();

    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| DEFAULT_ADDR.into());
    let name = args.next().unwrap_or_else(|| DEFAULT_NAME.into());

    let app = App::connect(&addr, &name).await?;

    // Container — typed local mirror of the broker's Pinger cache.
    let pingers = app.container::<Pinger>();
    let pingers_for_handler = pingers.handle();

    // Synchronous callback handler — fires from App::run's read loop.
    let name_for_handler = Arc::new(name.clone());
    app.subscribe::<Pinger>(move |event| {
        let from_me = event.header.is_from_myself == Some(true);
        println!(
            "✦ Pinger:  id={:?}  message={:?}  seq={:?}  from={:?}  cache_len={}{}",
            event.value.id,
            event.value.message,
            event.value.sequence,
            event.header.sender,
            pingers_for_handler.len(),
            if from_me { "  (from me)" } else { "" },
        );
        let _ = name_for_handler;
    })
    .discard();

    // Per-type cache-end notifications from dotsd. Sent after the
    // broker streams the cached objects following a DotsMember(join).
    app.subscribe::<DotsCacheInfo>(|event| {
        if event.value.end_transmission == Some(true) {
            if let Some(name) = event.value.type_name.as_deref() {
                eprintln!("⌛ cache transmission complete for `{name}`");
            }
        } else if event.value.end_descriptor_request == Some(true) {
            eprintln!("⌛ descriptor request complete");
        }
    })
    .discard();

    // Periodic publisher running concurrently with App::run.
    let client = app.client();
    let pinger_name = name.clone();
    tokio::spawn(async move {
        let mut sequence: u64 = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            sequence += 1;
            let p = Pinger {
                id: Some(0),
                message: Some(format!("hello from {pinger_name}")),
                sequence: Some(sequence),
            };
            if client.publish(&p).is_err() {
                eprintln!("connection closed; publisher exiting.");
                break;
            }
        }
    });

    eprintln!("subscribed; publishing one Pinger/sec; press Ctrl-C to exit.");
    eprintln!("(initial container size after preload: {})", pingers.len());

    app.run_until_signal().await?;
    eprintln!("exited.");
    Ok(())
}
