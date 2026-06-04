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
//! DOTS_ENDPOINT=uds:///tmp/dotsd.sock cargo run --bin dots-demo-client
//! ```
//!
//! Logging is via `tracing` + `tracing-subscriber`. Override the default
//! `info` level with `RUST_LOG`, e.g.
//!
//! ```text
//! RUST_LOG=dots_transport=debug cargo run --bin dots-demo-client
//! ```

use std::time::Duration;
use dots_core::{PUBLISHED_TYPES, SUBSCRIBED_TYPES, dots};
use dots_model::DotsCacheInfo;
use dots_transport::App;

mod model {
    use dots_derive::DotsStruct;
    #[derive(DotsStruct, Default, Debug, Clone)]
    #[dots(name = "Pinger", cached)]
    pub struct Pinger {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub message: Option<String>,
        #[dots(tag = 3)]
        pub sequence: Option<u64>,
    }
}
use model::*;

const CLIENT_NAME: &str = "dots-demo-client";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dots_transport::init_tracing("");

    // Link-time-collected lists of types this binary actually touches.
    // Populated by `subscribe::<T>` / `publish::<T>` monomorphizations
    // (via the `GlobalRegistration` linkme distributed slices).
    let subscribed: Vec<&str> = SUBSCRIBED_TYPES.iter().map(|d| d.name).collect();
    let published: Vec<&str> = PUBLISHED_TYPES.iter().map(|d| d.name).collect();
    eprintln!("subscribed types ({}): {subscribed:?}", subscribed.len());
    eprintln!("published types  ({}): {published:?}", published.len());

    let app = App::new(CLIENT_NAME).await?;

    // Container — typed local mirror of the broker's Pinger cache.
    // Cheap to clone; clones share the same backing store and the
    // dispatch-unregister fires only when the last clone drops.
    let pingers = app.container::<Pinger>();
    let pingers_for_handler = pingers.clone();

    // Synchronous callback handler — fires from App::run's read loop.
    app.subscribe::<Pinger>(move |event| {
        let from_me = event.header.is_from_myself == Some(true);
        let obj = &event.value;
        println!(
            "✦ Pinger:  id={:?}  message={:?}  seq={:?}  from={:?}  cache_len={}{}",
            obj.id,
            obj.message,
            obj.sequence,
            event.header.sender,
            pingers_for_handler.len(),
            if from_me { "  (from me)" } else { "" },
        );
    })
    .discard();

    // Per-type cache-end notifications from dotsd. Sent after the
    // broker streams the cached objects following a DotsMember(join).
    app.subscribe::<DotsCacheInfo>(|event| {
        let cache_info = &event.value;
        if cache_info.end_transmission == Some(true) {
            if let Some(name) = cache_info.type_name.as_deref() {
                eprintln!("⌛ cache transmission complete for `{name}`");
            }
        } else if cache_info.end_descriptor_request == Some(true) {
            eprintln!("⌛ descriptor request complete");
        }
    })
    .discard();

    // Periodic publisher running concurrently with App::run. Races
    // the ticker against `client.closed()` so the task exits cleanly
    // when the driver shuts down (instead of spinning publishes into
    // a closed channel).
    let client = app.client();
    let pinger_name = CLIENT_NAME;
    tokio::spawn(async move {
        let mut sequence: u64 = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    sequence += 1;
                    let p = dots!(Pinger {
                        id: 0_u32,
                        message: format!("hello from {pinger_name}"),
                        sequence: sequence,
                    });
                    client.publish(&p);
                }
                _ = client.closed() => {
                    eprintln!("connection closed; publisher exiting.");
                    break;
                }
            }
        }
    });

    eprintln!("subscribed; publishing one Pinger/sec; press Ctrl-C to exit.");
    eprintln!("(initial container size after preload: {})", pingers.len());

    app.run_until_signal().await?;
    eprintln!("exited.");
    Ok(())
}
