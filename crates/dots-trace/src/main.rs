//! DOTS trace tool.
//!
//! Connects to a broker, asks for every type's descriptor, and prints
//! every transmission that arrives — both internal DOTS lifecycle
//! traffic (DotsClient, DotsMember, DotsCacheInfo, …) and any user
//! types the broker knows about. Demonstrates the dynamic-client API
//! end to end:
//!
//! - [`App::new`] for the default endpoint (`tcp://127.0.0.1:11235`),
//!   overridable via the `DOTS_ENDPOINT` env var,
//! - [`App::publish`] of `DotsDescriptorRequest` to ask the broker to
//!   stream every cached descriptor,
//! - [`App::subscribe_all_types`] for one handler that fires for
//!   every transmission of every learned type,
//! - the [`Display`] impl for `DynamicStruct` for human-readable
//!   payload output.
//!
//! ```text
//! ./dotsd                                              # in one terminal
//! cargo run --bin dots-trace                           # default tcp://127.0.0.1:11235
//! DOTS_ENDPOINT=uds:///tmp/dotsd.sock cargo run --bin dots-trace
//! ```
//!
//! Override the log level via `RUST_LOG`, e.g.
//! `RUST_LOG=dots_transport=debug cargo run --bin dots-trace`.

use dots_model::DotsDescriptorRequest;
use dots_transport::App;

const CLIENT_NAME: &str = "dots-trace";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dots_transport::init_tracing("");

    let app = App::new(CLIENT_NAME).await?;

    // Ask the broker to stream every cached descriptor — this is what
    // turns dots-trace into a *dynamic* client. Without it, we'd only
    // see types whose descriptors arrived during preload.
    app.publish(&DotsDescriptorRequest::default());

    // One handler for every type — the composite helper auto-installs
    // a dynamic subscription per descriptor (now and as new ones land).
    let _all = app.subscribe_all_types(|event| {
        let type_name = &event.value.descriptor.name;
        let sender = event
            .header
            .sender
            .map(|s| s.to_string())
            .unwrap_or_else(|| "?".into());
        let from_cache = event
            .header
            .from_cache
            .map(|n| format!(" cached={n}"))
            .unwrap_or_default();
        let removal = if event.header.remove_obj == Some(true) {
            " [REMOVE]"
        } else {
            ""
        };
        println!("[{type_name:<26}] from={sender:<4}{from_cache}{removal}  {}", event.value);
    });

    eprintln!("subscribed to every known type; press Ctrl-C to exit.");
    app.run_until_signal().await?;
    eprintln!("exited.");
    Ok(())
}
