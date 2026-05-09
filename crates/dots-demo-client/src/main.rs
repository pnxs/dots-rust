//! DOTS demo client.
//!
//! Connects to a `dotsd` broker over TCP, runs the handshake, then:
//!
//! - Subscribes to `Pinger` events.
//! - Publishes a `Pinger` once per second with an incrementing sequence
//!   number.
//! - Drives the connection's read loop, printing every incoming
//!   transmission's header summary.
//! - Prints typed `Pinger` events as they arrive — including its own
//!   publications looped back through the broker, plus any other
//!   `Pinger` traffic from other clients on the same broker.
//!
//! ## Running
//!
//! Start a `dotsd` (from the dots-cpp repo) on its default port:
//!
//! ```text
//! ./dotsd
//! ```
//!
//! Then in another terminal:
//!
//! ```text
//! cargo run --bin dots-demo-client
//! cargo run --bin dots-demo-client -- 127.0.0.1:11235 my-name
//! ```
//!
//! Run two instances at once to see the broker route Pinger publications
//! between them.

use std::sync::Arc;
use std::time::Duration;

use dots_core::decode_typed_from_slice;
use dots_derive::DotsStruct;
use dots_model::{
    DotsMsgError, StructDescriptorData, Transmission, registry_with_internal_types,
};
use dots_transport::{Connection, ConnectionError, TransportError};
use tokio::net::TcpStream;
use tokio::signal::ctrl_c;

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
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| DEFAULT_ADDR.into());
    let name = args.next().unwrap_or_else(|| DEFAULT_NAME.into());

    eprintln!("connecting to {addr} as `{name}` ...");
    let stream = TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;

    let mut registry = registry_with_internal_types();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    let registry = Arc::new(registry);

    let mut conn = match Connection::establish(stream, &name, registry).await {
        Ok(c) => c,
        Err(e) => return handshake_failed(e),
    };
    eprintln!(
        "connected: server=`{}` client_id={:?}",
        conn.server_name().unwrap_or("?"),
        conn.client_id()
    );

    let mut pinger_sub = conn.subscribe::<Pinger>();
    let our_id = conn.client_id().unwrap_or(0);

    // Publish a Pinger every second.
    let mut publish_timer = tokio::time::interval(Duration::from_secs(1));
    publish_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sequence: u64 = 0;

    eprintln!("subscribed to `Pinger`; publishing one per second; press Ctrl-C to exit");

    loop {
        tokio::select! {
            biased;
            _ = ctrl_c() => {
                eprintln!("\ninterrupted, exiting.");
                break;
            }
            _ = publish_timer.tick() => {
                sequence += 1;
                let pinger = Pinger {
                    id: Some(our_id),
                    message: Some(format!("hello from {name}")),
                    sequence: Some(sequence),
                };
                if let Err(e) = conn.publish(&pinger).await {
                    eprintln!("publish error: {e}");
                    break;
                }
                eprintln!("→ published Pinger seq={sequence}");
            }
            event = pinger_sub.recv() => {
                match event {
                    Some(ev) => println!(
                        "✦ Pinger event:  id={:?}  message={:?}  seq={:?}  sender={:?}{}",
                        ev.value.id, ev.value.message, ev.value.sequence, ev.header.sender,
                        if ev.header.is_from_myself == Some(true) { "  (from me)" } else { "" },
                    ),
                    None => {
                        eprintln!("subscription channel closed.");
                        break;
                    }
                }
            }
            maybe = conn.next() => {
                match maybe {
                    Some(Ok(txn)) => print_transmission(&txn),
                    Some(Err(e)) => return decode_failed(e),
                    None => {
                        eprintln!("server closed the connection.");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

fn handshake_failed(err: ConnectionError) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("handshake failed: {err}");
    Err(Box::new(err))
}

fn decode_failed(err: TransportError) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("transport error: {err}");
    Err(Box::new(err))
}

fn print_transmission(txn: &Transmission) {
    let type_name = txn.header.type_name.as_deref().unwrap_or("?");
    // Don't double-print Pinger here — the typed subscription handler
    // already prints them with full detail.
    if type_name == "Pinger" {
        return;
    }
    let sender = txn
        .header
        .sender
        .map(|s| s.to_string())
        .unwrap_or_else(|| "-".into());
    let valid = txn.payload.valid.len();
    println!(
        "← {type_name}  sender={sender}  fields={valid}  remove={}",
        txn.header.remove_obj.unwrap_or(false),
    );
    annotate_known_internal(type_name, txn);
}

/// Pretty-print a few of the DOTS-internal types we care about.
fn annotate_known_internal(type_name: &str, txn: &Transmission) {
    let bytes = txn.payload.encode();
    match type_name {
        "DotsMsgError" => {
            if let Ok(err) = decode_typed_from_slice::<DotsMsgError>(&bytes) {
                println!(
                    "    error_code={:?} text={:?}",
                    err.error_code, err.error_text
                );
            }
        }
        "StructDescriptorData" => {
            if let Ok(d) = decode_typed_from_slice::<StructDescriptorData>(&bytes) {
                let n_props = d.properties.as_ref().map(|p| p.len()).unwrap_or(0);
                println!(
                    "    name={:?}  properties={}  publisher_id={:?}",
                    d.name, n_props, d.publisher_id
                );
            }
        }
        _ => {}
    }
}
