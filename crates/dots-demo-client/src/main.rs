//! Minimal DOTS demo client.
//!
//! Connects to a `dotsd` broker over TCP, runs the handshake, and
//! prints every transmission that arrives until either the broker
//! closes the connection or the user sends Ctrl-C.
//!
//! ## Running
//!
//! Start a `dotsd` (from the C++ dots-cpp repo) on the default port:
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
//! The client doesn't subscribe to anything and doesn't request
//! preload, so the only traffic it observes is whatever the broker
//! pushes unsolicited (typically `DotsMember` join/leave events when
//! other clients come and go, plus any descriptor exchange).

use std::sync::Arc;

use dots_core::decode_typed_from_slice;
use dots_model::{
    DotsMsgError, StructDescriptorData, Transmission, registry_with_internal_types,
};
use dots_transport::{Connection, ConnectionError, TransportError};
use tokio::net::TcpStream;
use tokio::signal::ctrl_c;

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

    let registry = Arc::new(registry_with_internal_types());
    let mut conn = match Connection::establish(stream, &name, registry).await {
        Ok(c) => c,
        Err(e) => return handshake_failed(e),
    };

    eprintln!(
        "connected: server=`{}` client_id={:?}",
        conn.server_name().unwrap_or("?"),
        conn.client_id()
    );
    eprintln!("listening for transmissions; press Ctrl-C to exit");

    loop {
        tokio::select! {
            biased;
            _ = ctrl_c() => {
                eprintln!("\ninterrupted, exiting.");
                break;
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
    let sender = txn.header.sender.map(|s| s.to_string()).unwrap_or_else(|| "-".into());
    let valid = txn.payload.valid.len();
    println!(
        "← {type_name}  sender={sender}  fields={valid}  remove={}  fromCache={:?}",
        txn.header.remove_obj.unwrap_or(false),
        txn.header.from_cache,
    );
    annotate_known_internal(type_name, txn);
}

/// Pretty-print a few of the DOTS-internal types we care about. For
/// everything else, the receive-loop summary already shows the type
/// name and basic header fields.
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
                let n_props = d
                    .properties
                    .as_ref()
                    .map(|p| p.len())
                    .unwrap_or(0);
                println!(
                    "    name={:?}  properties={}  publisher_id={:?}",
                    d.name, n_props, d.publisher_id
                );
            }
        }
        _ => {}
    }
}
