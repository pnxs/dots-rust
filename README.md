# dots-rust

A Rust 2024 port of [DOTS](https://github.com/pnxs/dots-cpp), a type-oriented
stateful pub/sub IPC system. Wire-compatible with `dots-cpp`'s v2 transmission
framing — guests and brokers in either language interoperate over the same
TCP or Unix-domain socket.

## What this is

DOTS treats every published value as a typed instance with a key. The broker
mirrors a per-type cache; new subscribers receive the current cache state
automatically before live updates start. Types are described in `.dots` IDL
files, code-generated for each language, and exchanged at handshake time so
peers without a compiled-in copy can still decode wire payloads.

This crate set is a from-scratch Rust implementation. It links against
nothing from `dots-cpp`; the wire format is the only contract.

## Layout

| Crate              | Role                                                                            |
|--------------------|---------------------------------------------------------------------------------|
| `dots-core`        | Type-system primitives: descriptors, `StructValue`, `PropertySet`, encode/decode |
| `dots-derive`      | `#[derive(DotsStruct)]` and `#[derive(DotsEnum)]` proc-macros                    |
| `dots-model`       | DOTS-internal types (handshake, framing, daemon records, descriptor exchange)    |
| `dots-transport`   | Async transport: codec, `Connection`, `GuestTransceiver`, `HostTransceiver`, `App` |
| `dots-build`       | Compile `.dots` files into Rust source from a `build.rs`                         |
| `dots-build-test`  | Compile-test fixture for `dots-build`                                            |
| `dotsd`            | Broker daemon binary — listens on `tcp://` and/or `uds://` endpoints             |
| `dots-demo-client` | Sample guest that publishes and subscribes to `Pinger`                           |
| `dots-example`    | Minimal `#[derive(DotsStruct)]` demonstration                                     |

## Quick start

Build everything:

```bash
cargo build --workspace
```

Run the broker and a client in two terminals:

```bash
# terminal 1
cargo run -p dotsd

# terminal 2
cargo run -p dots-demo-client
```

To watch routing between two clients, start a second `dots-demo-client` with
a different name:

```bash
cargo run -p dots-demo-client -- 127.0.0.1:11235 alice
cargo run -p dots-demo-client -- 127.0.0.1:11235 bob
```

For a UDS deployment:

```bash
cargo run -p dotsd -- uds:///tmp/dotsd.sock
```

`dotsd` accepts `tcp://addr:port` and `uds:///path` URIs. Multiple endpoints
on the same daemon are fine:

```bash
cargo run -p dotsd -- tcp://0.0.0.0:11235 uds:///tmp/dotsd.sock
```

## Defining types

Either declare types directly in Rust:

```rust
use dots_core::{StructValue, dots};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "Pinger", cached)]
struct Pinger {
    #[dots(tag = 1, key)] id: Option<u32>,
    #[dots(tag = 2)]      message: Option<String>,
    #[dots(tag = 3)]      sequence: Option<u64>,
}

let p = dots!(Pinger { id: 1_u32, message: "hi", sequence: 42_u64 });
```

Or compile `.dots` files at build time:

```toml
# Cargo.toml
[build-dependencies]
dots-build = { path = "../dots-build" }
```

```rust
// build.rs
fn main() {
    dots_build::compile(&["proto/types.dots"]).unwrap();
}
```

```rust
// src/lib.rs
include!(concat!(env!("OUT_DIR"), "/dots_generated.rs"));
```

The generated file lives in `OUT_DIR` (so it's not checked in) but is
visible to `rust-analyzer` for completion and hover-docs.

## Using the App API

```rust
use dots_transport::App;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = App::connect("127.0.0.1:11235", "my-client").await?;

    let _sub = app.subscribe::<Pinger>(|event| {
        println!("got {:?} from {:?}", event.value, event.header.sender);
    });

    let client = app.client();
    tokio::spawn(async move {
        for i in 1u64.. {
            client.publish(&Pinger {
                id: Some(0), message: Some("hi".into()), sequence: Some(i),
            }).ok();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    app.run_until_signal().await?;
    Ok(())
}
```

## Status

Working:

- v2 transmission framing, wire-compatible with `dots-cpp`
- `#[derive(DotsStruct)]`, `#[derive(DotsEnum)]`, `dots!` macro
- `.dots` parser + code generator (in `dots-build`)
- Client-side (`Connection`, `GuestTransceiver`, `App`) with TCP + UDS transport
- Broker (`HostTransceiver`, `dotsd` binary) with TCP + UDS, group routing,
  cache pool with replay on subscribe, `[cleanup]`-on-disconnect, descriptor
  fan-out, `DotsDescriptorRequest` / `DotsClearCache` / `DotsEcho` handlers
- SHA-256 challenge-response authentication (client-side)
- `DotsClient` lifecycle publishes; `DotsMember(Join/Leave)` ref-counted
- `Container<T>` typed local cache mirror
- Auto-registration of nested struct + enum descriptors
- Compile-time substruct-only enforcement: `#[dots(substruct_only)]`
  types don't implement `Publishable`, so passing them to
  `publish` / `remove` is rejected at the call site

Performance: the broker matches or beats `dots-cpp` on the shared
producer/subscriber benchmark — currently ~5% under the C++ baseline at
single-subscriber, ~26% under at 16 subscribers (per-guest drainer task,
sync producer path).

Known gaps:

- Server-side authentication: host always replies `auth_required=false`;
  the verification path and `DotsAuthentication` rule store aren't wired
- `DotsDaemonStatus` periodic publish (broker observability)
- WebSocket transport
- Producer/consumer split example (the demo client mixes both roles)

## License

MIT OR Apache-2.0, matching the workspace `Cargo.toml`.
