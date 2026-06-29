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

## Installation

Applications depend on a single crate, **`dots-rs`**, which re-exports the
whole API (runtime, derive macros, async transport):

```toml
[dependencies]
dots-rs = "0.1"

# Only when compiling `.dots` schema files at build time:
[build-dependencies]
dots-rs-build = "0.1"
```

## Layout

The published crates are named under the `dots-rs` prefix (the bare `dots`
name was taken on crates.io). `dots-rs` is the umbrella; the rest are its
building blocks and are pulled in transitively.

| Crate               | Role                                                                              |
|---------------------|----------------------------------------------------------------------------------|
| `dots-rs`           | **Umbrella** — re-exports everything; the only crate apps depend on              |
| `dots-rs-core`      | Type-system primitives: descriptors, `StructValue`, `PropertySet`, encode/decode |
| `dots-rs-derive`    | `#[derive(DotsStruct)]` and `#[derive(DotsEnum)]` proc-macros                    |
| `dots-rs-model`     | DOTS-internal types (handshake, framing, daemon records, descriptor exchange)    |
| `dots-rs-transport` | Async transport: codec, `Connection`, `GuestTransceiver`, `HostTransceiver`, `App` |
| `dots-rs-build`     | Compile `.dots` files into Rust source from a `build.rs` (build-dependency)      |
| `dots-build-test`   | Compile-test fixture for `dots-rs-build`                                         |
| `dotsd`             | Broker daemon binary — listens on `tcp://` and/or `uds://` endpoints             |
| `dots-demo-client`  | Sample guest that publishes and subscribes to `Pinger`                           |
| `dots-example`      | Minimal `#[derive(DotsStruct)]` demonstration                                    |

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
use dots_rs::{StructValue, dots, DotsStruct};

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
dots-rs-build = "0.1"
```

```rust
// build.rs
fn main() {
    dots_rs_build::compile(&["proto/types.dots"]).unwrap();
}
```

The generated code refers to the runtime through `dots_rs::…`, so the
crate must also depend on `dots-rs`.

```rust
// src/lib.rs
include!(concat!(env!("OUT_DIR"), "/dots_generated.rs"));
```

The generated file lives in `OUT_DIR` (so it's not checked in) but is
visible to `rust-analyzer` for completion and hover-docs.

## Using the App API

```rust
use dots_rs::App;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = App::connect_tcp("127.0.0.1:11235", "my-client").await?;

    let _sub = app.subscribe::<Pinger>(|event| {
        println!("got {:?} from {:?}", event.transmitted, event.header.sender);
    });

    let client = app.client();
    tokio::spawn(async move {
        for i in 1u64.. {
            client.publish(&Pinger {
                id: Some(0), message: Some("hi".into()), sequence: Some(i),
            });
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

LGPL-3.0-only
