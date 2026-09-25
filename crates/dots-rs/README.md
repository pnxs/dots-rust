# dots-rs

DOTS (Distributed Objects in Time and Space) pub/sub for Rust — a Rust 2024
port of [dots-cpp](https://github.com/pnxs/dots-cpp), wire-compatible with its
v2 transmission framing.

`dots-rs` is the **umbrella crate**: it re-exports the runtime types, the
`#[derive(DotsStruct)]` / `#[derive(DotsEnum)]` / `dots!` macros, and the async
transport, so an application needs only this one dependency.

```toml
[dependencies]
dots-rs = "0.1"

# Only when compiling `.dots` schema files at build time:
[build-dependencies]
dots-rs-build = "0.1"
```

## Guidance for AI agents

Read the [general AI usage guide](https://github.com/pnxs/dots-rust/blob/master/crates/dots-rs/AI_USAGE_GUIDE.md)
for models, connections, publishing, subscription replay, dynamic types, and
testing. It ships as `AI_USAGE_GUIDE.md` and appears in the crate API docs as
`dots_rs::ai_usage`. Add a pointer in downstream projects' agent instructions.

## Container access guidance

The [container usage guide](https://github.com/pnxs/dots-rust/blob/master/crates/dots-rs/CONTAINER_GUIDE.md)
covers efficient lookups, borrowed iteration, lock lifetimes, and snapshots
for developers and AI agents. It is also bundled as `CONTAINER_GUIDE.md` and
included in API documentation under `dots_rs::container_usage`. Match the
guide to your resolved dependency revision; development APIs may not yet be
published. Client projects can link the guide from their own `AGENTS.md`.

## Example

```rust
use dots_rs::{DotsStruct, dots, App};

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "Pinger", cached)]
struct Pinger {
    #[dots(tag = 1, key)] id: u32,
    #[dots(tag = 2)]      message: Option<String>,
    #[dots(tag = 3)]      sequence: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = App::connect_tcp("127.0.0.1:11235", "my-client").await?;
    let _sub = app.subscribe::<Pinger>(|event| {
        println!("got {:?}", event.transmitted);
    });
    app.client().publish(&dots!(Pinger { id: 1_u32, sequence: 42_u64 }));
    app.run_until_signal().await?;
    Ok(())
}
```

## Feature flags

- `stats` (default) — per-guest I/O statistics.
- `tracing-init` (default) — the `init_tracing` helper (pulls `tracing-subscriber`).
- `testing` (default) — the in-process Host + Guest test harness, as `dots_rs::testing`.

See the [repository](https://github.com/pnxs/dots-rust) for the broker daemon
(`dotsd`), runnable examples, and design notes.

## License

LGPL-3.0-only
