# dots-testing

In-process **Host + Guest test harness** for [dots-rust]. The Rust
counterpart of dots-cpp's `dots::testing::EventTestBase`: write unit
tests that exercise functions and structs using the global DOTS API
(`dots_transport::global::*`) — or a `GuestTransceiver` handle —
without a broker process or a socket.

## Why there's no `LocalChannel` type

In dots-cpp, `LocalChannel` / `LocalListener` exist because `Channel`
is an abstract base with several concrete transports, and tests need an
in-memory one. In dots-rust the transport is already generic:
`Connection<S>` works over any `AsyncRead + AsyncWrite`, so the
in-memory loopback is simply [`tokio::io::duplex`]. `TestHarness` wires
that pipe between an in-process `HostTransceiver` and a guest, runs the
handshake + EarlySubscribe phase, spawns the guest driver, and installs
the primary guest as the process-wide global.

## Usage

Add it as a dev-dependency:

```toml
[dev-dependencies]
dots-testing = { path = "../dots-testing" } # or version = "0.1"
```

```rust
use dots_core::dots;
use dots_transport::global as dots;
use dots_testing::TestHarness;

#[tokio::test]
async fn my_type_roundtrips() {
    let harness = TestHarness::new().await;

    // The primary guest is the global, so the free functions work:
    let mut sub = dots::subscribe_stream::<MyType>();
    dots::publish(&dots!(MyType { id: 1_u32 }));

    let event = harness.recv(&mut sub).await.expect("event");
    assert_eq!(event.value.id, Some(1));
}
```

Simulate another client (the dots-cpp "spoof guest"):

```rust
let harness = TestHarness::new().await;
let mut sub = harness.subscribe_stream::<MyType>();

let other = harness.add_spoof_guest().await?;
other.publish(&dots!(MyType { id: 2_u32 }));

let event = harness.recv(&mut sub).await.expect("routed to primary");
assert_eq!(event.value.id, Some(2));
```

## Key points

- **Determinism.** There is no synchronous `processEvents()` —
  the guest drivers are spawned tokio tasks. Assert by *awaiting*:
  `harness.recv(&mut sub).await`. `harness.settle().await` is a
  best-effort scheduler barrier only.
- **Serialization.** The global DOTS API is a process-wide singleton,
  so building a `TestHarness` holds a process-wide lock for its
  lifetime; harness-based tests run one-at-a-time within a test binary,
  even on a multi-threaded runtime. The lock never poisons, so a
  panicking test doesn't wedge the rest.
- **No type enumeration.** The guest ships the binary's link-time
  `PUBLISHED_TYPES` / `SUBSCRIBED_TYPES` descriptors to the host during
  EarlySubscribe (same path a real `App` uses), so you don't register
  types by hand.
- **Teardown.** Dropping the harness exits every guest, clears the
  global slot, aborts the driver tasks, and releases the lock.

## Test structs and the `dots!` companion macro

`#[derive(DotsStruct)]` emits a per-type companion macro at the
enclosing module's top level. Defining several DOTS structs at a test
file's top level collides; wrap them in a submodule (the in-tree
convention):

```rust
mod model {
    use dots_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, Clone, PartialEq)]
    #[dots(name = "MyType", cached)]
    pub struct MyType {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
    }
}
use model::*;
```

[dots-rust]: https://github.com/pnxs/dots-rust
[`tokio::io::duplex`]: https://docs.rs/tokio/latest/tokio/io/fn.duplex.html
