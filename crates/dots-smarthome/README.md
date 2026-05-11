# dots-smarthome

Rust port of the smart-home example from
[`dots-cpp`](https://github.com/pnxs/dots-cpp/tree/master/bin/examples/smart-home).

A fictional home owner wires three reactive components against a DOTS
broker:

- **Living room** — one master `Dimmer` controls two `LightControl`s
  (`CouchLight`, `CeilingLight`).
- **Stairwell** — either of two stateless switches (`Lower`, `Upper`)
  toggles a single `LightControl`.
- **Basement** — motion-sensor `Switch` turns a `LightControl` on; when
  motion clears, schedule a turn-off after a configurable timeout
  (re-triggering motion cancels the pending turn-off).

The example mirrors the dots-cpp original in shape: each component is
a small struct owning its subscription handle(s) and any state it
needs. The four DOTS types are defined in [`proto/model.dots`](proto/model.dots)
— same source format as dots-cpp — and compiled into Rust by
[`build.rs`](build.rs) via the `dots-build` crate (output written to
`$OUT_DIR/dots_generated.rs`, included by `src/lib.rs`). The
application-level device-id constants (e.g. `LIVING_ROOM_MASTER_DIMMER`)
live in [`src/ids.rs`](src/ids.rs) since they're not part of the DOTS
model.

## Running

You need a broker. The default is `tcp://127.0.0.1:11235`; override
with `DOTS_ENDPOINT` (e.g. `uds:///tmp/dotsd.sock`).

```sh
# Terminal 1 — broker
cargo run --bin dotsd

# Terminal 2 — the reactor
cargo run --bin smart-home

# Terminal 3 — drive the reactor with a canned sequence
cargo run --bin smart-home-sim
```

The simulator publishes a short sequence of `Dimmer`,
`StatelessSwitch`, and `Switch` events and exits. The reactor stays
alive until you Ctrl-C it.

## What to expect

With `RUST_LOG=info` (the default), the reactor prints lines like

```
INFO smart-home LivingRoom: master dimmer changed brightness=Some(42)
INFO smart-home Stairwell: toggling light from=Stairwell_LowerSwitch next_brightness=100
INFO smart-home Basement: motion → light on
INFO smart-home Basement: motion cleared → turn-off scheduled light_timeout=30s
INFO smart-home Basement: timeout elapsed → light off
```

The basement timer defaults to 30 seconds, so by the time the
simulator exits, the basement light is still on — wait for the timer
to fire, or interrupt the reactor.

## Talking points

- **`App::new("smart-home")`** — connects to the default endpoint, runs
  the DOTS handshake. No CLI arg parsing.
- **`app.container::<LightControl>()`** — returns a clone of the
  transceiver-owned container. Multiple components asking for the same
  type get the same backing store; `lights.get(&query)` matches on the
  `#[dots(key)]` field.
- **Subscriptions are dispatch-only** — the components hold their
  `SubscriptionHandle`s to keep the handlers attached; the broker
  receives `DotsMember(Join)` once per type and `DotsMember(Leave)`
  when the transceiver is dropped, not per subscription.
- **Timer cancellation** — the basement keeps a `Mutex<Option<JoinHandle>>`
  so a fresh motion event aborts the prior pending turn-off.
