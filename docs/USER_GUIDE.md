# DOTS User Guide

How to use DOTS from Rust — for human developers and AI agents alike. This
guide takes you from "what is this" to a working publisher/subscriber, then
covers the full client API surface.

AI agents should also read the [general usage guidance](../crates/dots-rs/AI_USAGE_GUIDE.md),
which covers API selection, lifecycle, and verification against the resolved
dependency version.

If you only read one section, read [Mental model](#mental-model) and
[Five-minute example](#five-minute-example).

---

## Table of contents

1. [Mental model](#mental-model)
2. [Five-minute example](#five-minute-example)
3. [Setting up a crate](#setting-up-a-crate)
4. [Defining types](#defining-types)
   - [In Rust with `#[derive(DotsStruct)]`](#in-rust-with-derivedotsstruct)
   - [Struct and field attributes](#struct-and-field-attributes)
   - [Enums](#enums)
   - [Supported field types](#supported-field-types)
   - [The `dots!` macro](#the-dots-macro)
   - [Generated items](#generated-items)
   - [In `.dots` IDL files](#in-dots-idl-files)
5. [Connecting](#connecting)
   - [Early subscription and startup state](#early-subscription-and-startup-state)
6. [Publishing](#publishing)
7. [Subscribing](#subscribing)
8. [The local cache: `Container<T>`](#the-local-cache-containert)
9. [Filtered subscriptions: `View<T>`](#filtered-subscriptions-viewt)
10. [Running the event loop](#running-the-event-loop)
11. [Running a broker (`dotsd`)](#running-a-broker-dotsd)
12. [Dynamic / wire-only types](#dynamic--wire-only-types)
13. [Pitfalls and gotchas](#pitfalls-and-gotchas)
14. [API cheat sheet](#api-cheat-sheet)

---

## Mental model

DOTS is a **type-oriented, stateful publish/subscribe** system for
inter-process communication. Three ideas are worth internalizing up front,
because they differ from ordinary message buses:

1. **You publish typed values, not messages.** Every payload is an instance
   of a declared struct type. The type's name *is* the topic.

2. **Instances have identity.** One or more fields are marked as the
   **key**. Publishing a value with a key that already exists is an *update*
   to that instance, not a new message. There is one logical "current value"
   per key.

3. **The broker keeps a cache, and new subscribers get it for free.** When a
   type is cached (the default in `.dots` schemas), the broker remembers the
   latest value for every key. The instant you subscribe, the broker replays the entire current
   cache to you *before* live updates begin. You never miss state that was
   published before you connected.

So a subscriber's job is usually to **mirror state**, not to consume a stream
of events. The [`Container<T>`](#the-local-cache-containert) type makes this
explicit: it's a local, always-current copy of the broker's cache for one
type.

A **broker** (the `dotsd` daemon) sits in the middle. Clients ("guests")
connect to it over TCP or a Unix domain socket. The broker routes
publications to interested subscribers and maintains the per-type caches.
The wire format is compatible with the C++ implementation
([`dots-cpp`](https://github.com/pnxs/dots-cpp)), so Rust and C++ peers
interoperate freely.

---

## Five-minute example

A complete program that connects, publishes a value once per second, and
prints everything it sees:

```rust
use dots_core::dots;
use dots_derive::DotsStruct;
use dots_transport::App;
use std::time::Duration;

// 1. Declare a type. Every field is Option<_>; #[dots(tag = N)] is required.
#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "Pinger", cached)]
struct Pinger {
    #[dots(tag = 1, key)] id: Option<u32>,
    #[dots(tag = 2)]      message: Option<String>,
    #[dots(tag = 3)]      sequence: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Connect to the broker. Reads DOTS_ENDPOINT, defaults to TCP :11235.
    let app = App::new("my-client").await?;

    // 3. Subscribe. The handler fires for the cache replay AND live updates.
    app.subscribe::<Pinger>(|event| {
        println!(
            "Pinger id={:?} msg={:?} seq={:?} from={:?}",
            event.value.id, event.value.message,
            event.value.sequence, event.header.sender,
        );
    })
    .discard(); // keep the subscription alive for the app's lifetime

    // 4. Publish from a background task.
    let client = app.client();
    tokio::spawn(async move {
        let mut seq = 0u64;
        loop {
            seq += 1;
            client.publish(&dots!(Pinger {
                id: 0_u32,
                message: "hello",
                sequence: seq,
            }));
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    // 5. Drive the I/O loop until Ctrl-C.
    app.run_until_signal().await?;
    Ok(())
}
```

Run it against a broker:

```bash
cargo run -p dotsd          # terminal 1: the broker
cargo run --bin my-client   # terminal 2: this program
```

---

## Setting up a crate

Add the client crates you need. At minimum you want `dots-core` (types and
the `dots!` macro), `dots-derive` (the derive macros), and `dots-transport`
(the `App`/client API). `tokio` drives the async runtime.

```toml
[dependencies]
dots-core      = { path = "../dots-rust/crates/dots-core" }
dots-derive    = { path = "../dots-rust/crates/dots-derive" }
dots-transport = { path = "../dots-rust/crates/dots-transport" }
tokio          = { version = "1", features = ["full"] }
```

> Paths are shown because this is a pre-release workspace. Adjust to git or
> registry coordinates as appropriate.

---

## Defining types

There are two ways to declare DOTS types. Use the **derive macro** when your
types live in Rust and you control them. Use **`.dots` IDL files** when you
want a language-neutral schema (e.g. shared with a C++ peer) compiled at
build time.

### In Rust with `#[derive(DotsStruct)]`

```rust
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "RoundtripData", cached, persistent)]
struct RoundtripData {
    #[dots(tag = 1, key)] id: Option<u32>,
    #[dots(tag = 2)]      payload: Option<String>,
    #[dots(tag = 3)]      counter: Option<u64>,
    #[dots(tag = 4)]      flag: Option<bool>,
    #[dots(tag = 5)]      home: Option<Address>,      // nested DOTS struct
    #[dots(tag = 6)]      raw: Option<Vec<u8>>,
    #[dots(tag = 7)]      counters: Option<Vec<u32>>, // repeated field
}

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "Address")]
struct Address {
    #[dots(tag = 1)] street: Option<String>,
    #[dots(tag = 2)] number: Option<u32>,
}
```

Rules that the macro enforces at compile time:

- **Every field must be `Option<T>`.** A property is either *set* or *not
  set* on the wire; `Option` models that directly. `None` means "absent",
  and absent fields are not transmitted.
- **Every field needs `#[dots(tag = N)]`** with a unique tag in `1..=254`.
  Tags are the wire identity of a field — keep them stable across versions.
- Deriving `Default` is required (the transport constructs values via
  `Default` before filling them in). `Debug` and `Clone` are conventional
  and needed for some APIs (e.g. `Container::snapshot`, `View<T>`).

### Struct and field attributes

On the struct, inside `#[dots(...)]`:

| Attribute        | Meaning                                                                 |
|------------------|-------------------------------------------------------------------------|
| `name = "Wire"`  | Wire-format type name. Defaults to the Rust struct name.                |
| `cached`         | Broker keeps the latest value per key and replays it to new subscribers.|
| `persistent`     | Cache survives broker restarts. Implies/requires `cached`.              |
| `internal`       | Marks a DOTS-internal type. Not for application types.                  |
| `substruct_only` | Type can only be *nested* in another struct, never published directly. The macro deliberately omits the `Publishable` impl, so passing one to `publish`/`remove` fails to compile. |

On a field, inside `#[dots(...)]`:

| Attribute   | Meaning                                                                       |
|-------------|-------------------------------------------------------------------------------|
| `tag = N`   | **Required.** Wire tag, `1..=254`, unique within the struct.                  |
| `key`       | This field is part of the instance key. Multiple `key` fields form a compound key. A type with no key is a singleton (one instance). |

### Enums

```rust
use dots_derive::DotsEnum;

#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(name = "Status")]
enum Status {
    #[default]
    #[dots(tag = 1)] Idle,
    #[dots(tag = 2)] Running,
    #[dots(tag = 3)] Failed,
}

// Explicit metadata values (e.g. to mirror an errno space) use `value`:
#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[dots(name = "Errno")]
enum Errno {
    #[default]
    #[dots(tag = 1, value = 0)]   Ok,
    #[dots(tag = 2, value = -1)]  Refused,
    #[dots(tag = 3, value = -42)] BadMessage,
}
```

Enums must be **unit-only** (no data-carrying variants). Each variant needs a
`tag` — that tag is what instance payloads carry on the wire (the dots-cpp
convention). `value` optionally sets the `int32` metadata value exchanged in
descriptors (defaults to `tag - 1`, matching the dots-cpp code generator);
it is never transmitted in payloads. Use enums as struct field types like
any other:

```rust
#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "Job")]
struct Job {
    #[dots(tag = 1, key)] id: Option<u32>,
    #[dots(tag = 2)]      status: Option<Status>,
    #[dots(tag = 3)]      last_error: Option<Errno>,
    #[dots(tag = 4)]      history: Option<Vec<Status>>,
}
```

### Supported field types

- Integers: `u8 u16 u32 u64`, `i8 i16 i32 i64`
- `bool`, `f32`, `f64`
- `String`
- `Vec<u8>` (byte blob) and `Vec<T>` for any supported `T` (repeated field)
- Nested DOTS structs and DOTS enums
- Time types from `dots-model` (e.g. `Timepoint`, `Duration`) and `Uuid`

Wrap each in `Option<...>` as always.

### The `dots!` macro

Prefer `dots!` for constructing DOTS publications, key-only removals, and
lookup queries. It removes repetitive `Some(...)`, string conversions, and
explicit assignments for every unused optional property:

```rust
use dots_rs::dots;

let p = dots!(Pinger {
    id: 0_u32,
    message: "hello",
});
// The omitted optional `sequence` field is None.
let key = dots!(Pinger { id: 0_u32 });
// Optional `message` and `sequence` are both None.
```

Bare values supplied to optional fields are converted via `Into` and wrapped
in `Some`. An `Option` expression preserves absence; bare key fields stay bare.
Nested DOTS struct literals are supported. The normal form without `..base`
sets every omitted optional field explicitly to **`None`**, without requiring
`Default`. Required bare key fields must be provided, and unknown field names
are compile errors.

This helps clients tolerate **new optional model fields**: after regenerating
the model, existing macro calls remain valid and the new fields stay absent.
The client neither needs another `field: None` assignment nor accidentally
publishes a default for a property it does not handle. This is additive
compatibility, not permission to change wire tags, existing property types,
or keys incompatibly.

Omitted properties normally remain unsent, leaving existing cached values
unchanged. Clearing a stored property requires an explicit publish mask that
addresses the absent property. The explicit `..base` form instead inherits
unspecified fields from its base, so use the no-base form for fresh partial
values. See the [AI guide's tested macro example](../crates/dots-rs/AI_USAGE_GUIDE.md#prefer-dots-for-constructing-client-values).

### Generated items

For a struct `Pinger`, the derive macro produces (among other things):

- `Pinger::DESCRIPTOR` — the `&'static StructDescriptor` (type metadata).
- Accessors per field `foo`:
  - `pinger.foo() -> Option<&T>` — read.
  - `pinger.has_foo() -> bool` — is it set?
  - `pinger.with_foo(value) -> Self` — builder-style set (takes `impl Into`).
  - `pinger.clear_foo() -> Self` — unset.
- `Pinger::PROP_FOO` — a `PropertySet` constant for field `foo`, used to build
  masks (see [`publish_with_mask`](#publishing) and
  [`View` projection](#filtered-subscriptions-viewt)). Combine with `|`.
- `Pinger::FOO` — an `Attr` constant for field `foo`, used to build filter
  predicates (see [`View`](#filtered-subscriptions-viewt)).
- A `Pinger::new(...)` constructor and a `StructValue` / `Publishable` impl.

(The constant/attr names are the field name upper-cased: field `sequence` →
`PROP_SEQUENCE` and `SEQUENCE`.)

### In `.dots` IDL files

Declare types in a `.dots` schema and compile them in `build.rs`. This is the
cross-language path — the same `.dots` file generates C++ and Rust.

```dots
// proto/types.dots
struct Pinger [cached] {
    1: [key] uint32 id;
    2: string message;
    3: uint64 sequence;
}

enum DotsConnectionState {
    1: connecting,
    2: connected,
    3: closed
}
```

```toml
# Cargo.toml
[build-dependencies]
dots-build = { path = "../dots-rust/crates/dots-build" }
```

```rust
// build.rs
fn main() {
    dots_build::compile(&["proto/types.dots"]).unwrap();
}
```

```rust
// src/lib.rs (or main.rs)
include!(concat!(env!("OUT_DIR"), "/dots_generated.rs"));
```

The generated Rust lands in `OUT_DIR` (so it's never checked in) but
`rust-analyzer` still indexes it for completion and hover docs. The generated
types are ordinary `#[derive(DotsStruct)]` types — everything in this guide
applies to them unchanged.

---

## Connecting

The entry point is `App`. Pick the constructor that matches your transport:

```rust
use dots_transport::App;

// Resolve endpoint from the DOTS_ENDPOINT env var, else default to
// tcp://127.0.0.1:11235. The most common choice.
let app = App::new("my-client").await?;

// Explicit TCP.
let app = App::connect_tcp("127.0.0.1:11235", "my-client").await?;

// Unix domain socket.
let app = App::connect_unix("/tmp/dotsd.sock", "my-client").await?;

// Parsed endpoint URI ("tcp://host[:port]" or "uds:///path"; a missing
// TCP port defaults to 11235).
let app = App::connect(endpoint, "my-client").await?;
```

The second argument is the **client name** — how this guest is identified to
the broker and other clients (it shows up in `DotsClient` records).

`DOTS_ENDPOINT` accepts the same URI forms as `dotsd`: `tcp://addr[:port]`
(port defaults to `11235`) or `uds:///path`. A malformed value makes
`App::new` return `AppError::Endpoint`. Unlike dots-cpp, credentials in the
URI (`tcp://user:password@host`) are rejected — pass them to
`connect_with_auth` instead.

**Authentication.** Each constructor has a `_with_auth` variant that takes a
shared secret for SHA-256 challenge–response:

```rust
let app = App::connect_tcp_with_auth("127.0.0.1:11235", "my-client", "secret").await?;
```

> Note: server-side auth verification is not yet wired in `dotsd` (it always
> replies `auth_required=false`). The client path works and is ready for a
> broker that enforces it.

---

## Early subscription and startup state

Normal `App` connection constructors finish preload before returning. Typed
`subscribe::<T>()` and `container::<T>()` calls anchor subscriptions at link
time, even when those calls execute after connection. The connection joins
those types and fills their containers from the broker cache during preload.
An application can therefore inspect existing system state immediately after
`App::new(...).await?`, initialize itself, and then start reactive handling.
New handlers replay the local state synchronously; the regular driver loop
must run for subsequent network traffic.

Configure `[profile.release] lto = true` in the downstream application's
workspace root and use a release build. On supported ELF targets, LTO removes
unused type registrations, preventing unnecessary early joins and cache
traffic. It is needed for precise pruning, not for preload correctness;
non-LTO builds can retain extra entries. Runtime-discovered subscriptions,
filtered views opened later, and explicitly non-preload connections do not
share this immediate-cache guarantee.

See the [early-subscription and LTO guide](../crates/dots-rs/AI_USAGE_GUIDE.md#early-subscription-inspect-startup-state-before-reacting)
for a complete startup example and platform details.

## Publishing

`publish` takes a reference to any `Publishable` value (anything that derives
`DotsStruct` without `substruct_only`). It is **fire-and-forget** — it
enqueues the value on the write path and returns `()`.

```rust
// Publish a full value. Creates the instance, or updates it if a value with
// the same key already exists.
app.publish(&dots!(Pinger { id: 1_u32, message: "hi", sequence: 7_u64 }));
```

**Prefer partial updates.** In most cases, publish the complete key and only
the fields that changed. A fresh `dots!` value automatically leaves unrelated
optional fields absent, so ordinary `publish()` preserves their cached values:

```rust
// Client A updates sequence for instance 1.
app.publish(&dots!(Pinger { id: 1_u32, sequence: 8_u64 }));
// Client B updates message for the same instance.
other_client.publish(&dots!(Pinger { id: 1_u32, message: "ready" }));
```

After both updates are processed, the instance contains both values. Clients
can maintain different fields of the same instance without overwriting each
other, provided their addressed fields remain disjoint. The first update can
create the instance; readers must tolerate other fields arriving later. Use
`event.updated()` or the container to read the combined state.

If two clients address the same field, the last update processed by the broker
wins. A full cached-object publication can overwrite another client's newer
field value with a stale one. Prefer fresh partial values over modifying and
republishing whole cached copies. Removing an instance affects every client's
fields, and separate updates are not an atomic transaction.

Use `publish_with_mask()` when restricting an already-populated value to
selected fields or explicitly clearing a field. Keys are added automatically:

```rust
app.publish_with_mask(&pinger, Pinger::PROP_SEQUENCE);
```

See [publishing only changed properties](../crates/dots-rs/AI_USAGE_GUIDE.md#publish-only-the-properties-you-changed)
for the shared-instance example and coordination limits.

**Invalidation is different from omission.** If an existing object's
`message` is valid, `publish(&dots!(Pinger { id: 1_u32 }))` leaves it unchanged.
Setting `message` to `None` locally and using ordinary `publish()` has the
same effect: the invalid property is absent from the automatically derived
update mask. To invalidate it remotely, address it explicitly:

```rust
app.publish_with_mask(
    &dots!(Pinger { id: 1_u32 }), // message remains None in this payload
    Pinger::PROP_MESSAGE,       // but is explicitly addressed by the update
);
```

Keys are added to the mask automatically. Other cached fields remain
unchanged, and the instance is not removed. To set one field and clear
another, supply the first value and include both properties in the mask.
Do not derive a clear mask solely from `valid_set()`, and do not use an
all-property mask with a sparse payload: it clears every addressed absent
field. A zero, false, or empty string is still a valid value, not invalidation.
See the [invalidation pitfalls guide](../crates/dots-rs/AI_USAGE_GUIDE.md#pitfalls-invalidating-an-already-valid-property)
for a complete example and the distinction between payload and update mask.

### Checking fields and building masks with PropertySet

`PropertySet` represents a set of property tags, not their values. Generated
`Type::PROP_FIELD` constants each select one field. Import `StructValue` for
`valid_set()`, which reports the fields currently present in an instance:

```rust
use dots_rs::StructValue;

let valid = pinger.valid_set();
let wanted = Pinger::PROP_MESSAGE | Pinger::PROP_SEQUENCE;
let has_message = Pinger::PROP_MESSAGE.is_subset_of(valid);
let has_both = wanted.is_subset_of(valid);
let has_either = !(valid & wanted).is_empty();
let missing = wanted - valid;

// Publish only present fields from this allowlist. Keys are added for us.
app.publish_with_mask(&pinger, valid & wanted);
```

Use `|` for union, `&` for intersection, and `-` to exclude fields. Rust `^`
is symmetric difference (unlike dots-cpp, where it means intersection).
Build a conditional mask from `PropertySet::EMPTY` with assignments such as
`mask = mask | Pinger::PROP_MESSAGE`. Sets also offer `has(tag)`,
`with_tag(tag)`, `without_tag(tag)`, `iter()`, `len()`, and `is_empty()`;
`with_tag` and `without_tag` return new sets. Prefer generated constants for
known model fields, and never mix constants from unrelated types.

`valid & wanted` excludes absent fields; passing `wanted` directly also
clears any addressed absent fields. Masks select fields, not differences
between old and new values. `PropertySet::ALL` is every representable bit,
not a safe default publish mask for a sparse value.

To check current cached field presence, use `event.updated().valid_set()`.
For supplied fields, use `event.transmitted().valid_set()`; for addressed
fields, inspect the optional `event.header.attributes` mask. An addressed
but absent field is an invalidation. Field presence in a value is distinct
from field declaration in the schema, which is described by its descriptor.
See the [PropertySet guide](../crates/dots-rs/AI_USAGE_GUIDE.md#propertyset-check-fields-and-shape-publish-masks)
for a complete example and additional API details.

### Removing instances

**Removal.** To delete an instance from the (cached) broker, publish a
key-only removal:

```rust
app.remove(&dots!(Pinger { id: 1_u32 }));
```

Subscribers receive this as an event with `event.header.remove_obj ==
Some(true)`.

From inside spawned tasks or handlers, publish through a `Client` handle —
`app.client()` returns a cheap, cloneable `Arc<GuestTransceiver>` exposing the
same `publish` / `publish_with_mask` / `remove` methods. See the
[five-minute example](#five-minute-example).

---

## Subscribing

`subscribe::<T>` registers a callback that fires for **both** the initial
cache replay and all subsequent live updates. The handler runs on the
dispatch path, so keep it short.

```rust
let handle = app.subscribe::<Pinger>(|event: &Event<Pinger>| {
    // event.value is the decoded Pinger
    // event.header carries metadata about the publication
    println!("{:?} from {:?}", event.value, event.header.sender);
});
```

**The `Event<T>` you receive:**

```rust
pub struct Event<T> {
    pub header: DotsHeader,
    pub value: T,
}
```

Useful `header` fields:

| Field              | Meaning                                                              |
|--------------------|----------------------------------------------------------------------|
| `type_name`        | The published type's name.                                           |
| `sender`           | Originating client id (`Option<u32>`).                               |
| `is_from_myself`   | `Some(true)` when this is a loopback of your own publication.        |
| `remove_obj`       | `Some(true)` when the event is a deletion.                           |
| `from_cache`       | During replay, the count of cache objects still to come after this one. `None` means "live, not from cache". |
| `sent_time`        | The sender's send timestamp.                                         |
| `attributes`       | `Option<PropertySet>` of addressed properties, including explicit clears.                 |

**Subscription lifetime.** `subscribe` returns a `SubscriptionHandle`. When
that handle drops, the subscription ends (and DOTS publishes a
`DotsMember(Leave)` once the last subscriber for the type is gone). So you
must hold the handle for as long as you want events:

```rust
let _sub = app.subscribe::<Pinger>(handler); // dropped at end of scope → unsubscribes
```

If you want a subscription to live for the whole process and don't want to
hold the handle, call `.discard()`:

```rust
app.subscribe::<Pinger>(handler).discard(); // subscription stays active
```

**Stream style.** If you prefer pulling events instead of a callback, use
`subscribe_stream::<T>()`, which returns a `Subscription<T>` you can poll as a
stream — handy when you want `async`/`await` and backpressure rather than a
synchronous closure.

---

## The local cache: `Container<T>`

For choosing efficient lookup and iteration methods, read the
[container access guidelines](../crates/dots-rs/CONTAINER_GUIDE.md), including
borrowed `get_ref()` / `find_ref()`, `with_iter()`, and when to retain a snapshot.

A `Container<T>` is a **local, always-up-to-date mirror** of the broker's
cache for type `T`. The transport keeps it in sync automatically; you just
read it. This is the idiomatic way to consume `cached` state — instead of
accumulating events yourself, let the container hold "current state".

```rust
let pingers = app.container::<Pinger>();

// ... later, after some events have been delivered ...

println!("{} pingers cached", pingers.len());

// Look up by key (only the #[dots(key)] fields of the query are used):
if let Some(entry) = pingers.get(&dots!(Pinger { id: 1_u32 })) {
    println!("seq = {:?}", entry.sequence); // entry derefs to &Pinger
} // <- lock released when `entry` drops

// Iterate everything (holds the read lock for the closure's duration):
pingers.for_each(|_key_bytes, p: &Pinger, _info| {
    println!("{:?}", p.id);
});

// Owned snapshot, lock released before you process it:
for entry in pingers.snapshot() {
    println!("{:?}", entry.value);
}
```

A container and a callback subscription compose well: subscribe to react to
changes, read the container when you need the current full picture. Both stay
consistent because they're driven by the same dispatch path. `Container<T>`
is cheap to clone (it's a handle) — clone one into a handler closure.

> **Hold container borrows briefly.** `get` and `for_each` hold the
> container's read lock. That blocks the dispatch path from applying further
> updates *to this type* while you hold it. Never hold a `ContainerRef` (the
> thing `get` returns) across an `.await` — read what you need, clone it out,
> drop the borrow. Use `snapshot()` when you want to process entries after
> releasing the lock.

---

## Filtered subscriptions: `View<T>`

A `View<T>` is a server-side filtered subscription: the **broker** evaluates
a predicate and only sends you matching instances, optionally projecting away
fields you don't need. This saves bandwidth versus subscribing to everything
and filtering locally.

```rust
use dots_model::filter::predicate;
use dots_transport::ViewOp;

// Only Pingers with sequence < 100, and only ship {id, sequence}
// (drop `message` on the wire).
let view = app.view::<Pinger>(
    predicate(Pinger::SEQUENCE.lt(100_u64))
        .project(Pinger::PROP_ID | Pinger::PROP_SEQUENCE)
        .build(),
)?;

let _sub = view.subscribe(|event| {
    // event.op is ViewOp::Create | Update | Remove as instances cross the
    // filter boundary. An instance whose update no longer matches the
    // predicate arrives as a Remove.
    match event.op {
        ViewOp::Create => println!("entered view: {:?}", event.value.id),
        ViewOp::Update => println!("updated in view: {:?}", event.value.id),
        ViewOp::Remove => println!("left view: {:?}", event.value.id),
    }
});

// A View also exposes a Container of just the matching instances:
println!("{} matching", view.container().len());
```

Predicate building blocks come from the per-field `Attr` constants
(`Pinger::SEQUENCE`, etc.): `.eq`, `.neq`, `.lt`, `.le`, `.gt`, `.ge`,
`.is_in(vec)`, `.not_in(vec)`, `.is_null()`, `.not_null()`. Projection masks
are `PROP_*` constants combined with `|`. Use `project_only(mask)` if you want
projection with no predicate.

**Capability check.** Filtered subscriptions require broker support.
`app.view::<T>(...)` returns `ViewError::Unsupported` if the broker didn't
advertise the capability. You can probe ahead of time:

```rust
let supported = app
    .transceiver()
    .peer_capabilities()
    .and_then(|c| c.filtered_subscriptions)
    .unwrap_or(false);
```

---

## Running the event loop

`App` constructors already drive the initial handshake and cache preload.
Ongoing network traffic requires the normal I/O loop. `App` owns a driver
that you consume with one of:

```rust
app.run().await?;              // runs until exit() is called or the connection closes
app.run_until_signal().await?; // same, plus a Ctrl-C handler that calls exit()
```

Both take `self` — after calling, the `App` is consumed. Set up all your
subscriptions, containers, and spawned publisher tasks *before* calling
`run`. To stop programmatically from elsewhere, call `app.exit()` (or
`client.exit()` on a cloned handle). `client.closed().await` resolves when the
connection goes down — useful as a `select!` arm in a publisher loop:

```rust
let client = app.client();
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = interval.tick() => client.publish(&dots!(Pinger { id: 0_u32 })),
            _ = client.closed() => break,
        }
    }
});
```

---

## Running a broker (`dotsd`)

Clients need a broker to talk to. `dotsd` is the daemon:

```bash
# Default: TCP on 127.0.0.1:11235
cargo run -p dotsd

# Unix domain socket
cargo run -p dotsd -- uds:///tmp/dotsd.sock

# Multiple endpoints on one daemon
cargo run -p dotsd -- tcp://0.0.0.0:11235 uds:///tmp/dotsd.sock
```

`dotsd` accepts `tcp://addr[:port]` and `uds:///path` URIs; an omitted TCP
port defaults to `11235`, so `tcp://0.0.0.0` binds `0.0.0.0:11235` (same rule
as dots-cpp). It maintains the
per-type caches, replays them to new subscribers, routes publications, and
cleans up a disconnected client's `[cleanup]` instances. It also publishes
its own observability types (`DotsClient`, `DotsClientStatistics`) that you
can subscribe to like any other type.

There is no separate config file for the common case — endpoints on the
command line are all most deployments need.

---

## Dynamic / wire-only types

Most code uses statically-derived types. But DOTS can also work with types it
has *no* compiled-in knowledge of, because peers exchange type descriptors at
handshake time. This powers tools like tracers and bridges.

- `app.subscribe_new_struct_type(handler)` — be notified of each new type
  descriptor the broker announces.
- `app.subscribe_dynamic(descriptor, handler)` — subscribe to a type
  described only at runtime; events carry a `DynamicStruct` you inspect via
  its descriptor.
- `app.subscribe_all_types(handler)` — fire a single handler for *every* type
  on the bus. This is exactly how `dots-trace` prints all traffic.

If you're building an application, you almost certainly want the static
[`subscribe`](#subscribing) instead. Reach for these only for generic
infrastructure.

---

## Pitfalls and gotchas

- **Every field is `Option`.** Forgetting the `Option` wrapper is a compile
  error from the derive macro. `None` = absent on the wire.
- **Hold your `SubscriptionHandle`.** Dropping it unsubscribes. If you don't
  want to keep the binding, call `.discard()`. A dropped `_` binding
  (`let _ = app.subscribe(...)`) unsubscribes *immediately* — use `let _sub =`
  or `.discard()`.
- **Subscribe before you `run`.** `run`/`run_until_signal` consume the `App`;
  set everything up first.
- **Don't hold container borrows across `.await`.** `get`/`for_each` pin a
  read lock that blocks dispatch for that type. Clone out what you need and
  drop the borrow; use `snapshot()` for deferred processing.
- **Keys define identity.** Re-publishing with an existing key updates that
  instance — it is not a new message. A type with no `key` field is a
  singleton.
- **Keep handlers fast.** Subscription callbacks run on the dispatch path. Do
  heavy work elsewhere (e.g. send to a channel, or read from a `Container`
  off-path).
- **State vs events.** `.dots` schema types are cached by default. Use
  `[cached=false]` for event-only types that should not retain/replay state.
  Handwritten Rust derives currently require `#[dots(cached)]` to enable
  caching; omitting it produces an uncached type. The schema generator
  handles this difference automatically.
- **Stable tags.** Field tags are the wire contract. Don't renumber them
  across versions if you care about compatibility.

---

## API cheat sheet

```rust
// ---- define ----
#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "T", cached)]                // cached | persistent | internal | substruct_only
struct T { #[dots(tag = 1, key)] id: Option<u32>, /* ... */ }

#[derive(DotsEnum, Default, Debug, Clone, Copy)]
#[dots(name = "E")]
enum E { #[default] #[dots(tag = 1)] A, #[dots(tag = 2, value = 9)] B }

let v = dots!(T { id: 1_u32, /* bare values auto-Some + Into */ });

// ---- connect ----
let app = App::new("name").await?;                          // DOTS_ENDPOINT or tcp :11235
let app = App::connect_tcp("host:port", "name").await?;
let app = App::connect_unix("/path.sock", "name").await?;
// ..._with_auth(.., secret) variants exist

// ---- publish ----
app.publish(&v);                                            // create/update
app.publish_with_mask(&v, T::PROP_ID | T::PROP_X);          // partial update
app.remove(&dots!(T { id: 1_u32 }));                        // delete (key only)

// ---- subscribe ----
let sub = app.subscribe::<T>(|e| { e.value; e.header; });   // callback; hold or .discard()
let stream = app.subscribe_stream::<T>();                   // pull-style

// ---- local cache ----
let c = app.container::<T>();
c.len(); c.get(&dots!(T { id: 1_u32 })); c.for_each(|k, v, info| {}); c.snapshot();

// ---- filtered view ----
let view = app.view::<T>(predicate(T::X.lt(100)).project(T::PROP_ID).build())?;
let sub = view.subscribe(|e| { e.op; e.value; });

// ---- run ----
let client = app.client();   // cloneable handle for tasks/handlers
app.run_until_signal().await?;
```

---

*This guide covers the client (guest) API. For embedding a broker in-process,
building custom transports, or the descriptor-driven codec internals, see the
crate-level docs in `dots-transport` (`HostTransceiver`, `GuestTransceiver`,
`Connection`) and `dots-core`.*
