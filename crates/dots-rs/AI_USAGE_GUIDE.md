# Using dots-rust: guidance for AI agents

Use this guide when implementing or reviewing a DOTS client, model, or test.
It describes the source revision that contains it. Before editing a client,
check `Cargo.lock`, path/git patches, enabled features, and the resolved crate
source. Development documentation may describe APIs not yet published, even
when the checkout and registry crate have the same version number. Treat the
resolved signatures and tests as authoritative when older examples disagree.

## Dependencies and model ownership

Applications normally depend on the umbrella crate `dots-rs` and import it as
`dots_rs`. It re-exports the public runtime, model, derive, and transport APIs.
Do not copy obsolete `dots_core` or `dots_transport` imports from old examples.
Use explicit `Client`/`GuestTransceiver` handles in reusable components where
possible. The optional `testing` feature exposes `dots_rs::testing`.

Reuse the application's existing model crate and `.dots` schemas. Do not
invent a second definition with the same wire type name. For generated models,
add `dots-rs-build` as a build dependency, call
`dots_rs_build::compile(&["proto/model.dots"])` from `build.rs`, and include
`concat!(env!("OUT_DIR"), "/dots_generated.rs")` from the model module. Edit
the schema, not generated files in `target/`.

For handwritten models, use `DotsStruct` / `DotsEnum` and stable wire names,
property tags, and enum values. Keys define instance identity; a type without
keys represents a singleton. Inspect the actual generated Rust field types:
keys can be bare values, while optional properties use `Option<T>`. Do not
assume that every field is an `Option`. `None` represents an absent property,
not necessarily an instruction to clear an existing cached property.

In `.dots` schemas, types are **cached by default**: no explicit `cached`
annotation is needed for state types. Set `[cached=false]` only for types
with event/message semantics that should not retain state or replay it to
late subscribers. For example:

```text
struct DeviceState { 1: [key] string name; 2: bool active; }
struct ButtonPressed [cached=false] { 1: string name; }
```

The current handwritten Rust derive has a different default: use
`#[dots(cached)]` for cached state and omit `cached` for event-only types.
Do not write `#[dots(cached=false)]`; that syntax belongs to `.dots` schemas.
The schema generator supplies the appropriate Rust attributes automatically.
Do not use event-only types as durable state. Substruct-only types describe
nested values and cannot be published or subscribed to as top-level messages.
Schema changes must preserve compatibility with other Rust and C++ peers;
renumbering tags or changing keys is a protocol change.

## Prefer `dots!` for constructing client values

Import `dots_rs::dots` and use `dots!(Type { ... })` for publications,
key-only removals, and lookup queries. It expresses only the properties the
client intends to supply, avoiding repeated `Some(...)`, string conversions,
and explicit `None` assignments for unrelated fields.

```rust
use dots_rs::dots;

mod model {
    use dots_rs::DotsStruct;

    #[derive(DotsStruct, Debug, Clone)]
    #[dots(name = "AgentGuideReading", cached)]
    pub struct Reading {
        #[dots(tag = 1, key)]
        pub id: u32,
        #[dots(tag = 2)]
        pub label: Option<String>,
        #[dots(tag = 3)]
        pub temperature: Option<f64>,
    }
}
use model::*;

let reading = dots!(Reading { id: 7_u32, label: "outside" });
assert_eq!(reading.id, 7);
assert_eq!(reading.label.as_deref(), Some("outside"));
assert_eq!(reading.temperature, None);

// A key-only query or removal value: all optional properties are absent.
let key = dots!(Reading { id: 7_u32 });
assert!(key.label.is_none());
assert!(key.temperature.is_none());
```

For optional fields, bare values are converted via `Into` and wrapped in
`Some`; an `Option` expression preserves `None` or converts its `Some` value.
Bare key fields receive the converted value without an `Option` wrapper.
Nested DOTS struct literals are supported. Use the model's constructor macro
imports alongside its types; the generator/derive supplies the per-type
companion macros used by `dots!`.

In the normal form without `..base`, **every unspecified optional field is
explicitly initialized to `None`**, without requiring or consulting `Default`.
Required bare key fields must be supplied; omitting them is a compile error,
as is supplying an unknown field. This protects instance identity while
keeping partial-state construction concise.

This also improves compatibility when a model adds optional properties. After
regenerating/updating the model, existing `dots!(Reading { id, ... })` call
sites can stay unchanged: newly added optional fields automatically remain
`None`. An exhaustive Rust struct literal would need another field assignment.
Absent fields are normally omitted from the publication, so the old client
does not inadvertently write a default value into a property it does not
understand. This supports additive model evolution; it does not make changes
to keys, existing property types/tags, or application semantics compatible.

`None` means absent, not “delete the cached value.” Normal partial publishing
leaves omitted cached properties unchanged. To clear a property explicitly,
use `publish_with_mask()` with that property addressed and absent from the
value. Avoid an explicit `..base` when the intent is a fresh partial value:
that form inherits omitted fields from the base instead of setting them to
`None`. Use ordinary Rust construction when intentional defaults or copying
an existing value are the actual requirement.

## Connect, retain handles, and run the driver

`App::new(name)` resolves `DOTS_ENDPOINT` or the default endpoint. Explicit
TCP and Unix-socket constructors are available. Obtain `app.client()` for
callbacks/tasks, install subscriptions, and then run `app.run()` or
`app.run_until_signal()`. The running driver is required for ongoing network
traffic. Connection/preload can populate caches before `run`, but does not
replace the driver loop.

`App` installs a process-wide global transceiver. Do not construct concurrent
`App` instances in one process. Lower-level code needing independent clients
can use `Connection` plus `GuestTransceiver::from_connection` and explicitly
run each returned driver. The global API is `dots_rs::global`; it requires a
live installed application or test harness.

```no_run
use dots_rs::{dots, App, DotsStruct};

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "AgentGuideStatus", cached)]
struct Status {
    #[dots(tag = 1, key)]
    id: u32,
    #[dots(tag = 2)]
    message: Option<String>,
}

async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    let app = App::new("status-client").await?;
    let client = app.client();
    let _subscription = app.subscribe::<Status>(|event| {
        if event.is_remove() {
            println!("removed {}", event.updated().id);
        } else {
            println!("{}: {:?}", event.updated().id, event.updated().message);
        }
    });

    client.publish(&dots!(Status { id: 1_u32, message: "ready" }));
    // Addressing an absent property explicitly clears it; keys are included.
    client.publish_with_mask(&dots!(Status { id: 1_u32 }), Status::PROP_MESSAGE);
    app.run_until_signal().await?;
    Ok(())
}
```

Keep subscription handles alive. `let _sub = subscribe(...)` retains a handle;
`let _ = subscribe(...)` drops it immediately. Use `.discard()` only when a
callback is intentionally installed for the rest of the connection. For an
async worker, use `subscribe_stream()` or `subscribe_tasked()` and establish
how the worker stops; dropping a Tokio task handle does not cancel its task.
Use `client.exit()` to request driver shutdown and `client.closed().await`
when coordinating worker lifetimes.

## Early subscription: inspect startup state before reacting

Normal `App` connections use preload. A successful `App::new(...)`,
`App::connect_tcp(...)`, or `App::connect_unix(...)` returns after early
subscription and broker cache replay complete for the statically registered
unfiltered types. The application can immediately read their containers,
before installing handlers or entering `app.run()`. This supports startup
checks, initial rendering, and reconciliation based on the existing system
state instead of waiting for the next live update.

This works through **link-time registration**, not the order in which the
application executes its calls:

1. Reachable typed `publish`, `subscribe`, `container::<T>()`, and `view::<T>()`
   calls anchor descriptor entries in lists collected into the final binary.
   A `container::<T>()` call after connection is enough to mark `T` as an
   unfiltered subscribed type at link time; no dummy runtime subscription
   before connecting is needed.
2. During connection, the client exchanges descriptors, including their
   nested type dependencies, and sends group joins for unfiltered subscribed
   types. Publish-only and filtered-only registrations do not request a full
   unfiltered cache preload.
3. The driver creates the local containers before finishing preload. It
   processes the broker's replay into those containers and waits for the
   preload handshake to reach `Connected` before returning the `App`.
4. The application reads that state and initializes itself. Subsequent event
   subscriptions replay the current local state to each new handler. The
   regular driver loop then continues processing live traffic.

### Enable LTO in the application workspace

On supported ELF targets (the current implementation selects Linux, Android,
and FreeBSD), link-time optimization lets dead-code elimination remove unused
registration entries across crate boundaries. Enable it in the **application
workspace root** `Cargo.toml`, not merely in a model/dependency crate:

```toml
[profile.release]
lto = true
```

Build or run that profile with `cargo build --release` or `cargo run --release`.
A plain `cargo run` uses the development profile, so the release setting does
not apply. The dots-rust workspace already enables release LTO; downstream
applications must configure their own profiles.

LTO is important for accurate, minimal early-subscription sets, especially
with a large shared model crate. Without it, extra type registrations may
survive, leading to unnecessary descriptor exchange, group joins, cache
traffic, memory use, and startup work. **LTO is not required for preload
correctness**: the non-LTO behavior is conservative, retaining more entries.
On other targets the current derive uses retained `linkme` entries, so enabling
LTO alone does not provide the same per-use pruning. Registration describes
link-time reachability, not which runtime branch a particular invocation takes.

### Startup inspection followed by reactive handling

```no_run
use dots_rs::{App, DotsStruct};

// Handwritten Rust requires `cached`; the equivalent .dots schema is
// cached by default. With no key fields, this is a singleton.
#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "AgentGuideSystemState", cached)]
struct SystemState {
    #[dots(tag = 1)]
    ready: Option<bool>,
}

async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let app = App::new("startup-observer").await?;

    // This typed access anchors early subscription in the binary even
    // though execution reaches it after the connection has completed.
    let states = app.container::<SystemState>();
    let initial_ready = states
        .get_ref(&SystemState::default())
        .and_then(|state| state.ready);
    // The read lock is already released here.
    match initial_ready {
        Some(true) => println!("System was ready at connection time"),
        Some(false) => println!("System was not ready at connection time"),
        None => println!("No readiness value was cached"),
    }

    // Initialize application state before subscribing: the callback may
    // run synchronously here for the already-cached singleton.
    let _subscription = app.subscribe::<SystemState>(|event| {
        if event.is_remove() {
            println!("System state removed");
        } else {
            println!("Observed readiness: {:?}", event.updated().ready);
        }
    });
    app.run_until_signal().await?;
    Ok(())
}
```

No polling loop or sleep is needed merely to wait for the initial preload of
these cached types. An empty container after completed preload means there
was no cached instance to replay, not that every subsystem is healthy or
ready. Readiness and required-instance checks remain application policy.
Cached state can also become stale; it is an observation, not a transaction
across all system components. Keep startup work short and start the normal
driver to process subsequent traffic.

Do not process the initial read and later replay as two independent commands.
Use idempotent state reconciliation or explicitly distinguish replay with
`event.is_from_cache()`. Dropping a cached subscription and resubscribing is
not a request to reload all state from the broker.

The immediate-inspection guarantee applies to the preload path and the
statically registered unfiltered types. Runtime-only types first joined via
`subscribe_dynamic()` later, filtered views opened after connection, and
connections explicitly configured with `preload = false` require their own
traffic to be processed. For manually constructed transceivers, provide the
appropriate type lists and complete `GuestDriver::early_subscribe()` on a
preload-enabled connection before relying on startup cache contents.

## Publishing and partial updates

`publish(&value)` queues a publication; return does not mean that the broker
accepted it, a subscriber processed it, or the local container has updated.
If the operation needs confirmation, await the appropriate event or domain
acknowledgment while the driver runs. Do not substitute arbitrary sleeps.

The key selects the instance. For cached state, transmitted valid properties
merge into the existing instance; omitted properties remain unchanged.
`publish_with_mask()` explicitly addresses properties and automatically
includes keys. Addressing an absent property clears that property. `remove()`
removes the instance identified by its key; it is distinct from clearing a
property. Never treat a partial transmission as a complete replacement value
unless the application protocol explicitly requires that behavior.

### Publish only the properties you changed

In most cases, publish **the complete key plus only the changed properties**.
Build a fresh partial value with `dots!`; ordinary `publish()` is enough when
all supplied optional properties are intended updates. Use
`publish_with_mask()` when restricting a populated value to selected fields
or explicitly invalidating a property. Sending fewer fields reduces wire and
merge work and avoids overwriting values owned by other clients.

One cached instance can combine state from several clients. They use the
same type and key, but each publishes only its own properties. For example,
a sensor client can maintain temperature while an administration client
maintains a label:

```no_run
use dots_rs::{dots, Client};

mod model {
    use dots_rs::DotsStruct;

    #[derive(DotsStruct, Debug, Clone)]
    #[dots(name = "AgentGuideSharedDevice", cached)]
    pub struct Device {
        #[dots(tag = 1, key)]
        pub id: u32,
        #[dots(tag = 2)]
        pub temperature: Option<f64>,
        #[dots(tag = 3)]
        pub label: Option<String>,
    }
}
use model::*;

fn sensor_update(client: &Client) {
    client.publish(&dots!(Device { id: 42_u32, temperature: 22.0_f64 }));
}

fn administrator_update(client: &Client) {
    client.publish(&dots!(Device { id: 42_u32, label: "outside" }));
}
```

The first publication can create the keyed instance. After both are processed,
its merged state contains `temperature = Some(22.0)` and
`label = Some("outside")`, regardless of their relative arrival order,
provided no intervening removal or conflicting update occurs. Neither client
needs to fetch and republish the other's property. Later temperature changes
preserve the label, and later label changes preserve the temperature. During
initial creation, readers must tolerate the other properties not having
arrived yet. Read the combined state through `event.updated()` or the container;
`event.transmitted()` contains just that publication's fields.

This pattern relies on **disjoint addressed properties**. If clients update
or invalidate the same property, the last update processed by the broker wins;
DOTS does not automatically reconcile competing intentions. A full-object
publication or a broad clear mask can therefore destroy the separation.
Likewise, deletion removes the whole instance, including other clients'
properties, and separate publications are not an atomic multi-client change.
Coordinate these actions at the application level when necessary. Treat a
nested struct/list property as a shared property for ownership purposes; do
not assume independent nested edits have disjoint top-level update masks.

Avoid cloning a cached instance, changing one field, and publishing the full
clone. That republishes stale values for unrelated fields and can undo another
client's work. Full publications are appropriate when a client deliberately
owns and replaces all addressed properties; partial updates should be the
normal choice.

## PropertySet: check fields and shape publish masks

`PropertySet` is a small, copyable set of property tags, imported from
`dots_rs::PropertySet`. It stores field membership, not field values. Prefer
generated `Type::PROP_FIELD` constants to numeric tags: each constant is a
one-property set, and combining them describes the fields of interest.
A set carries no type identity, so only combine constants belonging to the
intended model type.

Distinguish three uses:

- `value.valid_set()` reports fields currently present in that value.
  Optional fields with `Some(...)` are present; `None` is absent. This checks
  instance data, not whether a field is declared in the schema. Use the type
  descriptor for schema inspection.
- `Transmittable::key_set(&value)` reports the fields declared as keys,
  independently of their current validity. Always supply complete key values.
- A publish mask describes fields the update **addresses**. It may include
  absent fields to invalidate them, so it need not equal `valid_set()`.

For typed values, import `StructValue` to call `valid_set()`. Runtime-described
values expose it through `Transmittable`. If both traits are imported for a
typed value, use `StructValue::valid_set(&value)` to avoid method ambiguity.

| Expression | Meaning |
|---|---|
| `a | b` | Union: include fields from either set |
| `a & b` | Intersection: keep fields present in both sets |
| `a - b` | Difference: exclude `b` from `a` |
| `a ^ b` | Symmetric difference: fields in exactly one set |
| `required.is_subset_of(valid)` | Every required field is present |
| `valid.is_superset_of(required)` | The same all-fields check |
| `!(valid & wanted).is_empty()` | At least one wanted field is present |

Rust `^` means symmetric difference; dots-cpp's PropertySet uses `^` for
intersection. Use `&` for intersection in Rust.

```rust
use dots_rs::{dots, PropertySet, StructValue};

mod model {
    use dots_rs::DotsStruct;

    #[derive(DotsStruct, Debug, Clone)]
    #[dots(name = "AgentGuidePropertySets", cached)]
    pub struct Reading {
        #[dots(tag = 1, key)]
        pub id: u32,
        #[dots(tag = 2)]
        pub temperature: Option<f64>,
        #[dots(tag = 3)]
        pub label: Option<String>,
    }
}
use model::*;

let value = dots!(Reading { id: 42_u32, temperature: 22.0_f64 });
let valid = value.valid_set();
let wanted = Reading::PROP_TEMPERATURE | Reading::PROP_LABEL;

assert!(Reading::PROP_TEMPERATURE.is_subset_of(valid));
assert!(!wanted.is_subset_of(valid)); // label is missing
assert!(!(valid & wanted).is_empty()); // at least one is present
assert_eq!(wanted - valid, Reading::PROP_LABEL); // missing fields

// Include only currently present fields from an allowlist: no clears.
let set_only_mask = valid & wanted;
assert_eq!(set_only_mask, Reading::PROP_TEMPERATURE);

// Build a mask incrementally. Set operations return a new set.
let mut mask = PropertySet::EMPTY;
mask = mask | Reading::PROP_TEMPERATURE | Reading::PROP_LABEL;
mask = mask - Reading::PROP_LABEL;
assert_eq!(mask, set_only_mask);

fn publish_selected(client: &dots_rs::Client, value: &Reading) {
    let allowed = Reading::PROP_TEMPERATURE | Reading::PROP_LABEL;
    client.publish_with_mask(value, value.valid_set() & allowed);
    // To intentionally clear an absent label, include its bit explicitly:
    // client.publish_with_mask(value, Reading::PROP_LABEL);
}
```

`publish_with_mask()` automatically adds key properties. Intersecting an
allowlist with `valid_set()` excludes absent fields and thus suppresses clears;
passing the allowlist directly also invalidates its absent fields. Conversely,
`value.valid_set() - Type::PROP_FIELD` publishes all currently valid fields
except that field. This is useful for intentional exclusions, but can still
republish unrelated stale values; an explicit allowlist is usually safer.
Masks select properties; they do not detect which values changed since a
previous version.

For event handling, check `event.updated().valid_set()` for fields present in
the merged cached instance, and `event.transmitted().valid_set()` for fields
supplied in that publication. `event.header.attributes` is an
`Option<PropertySet>` describing the addressed fields; handle `None` explicitly
when inspecting it. An addressed field absent from the transmitted valid set
is a clear, not an unchanged field. Being addressed does not by itself prove
that the value changed.

For runtime tag handling, use `set.has(tag)`, `set.with_tag(tag)`, and
`set.without_tag(tag)`; the latter two return new sets. `set.iter()` yields
numeric tags in ascending order, while `len()` and `is_empty()` inspect the
set size. Usable tags are `1..=PropertySet::MAX_TAG` (currently 31).
`PropertySet::ALL` contains every bit, including bits outside a particular
schema; it is not that type's declared-property set. Avoid using it as a
publish mask for a sparse value. `bits()` / `from_bits()` are available for
raw mask interoperability; normal clients should prefer the named constants.

## Pitfalls: invalidating an already-valid property

DOTS distinguishes **a property not addressed by an update** from **an
addressed property whose value is invalid/unset**. Both may look like `None`
in a Rust value, but their update masks differ.

For an existing cached instance:

| Property in update mask? | Value supplied? | Effect on cached property |
|---|---|---|
| No | No | Preserve the existing value |
| No | Yes, in the local object | Preserve it; masked publishing excludes this property |
| Yes | Yes | Set/replace it with the supplied value |
| Yes | No (`None`) | Invalidate it: remove its cached value |

Plain `publish()` derives its mask from the valid properties of the value.
Consequently, setting a field to `None` and calling `publish()` does **not**
invalidate an already-valid remote field. The absent field is no longer in
that automatically derived mask. Use `publish_with_mask()` to express the
clear explicitly.

```no_run
use dots_rs::{dots, Client};

mod model {
    use dots_rs::DotsStruct;

    #[derive(DotsStruct, Debug, Clone)]
    #[dots(name = "AgentGuideSensor", cached)]
    pub struct Sensor {
        #[dots(tag = 1, key)]
        pub id: u32,
        #[dots(tag = 2)]
        pub temperature: Option<f64>,
        #[dots(tag = 3)]
        pub label: Option<String>,
    }
}
use model::*;

fn demonstrate_invalidation(client: &Client) {
    client.publish(&dots!(Sensor {
        id: 42_u32,
        temperature: 21.5_f64,
        label: "outside",
    }));

    // NOT a clear: temperature is absent and not addressed.
    // Once processed, the existing temperature remains 21.5.
    client.publish(&dots!(Sensor { id: 42_u32 }));

    // Explicitly invalidate temperature. The key is included automatically.
    // Label remains "outside"; the Sensor instance itself remains present.
    client.publish_with_mask(
        &dots!(Sensor { id: 42_u32 }),
        Sensor::PROP_TEMPERATURE,
    );

    // Set label and invalidate temperature in one update.
    client.publish_with_mask(
        &dots!(Sensor { id: 42_u32, label: "offline" }),
        Sensor::PROP_LABEL | Sensor::PROP_TEMPERATURE,
    );
}
```

These are the effects once the broker/receivers process the updates, assuming
no intervening writes. Publishing only queues data. Verify the resulting
`event.updated()` or container state after observing the relevant live update.
The transmitted value alone cannot distinguish “unchanged” from “invalidated”:
inspect `header.attributes` as well when interpreting a patch.

Common mistakes to avoid:

- Calling a generated `clear_*()` method or assigning `None` changes only the
  local Rust value. A later ordinary `publish()` still omits the field.
- Building the mask from that value's `valid_set()` drops precisely the
  invalid field you intended to address. Include its generated `PROP_*`
  constant explicitly.
- Passing a broad mask with a sparse value invalidates **every addressed but
  absent property**, not just the one intended. Use the narrowest mask that
  describes the change; avoid all-property masks for single-field updates.
- Cloning a full cached instance to change one field and republishing every
  valid field can overwrite unrelated concurrent changes with stale values.
  Prefer a fresh `dots!` partial value and a targeted mask.
- Publishing `Some(0)`, `Some(false)`, or `Some("")` sets a valid value; it
  does not invalidate the property.
- `remove()` deletes the entire keyed instance. Do not use it to clear one
  field. Supply complete keys when addressing an instance; keys identify it
  and are not optional properties to invalidate.

The same distinction makes additive model evolution safe: an older client's
ordinary partial publication does not clear fields it omitted. An explicit
mask is a statement of intent, so review mask construction together with the
payload, rather than judging the payload alone.

## Subscriptions, replay, and event semantics

A cached type's initial join requests broker state. Additional local event
subscribers receive replay from the current local cache without a new network
join. Typed and dynamic callback subscriptions replay synchronously before
returning; initialize captured state before subscribing and avoid holding a
mutex that the callback will need. For a type first joined after initial preload, a locally empty cache can
still be waiting for broker traffic. Normal `App` construction has already
completed preload for its statically registered unfiltered types; see
[early subscription](#early-subscription-inspect-startup-state-before-reacting).

Use `event.updated()` for the merged cached state. Use `event.transmitted()`
for the actual publication, which may contain only a subset of properties.
For cached removals, `updated()` contains the pre-removal instance. Inspect
`is_create()`, `is_update()`, `is_remove()`, or `operation()` rather than
inferring event kind from field presence.

Replay events are create-shaped observations of current state, not newly
issued commands. `is_from_cache()` identifies them; `header.from_cache`
counts down to `Some(0)` on the last replayed entry. An empty replay has no
entry carrying that marker, so do not wait unconditionally for it. Echo-based
confirmation should exclude replay and correlate the expected instance/change;
a cached value is not proof that a new publication was acknowledged.

Dropping a subscription removes that observer. Do not assume it also sends a
network Leave: transceiver-owned cached containers keep group membership alive.
Keep callbacks short. Schedule expensive or asynchronous work outside the
read/dispatch path. Do not promise bounded backpressure merely because an API
returns an async subscription stream.

## Read containers according to the result needed

Prefer `get_ref()` for a complete key, `find_ref()` for a borrowed predicate
match, and `with_iter()` for field extraction, counting, or custom collections.
Use `find()` / `filter()` when owned matching instances are needed. `get()`
currently remains a borrowed compatibility alias, not an owned lookup.

Never hold container borrows across `.await`. Avoid calling user handlers
under a cache read lock. A full `snapshot()` is useful for complete owned or
historical state, but wasteful for a count or one field. See the
[container access guide](https://github.com/pnxs/dots-rust/blob/master/crates/dots-rs/CONTAINER_GUIDE.md)
(bundled as `CONTAINER_GUIDE.md`, API docs: `dots_rs::container_usage`) for
examples and lock-lifetime rules. For several consistent key lookups, use one
read guard rather than several independent reads.

## Filtered views and runtime-described types

Use `view::<T>(filter)` when the broker should restrict routing to matching
instances. It requires the broker's filtered-subscription capability; handle
`ViewError` rather than silently treating it as an empty result. Retain the
view and its subscription. Interpret `ViewEvent` / `ViewOp` separately from
ordinary events: leaving the filter produces a view removal even when the
underlying instance still exists. A local `Container::filter()` selects local
cached values and does not change network routing.

Use `subscribe_dynamic(descriptor, handler)` for runtime-described types and
retain its handle. Type discovery via `subscribe_new_struct_type()` includes
synchronous catch-up for already-known descriptors. `subscribe_all_types()`
is useful for inspection tools but includes internal types and can subscribe
to a large set of types; prefer specific subscriptions in ordinary clients.

Use the registry's descriptor when constructing/comparing `DynamicStruct`
values; descriptor identity matters. Do not assume a runtime subscription
means storage is dynamic: static storage can serve dynamic subscribers, and
typed access can promote a dynamic container. Do not invent public runtime
container-access methods that the resolved library does not provide.

## Test observable behavior

Use `dots_rs::testing::TestHarness` for in-process broker/guest tests when the
`testing` feature is enabled. It drives transport and serializes access to the
global client. Await events with its receive helpers; `settle()` is only a
best-effort scheduling aid. Add a spoof guest when testing cross-client
routing. Make assertions about merged `updated()` state versus partial
`transmitted()` values intentionally, and account for subscription replay.

Choose tests for the changed behavior: create/update/remove, partial fields
and explicit clears, cached replay for late subscribers, handler lifetime,
view entry/exit, or dynamic descriptor handling. Keep existing tests that
exercise compatibility APIs. Verify against the client's actual dependency
revision rather than only a newer local checkout.

In this workspace, typical focused checks are:

```sh
cargo test -p dots-rs-transport --test container
cargo test -p dots-rs-transport --test dynamic_classification
cargo test -p dots-rs-transport --test view
cargo test -p dots-rs-testing
cargo test -p dots-rs --doc
```

Run checks appropriate to the affected code; a downstream client has its own
package and test targets.

## Discoverability in downstream projects

This guide ships as `AI_USAGE_GUIDE.md` in the `dots-rs` crate and is included
in API docs as `dots_rs::ai_usage`. Add a pointer to the client repository's
own agent instructions; a Cargo dependency does not install those instructions.
For example, put this in its `AGENTS.md` or equivalent:

```text
When working with dots-rs, read AI_USAGE_GUIDE.md from the resolved dependency
or its dots_rs::ai_usage API documentation. For container access, also read
CONTAINER_GUIDE.md / dots_rs::container_usage. Check the installed version's
signatures before using methods documented only on the development branch.
```

For older releases without these guides, use a compatible source checkout and
verify each API against the resolved dependency. Prefer the versioned API
documentation or packaged guide when available.
