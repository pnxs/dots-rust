# Container access guidelines for developers and AI agents

Choose the smallest read operation that produces the result the client needs.
Do not default to cloning a whole container and then searching the snapshot.
Check the resolved dependency source and `Cargo.lock` before using these APIs:
this guide follows the source revision that contains it, and a published crate
with the same version number may not yet contain recent additions.

## Choose an operation

| Need | API | Copies and locking |
|---|---|---|
| Instance identified by its complete key | `get_ref(&query)` | Indexed lookup; no instance clone; result holds a read lock |
| First instance matching a predicate | `find_ref(predicate)` | Linear scan, stops at first match; result holds a read lock |
| Owned first match | `find(predicate)` | Clones only the match; releases the lock before returning |
| Owned matching instances | `filter(predicate)` | Scans once, clones only matches; releases the lock before returning |
| Fields, transformed results, counts, or custom collections | `with_iter(closure)` | Borrows entries; only copies what the closure explicitly copies |
| Several indexed lookups in one consistent state | `lock()` and guard `get()` | One read lock for the batch |
| Number of entries or emptiness | `len()` / `is_empty()` | No instance copies |
| Complete owned state, including metadata, for later use | `snapshot()` | Clones every instance and its metadata |

`get()` currently remains a borrowed compatibility alias for `get_ref()`.
Prefer `get_ref()` in new client code; do not assume `get()` returns an owned
value. Only key properties participate in key lookup; non-key query fields
are ignored. Do not replace indexed lookup with a linear predicate scan when
the complete key is available. For keyless singleton types that implement
`Default`, use `get_ref(&T::default())`.

## Read startup state immediately after connecting

Normal `App` constructors finish early subscription and preload before
returning. A reachable `container::<T>()` call anchors `T` in the binary's
unfiltered subscription list, so its cached instances are available for
inspection immediately after connecting, before the normal driver loop.
Use this for initial system-state checks, then install reactive handlers and
run the app. Handlers receive synchronous local replay on subscription.

Enable `[profile.release] lto = true` in the application's workspace root to
prune unused registrations on supported ELF targets. Without LTO, preload
still works but may include unnecessary types. This does not preload runtime
subscriptions or filtered views created later. See the
[general guide's early-subscription section](https://github.com/pnxs/dots-rust/blob/master/crates/dots-rs/AI_USAGE_GUIDE.md#early-subscription-inspect-startup-state-before-reacting)
for the lifecycle, platform limitations, and a startup example.

## Borrow first, copy only what must outlive the read

```rust
use dots_rs::{Container, DotsStruct};

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "ContainerGuideItem", cached)]
struct Item {
    #[dots(tag = 1, key)]
    id: u32,
    #[dots(tag = 2)]
    name: Option<String>,
    #[dots(tag = 3)]
    active: Option<bool>,
}

fn read_items(container: &Container<Item>) {
    // Complete key: read a field without cloning the instance.
    let active = container
        .get_ref(&Item { id: 42, ..Default::default() })
        .and_then(|item| item.active);

    // Predicate: clone just the selected name and release the borrow.
    let first_name = container
        .find_ref(|item| item.active == Some(true))
        .and_then(|item| item.name.clone());

    // Standard iterator operations; no automatic instance cloning.
    let mut names = container.with_iter(|iter| {
        iter.filter(|(_, item, _)| item.active == Some(true))
            .filter_map(|(_, item, _)| item.name.clone())
            .collect::<Vec<_>>()
    });
    names.sort(); // Sort after releasing the read lock.

    let active_count = container.with_iter(|iter| {
        iter.filter(|(_, item, _)| item.active == Some(true)).count()
    });

    // When ownership of an entire matching instance is needed:
    let owned = container.find(|item| item.active == Some(true));
    let selected = container.filter(|item| item.active == Some(true));
    let _ = (active, first_name, names, active_count, owned, selected);
}
```

`Container<T>::with_iter()` yields `(key_bytes, &T, &CloneInfo)`. Borrowed
entries cannot escape the closure. Return owned values, copied scalar fields,
or explicitly cloned fields instead. `get_ref()` and `find_ref()` return a
borrow tied to the container handle; bind the handle to a variable when
retaining the result.

## Lock lifetimes and callbacks

A borrowed result or explicit read guard blocks updates to that container
until dropped. Never retain it across `.await`. Keep predicates and
`with_iter()` closures short; avoid re-entering the same container. Collect
needed output before doing I/O, expensive processing, or acquiring unrelated
locks. Several independent reads do not form a single consistent snapshot;
use one guard when consistency across lookups matters.

Do not invoke subscription handlers under a container read lock. For replay,
construct owned events inside `with_iter()`, release the lock, and then call
handlers. An owned replay event may legitimately require cloning an instance.

## Dynamic and static storage

Given a `DynContainer` handle, `with_iter()` yields
`(key_bytes, &ContainerValue, &CloneInfo)`. It works with both static and
dynamic storage without converting either. Inspect `as_typed::<T>()` or
`as_dynamic()` on the value as appropriate. A runtime subscriber can be backed
by static storage, including a container promoted from dynamic storage; do
not assume every value is a `DynamicStruct`.

The typed search methods belong to `Container<T>`; do not invent a dynamic
`get_ref()` or `find_ref()`. A typed handle exposes its backing container via
`as_dyn()`. Runtime-descriptor pool creation is currently internal to the
transceiver; `subscribe_dynamic()` returns a subscription handle, not a
public dynamic-container accessor.

## Keep snapshots when their semantics matter

A full snapshot is appropriate for retaining historical/startup state or
processing all owned values after releasing the lock. Replacing it with
`with_iter()` followed by cloning every entry still copies every entry.
Replacing a retained snapshot with later live lookups changes semantics.
A client repeatedly searching historical state can build an owned keyed map
once rather than repeatedly scanning it.

When reviewing a migration, check key completeness, clone requirements,
metadata use, callback re-entry, and when locks are released. Preserve tests
that specifically exercise `snapshot()` or compatibility APIs.

## Making this guide discoverable in client repositories

This file ships inside the `dots-rs` crate and is included in its generated
API documentation as `dots_rs::container_usage`. Repository and crate READMEs
also link here. For a release that includes the guide, prefer that release's
API documentation or packaged source over a moving development branch.

Depending on `dots-rs` does not itself install agent instructions in the
client repository. Add a pointer to the client's own `AGENTS.md` (or the
instruction file used by its agent), for example:

```text
When changing dots-rs container access, read CONTAINER_GUIDE.md from the
resolved dots-rs dependency, or its dots_rs::container_usage API docs.
Check Cargo.lock and the installed API before using newer methods.
Prefer key lookup for known keys and borrowed iteration for projections;
use full snapshots only when complete owned state is needed.
```

For Codex, instruction discovery follows the working repository's directory
hierarchy, rather than recursively loading dependency instructions. See
[the official AGENTS.md guidance](https://developers.openai.com/codex/guides/agents-md).
