# Changelog

Notable changes across the workspace, grouped by the versions in the root
`Cargo.toml`. Historical entries are reconstructed from Git history, using
version-bump commits as boundaries; dates indicate those commits, not verified
publication dates. Version 0.1.0 covers the initial implementation through the
crates.io workspace restructuring.

## 0.2.0 — 2026-10-29

### Added

- `InstanceRef<T>::materialize_key()` to decode a reference's canonical key
  directly into `T`, leaving non-key properties absent. Uses the static type
  descriptor without a dynamic-value round trip, cache lookup, or `T: Default`
  requirement; rejects keys with incompatible counts or field types.
- Generate `Display` and `FromStr` implementations for DOTS enums. Display
  emits the exact descriptor variant name, including with `-Zfmt-debug=none`.
  Parsing accepts exact names or known decimal wire tags, ignores surrounding
  whitespace, and returns `ParseValueError` for unknown names or tags. Text
  names are compatible with the existing descriptor-directed enum parser;
  numeric inputs refer to wire tags, not enum metadata values.
- `dots_rs::Uuid` with standard lowercase hyphenated `Display`, compatible
  `FromStr`, byte-array conversions, and support for keys, vectors, and typed
  filter predicates. Parsing accepts uppercase or lowercase hexadecimal text
  and retains the existing compact forms, including the optional `0x` prefix.

### Changed

- Generated `uuid` fields now use `dots_rs::Uuid` instead of `[u8; 16]`.
  Convert existing arrays with `.into()`
  or `Uuid::from_bytes`, and retrieve bytes with `as_bytes` or `into_bytes`.
  The 16-byte CBOR wire encoding and handwritten array fields remain supported.
- Dynamic UUID display and `uuid_to_string` now emit
  `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` instead of `0x`-prefixed hexadecimal.
  UUID text formatting remains available with `-Zfmt-debug=none`.

### Fixed

- Preserve quoted and escaped string keys in `InstanceRef` display when built
  with `-Zfmt-debug=none`, including dynamic reference display.

## 0.1.7 - 2026-09-25

### Added

- `Container::get_ref` for explicit borrowed key lookup, retaining `get` as a
  compatibility alias; `find_ref`, `find`, and `filter` for predicate searches
  that borrow matches or clone only matching values.
- `Container::with_iter` and `DynContainer::with_iter` for borrowed iteration,
  projections, and counting under one read lock. Export `ContainerIter` and
  `DynContainerIter` from the transport crate.
- AI client usage and container access guides, repository agent guidance, and
  expanded user documentation covering startup preload, application LTO,
  partial updates, property masks, replay, and borrowed access.

### Changed

- Reduce unnecessary cache snapshots and clones in subscription replay, view
  handling, and smarthome examples by using borrowed container access and shared
  replay builders.

### Fixed

- Dynamic subscriptions replay existing local cached state as create events
  before live delivery, including containers promoted to typed storage.
- Prevent missed or duplicate updates when subscriptions register concurrently
  with dispatch, making registration and replay snapshots atomic with cache
  updates for typed and dynamic callbacks, streams, and filtered views.
- Include `server_sent_time` in replayed view events.

## 0.1.6 — 2026-09-08

### Added

- Descriptor-directed text parsing and formatting for dynamic values, including
  integers, strings, UUIDs, property sets, timepoints, durations, and vectors.
  Add `FromStr` for `Timepoint`, `Duration`, and `PropertySet`, plus UUID helpers.
- Generic `InstanceRef` and typed `InstanceRef<T>` identity references, with wire
  encoding, decoding, comparison, hashing, diagnostic display, and key
  materialization. Support `instance_ref` and `instance_ref<TypeName>` in `.dots`
  schemas, generated Rust types, and descriptor exchange.
- Semantic validation of `.dots` reference targets and key field kinds, with
  matching derive-time checks and canonical-key conformance tests.

### Changed

- Canonical instance keys use deterministic CBOR arrays with key properties
  ordered by tag. Reordering declarations no longer changes instance identity;
  generated constructor arguments still follow declaration order.
- Key declarations require kinds with byte-stable equality: booleans, integers,
  strings, UUIDs, enums, or instance references. These checks apply to local
  declarations; received descriptors remain accepted without key-kind policing.
- Timepoint and duration display uses ISO-8601 and suffixed duration text instead
  of raw floating-point seconds, with a raw-seconds fallback for timepoints
  outside the supported calendar range.
- Update the procedural-macro dependency `syn` to version 3.

### Fixed

- Reference encoding follows the property's schema for generic versus typed wire
  forms. Validate reference keys and bound nested-key diagnostic rendering.
- Keep canonical-key encoding free of temporary sorting allocations.
- Reject invalid calendar dates and out-of-range finite `float32` text inputs;
  improve negative fractional timepoint round trips and extreme-value handling.
- Validate key types by their actual kind so enums named like temporal types are
  not incorrectly rejected.

### Removed

- Move `dots-inspect`, `dots-trace`, and `dots-tui` out of this workspace into the
  `dots-utils-rs` repository, removing their workspace dependencies.

## 0.1.5 — 2026-08-16

### Added

- `StaticTypePolicy::{FundamentalOnly, InternalOnly, All}` and
  `Registry::with_static_types` to choose which compiled-in types populate the
  local codec registry. Applications and hosts default to `All`; `dotsd` uses
  `InternalOnly` and learns user types from guests.
- Default TCP port `11235` when parsing endpoints with an absent or empty port,
  including bracketed IPv6 addresses. Export `DEFAULT_TCP_PORT` and report
  malformed authorities, missing hosts, invalid ports, and unsupported userinfo.

### Changed

- Replace `registry_with_internal_types()` with
  `Registry::with_static_types(StaticTypePolicy::InternalOnly)`.
- Track filtered subscriptions separately from unfiltered subscriptions;
  `from_connection` now takes a filtered-type list, and `dots-inspect` reports
  that category separately.
- Reduce broker fan-out allocations, payload/header clones, and repeated lookups;
  use hash-based registry storage while retaining deterministic iteration.

### Fixed

- Preserve descriptor identity when registering the same static type again and
  notify new-type observers only once per type name, preventing duplicate events.
- Isolate view containers from unfiltered traffic so create/update classification
  and leave-view snapshots stay correct. View-only subscriptions no longer cause
  an unfiltered cache preload or fan-out.
- Avoid stale filtered subscriptions during disconnect races and unbounded
  membership tracking for uncached types.
- Redact URI credentials from endpoint errors, including debug output.

## 0.1.4 — 2026-07-12

### Added

- Replay existing local cache contents as create events when callback, stream,
  or tasked subscriptions are installed, before live delivery. Expose
  `Event::is_from_cache` and a countdown in `header.from_cache`.
- `dots-inspect` CLI to list published/subscribed types from ELF binaries without
  running them; `--detail` renders descriptors and dependencies as `.dots` IDL.

### Changed

- Give descriptors explicit C-compatible layouts and field kinds stable,
  append-only discriminants for binary inspection.
- Allow dead-code elimination to discard unused type-registration entries on ELF
  targets; non-ELF targets retain conservative registration.
- Make testing helpers skip cache replay when checking live publishes/removes.

## 0.1.3 — 2026-07-09

### Added

- Matching value operations for static/type-erased structs and `DynamicStruct`:
  equality, masked comparison, property differences, assignment, recursive merge,
  swap, clear, and required-property checks with `MissingProperties` errors.
- `DotsFieldOps` for field equality, comparison, and merge; `PropertySet::ALL`,
  symmetric difference, subset checks, and descriptor property/key masks.
- `PartialEq` for `AnyStruct` and shared total-order comparison semantics for
  dynamic structs.

### Changed

- Rename the former update-overlay `merge_from`/`merge_take` operations to
  `copy_from`/`copy_take`. The new `merge_from` recursively merges without clearing
  absent fields. Mutating value operations preserve key properties.
- Derived structs now require `Clone`. Float comparisons use `total_cmp`, and
  enum ordering uses the declared integer value.

## 0.1.2 — 2026-07-05

### Added

- Container-backed create/update/remove classification for dynamic subscriptions,
  including types learned from the wire. Dynamic cache entries can be promoted
  in place when accessed through typed containers.
- Public `ContainerDescriptor`, `ContainerValue`, `DynContainer`, and
  `DynContainerEntry` exports.
- Transport `ascii` rendering with `StructDisplay` and `ColorSchema`, including
  registry-aware expansion of `any` fields.

### Changed

- Move `StructDisplay` from the model registry into the transport `ascii` module.
- Improve `dots-trace` with local RFC 3339 timestamps, classified operations,
  terminal colors honoring `NO_COLOR`, and filtering of noisy internal types.

## 0.1.1 — 2026-07-05

### Changed

- Centralize the version in `[workspace.package]` and inherit it across crates.
- Enable `tracing-init` and `testing` by default in the umbrella crate.

### Fixed

- Encode enums using unsigned enumerator tags rather than integer values for
  C++ wire compatibility. Preserve explicit `.dots` enum values, default metadata
  values to `tag - 1`, and reject unknown wire tags.
- Import received struct and enum descriptors before dispatch, independently of
  subscriptions, while preserving existing registry entries and their identity.
- Honor unsubscribes during dispatch. Dynamic callbacks use available cache
  outcomes for classification and merged values, suppressing absent-key removes.
- Expand C++ parity coverage for wire bytes, temporal values, authentication,
  filtering, descriptors, endpoints, and dispatch behavior.

## 0.1.0

### Added

- Initial Rust 2024 implementation of DOTS with C++-compatible v2 framing, CBOR
  codecs, static and dynamic descriptors, and runtime descriptor exchange.
- `DotsStruct`/`DotsEnum` derives, the `dots!` construction macro, and a native
  `.dots` parser/code generator. Support nested structs, enums, vectors, `any`,
  UUIDs, property sets, timepoints, and durations.
- Async TCP and Unix-domain socket connections, guest/host transceivers, the
  `App` API, process-wide helpers, and client-side challenge-response authentication.
- Typed and dynamic publish/subscribe APIs, partial updates and field
  invalidation, async tasked handlers, typed/type-erased cache containers, and
  create/update/remove events.
- Broker routing, cache replay, disconnect cleanup, descriptor fan-out, client
  lifecycle records, and echo/cache-management handlers in `dotsd`.
- Filtered subscriptions with a predicate builder and `View<T>`, including
  projected cache replay and enter/update/leave events.
- Link-time type registration, compile-time restrictions on publishing
  substruct-only types, and constructors with required bare key fields.
- In-process host/guest testing helpers, optional tracing and statistics,
  `dots-trace` and `dots-tui` utilities, examples, and a user guide.
- Workspace packaging under the `dots-rs` crate family, targeting Rust 1.85 or
  later under the LGPL-3.0-only license.
