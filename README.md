# dots-rust

A Rust 2024 implementation of [DOTS](https://github.com/dotsorg/dots-cpp), a type-oriented stateful pub/sub IPC system.

## Status

Early. This iteration implements the type-system foundation:

- `dots-core` — `PropertySet`, descriptors, `StructValue` trait, the global `dots!` literal macro
- `dots-derive` — `#[derive(DotsStruct)]` proc-macro
- `dots-example` — minimal demo

Codec, transport, container, and dispatcher follow in later iterations.

## Design notes

- Single-threaded by design (`!Send` futures, no `unsafe` anywhere — `unsafe_code = "forbid"` at workspace level).
- Partial objects represented as `Option<T>` per field — same idiom as `prost`. `PropertySet` is a *derived view*, not stored state, eliminating double-bookkeeping.
- Construction via the global `dots!` macro for terseness, or via builder methods (`.with_foo()`) for incremental construction.
- Reads via accessor methods that return `Option<&T>`.

## Quick taste

```rust
use dots_core::{StructValue, dots};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug)]
#[dots(name = "RoundtripData", cached)]
struct RoundtripData {
    #[dots(tag = 1, key)] id: Option<u32>,
    #[dots(tag = 2)]      payload: Option<String>,
}

fn main() {
    let s = dots!(RoundtripData { id: 42, payload: "hello" });
    assert_eq!(s.id(), Some(&42));
    assert_eq!(s.payload().map(String::as_str), Some("hello"));
}
```
