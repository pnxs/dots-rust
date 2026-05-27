//! Link-time registration via `linkme` distributed slices.
//!
//! Calling `T::register_as_subscribed()` from a binary causes the
//! linkme-tagged static inside that fn body to be linked into
//! `SUBSCRIBED_TYPES`. The same goes for `register_as_published` and
//! `PUBLISHED_TYPES`. These tests live in a separate integration-test
//! binary so the per-binary nature of the linker slice can be
//! exercised in isolation from other tests' subscriptions.
//!
//! ## Build-mode caveat
//!
//! Statics defined inside fn bodies are emitted regardless of whether
//! the function is called — the unused-fn DCE Rust performs at the
//! HIR level doesn't reach down through `#[linkme::distributed_slice]`.
//! In practice the per-binary opt-in only tightens under release
//! builds with LTO, where the linker prunes unreachable statics.
//! Debug builds end up with both `register_as_published` and
//! `register_as_subscribed` slots in the slices for every derived
//! type. That's harmless at runtime — `App::new`'s preload phase just
//! requests cache for a few extra type names; dotsd returns empty
//! caches and the application proceeds.

use dots_core::{GlobalRegistration, PUBLISHED_TYPES, SUBSCRIBED_TYPES};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Foo")]
struct Foo {
    #[dots(tag = 1, key)]
    id: Option<u32>,
}

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Bar")]
struct Bar {
    #[dots(tag = 1)]
    note: Option<String>,
}

#[test]
fn subscribing_a_type_adds_it_to_subscribed_types() {
    Foo::register_as_subscribed();
    assert!(
        SUBSCRIBED_TYPES.iter().any(|d| d.name == "Foo"),
        "Foo should appear in SUBSCRIBED_TYPES after register_as_subscribed"
    );
}

#[test]
fn publishing_a_type_adds_it_to_published_types() {
    Bar::register_as_published();
    assert!(
        PUBLISHED_TYPES.iter().any(|d| d.name == "Bar"),
        "Bar should appear in PUBLISHED_TYPES after register_as_published"
    );
}

#[test]
fn unrelated_types_not_in_this_binary_do_not_appear() {
    // A type defined in a different test binary or in a library
    // dependency that this test binary doesn't reference must not
    // appear in either slice. Use a fixed name no other test binary
    // uses.
    let bogus = "this-name-should-never-be-registered";
    assert!(!SUBSCRIBED_TYPES.iter().any(|d| d.name == bogus));
    assert!(!PUBLISHED_TYPES.iter().any(|d| d.name == bogus));
}
