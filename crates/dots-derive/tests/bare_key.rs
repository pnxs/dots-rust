//! Prototype: bare-`T` `#[dots(key)]` fields (Approach A).
//!
//! A key field declared as a bare `T` (here `id: String`) instead of
//! `Option<T>` is:
//!   * accessed infallibly as `&T` (no `Option`);
//!   * always present in `valid_set()`;
//!   * stored unwrapped in both the typed struct and the dynamic
//!     `AnyStruct` buffer (`as_typed` reinterprets the same bytes).
//!
//! The dynamic buffer stays sound because `AnyStruct::new` initializes
//! the bare-`String` key slot to a valid `String::default()` instead of
//! leaving the null-pointer zeroed bytes. Decoding rejects any wire
//! value missing a key.

use dots_core::{AnyStruct, PropertySet, StructValue, dots, encode_to_vec, minicbor};

// DOTS types live in a module (as generated code does) so the `dots!`
// companion macro the derive emits is reachable; `use model::*` brings
// both the type and its companion into scope.
mod model {
    use dots_derive::DotsStruct;

    #[derive(DotsStruct, Default, Clone, Debug, PartialEq)]
    #[dots(name = "OneKey")]
    pub struct OneKey {
        /// Primary key — bare `String`, not `Option<String>`.
        #[dots(tag = 1, key)]
        pub id: String,
        #[dots(tag = 2)]
        pub value: Option<u32>,
    }
}
use model::*;

#[test]
fn key_getter_is_infallible_and_always_in_valid_set() {
    let v = OneKey::new("primary").with_value(7_u32);

    // `id()` returns `&String`, not `Option<&String>`.
    let id: &String = v.id();
    assert_eq!(id, "primary");
    assert_eq!(v.value(), Some(&7));

    // The key is always reported as set; the optional field reflects use.
    assert!(v.has_id());
    assert!(v.valid_set().has(1));
    assert!(v.valid_set().has(2));

    // Descriptor exposes the key mask.
    assert_eq!(OneKey::DESCRIPTOR.key_mask(), PropertySet::EMPTY.with_tag(1));
}

#[test]
fn roundtrip_through_anystruct_preserves_bare_key() {
    let v = OneKey::new("primary").with_value(7_u32);
    let bytes = encode_to_vec(&v);

    // Decode type-erased, then reinterpret the buffer as &OneKey.
    let any = AnyStruct::decode_from_slice(OneKey::DESCRIPTOR, &bytes).expect("decode");
    let back: &OneKey = any.as_typed::<OneKey>().expect("descriptor identity");
    assert_eq!(back.id(), "primary");
    assert_eq!(back.value(), Some(&7));

    // Clone of the AnyStruct (exercises key_clone over the String key).
    let cloned = any.clone();
    assert_eq!(cloned.as_typed::<OneKey>().unwrap().id(), "primary");
}

#[test]
fn fresh_anystruct_new_then_drop_is_sound_for_string_key() {
    // A bare `String` key zeroed = null pointer = invalid. `AnyStruct::new`
    // must `init` it to a valid `String` so this allocate-then-drop is sound.
    // (Run under Miri to actually catch a regression; here it must at least
    // not abort, and the buffer must read back as a valid empty string.)
    let any = AnyStruct::new(OneKey::DESCRIPTOR);
    let view = any.as_typed::<OneKey>().expect("descriptor identity");
    assert_eq!(view.id(), ""); // valid placeholder, not UB
    drop(any);
}

#[test]
fn dots_macro_constructs_bare_key_struct() {
    // `dots!` delegates to the generated companion macro, which coerces
    // the bare-`String` key (no `Some` wrapper) and `Some`-wraps the
    // optional `value`.
    let v = dots!(OneKey { id: "primary", value: 7_u32 });
    assert_eq!(v.id(), "primary");
    assert_eq!(v.value(), Some(&7));

    // Optional field omitted -> None; key still required & present.
    let only_key = dots!(OneKey { id: "k" });
    assert_eq!(only_key.id(), "k");
    assert_eq!(only_key.value(), None);
}

// Compile-time key enforcement (Escape B): omitting the bare key is a
// `compile_error!`, not a silent default. Demonstrated here as a doc
// example so the failure is visible without a trybuild harness:
//
// ```compile_fail
// use dots_core::dots;
// use dots_derive::DotsStruct;
// #[derive(DotsStruct, Default)]
// #[dots(name = "K")]
// struct K { #[dots(tag = 1, key)] id: String, #[dots(tag = 2)] v: Option<u32> }
// let _ = dots!(K { v: 1_u32 }); // error: missing required `#[dots(key)]` field `id`
// ```
#[allow(dead_code)]
fn compile_fail_doc_anchor() {}

// Replicates the dots-build-generated layout: the type and its companion
// macro live in a `mod model`-style submodule; `dots!` is used from a
// sibling/outer scope after a glob import. This is the case that
// `#[macro_export]` could not satisfy and the `pub use` re-export does.
mod generated_like {
    use dots_derive::DotsStruct;

    #[derive(DotsStruct, Default, Clone, Debug, PartialEq)]
    #[dots(name = "SubKey")]
    pub struct SubKey {
        #[dots(tag = 1, key)]
        pub id: String,
        #[dots(tag = 2)]
        pub v: Option<u32>,
    }
}

#[test]
fn dots_macro_works_on_submodule_generated_type() {
    use generated_like::*; // module glob brings `SubKey` and its companion into scope
    let x = dots!(SubKey { id: "z", v: 9_u32 });
    assert_eq!(x.id(), "z");
    assert_eq!(x.v(), Some(&9));
}

#[test]
fn decode_rejects_value_missing_the_key() {
    // Hand-encode a CBOR map {2: 7} — only the non-key `value`, no key.
    let mut buf = Vec::new();
    let mut e = minicbor::Encoder::new(&mut buf);
    e.map(1).unwrap();
    e.u32(2).unwrap();
    e.u32(7).unwrap();

    let err = AnyStruct::decode_from_slice(OneKey::DESCRIPTOR, &buf);
    assert!(err.is_err(), "missing key must be a decode error");
}
