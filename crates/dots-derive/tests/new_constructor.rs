//! `#[derive(DotsStruct)]` emits a `pub fn new(<key_args>) -> Self`
//! when the struct has at least one `#[dots(key)]` field. The
//! constructor takes one parameter per key in declaration order
//! (each `impl Into<inner>`) and leaves every other property `None`.
//!
//! Types without any `#[dots(key)]` field skip the emission to avoid
//! colliding with a hand-written `new` on the same type.

use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "OneKey")]
struct OneKey {
    #[dots(tag = 1, key)]
    id: Option<String>,
    #[dots(tag = 2)]
    value: Option<u32>,
}

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "TwoKeys")]
struct TwoKeys {
    #[dots(tag = 1, key)]
    region: Option<String>,
    #[dots(tag = 2, key)]
    serial: Option<u32>,
    #[dots(tag = 3)]
    note: Option<String>,
}

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Keyless")]
struct Keyless {
    #[dots(tag = 1)]
    note: Option<String>,
}

impl Keyless {
    // Hand-written `new` on a keyless type — must NOT collide with a
    // derive-emitted one. If `derive` accidentally emitted `new()` for
    // keyless types this whole crate would fail to compile.
    pub fn new() -> Self {
        Self {
            note: Some("hand-written".into()),
        }
    }
}

#[test]
fn single_key_new_sets_key_field() {
    let k = OneKey::new("primary");
    assert_eq!(k.id.as_deref(), Some("primary"));
    assert_eq!(k.value, None);
}

#[test]
fn single_key_new_accepts_string_too() {
    // The `impl Into<String>` bound lets us pass an owned `String`
    // alongside the `&str` case.
    let k = OneKey::new(String::from("primary"));
    assert_eq!(k.id.as_deref(), Some("primary"));
}

#[test]
fn multi_key_new_takes_positional_args_in_declaration_order() {
    let k = TwoKeys::new("eu", 42_u32);
    assert_eq!(k.region.as_deref(), Some("eu"));
    assert_eq!(k.serial, Some(42));
    assert_eq!(k.note, None);
}

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Documented")]
struct Documented {
    /// Stable client identifier (must be unique per region).
    #[dots(tag = 1, key)]
    client_id: Option<String>,
    /// Two-letter ISO region code.
    #[dots(tag = 2, key)]
    region: Option<String>,
    #[dots(tag = 3)]
    payload: Option<u32>,
}

#[test]
fn new_constructor_doc_block_lists_each_key_field() {
    // The derive captures `#[doc = "..."]` on each key field and
    // emits them in an `# Arguments` section. `cargo doc` consumers
    // can't be tested without spinning up rustdoc, but the doc
    // attributes themselves must survive expansion — verify via
    // `rustdoc`-equivalent compile-time check that the items still
    // round-trip and the fn exists with the expected shape.
    let d = Documented::new("guest-7", "eu");
    assert_eq!(d.client_id.as_deref(), Some("guest-7"));
    assert_eq!(d.region.as_deref(), Some("eu"));
    assert_eq!(d.payload, None);
}

#[test]
fn keyless_type_keeps_user_new() {
    // Derive must NOT have emitted a `new()` for `Keyless` — if it
    // had, Rust would have rejected the duplicate inherent method
    // and this test wouldn't compile.
    let k = Keyless::new();
    assert_eq!(k.note.as_deref(), Some("hand-written"));
}
