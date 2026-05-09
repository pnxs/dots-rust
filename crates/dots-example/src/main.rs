//! Demo of the dots-rust descriptor-driven core.
//!
//! Shows that the same wire format and the same code path serve both
//! statically-typed structs and dynamic [`AnyStruct`] instances built
//! from a descriptor alone:
//!
//!   typed Foo  → encode → bytes ─┐
//!                                 ├─→ identical bytes
//!   AnyStruct  → encode → bytes ─┘
//!
//! Plus a roundtrip in each direction:
//!
//!   typed Foo → bytes → decode-typed → typed Foo
//!   bytes     → decode-dynamic       → AnyStruct
//!
//! [`AnyStruct`]: dots_core::AnyStruct

use dots_core::{AnyStruct, FieldKind, StructValue, decode_typed_from_slice, dots, encode_to_vec};
use dots_derive::DotsStruct;

#[derive(DotsStruct, Default, Debug)]
#[dots(name = "Address")]
struct Address {
    #[dots(tag = 1)]
    street: Option<String>,
    #[dots(tag = 2)]
    number: Option<u32>,
}

#[derive(DotsStruct, Default, Debug)]
#[dots(name = "RoundtripData", cached, persistent)]
struct RoundtripData {
    #[dots(tag = 1, key)]
    id: Option<u32>,

    #[dots(tag = 2)]
    payload: Option<String>,

    #[dots(tag = 3)]
    counter: Option<u64>,

    #[dots(tag = 4)]
    flag: Option<bool>,

    #[dots(tag = 5)]
    home: Option<Address>,
}

fn print_summary(label: &str, value: &dyn StructValue) {
    let d = value.descriptor();
    println!("--- {label} ---");
    println!("  type:       {}", d.name);
    println!("  size/align: {}/{}", d.size, d.align);
    println!("  cached:     {}", d.flags.is_cached());
    println!("  persistent: {}", d.flags.is_persistent());
    println!("  valid_set:  {:?}", value.valid_set());
}

fn main() {
    // -- Construction via the global `dots!` macro, including a nested struct --
    let from_macro = dots!(RoundtripData {
        id: 42_u32,
        payload: "hello",
        counter: 9000_u64,
        home: Address {
            street: Some("Lovelace Lane".into()),
            number: Some(11_u32),
        },
    });
    print_summary("from dots! macro", &from_macro);

    // -- Construction via builder methods --
    let from_builder = RoundtripData::default()
        .with_id(7_u32)
        .with_payload("world")
        .with_flag(true);
    print_summary("from builder", &from_builder);

    // -- Inspect the static descriptor (including the nested struct's name) --
    println!();
    println!("descriptor properties:");
    for p in RoundtripData::DESCRIPTOR.properties {
        let kind_label = match p.kind {
            FieldKind::Struct(d) => format!("Struct({})", d.name),
            other => format!("{other:?}"),
        };
        println!(
            "  tag {:>2}  {:<10}  offset={:<3}  kind={:<14}  key={}",
            p.tag, p.name, p.offset, kind_label, p.is_key
        );
    }

    // -- Type erasure roundtrip --
    let erased: &dyn StructValue = &from_macro;
    let downcast = erased.as_any().downcast_ref::<RoundtripData>();
    assert!(downcast.is_some(), "downcast must succeed");

    // -- CBOR roundtrip via the descriptor-driven codec --
    println!();
    let typed_bytes = encode_to_vec(&from_macro);
    println!(
        "typed-encoded ({} bytes): {}",
        typed_bytes.len(),
        hex(&typed_bytes)
    );

    // Decode back into the typed struct.
    let decoded_typed: RoundtripData =
        decode_typed_from_slice(&typed_bytes).expect("typed decode must succeed");
    assert_eq!(decoded_typed.id(), from_macro.id());
    assert_eq!(decoded_typed.payload(), from_macro.payload());
    assert_eq!(decoded_typed.counter(), from_macro.counter());
    assert_eq!(decoded_typed.flag(), from_macro.flag());
    println!("typed → bytes → typed: ok");

    // -- Dynamic decode: same bytes, no compiled-in type knowledge --
    let dynamic = AnyStruct::decode_from_slice(RoundtripData::DESCRIPTOR, &typed_bytes)
        .expect("dynamic decode must succeed");
    print_summary("dynamic AnyStruct (decoded from typed bytes)", &dynamic);

    // -- Cross-roundtrip: encode the AnyStruct and verify byte equality --
    let dynamic_bytes = encode_to_vec(&dynamic);
    println!(
        "dyn-encoded   ({} bytes): {}",
        dynamic_bytes.len(),
        hex(&dynamic_bytes)
    );
    assert_eq!(
        typed_bytes, dynamic_bytes,
        "typed and dynamic encode paths must produce identical bytes"
    );
    println!("typed-bytes == dynamic-bytes: ok");
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&format!("{b:02x}"));
    }
    out
}
