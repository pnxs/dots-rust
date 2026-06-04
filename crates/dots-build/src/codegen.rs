//! AST → Rust source.
//!
//! Emits structs / enums backed by `#[derive(DotsStruct)]` and
//! `#[derive(DotsEnum)]` from `dots-derive`, so generated types are
//! indistinguishable from hand-written ones at the API level.

use core::fmt::Write;
use std::collections::HashMap;

use crate::ast::{EnumDef, EnumItem, File, Item, Opt, OptValue, Property, PropertyType, StructDef};

/// Render a parsed file as Rust source. `import` directives are
/// recorded but produce no `use` statements — the resulting code
/// only compiles when every referenced external type lives in the
/// same generated module. For multi-file projects use
/// [`generate_with_imports`] instead, which resolves cross-file
/// references via a type→module index.
pub fn generate(file: &File) -> String {
    generate_with_imports(file, "", &HashMap::new())
}

/// Render a parsed file as Rust source, resolving each `import T`
/// directive to a `use super::<other_module>::T;` line based on the
/// supplied type→module index. `current_module` names this file's
/// own generated module — types imported from that same module are
/// skipped (no self-imports). Types not present in the index pass
/// through silently; the resulting compile error is the user's
/// signal that the imported type isn't part of the build.
pub fn generate_with_imports(
    file: &File,
    current_module: &str,
    type_locations: &HashMap<String, String>,
) -> String {
    let mut out = String::new();
    // Import only the derive macros this file actually needs — emitting
    // both unconditionally produces an `unused_imports` warning in the
    // consuming crate when the file has structs but no enums (or vice
    // versa).
    let has_struct = file.items.iter().any(|i| matches!(i, Item::Struct(_)));
    let has_enum = file.items.iter().any(|i| matches!(i, Item::Enum(_)));
    match (has_struct, has_enum) {
        (true, true) => out.push_str("use dots_derive::{DotsEnum, DotsStruct};\n"),
        (true, false) => out.push_str("use dots_derive::DotsStruct;\n"),
        (false, true) => out.push_str("use dots_derive::DotsEnum;\n"),
        (false, false) => {}
    }

    // Emit `use super::<other_mod>::<Type>;` for each `import T`
    // directive whose target is defined in another generated module.
    for item in &file.items {
        if let Item::Import { name } = item {
            if let Some(target) = type_locations.get(name) {
                if target != current_module {
                    let _ = writeln!(&mut out, "use super::{}::{};", target, name);
                }
            }
        }
    }
    out.push('\n');

    for item in &file.items {
        match item {
            Item::Struct(s) => emit_struct(&mut out, s),
            Item::Enum(e) => emit_enum(&mut out, e),
            // imports already handled above; packages are
            // informational only.
            Item::Import { .. } | Item::Package { .. } => {}
        }
    }
    out
}

fn emit_struct(out: &mut String, s: &StructDef) {
    for line in &s.doc {
        let _ = writeln!(out, "/// {line}");
    }
    emit_struct_wire_shape(out, s);
    // A keyed struct stores its `#[dots(key)]` fields as bare `T`, so a
    // derived `Default` could only fill them with a placeholder key —
    // a value that violates "keys are always present by contract". The
    // decode and `new` paths no longer rely on `Default` (the decode
    // seed comes from the descriptor's `init` thunks), so we omit it for
    // keyed structs: construct them with `Foo::new(keys…)` or `dots!`.
    // Keyless structs keep `Default` — an all-`None` value is meaningful
    // and convenient there.
    let has_key = s.properties.iter().any(|p| p.is_key());
    if has_key {
        let _ = writeln!(out, "#[derive(DotsStruct, Debug, Clone, PartialEq)]");
    } else {
        let _ = writeln!(out, "#[derive(DotsStruct, Default, Debug, Clone, PartialEq)]");
    }

    // Build the #[dots(...)] container attribute: name, plus flags.
    let mut dots_attr_parts: Vec<String> = Vec::new();
    dots_attr_parts.push(format!("name = \"{}\"", s.name));
    if s.is_cached() {
        dots_attr_parts.push("cached".into());
    }
    if s.is_internal() {
        dots_attr_parts.push("internal".into());
    }
    if s.is_persistent() {
        dots_attr_parts.push("persistent".into());
    }
    if s.is_cleanup() {
        dots_attr_parts.push("cleanup".into());
    }
    if s.is_local() {
        dots_attr_parts.push("local".into());
    }
    if s.is_substruct_only() {
        dots_attr_parts.push("substruct_only".into());
    }
    let _ = writeln!(out, "#[dots({})]", dots_attr_parts.join(", "));

    let _ = writeln!(out, "pub struct {} {{", s.name);
    for prop in &s.properties {
        emit_property(out, prop);
    }
    out.push_str("}\n\n");
}

fn emit_property(out: &mut String, prop: &Property) {
    for line in &prop.doc {
        let _ = writeln!(out, "    /// {line}");
    }
    for line in &prop.trailing_doc {
        let _ = writeln!(out, "    /// {line}");
    }
    let mut attr_parts: Vec<String> = Vec::new();
    attr_parts.push(format!("tag = {}", prop.tag));
    if prop.is_key() {
        attr_parts.push("key".into());
    }
    let _ = writeln!(out, "    #[dots({})]", attr_parts.join(", "));

    let rust_ty = render_type(&prop.ty);
    let field_name = rustify_field_name(&prop.name);
    if prop.is_key() {
        // Key properties are always present by contract, so they are
        // stored as a bare `T` rather than `Option<T>`: an infallible
        // `&T` accessor, always in the valid-set, and a smaller layout
        // for scalar keys. The decode seed comes from the descriptor's
        // `init` thunk (`T::default()` on the inner key type), not the
        // struct's `Default`; decoding enforces that the key is present.
        let _ = writeln!(out, "    pub {field_name}: {rust_ty},");
    } else {
        let _ = writeln!(out, "    pub {field_name}: Option<{rust_ty}>,");
    }
}

fn emit_enum(out: &mut String, e: &EnumDef) {
    for line in &e.doc {
        let _ = writeln!(out, "/// {line}");
    }
    emit_enum_wire_shape(out, e);
    let _ = writeln!(
        out,
        "#[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]"
    );
    let _ = writeln!(out, "#[dots(name = \"{}\")]", e.name);
    let _ = writeln!(out, "pub enum {} {{", e.name);
    for (i, item) in e.items.iter().enumerate() {
        emit_enum_item(out, item, i == 0);
    }
    out.push_str("}\n\n");
}

fn emit_enum_item(out: &mut String, item: &EnumItem, is_first: bool) {
    for line in &item.doc {
        let _ = writeln!(out, "    /// {line}");
    }
    for line in &item.trailing_doc {
        let _ = writeln!(out, "    /// {line}");
    }
    if is_first {
        out.push_str("    #[default]\n");
    }
    let _ = writeln!(out, "    #[dots(tag = {})]", item.tag);
    let _ = writeln!(out, "    {},", rustify_variant_name(&item.name));
}

/// Emit a `///`-doc fenced block reconstructing the `.dots` source
/// of `s`. Mirrors the `@code` block dots-cpp puts on each generated
/// header, so IDE hover (RustRover Ctrl-Q, rust-analyzer, etc.)
/// shows the wire shape — tags, flags, type names, and any trailing
/// per-property comments — verbatim.
fn emit_struct_wire_shape(out: &mut String, s: &StructDef) {
    let _ = writeln!(out, "///");
    let _ = writeln!(out, "/// # Wire shape");
    let _ = writeln!(out, "///");
    let _ = writeln!(out, "/// ```text");
    let opts = render_dots_options(&s.options);
    let _ = writeln!(out, "/// struct {}{} {{", s.name, opts);
    for prop in &s.properties {
        let _ = writeln!(out, "/// {}", render_dots_property(prop));
    }
    let _ = writeln!(out, "/// }}");
    let _ = writeln!(out, "/// ```");
}

fn emit_enum_wire_shape(out: &mut String, e: &EnumDef) {
    let _ = writeln!(out, "///");
    let _ = writeln!(out, "/// # Wire shape");
    let _ = writeln!(out, "///");
    let _ = writeln!(out, "/// ```text");
    let _ = writeln!(out, "/// enum {} {{", e.name);
    for item in &e.items {
        let _ = writeln!(out, "/// {}", render_dots_enum_item(item));
    }
    let _ = writeln!(out, "/// }}");
    let _ = writeln!(out, "/// ```");
}

fn render_dots_options(opts: &[Opt]) -> String {
    if opts.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = opts.iter().map(render_dots_opt).collect();
    format!(" [{}]", parts.join(", "))
}

fn render_dots_opt(o: &Opt) -> String {
    match &o.value {
        OptValue::Bool(true) => o.name.clone(),
        OptValue::Bool(false) => format!("{}=false", o.name),
        OptValue::Str(s) => format!("{}=\"{}\"", o.name, s),
    }
}

fn render_dots_property(prop: &Property) -> String {
    let opts = render_dots_options(&prop.options);
    let ty = render_dots_type(&prop.ty);
    let trailing = if prop.trailing_doc.is_empty() {
        String::new()
    } else {
        format!(" // {}", prop.trailing_doc.join(" "))
    };
    format!(
        "    {}:{} {} {};{}",
        prop.tag, opts, ty, prop.name, trailing
    )
}

fn render_dots_enum_item(item: &EnumItem) -> String {
    let trailing = if item.trailing_doc.is_empty() {
        String::new()
    } else {
        format!(" // {}", item.trailing_doc.join(" "))
    };
    match item.value {
        Some(v) => format!("    {}: {} = {};{}", item.tag, item.name, v, trailing),
        None => format!("    {}: {};{}", item.tag, item.name, trailing),
    }
}

fn render_dots_type(ty: &PropertyType) -> String {
    match ty {
        PropertyType::Named(n) => n.clone(),
        PropertyType::Vector(inner) => format!("vector<{}>", render_dots_type(inner)),
    }
}

/// Map a `.dots` type name to a Rust type name.
///
/// Primitives match dots-cpp / dots-rust conventions:
/// `bool`, `int8..int64`, `uint8..uint64`, `float32/64` → built-ins;
/// `string` → `String`; `timepoint`/`steady_timepoint` → `Timepoint`;
/// `duration` → `Duration`; `property_set` → `u64`; `uuid` → `[u8; 16]`;
/// `any` → `dots_core::AnyObject`.
/// Unknown identifiers pass through unchanged (they're user types).
fn render_type(ty: &PropertyType) -> String {
    match ty {
        PropertyType::Named(name) => map_primitive(name).to_string(),
        PropertyType::Vector(inner) => format!("Vec<{}>", render_type(inner)),
    }
}

fn map_primitive(name: &str) -> &str {
    match name {
        "bool" => "bool",
        "int8" => "i8",
        "int16" => "i16",
        "int32" => "i32",
        "int64" => "i64",
        "uint8" => "u8",
        "uint16" => "u16",
        "uint32" => "u32",
        "uint64" => "u64",
        "float32" => "f32",
        "float64" => "f64",
        "string" => "String",
        "timepoint" | "steady_timepoint" => "dots_core::Timepoint",
        "duration" => "dots_core::Duration",
        "property_set" => "u64",
        "uuid" => "[u8; 16]",
        // Open `any` — an arbitrary DOTS object stored as an opaque
        // self-describing envelope. (`variant { ... }` is not yet
        // supported by the parser.)
        "any" => "dots_core::AnyObject",
        // Unknown identifier — assume it's a user-defined struct/enum.
        other => other,
    }
}

/// Rust keyword set that needs `r#` raw-identifier prefixing when
/// used as a field name. Mirrors the keyword list embedded in the
/// existing `struct_dots.rs.dotsT` template.
const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn", "for",
    "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return",
    "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe", "use",
    "where", "while", "abstract", "become", "box", "do", "final", "macro", "override", "priv",
    "typeof", "unsized", "virtual", "yield", "try", "macro_rules", "union", "dyn",
];

fn rustify_field_name(name: &str) -> String {
    let snake = snake_case(name);
    if RUST_KEYWORDS.contains(&snake.as_str()) {
        format!("r#{snake}")
    } else {
        snake
    }
}

fn rustify_variant_name(name: &str) -> String {
    // Variants are PascalCase. Convert from .dots's typical
    // lowercase-with-underscores or camelCase to PascalCase.
    let mut out = String::with_capacity(name.len());
    let mut next_upper = true;
    for c in name.chars() {
        if c == '_' {
            next_upper = true;
        } else if next_upper {
            out.extend(c.to_uppercase());
            next_upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let mut prev_lower = false;
    for c in name.chars() {
        if c.is_uppercase() {
            if prev_lower {
                out.push('_');
            }
            out.extend(c.to_lowercase());
            prev_lower = false;
        } else {
            out.push(c);
            prev_lower = c.is_alphanumeric();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_str;

    #[test]
    fn struct_with_key_and_vector() {
        let src = r#"
            struct DotsClient [internal] {
                1: [key] uint32 id;
                2: string name;
                4: vector<string> publishedTypes;
            }
        "#;
        let file = parse_str(src).unwrap();
        let out = generate(&file);
        assert!(out.contains("pub struct DotsClient"));
        assert!(out.contains("#[dots(name = \"DotsClient\", cached, internal)]"));
        assert!(out.contains("#[dots(tag = 1, key)]"));
        // Key fields are emitted as bare `T` (not `Option<T>`).
        assert!(out.contains("pub id: u32,"));
        assert!(!out.contains("pub id: Option<u32>,"));
        assert!(out.contains("pub published_types: Option<Vec<String>>,"));
    }

    #[test]
    fn struct_uncached_via_cached_false() {
        let src = "struct DotsHeader [internal,cached=false] { 1: string typeName; }";
        let file = parse_str(src).unwrap();
        let out = generate(&file);
        // `cached` must NOT appear in the dots(...) attr list.
        assert!(out.contains("#[dots(name = \"DotsHeader\", internal)]"));
        assert!(out.contains("pub type_name: Option<String>,"));
    }

    #[test]
    fn enum_first_variant_is_default() {
        let src = "enum Mt { 1: create, 2: update, 3: remove }";
        let file = parse_str(src).unwrap();
        let out = generate(&file);
        assert!(out.contains("pub enum Mt"));
        assert!(out.contains("#[default]"));
        assert!(out.contains("Create,"));
        assert!(out.contains("Update,"));
        assert!(out.contains("Remove,"));
    }

    #[test]
    fn doc_comments_become_rust_doc_attrs() {
        let src = r#"
            /// header docs
            struct H {
                /// id docs
                1: [key] uint32 id; /// trailing too
            }
        "#;
        let file = parse_str(src).unwrap();
        let out = generate(&file);
        assert!(out.contains("/// header docs"));
        assert!(out.contains("/// id docs"));
        assert!(out.contains("/// trailing too"));
    }

    #[test]
    fn temporal_types_resolve_to_dots_core() {
        let src = "struct Status { 1: timepoint t; 2: duration d; }";
        let file = parse_str(src).unwrap();
        let out = generate(&file);
        assert!(out.contains("Option<dots_core::Timepoint>"));
        assert!(out.contains("Option<dots_core::Duration>"));
    }

    #[test]
    fn rust_keyword_field_name_is_raw_prefixed() {
        let src = "struct K { 1: string type; }";
        let file = parse_str(src).unwrap();
        let out = generate(&file);
        assert!(out.contains("pub r#type: Option<String>,"));
    }
}
