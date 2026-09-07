//! Compile `.dots` files into Rust source from a `build.rs`.
//!
//! Provides a hand-rolled lexer + recursive-descent parser implementing
//! the grammar in [`dots.lark`](https://github.com/pnxs/dots-code-generator/blob/master/dots.lark)
//! used by `dots-cpp`'s code generator, then renders the parsed AST as
//! plain Rust source backed by `#[derive(DotsStruct)]` /
//! `#[derive(DotsEnum)]` from the `dots-derive` crate.
//!
//! The intended workflow:
//!
//! ```ignore
//! // build.rs
//! fn main() {
//!     dots_rs_build::compile(&["proto/types.dots"]).unwrap();
//! }
//!
//! // src/lib.rs
//! include!(concat!(env!("OUT_DIR"), "/dots_generated.rs"));
//! ```
//!
//! Generated files live in `$OUT_DIR` so they aren't checked in but
//! are visible to `rust-analyzer` (completion, hover, doc-on-hover).

mod ast;
mod codegen;
mod lexer;
mod parser;

pub use ast::{EnumDef, EnumItem, File, Item, Property, PropertyType, StructDef};
pub use codegen::{generate, generate_with_imports};
pub use lexer::{LexError, Span};
pub use parser::ParseError;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Errors produced by [`compile`].
#[derive(Debug)]
pub enum BuildError {
    Io {
        path: PathBuf,
        error: std::io::Error,
    },
    Lex {
        path: PathBuf,
        error: LexError,
    },
    Parse {
        path: PathBuf,
        error: ParseError,
    },
    /// A semantic rule violation (e.g. the key-property constraints of
    /// wire-format spec §6.3).
    Validate {
        path: PathBuf,
        message: String,
    },
    OutDirNotSet,
}

impl core::fmt::Display for BuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io { path, error } => write!(f, "{}: {}", path.display(), error),
            Self::Lex { path, error } => write!(f, "{}: {}", path.display(), error),
            Self::Parse { path, error } => write!(f, "{}: {}", path.display(), error),
            Self::Validate { path, message } => write!(f, "{}: {}", path.display(), message),
            Self::OutDirNotSet => f.write_str("OUT_DIR environment variable not set"),
        }
    }
}

impl std::error::Error for BuildError {}

/// Compile a list of `.dots` source files into a single Rust file at
/// `$OUT_DIR/dots_generated.rs`. Emits `cargo:rerun-if-changed`
/// directives for each input so cargo invalidates the build when any
/// changes.
///
/// Each input file becomes a `mod <stem> { ... }` namespace. Type
/// names are unchanged (no rewriting), so `struct Foo` in
/// `proto/bar.dots` becomes `bar::Foo`.
pub fn compile<P: AsRef<Path>>(files: &[P]) -> Result<(), BuildError> {
    let out_dir = std::env::var_os("OUT_DIR").ok_or(BuildError::OutDirNotSet)?;
    compile_to_dir(files, Path::new(&out_dir))
}

/// Lower-level form of [`compile`] that writes to a caller-specified
/// directory. Useful for tests and for projects that want a non-default
/// output location.
pub fn compile_to_dir<P, D>(files: &[P], out_dir: D) -> Result<(), BuildError>
where
    P: AsRef<Path>,
    D: AsRef<Path>,
{
    let out_dir = out_dir.as_ref();
    std::fs::create_dir_all(out_dir).map_err(|e| BuildError::Io {
        path: out_dir.into(),
        error: e,
    })?;

    // Pass 1: parse every input, capturing path / module name / AST.
    // We need everything parsed before we generate code so cross-file
    // `import` directives can resolve.
    struct Parsed {
        module: String,
        file: File,
    }
    let mut parsed_files: Vec<Parsed> = Vec::with_capacity(files.len());
    for file in files {
        let path = file.as_ref();
        println!("cargo:rerun-if-changed={}", path.display());
        let source = std::fs::read_to_string(path).map_err(|e| BuildError::Io {
            path: path.into(),
            error: e,
        })?;
        let parsed = parse_str(&source).map_err(|e| match e {
            ParseSourceError::Lex(e) => BuildError::Lex {
                path: path.into(),
                error: e,
            },
            ParseSourceError::Parse(e) => BuildError::Parse {
                path: path.into(),
                error: e,
            },
        })?;
        parsed_files.push(Parsed {
            module: module_name_from_path(path),
            file: parsed,
        });
    }

    // Pass 2a: build a global type→module index from every parsed
    // file's struct + enum declarations. Used to resolve `import`
    // directives into `use super::<other_mod>::<TypeName>;` lines.
    let mut type_locations: HashMap<String, String> = HashMap::new();
    for p in &parsed_files {
        if p.module.is_empty() {
            continue;
        }
        for item in &p.file.items {
            match item {
                Item::Struct(s) => {
                    type_locations.insert(s.name.clone(), p.module.clone());
                }
                Item::Enum(e) => {
                    type_locations.insert(e.name.clone(), p.module.clone());
                }
                _ => {}
            }
        }
    }

    // Pass 2a': semantic validation. Needs the cross-file index so an
    // `instance_ref<X>` target or a named key type can be classified
    // as struct vs. enum.
    let mut local_kinds: HashMap<&str, LocalKind> = HashMap::new();
    for p in &parsed_files {
        for item in &p.file.items {
            match item {
                Item::Struct(s) => {
                    local_kinds.insert(&s.name, LocalKind::Struct);
                }
                Item::Enum(e) => {
                    local_kinds.insert(&e.name, LocalKind::Enum);
                }
                _ => {}
            }
        }
    }
    for (p, file) in parsed_files.iter().zip(files) {
        validate_file(&p.file, &local_kinds).map_err(|message| BuildError::Validate {
            path: file.as_ref().into(),
            message,
        })?;
    }

    // Pass 2b: generate Rust source per file, splicing in resolved
    // imports.
    let mut combined = String::new();
    combined.push_str("// @generated by dots-build — do not edit\n\n");
    for p in &parsed_files {
        let generated = generate_with_imports(&p.file, &p.module, &type_locations);
        if p.module.is_empty() {
            combined.push_str(&generated);
        } else {
            combined.push_str(&format!("pub mod {} {{\n", p.module));
            for line in generated.lines() {
                if line.is_empty() {
                    combined.push('\n');
                } else {
                    combined.push_str("    ");
                    combined.push_str(line);
                    combined.push('\n');
                }
            }
            combined.push_str("}\n\n");
        }
    }

    let out_path = out_dir.join("dots_generated.rs");
    std::fs::write(&out_path, &combined).map_err(|e| BuildError::Io {
        path: out_path.clone(),
        error: e,
    })?;

    // Best-effort rustfmt: if it's available on PATH, format the
    // output file in place. Fail silently — formatting is purely
    // cosmetic and rustfmt may legitimately not be installed (e.g.
    // a minimal CI image).
    let _ = std::process::Command::new("rustfmt")
        .arg("--edition=2024")
        .arg(&out_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// Parse a single `.dots` source string into an AST. Useful for tools
/// that consume the AST directly (linters, descriptor exchangers).
pub fn parse_str(source: &str) -> Result<File, ParseSourceError> {
    let tokens = lexer::tokenize(source).map_err(ParseSourceError::Lex)?;
    parser::parse(tokens).map_err(ParseSourceError::Parse)
}

/// Error returned by [`parse_str`].
#[derive(Debug)]
pub enum ParseSourceError {
    Lex(LexError),
    Parse(ParseError),
}

impl core::fmt::Display for ParseSourceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Lex(e) => write!(f, "{e}"),
            Self::Parse(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ParseSourceError {}

/// Struct-vs-enum classification of a locally declared type, for the
/// semantic checks that need to tell the two apart.
#[derive(Clone, Copy, PartialEq)]
enum LocalKind {
    Struct,
    Enum,
}

/// Semantic validation of one parsed `.dots` file (wire-format spec
/// §6.3 plus the `instance_ref` target rule of §6.5). `kinds` is the
/// cross-file name index; names not in it (types imported from other
/// packages) pass here — the derive's const assertion on
/// `DotsTypeKind::KIND` is the backstop that still rejects them at
/// compile time.
fn validate_file(file: &File, kinds: &HashMap<&str, LocalKind>) -> Result<(), String> {
    for item in &file.items {
        let Item::Struct(s) = item else { continue };

        // Key properties must be declared in ascending tag order.
        let mut prev: Option<&Property> = None;
        for prop in s.properties.iter().filter(|p| p.is_key()) {
            if let Some(prev) = prev {
                if prop.tag <= prev.tag {
                    return Err(format!(
                        "struct {}: [key] properties must be declared in ascending tag \
                         order; `{}` (tag {}) is declared after `{}` (tag {})",
                        s.name, prop.name, prop.tag, prev.name, prev.tag
                    ));
                }
            }
            prev = Some(prop);

            // Key properties must be of byte-stable-equality kinds.
            if let Some(reason) = key_type_error(&prop.ty, kinds) {
                return Err(format!(
                    "struct {}: [key] property `{}` {reason}",
                    s.name, prop.name
                ));
            }
        }

        // `instance_ref<X>` targets must be struct types — checked for
        // every property, key or not.
        for prop in &s.properties {
            if let Some(reason) = ref_target_error(&prop.ty, kinds) {
                return Err(format!(
                    "struct {}: property `{}` {reason}",
                    s.name, prop.name
                ));
            }
        }
    }
    Ok(())
}

/// Why `ty` is illegal as a `[key]` property type, if it is.
fn key_type_error(ty: &PropertyType, kinds: &HashMap<&str, LocalKind>) -> Option<String> {
    match ty {
        PropertyType::Vector(_) => Some("has a vector type; vectors cannot be keys".into()),
        PropertyType::InstanceRef(_) => None,
        PropertyType::Named(name) => match name.as_str() {
            "bool" | "int8" | "int16" | "int32" | "int64" | "uint8" | "uint16" | "uint32"
            | "uint64" | "string" | "uuid" => None,
            "float32" | "float64" | "timepoint" | "steady_timepoint" | "duration"
            | "property_set" | "any" => Some(format!(
                "has type `{name}`, whose equality is not byte-stable; keys must be \
                 bool, integer, string, uuid, enum, or instance_ref"
            )),
            other => match kinds.get(other) {
                Some(LocalKind::Struct) => Some(format!(
                    "has struct type `{other}`; nested structs cannot be keys"
                )),
                // Enums are key-legal; unknown names are assumed to be
                // enums from another package (derive backstop applies).
                Some(LocalKind::Enum) | None => None,
            },
        },
    }
}

/// Why `ty` names an illegal `instance_ref` target, if it does.
/// Recurses through vectors so `vector<instance_ref<X>>` is covered.
fn ref_target_error(ty: &PropertyType, kinds: &HashMap<&str, LocalKind>) -> Option<String> {
    match ty {
        PropertyType::Vector(inner) => ref_target_error(inner, kinds),
        PropertyType::InstanceRef(Some(target)) => match kinds.get(target.as_str()) {
            Some(LocalKind::Struct) => None,
            Some(LocalKind::Enum) => Some(format!(
                "references enum `{target}`; instance_ref targets must be struct types"
            )),
            None if is_primitive_name(target) => Some(format!(
                "references primitive `{target}`; instance_ref targets must be struct types"
            )),
            // Unknown name: assumed to be a struct from another
            // package (derive backstop applies).
            None => None,
        },
        PropertyType::InstanceRef(None) | PropertyType::Named(_) => None,
    }
}

fn is_primitive_name(name: &str) -> bool {
    matches!(
        name,
        "bool"
            | "int8"
            | "int16"
            | "int32"
            | "int64"
            | "uint8"
            | "uint16"
            | "uint32"
            | "uint64"
            | "float32"
            | "float64"
            | "string"
            | "uuid"
            | "timepoint"
            | "steady_timepoint"
            | "duration"
            | "property_set"
            | "any"
    )
}

fn module_name_from_path(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.replace(['-', '.'], "_"))
        .unwrap_or_default()
}
