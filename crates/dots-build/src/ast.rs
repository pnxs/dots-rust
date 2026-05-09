//! AST produced by the parser, consumed by the codegen.
//!
//! Faithful to `dots.lark`: a [`File`] is a list of [`Item`]s
//! (struct / enum / import / package), where each carries its
//! source-order doc comments along with the parsed body.

/// Top-level parsed file.
#[derive(Debug, Clone, Default)]
pub struct File {
    pub items: Vec<Item>,
}

/// One top-level construct.
#[derive(Debug, Clone)]
pub enum Item {
    Struct(StructDef),
    Enum(EnumDef),
    /// `import Foo` — pulls in another type by name. Treated as a
    /// hint for the codegen (we currently emit no `use` statement
    /// since generated types live in flat per-file modules; future
    /// versions can use this to emit `pub use super::other::Foo`).
    Import {
        name: String,
    },
    /// `package com.example.foo` — purely informational in dots-cpp;
    /// not currently used by the Rust codegen but preserved for
    /// downstream tools.
    Package {
        name: String,
    },
}

#[derive(Debug, Clone)]
pub struct StructDef {
    pub doc: Vec<String>,
    pub name: String,
    pub options: Vec<Opt>,
    pub properties: Vec<Property>,
}

#[derive(Debug, Clone)]
pub struct EnumDef {
    pub doc: Vec<String>,
    pub name: String,
    pub items: Vec<EnumItem>,
}

#[derive(Debug, Clone)]
pub struct EnumItem {
    pub doc: Vec<String>,
    pub tag: u32,
    pub name: String,
    /// Optional explicit `= INT` value. When absent, the value
    /// defaults to the tag (matches dots-cpp behavior).
    pub value: Option<i64>,
    /// Trailing same-line `///` doc comment, if any.
    pub trailing_doc: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Property {
    pub doc: Vec<String>,
    /// Trailing same-line `///` doc comment, if any.
    pub trailing_doc: Vec<String>,
    pub tag: u32,
    pub options: Vec<Opt>,
    pub ty: PropertyType,
    pub name: String,
}

/// Property type: a bare identifier (primitive name like `int32`,
/// `string`, `timepoint`, or a user-defined type name) or a
/// `vector<T>`.
#[derive(Debug, Clone)]
pub enum PropertyType {
    Named(String),
    Vector(Box<PropertyType>),
}

/// One option from a `[name=value, name2, name3="literal"]` list. A
/// bare name (no `=`) is parsed as `Bool(true)` (matches
/// dots-cpp/dcg.py semantics for marker options like `[key]`,
/// `[deprecated]`, `[internal]`).
#[derive(Debug, Clone)]
pub struct Opt {
    pub name: String,
    pub value: OptValue,
}

#[derive(Debug, Clone)]
pub enum OptValue {
    Bool(bool),
    Str(String),
}

impl Property {
    /// True if the property has the `[key]` option.
    pub fn is_key(&self) -> bool {
        self.options
            .iter()
            .any(|o| o.name == "key" && matches!(o.value, OptValue::Bool(true)))
    }
}

impl StructDef {
    /// Lookup a struct-level option by name.
    pub fn option(&self, name: &str) -> Option<&Opt> {
        self.options.iter().find(|o| o.name == name)
    }

    pub fn is_internal(&self) -> bool {
        self.flag("internal", false)
    }

    /// Cached defaults to `true` unless `cached=false` is set
    /// explicitly. Matches the dots-cpp `.dots` semantics: `struct
    /// Foo {...}` is cached, `struct Foo [cached=false] {...}` is
    /// not.
    pub fn is_cached(&self) -> bool {
        self.flag("cached", true)
    }

    pub fn is_persistent(&self) -> bool {
        self.flag("persistent", false)
    }

    pub fn is_cleanup(&self) -> bool {
        self.flag("cleanup", false)
    }

    pub fn is_local(&self) -> bool {
        self.flag("local", false)
    }

    pub fn is_substruct_only(&self) -> bool {
        self.flag("substruct_only", false)
    }

    fn flag(&self, name: &str, default: bool) -> bool {
        match self.option(name).map(|o| &o.value) {
            Some(OptValue::Bool(b)) => *b,
            Some(_) => default,
            None => default,
        }
    }
}
