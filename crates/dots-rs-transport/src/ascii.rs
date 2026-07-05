//! Human-readable (ascii) struct rendering, ported from dots-cpp's
//! `AsciiSerialization`.
//!
//! [`StructDisplay`] wraps a [`DynamicStruct`] together with the
//! [`Registry`] that can resolve its `any` fields, so each contained
//! object is decoded and printed inline instead of the opaque
//! `any<Type>[N bytes]` the bare `Display` produces. [`ColorSchema`]
//! optionally wraps every rendered token in ANSI escapes — the trace
//! palette in [`ColorSchema::TRACE`] matches dots-cpp's
//! `TraceColorSchema`.
//!
//! Intended for trace/inspection tools (e.g. `dots-trace`) that hold
//! the registry and want to see *through* the envelope.

use dots_rs_core::{AnyObject, DynamicFieldKind, DynamicStruct, DynamicValue};
use dots_rs_model::{DecodedAny, Registry};

/// ANSI SGR escape sequences for colored struct rendering, ported from
/// dots-cpp's `ToAsciiColorSchema`.
///
/// The value-kind fields (`string`, `integer`, …, `type_name`) drive
/// [`StructDisplay::colored`]; every colored token is followed by
/// `all_off`. The event-level fields (`create`, `update`, `remove`,
/// `timestamp`) are not used by the struct renderer itself — they
/// exist so a trace tool can color its line prefix from the same
/// schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColorSchema {
    pub string: &'static str,
    pub integer: &'static str,
    pub floating_point: &'static str,
    pub enum_value: &'static str,
    /// Field (property) names.
    pub attribute: &'static str,
    pub type_name: &'static str,
    pub create: &'static str,
    pub update: &'static str,
    pub remove: &'static str,
    pub timestamp: &'static str,
    /// Reset, written after every colored token.
    pub all_off: &'static str,
}

impl ColorSchema {
    /// dots-cpp's `TraceColorSchema`: strings bright magenta, numbers
    /// bright red, enums bright yellow, type names bright blue, field
    /// names white; CREATE green / UPDATE yellow / REMOVE red,
    /// timestamps gray.
    pub const TRACE: Self = Self {
        string: "\x1b[1;35m",
        integer: "\x1b[1;31m",
        floating_point: "\x1b[1;31m",
        enum_value: "\x1b[1;33m",
        attribute: "\x1b[0;37m",
        type_name: "\x1b[1;34m",
        create: "\x1b[1;32m",
        update: "\x1b[1;33m",
        remove: "\x1b[1;31m",
        timestamp: "\x1b[1;30m",
        all_off: "\x1b[0m",
    };
}

/// A [`DynamicStruct`] paired with the [`Registry`] that can resolve
/// its `any` fields, rendered with those fields expanded inline.
///
/// The rendering mirrors the bare [`DynamicStruct`] `Display`
/// (descriptor-ordered `Type{ field: value, … }`, enum variant names
/// resolved) but recurses through `any` fields — decoding the
/// contained object and printing it in place. Nested `any` (an
/// expanded object that itself holds an `any` field) expands too. An
/// unresolved contained type stays opaque.
pub struct StructDisplay<'a> {
    value: &'a DynamicStruct,
    registry: &'a Registry,
    colors: Option<&'a ColorSchema>,
}

impl<'a> StructDisplay<'a> {
    /// Plain (uncolored) rendering.
    pub fn new(registry: &'a Registry, value: &'a DynamicStruct) -> Self {
        Self {
            value,
            registry,
            colors: None,
        }
    }

    /// Like [`new`](Self::new), but wraps each rendered token in the
    /// ANSI escapes of the given [`ColorSchema`]: type names, field
    /// names, and values are colored by kind, with a reset after each
    /// token. Callers are responsible for only using this when the
    /// output is a terminal that wants colors.
    pub fn colored(
        registry: &'a Registry,
        value: &'a DynamicStruct,
        colors: &'a ColorSchema,
    ) -> Self {
        Self {
            value,
            registry,
            colors: Some(colors),
        }
    }
}

impl core::fmt::Display for StructDisplay<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt_struct(self.value, self.registry, self.colors, f)
    }
}

/// Write `text` wrapped in `pick(colors)` … `all_off`, or bare when no
/// schema is set.
fn paint(
    f: &mut core::fmt::Formatter<'_>,
    colors: Option<&ColorSchema>,
    pick: impl Fn(&ColorSchema) -> &'static str,
    text: impl core::fmt::Display,
) -> core::fmt::Result {
    match colors {
        Some(cs) => write!(f, "{}{text}{}", pick(cs), cs.all_off),
        None => write!(f, "{text}"),
    }
}

fn fmt_struct(
    s: &DynamicStruct,
    reg: &Registry,
    colors: Option<&ColorSchema>,
    f: &mut core::fmt::Formatter<'_>,
) -> core::fmt::Result {
    paint(f, colors, |c| c.type_name, &s.descriptor.name)?;
    f.write_str("{")?;
    let mut first = true;
    for prop in &s.descriptor.properties {
        if !s.valid.has(prop.tag) {
            continue;
        }
        f.write_str(if first { " " } else { ", " })?;
        first = false;
        paint(f, colors, |c| c.attribute, &prop.name)?;
        f.write_str(": ")?;
        match s.properties.iter().find(|(t, _)| *t == prop.tag).map(|(_, v)| v) {
            Some(v) => fmt_value(v, &prop.kind, reg, colors, f)?,
            None => f.write_str("?")?,
        }
    }
    if !first {
        f.write_str(" ")?;
    }
    f.write_str("}")
}

fn fmt_value(
    v: &DynamicValue,
    kind: &DynamicFieldKind,
    reg: &Registry,
    colors: Option<&ColorSchema>,
    f: &mut core::fmt::Formatter<'_>,
) -> core::fmt::Result {
    match (v, kind) {
        // Resolve enum variant names from the property's descriptor,
        // matching the bare `DynamicStruct` Display.
        (DynamicValue::Enum(tag), DynamicFieldKind::Enum(enum_desc)) => {
            match enum_desc.element_by_tag(*tag) {
                Some(elem) => paint(f, colors, |c| c.enum_value, &elem.name),
                None => paint(f, colors, |c| c.enum_value, tag),
            }
        }
        // Recurse so nested `any` inside a sub-struct also expands.
        (DynamicValue::Struct(inner), _) => fmt_struct(inner, reg, colors, f),
        (DynamicValue::Vec(items), DynamicFieldKind::Vec(inner_kind)) => {
            f.write_str("[")?;
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                fmt_value(item, inner_kind, reg, colors, f)?;
            }
            f.write_str("]")
        }
        // The point of this renderer: expand the opaque envelope.
        (DynamicValue::Any(a), _) => fmt_any(a, reg, colors, f),
        // Leaves (and any value/kind mismatch): the value's own Display,
        // colored by value kind (booleans count as integers, matching
        // dots-cpp's ascii printer).
        (other, _) => paint(
            f,
            colors,
            |c| match other {
                DynamicValue::String(_) | DynamicValue::Uuid(_) => c.string,
                DynamicValue::F32(_)
                | DynamicValue::F64(_)
                | DynamicValue::Timepoint(_)
                | DynamicValue::Duration(_) => c.floating_point,
                _ => c.integer,
            },
            other,
        ),
    }
}

fn fmt_any(
    a: &AnyObject,
    reg: &Registry,
    colors: Option<&ColorSchema>,
    f: &mut core::fmt::Formatter<'_>,
) -> core::fmt::Result {
    match reg.from_any(a) {
        Ok(DecodedAny::Dynamic(s)) => fmt_struct(&s, reg, colors, f),
        Ok(DecodedAny::Typed(any_struct)) => {
            // Project the layout-compatible value into the wire-shaped
            // form so the same registry-aware walk applies.
            fmt_struct(&DynamicStruct::from_struct_value(&any_struct), reg, colors, f)
        }
        // Unknown or undecodable contained type: keep it opaque, same
        // as the bare `Display`.
        Err(_) => write!(f, "any<{}>[{} bytes]", a.type_name(), a.payload().len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dots_rs_core::to_any;
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Ping")]
    struct Ping {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        note: Option<String>,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Envelope")]
    struct Envelope {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        payload: Option<AnyObject>,
    }

    fn envelope_wrapping(ping: &Ping) -> DynamicStruct {
        let envelope = Envelope {
            id: Some(1),
            payload: Some(to_any(ping)),
        };
        DynamicStruct::from_struct_value(&envelope)
    }

    #[test]
    fn display_expands_any_inline() {
        let registry = Registry::new();
        registry.register_struct_static(Ping::DESCRIPTOR);

        let ping = Ping {
            id: Some(42),
            note: Some("hi".into()),
        };
        let dyn_env = envelope_wrapping(&ping);

        let rendered = format!("{}", StructDisplay::new(&registry, &dyn_env));
        assert_eq!(
            rendered,
            r#"Envelope{ id: 1, payload: Ping{ id: 42, note: "hi" } }"#
        );
    }

    #[test]
    fn colored_display_wraps_tokens_in_ansi() {
        let registry = Registry::new();
        registry.register_struct_static(Ping::DESCRIPTOR);

        let ping = Ping {
            id: Some(42),
            note: Some("hi".into()),
        };
        let dyn_env = envelope_wrapping(&ping);

        let rendered = format!(
            "{}",
            StructDisplay::colored(&registry, &dyn_env, &ColorSchema::TRACE)
        );
        // Same structure as the plain rendering, with type names blue,
        // field names white, integers red, strings magenta — each token
        // reset individually.
        assert_eq!(
            rendered,
            "\x1b[1;34mEnvelope\x1b[0m{ \x1b[0;37mid\x1b[0m: \x1b[1;31m1\x1b[0m, \
             \x1b[0;37mpayload\x1b[0m: \x1b[1;34mPing\x1b[0m{ \
             \x1b[0;37mid\x1b[0m: \x1b[1;31m42\x1b[0m, \
             \x1b[0;37mnote\x1b[0m: \x1b[1;35m\"hi\"\x1b[0m } }"
        );
    }

    #[test]
    fn display_keeps_unresolved_any_opaque() {
        // Ping is not registered, so the contained object can't be
        // decoded — the renderer falls back to the opaque form.
        let registry = Registry::new();
        let ping = Ping {
            id: Some(42),
            note: Some("hi".into()),
        };
        let dyn_env = envelope_wrapping(&ping);

        let rendered = format!("{}", StructDisplay::new(&registry, &dyn_env));
        assert!(
            rendered.starts_with("Envelope{ id: 1, payload: any<Ping>["),
            "got: {rendered}"
        );
    }
}
