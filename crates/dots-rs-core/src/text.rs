//! Text representation of dynamic values — parsing and formatting.
//!
//! [`DynamicFieldKind::parse_value`] turns human-written text into a
//! [`DynamicValue`] of that kind, and
//! [`DynamicFieldKind::format_value`] is its inverse: the text it
//! produces parses back to an equal value. The property's kind selects
//! the target variant, so the same input string yields `U8` for a
//! `uint8` property and `I64` for an `int64` one.
//!
//! The scalar types that exist as concrete Rust types also implement
//! [`FromStr`](core::str::FromStr) — [`Timepoint`](crate::Timepoint),
//! [`Duration`](crate::Duration), [`PropertySet`](crate::PropertySet), and
//! [`Uuid`](crate::Uuid). UUID byte arrays are also supported by the helpers
//! [`uuid_from_str`] / [`uuid_to_string`].
//!
//! Nested structs and open `any` values are not parsable from text:
//! they report [`ParseErrorReason::Unsupported`].

use alloc::{
    boxed::Box,
    format,
    string::{String, ToString},
    vec::Vec,
};

use crate::{DynamicFieldKind, DynamicValue, PropertySet};

// ===== Errors =====

/// Why a text-to-value conversion failed, and for which DOTS type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseValueError {
    /// DOTS type name of the value being parsed — `"uint8"`,
    /// `"vector<string>"`, or a struct/enum name.
    pub type_name: String,
    pub reason: ParseErrorReason,
}

impl ParseValueError {
    fn new(type_name: String, reason: ParseErrorReason) -> Self {
        Self { type_name, reason }
    }

    fn malformed(type_name: impl Into<String>, detail: &'static str) -> Self {
        Self::new(type_name.into(), ParseErrorReason::Malformed(detail))
    }

    fn out_of_range(type_name: impl Into<String>) -> Self {
        Self::new(type_name.into(), ParseErrorReason::OutOfRange)
    }

    fn empty(type_name: impl Into<String>) -> Self {
        Self::new(type_name.into(), ParseErrorReason::Empty)
    }
}

/// The failure modes of [`DynamicFieldKind::parse_value`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseErrorReason {
    /// The input was empty (or whitespace only).
    Empty,
    /// The input did not match the accepted syntax; the payload names
    /// what was expected.
    Malformed(&'static str),
    /// Syntactically a number, but outside the target type's range.
    OutOfRange,
    /// No enumerator of that name, and not a valid enumerator tag.
    UnknownEnumerator,
    /// This kind cannot be built from text (`struct`, `any`).
    Unsupported,
    /// A `vector` element failed; `index` is its position.
    Element {
        index: usize,
        source: Box<ParseValueError>,
    },
}

impl core::fmt::Display for ParseValueError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "invalid {}: {}", self.type_name, self.reason)
    }
}

impl core::fmt::Display for ParseErrorReason {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => f.write_str("empty input"),
            Self::Malformed(detail) => f.write_str(detail),
            Self::OutOfRange => f.write_str("out of range"),
            Self::UnknownEnumerator => f.write_str("unknown enumerator"),
            Self::Unsupported => f.write_str("not parsable from text"),
            Self::Element { index, source } => write!(f, "element {index}: {source}"),
        }
    }
}

impl core::error::Error for ParseValueError {}

// ===== Kind-directed parsing =====

impl DynamicFieldKind {
    /// DOTS type name of this kind, as it appears in
    /// `StructDescriptorData.type` strings.
    pub fn type_name(&self) -> String {
        match self {
            Self::Bool => "bool".into(),
            Self::U8 => "uint8".into(),
            Self::U16 => "uint16".into(),
            Self::U32 => "uint32".into(),
            Self::U64 => "uint64".into(),
            Self::I8 => "int8".into(),
            Self::I16 => "int16".into(),
            Self::I32 => "int32".into(),
            Self::I64 => "int64".into(),
            Self::F32 => "float32".into(),
            Self::F64 => "float64".into(),
            Self::PropertySet => "property_set".into(),
            Self::String => "string".into(),
            Self::Uuid => "uuid".into(),
            Self::Timepoint => "timepoint".into(),
            Self::Duration => "duration".into(),
            Self::Vec(inner) => format!("vector<{}>", inner.type_name()),
            Self::Struct(d) => d.name.clone(),
            Self::Enum(d) => d.name.clone(),
            Self::Any => "any".into(),
            Self::InstanceRef => "instance_ref".into(),
            Self::InstanceRefTo(target) => format!("instance_ref<{target}>"),
        }
    }

    /// Parse `text` into a [`DynamicValue`] of this kind.
    ///
    /// Accepted syntax per kind:
    ///
    /// - `bool` — `true`/`false`/`1`/`0`, case-insensitive.
    /// - integers — decimal, or `0x`/`0b`/`0o` prefixed; a leading `-`
    ///   on an unsigned kind is [`ParseErrorReason::OutOfRange`].
    /// - `float32`/`float64` — anything `f64::from_str` accepts,
    ///   including `inf` and `NaN`.
    /// - `string` — either a `"`-quoted literal with `\"`, `\\`, `\n`,
    ///   `\r`, `\t`, `\0`, `\'` and `\u{…}` escapes, or bare text taken
    ///   verbatim after trimming.
    /// - `property_set` — `PropertySet{1,2,3}`, `0b1011`, `0x…`, or
    ///   decimal bits.
    /// - `uuid` — 32 hex digits, dashes optional, `0x` prefix optional.
    /// - `timepoint` — fractional seconds since the Unix epoch, or
    ///   `YYYY-MM-DD[T| ]HH:MM:SS[.fraction][Z|±HH:MM]`, or a bare
    ///   `YYYY-MM-DD` date (midnight). Without an offset the text is
    ///   read as UTC.
    /// - `duration` — fractional seconds, with an optional `s` suffix.
    /// - `vector<T>` — elements separated by `,`, optionally wrapped in
    ///   `{…}` or `[…]`; separators inside quotes or nested brackets
    ///   are not split on. An empty list is `{}`.
    /// - enums — an enumerator name, or an enumerator tag.
    ///
    /// Empty input is rejected for every kind; a caller treating an
    /// empty field as "property not set" decides that itself. An empty
    /// `string` value is written `""`.
    pub fn parse_value(&self, text: &str) -> Result<DynamicValue, ParseValueError> {
        let text = text.trim();
        if text.is_empty() {
            return Err(ParseValueError::empty(self.type_name()));
        }
        match self {
            Self::Bool => {
                if text.eq_ignore_ascii_case("true") || text == "1" {
                    Ok(DynamicValue::Bool(true))
                } else if text.eq_ignore_ascii_case("false") || text == "0" {
                    Ok(DynamicValue::Bool(false))
                } else {
                    Err(ParseValueError::malformed(
                        self.type_name(),
                        "expected true or false",
                    ))
                }
            }
            Self::U8 => self
                .parse_unsigned(text, u8::MAX as u128)
                .map(|v| DynamicValue::U8(v as u8)),
            Self::U16 => self
                .parse_unsigned(text, u16::MAX as u128)
                .map(|v| DynamicValue::U16(v as u16)),
            Self::U32 => self
                .parse_unsigned(text, u32::MAX as u128)
                .map(|v| DynamicValue::U32(v as u32)),
            Self::U64 => self
                .parse_unsigned(text, u64::MAX as u128)
                .map(|v| DynamicValue::U64(v as u64)),
            Self::I8 => self
                .parse_signed(text, i8::MIN as i128, i8::MAX as i128)
                .map(|v| DynamicValue::I8(v as i8)),
            Self::I16 => self
                .parse_signed(text, i16::MIN as i128, i16::MAX as i128)
                .map(|v| DynamicValue::I16(v as i16)),
            Self::I32 => self
                .parse_signed(text, i32::MIN as i128, i32::MAX as i128)
                .map(|v| DynamicValue::I32(v as i32)),
            Self::I64 => self
                .parse_signed(text, i64::MIN as i128, i64::MAX as i128)
                .map(|v| DynamicValue::I64(v as i64)),
            Self::F32 => {
                let value = text.parse::<f32>().map_err(|_| {
                    ParseValueError::malformed(self.type_name(), "expected a number")
                })?;
                // Inspect the input after validating its syntax: parsing
                // can already have rounded a numeric literal to inf/zero.
                let unsigned = text.trim_start_matches(['+', '-']);
                let explicit_infinity = unsigned.eq_ignore_ascii_case("inf")
                    || unsigned.eq_ignore_ascii_case("infinity");
                let nonzero_significand = text
                    .split(['e', 'E'])
                    .next()
                    .unwrap_or("")
                    .bytes()
                    .any(|b| matches!(b, b'1'..=b'9'));
                if (value.is_infinite() && !explicit_infinity)
                    || (value == 0.0 && nonzero_significand)
                {
                    return Err(ParseValueError::out_of_range(self.type_name()));
                }
                Ok(DynamicValue::F32(value))
            }
            Self::F64 => self.parse_float(text).map(DynamicValue::F64),
            Self::String => unescape_string(text)
                .map(DynamicValue::String)
                .map_err(|detail| ParseValueError::malformed(self.type_name(), detail)),
            Self::PropertySet => text.parse::<PropertySet>().map(DynamicValue::PropertySet),
            Self::Uuid => uuid_from_str(text).map(DynamicValue::Uuid),
            Self::Timepoint => parse_timepoint(text).map(DynamicValue::Timepoint),
            Self::Duration => parse_duration(text).map(DynamicValue::Duration),
            Self::Vec(inner) => {
                let mut items = Vec::new();
                for (index, part) in split_elements(text)?.into_iter().enumerate() {
                    let value = inner.parse_value(part).map_err(|source| {
                        ParseValueError::new(
                            self.type_name(),
                            ParseErrorReason::Element {
                                index,
                                source: Box::new(source),
                            },
                        )
                    })?;
                    items.push(value);
                }
                Ok(DynamicValue::Vec(items))
            }
            Self::Enum(descriptor) => {
                if let Some(element) = descriptor.elements.iter().find(|e| e.name == text) {
                    return Ok(DynamicValue::Enum(element.tag));
                }
                // Enum values travel as their enumerator tag, so a bare
                // integer is read as one — including tags this
                // descriptor doesn't know, which keeps values received
                // from a newer peer round-trippable.
                text.parse::<u32>().map(DynamicValue::Enum).map_err(|_| {
                    ParseValueError::new(self.type_name(), ParseErrorReason::UnknownEnumerator)
                })
            }
            Self::Struct(_) | Self::Any | Self::InstanceRef | Self::InstanceRefTo(_) => Err(
                ParseValueError::new(self.type_name(), ParseErrorReason::Unsupported),
            ),
        }
    }

    /// Render `value` as text that [`parse_value`](Self::parse_value)
    /// reads back into an equal value. The one precision caveat is
    /// `timepoint`: the ISO-8601 form carries nanoseconds, which is
    /// exact for epoch-scale instants (an `f64` ulp exceeds a
    /// nanosecond beyond ~5·10⁶ s) but rounds finer fractions of tiny
    /// magnitudes.
    ///
    /// A `value` whose variant doesn't match this kind is rendered by
    /// its own [`Display`](core::fmt::Display) impl rather than
    /// panicking.
    pub fn format_value(&self, value: &DynamicValue) -> String {
        match (self, value) {
            // Only the two kinds whose text needs the descriptor are
            // handled here; every other kind's `Display` is already
            // round-trippable.
            (Self::Vec(inner), DynamicValue::Vec(items)) => {
                if items.is_empty() {
                    return "{}".into();
                }
                let mut out = String::from("{ ");
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&inner.format_value(item));
                }
                out.push_str(" }");
                out
            }
            (Self::Enum(descriptor), DynamicValue::Enum(tag)) => {
                match descriptor.element_by_tag(*tag) {
                    Some(element) => element.name.clone(),
                    None => tag.to_string(),
                }
            }
            (_, v) => v.to_string(),
        }
    }

    fn parse_unsigned(&self, text: &str, max: u128) -> Result<u128, ParseValueError> {
        if let Some(rest) = text.strip_prefix('-') {
            // Reject the sign as a range error rather than a syntax one
            // when what follows is a number: `-1` for `uint8` is a
            // range mistake, `-x` is a typo.
            return if parse_radix_u128(rest).is_ok() {
                Err(ParseValueError::out_of_range(self.type_name()))
            } else {
                Err(ParseValueError::malformed(
                    self.type_name(),
                    "expected an integer",
                ))
            };
        }
        let value = parse_radix_u128(text)
            .map_err(|detail| ParseValueError::malformed(self.type_name(), detail))?;
        if value > max {
            return Err(ParseValueError::out_of_range(self.type_name()));
        }
        Ok(value)
    }

    fn parse_signed(&self, text: &str, min: i128, max: i128) -> Result<i128, ParseValueError> {
        let (negative, digits) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let magnitude = parse_radix_u128(digits)
            .map_err(|detail| ParseValueError::malformed(self.type_name(), detail))?;
        // `i128::MIN.unsigned_abs()` so `-2^127` stays representable.
        let limit = if negative {
            min.unsigned_abs()
        } else {
            max as u128
        };
        if magnitude > limit {
            return Err(ParseValueError::out_of_range(self.type_name()));
        }
        let value = if negative {
            (magnitude as i128).wrapping_neg()
        } else {
            magnitude as i128
        };
        if value < min || value > max {
            return Err(ParseValueError::out_of_range(self.type_name()));
        }
        Ok(value)
    }

    fn parse_float(&self, text: &str) -> Result<f64, ParseValueError> {
        text.parse::<f64>()
            .map_err(|_| ParseValueError::malformed(self.type_name(), "expected a number"))
    }
}

/// Parse an unsigned integer in decimal or with a `0x` / `0b` / `0o`
/// radix prefix. Underscores are allowed as digit separators.
fn parse_radix_u128(text: &str) -> Result<u128, &'static str> {
    let (radix, digits) = match text.get(..2) {
        Some(prefix) if prefix.eq_ignore_ascii_case("0x") => (16, &text[2..]),
        Some(prefix) if prefix.eq_ignore_ascii_case("0b") => (2, &text[2..]),
        Some(prefix) if prefix.eq_ignore_ascii_case("0o") => (8, &text[2..]),
        _ => (10, text),
    };
    let digits: String = digits.chars().filter(|c| *c != '_').collect();
    if digits.is_empty() {
        return Err("expected an integer");
    }
    u128::from_str_radix(&digits, radix).map_err(|_| "expected an integer")
}

// ===== String escaping =====

/// Read a `string` value: a `"`-quoted literal is unescaped, bare text
/// is taken verbatim.
fn unescape_string(text: &str) -> Result<String, &'static str> {
    let inner = match text.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        // A lone `"` is not an empty quoted literal.
        Some(inner) if text.len() >= 2 => inner,
        _ => return Ok(text.into()),
    };

    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('0') => out.push('\0'),
            Some('u') => {
                // `\u{HEX}`, as `{:?}` writes control characters.
                if chars.next() != Some('{') {
                    return Err("expected \\u{...}");
                }
                let mut hex = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some(digit) => hex.push(digit),
                        None => return Err("unterminated \\u{...}"),
                    }
                }
                let code = u32::from_str_radix(&hex, 16).map_err(|_| "invalid \\u{...} escape")?;
                out.push(char::from_u32(code).ok_or("invalid \\u{...} escape")?);
            }
            Some(_) => return Err("unknown escape sequence"),
            None => return Err("trailing backslash"),
        }
    }
    Ok(out)
}

// ===== property_set =====

/// Parse a `property_set` from either the `PropertySet{1,2,3}` tag-list
/// form or a raw bitmask (`0b…`, `0x…`, decimal).
///
/// Reached through [`PropertySet`]'s
/// [`FromStr`](core::str::FromStr) impl.
pub(crate) fn property_set_from_str(text: &str) -> Result<PropertySet, ParseValueError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(ParseValueError::empty("property_set"));
    }

    let tag_list = text
        .strip_prefix("PropertySet")
        .unwrap_or(text)
        .trim_start()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'));

    if let Some(tags) = tag_list {
        let mut bits = 0u32;
        for tag in tags.split(',').map(str::trim).filter(|t| !t.is_empty()) {
            let tag: u32 = tag
                .parse()
                .map_err(|_| ParseValueError::malformed("property_set", "expected a tag list"))?;
            if tag > PropertySet::MAX_TAG {
                return Err(ParseValueError::out_of_range("property_set"));
            }
            // Bits are set directly rather than through `with_tag` so
            // that bit 0 — unused by DOTS but representable, and what
            // `Debug` prints as tag `0` — round-trips instead of
            // tripping its tag assertion.
            bits |= 1u32 << tag;
        }
        return Ok(PropertySet::from_bits(bits));
    }

    let bits = parse_radix_u128(text)
        .map_err(|_| ParseValueError::malformed("property_set", "expected a bitmask"))?;
    if bits > u32::MAX as u128 {
        return Err(ParseValueError::out_of_range("property_set"));
    }
    Ok(PropertySet::from_bits(bits as u32))
}

// ===== uuid =====

/// Parse a `uuid` from 32 hex digits, with optional dashes and an
/// optional `0x` prefix.
pub fn uuid_from_str(text: &str) -> Result<[u8; 16], ParseValueError> {
    let text = text.trim();
    let body = text
        .strip_prefix("0x")
        .or_else(|| text.strip_prefix("0X"))
        .unwrap_or(text);
    let hex: String = body.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ParseValueError::malformed("uuid", "expected 32 hex digits"));
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let pair = core::str::from_utf8(chunk)
            .map_err(|_| ParseValueError::malformed("uuid", "expected 32 hex digits"))?;
        bytes[i] = u8::from_str_radix(pair, 16)
            .map_err(|_| ParseValueError::malformed("uuid", "expected 32 hex digits"))?;
    }
    Ok(bytes)
}

/// Render a `uuid` in lowercase `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` form.
pub fn uuid_to_string(bytes: &[u8; 16]) -> String {
    crate::Uuid::from_bytes(*bytes).to_string()
}

// ===== timepoint / duration =====

const SECS_PER_DAY: i64 = 86_400;

/// Parse a `timepoint`: fractional seconds since the Unix epoch, or an
/// ISO-8601 date-time read as UTC when it carries no offset.
pub fn parse_timepoint(text: &str) -> Result<f64, ParseValueError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(ParseValueError::empty("timepoint"));
    }
    if let Ok(secs) = text.parse::<f64>() {
        return Ok(secs);
    }
    parse_iso8601(text)
        .ok_or_else(|| ParseValueError::malformed("timepoint", "expected seconds or ISO-8601"))
}

/// Earliest / latest instants the ISO-8601 form covers: years
/// `0000-01-01` through `9999-12-31` (four-digit years, as the parser
/// expects). Values outside fall back to raw seconds text.
const MIN_ISO_SECS: f64 = -62_167_219_200.0;
const MAX_ISO_SECS: f64 = 253_402_300_800.0;

/// Render a `timepoint` as an ISO-8601 UTC instant, with the
/// fractional part written to nanosecond precision and trailing zeros
/// trimmed. Non-finite values and instants outside years 0000–9999
/// fall back to their float text, which [`parse_timepoint`] also
/// accepts.
pub fn format_timepoint(secs: f64) -> String {
    if !secs.is_finite() || !(MIN_ISO_SECS..MAX_ISO_SECS).contains(&secs) {
        return format!("{secs}");
    }
    let mut whole = floor(secs) as i64;
    // `(x + 0.5) as i64` truncates toward zero, which rounds a
    // non-negative fraction to nearest.
    let mut nanos = ((secs - floor(secs)) * 1e9 + 0.5) as i64;
    if nanos >= 1_000_000_000 {
        nanos -= 1_000_000_000;
        whole += 1;
    }
    let (year, month, day) = civil_from_days(whole.div_euclid(SECS_PER_DAY));
    let sod = whole.rem_euclid(SECS_PER_DAY);
    let (hour, minute, second) = (sod / 3600, (sod % 3600) / 60, sod % 60);

    let mut out = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if nanos != 0 {
        let fraction = format!("{nanos:09}");
        out.push('.');
        out.push_str(fraction.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Parse a `duration` in fractional seconds, with an optional `s`
/// suffix.
pub fn parse_duration(text: &str) -> Result<f64, ParseValueError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(ParseValueError::empty("duration"));
    }
    let body = text.strip_suffix('s').unwrap_or(text).trim_end();
    body.parse::<f64>()
        .map_err(|_| ParseValueError::malformed("duration", "expected seconds"))
}

/// Render a `duration` as fractional seconds with an `s` suffix.
pub fn format_duration(secs: f64) -> String {
    format!("{secs}s")
}

/// `f64::floor` is std-only; the timestamps handled here are far
/// inside `i64` range, so truncate-and-adjust is equivalent.
fn floor(v: f64) -> f64 {
    let truncated = v as i64 as f64;
    if v < truncated {
        truncated - 1.0
    } else {
        truncated
    }
}

/// `YYYY-MM-DD[T| ]HH:MM:SS[.fraction][Z|±HH:MM]`, or a bare
/// `YYYY-MM-DD`. Returns seconds since the Unix epoch.
fn parse_iso8601(text: &str) -> Option<f64> {
    let bytes = text.as_bytes();
    // Byte offsets are used throughout, so non-ASCII input is rejected
    // up front rather than risking a char-boundary panic.
    if bytes.len() < 10 || !text.is_ascii() {
        return None;
    }
    let year: i64 = read_int(&text[0..4])?;
    if bytes[4] != b'-' {
        return None;
    }
    let month: i64 = read_int(&text[5..7])?;
    if bytes[7] != b'-' {
        return None;
    }
    let day: i64 = read_int(&text[8..10])?;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }

    let mut rest = &text[10..];
    let (mut hour, mut minute, mut second) = (0i64, 0i64, 0i64);
    // Fractional-second digits, kept as text so the final value is one
    // correctly rounded decimal-to-float conversion.
    let mut fraction_digits = "";

    if !rest.is_empty() {
        let separator = rest.as_bytes()[0];
        if !matches!(separator, b'T' | b't' | b' ') {
            return None;
        }
        rest = &rest[1..];
        if rest.len() < 5 {
            return None;
        }
        hour = read_int(&rest[0..2])?;
        if rest.as_bytes()[2] != b':' {
            return None;
        }
        minute = read_int(&rest[3..5])?;
        rest = &rest[5..];

        if rest.starts_with(':') {
            if rest.len() < 3 {
                return None;
            }
            second = read_int(&rest[1..3])?;
            rest = &rest[3..];
        }
        if rest.starts_with('.') || rest.starts_with(',') {
            let digit_count = rest[1..].bytes().take_while(u8::is_ascii_digit).count();
            if digit_count == 0 {
                return None;
            }
            fraction_digits = &rest[1..1 + digit_count];
            rest = &rest[1 + digit_count..];
        }
        if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=60).contains(&second) {
            return None;
        }
    }

    // Offset: `Z`, `±HH`, `±HHMM` or `±HH:MM`. Absent means UTC.
    let mut offset_secs = 0i64;
    let rest = rest.trim();
    if !rest.is_empty() {
        if rest.eq_ignore_ascii_case("z") {
            // UTC.
        } else {
            let sign = match rest.as_bytes()[0] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            let body = &rest[1..];
            let (offset_hours, offset_minutes) = match body.len() {
                2 => (read_int(body)?, 0),
                4 => (read_int(&body[0..2])?, read_int(&body[2..4])?),
                5 if body.as_bytes()[2] == b':' => (read_int(&body[0..2])?, read_int(&body[3..5])?),
                _ => return None,
            };
            if !(0..=23).contains(&offset_hours) || !(0..=59).contains(&offset_minutes) {
                return None;
            }
            offset_secs = sign * (offset_hours * 3600 + offset_minutes * 60);
        }
    }

    let days = days_from_civil(year, month, day);
    let whole = days * SECS_PER_DAY + hour * 3600 + minute * 60 + second - offset_secs;
    Some(seconds_from_parts(whole, fraction_digits))
}

/// Combine whole seconds and fractional digits into an `f64` through a
/// single decimal-to-float conversion, so the result is the correctly
/// rounded value of the written decimal — also for negative instants,
/// where `whole` (floored) and the fraction have opposite signs and a
/// naive `whole + fraction` would round twice.
fn seconds_from_parts(whole: i64, fraction_digits: &str) -> f64 {
    // Beyond 18 digits the fraction is far below f64 resolution.
    let digits = &fraction_digits[..fraction_digits.len().min(18)];
    let fraction: u64 = digits.parse().unwrap_or(0);
    if fraction == 0 {
        return whole as f64;
    }
    let text = if whole >= 0 {
        format!("{whole}.{digits}")
    } else {
        // -1 + 0.7 == -(0 + 0.3): borrow one second and complement the
        // fraction within its digit count.
        let scale = 10u64.pow(digits.len() as u32);
        format!(
            "-{}.{:0width$}",
            -(whole + 1),
            scale - fraction,
            width = digits.len()
        )
    };
    text.parse().unwrap_or(whole as f64)
}

/// Days in `month` of the proleptic-Gregorian `year`.
const fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap { 29 } else { 28 }
        }
    }
}

/// All-ASCII-digit string to integer, rejecting anything else (unlike
/// `str::parse`, which accepts signs and whitespace).
fn read_int(text: &str) -> Option<i64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Days since 1970-01-01 for a proleptic-Gregorian date (Hinnant's
/// `days_from_civil`).
const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Inverse of [`days_from_civil`] (Hinnant's `civil_from_days`).
const fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

// ===== vector elements =====

/// Split a `vector` literal into its element texts.
///
/// The optional `{…}` / `[…]` wrapper is stripped, then commas at
/// nesting depth zero and outside string literals separate elements —
/// so `{ "a,b", "c" }` is two elements, not three.
fn split_elements(text: &str) -> Result<Vec<&str>, ParseValueError> {
    let body = match (text.strip_prefix('{'), text.strip_prefix('[')) {
        (Some(inner), _) => inner
            .strip_suffix('}')
            .ok_or_else(|| ParseValueError::malformed("vector", "unbalanced braces"))?,
        (_, Some(inner)) => inner
            .strip_suffix(']')
            .ok_or_else(|| ParseValueError::malformed("vector", "unbalanced brackets"))?,
        _ => text,
    };
    if body.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut start = 0usize;
    for (index, c) in body.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            '{' | '[' | '(' if !in_string => depth += 1,
            '}' | ']' | ')' if !in_string => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| ParseValueError::malformed("vector", "unbalanced braces"))?;
            }
            ',' if !in_string && depth == 0 => {
                parts.push(body[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    if in_string {
        return Err(ParseValueError::malformed("vector", "unterminated string"));
    }
    if depth != 0 {
        return Err(ParseValueError::malformed("vector", "unbalanced braces"));
    }
    parts.push(body[start..].trim());
    // A trailing separator (`{ 1, 2, }`) is not an empty element.
    if parts.last().is_some_and(|last| last.is_empty()) {
        parts.pop();
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use alloc::{sync::Arc, vec};

    use super::*;
    use crate::{DynamicEnumDescriptor, DynamicEnumElement, DynamicStructDescriptor, StructFlags};

    fn mood() -> DynamicFieldKind {
        DynamicFieldKind::Enum(Arc::new(DynamicEnumDescriptor {
            name: "Mood".into(),
            elements: vec![
                DynamicEnumElement {
                    name: "Happy".into(),
                    tag: 1,
                    value: 0,
                },
                DynamicEnumElement {
                    name: "Grumpy".into(),
                    tag: 2,
                    value: 7,
                },
            ],
        }))
    }

    #[test]
    fn parses_scalars() {
        assert_eq!(
            DynamicFieldKind::Bool.parse_value("TRUE").unwrap(),
            DynamicValue::Bool(true)
        );
        assert_eq!(
            DynamicFieldKind::Bool.parse_value("0").unwrap(),
            DynamicValue::Bool(false)
        );
        assert_eq!(
            DynamicFieldKind::U8.parse_value("255").unwrap(),
            DynamicValue::U8(255)
        );
        assert_eq!(
            DynamicFieldKind::U32.parse_value("0xff").unwrap(),
            DynamicValue::U32(255)
        );
        assert_eq!(
            DynamicFieldKind::I16.parse_value("-42").unwrap(),
            DynamicValue::I16(-42)
        );
        assert_eq!(
            DynamicFieldKind::I64
                .parse_value("-9223372036854775808")
                .unwrap(),
            DynamicValue::I64(i64::MIN)
        );
        assert_eq!(
            DynamicFieldKind::F32.parse_value("1.5").unwrap(),
            DynamicValue::F32(1.5)
        );
        assert_eq!(
            DynamicFieldKind::F64.parse_value("1e3").unwrap(),
            DynamicValue::F64(1000.0)
        );
    }

    #[test]
    fn float32_rejects_numeric_overflow_and_underflow() {
        for text in ["1e40", "1e400", "1e-50", "1e-400", "1E400", "1E-400"] {
            for sign in ["", "+", "-"] {
                let input = format!("{sign}{text}");
                assert_eq!(
                    DynamicFieldKind::F32
                        .parse_value(&input)
                        .unwrap_err()
                        .reason,
                    ParseErrorReason::OutOfRange,
                    "{input}"
                );
            }
        }
    }

    #[test]
    fn float32_preserves_special_values_and_range_boundaries() {
        for (text, expected) in [
            ("inf", f32::INFINITY),
            ("+INFINITY", f32::INFINITY),
            ("-Inf", f32::NEG_INFINITY),
            ("0e400", 0.0),
            ("+0.000E-400", 0.0),
            ("-0e-400", -0.0),
        ] {
            let DynamicValue::F32(value) = DynamicFieldKind::F32.parse_value(text).unwrap() else {
                panic!("expected float32");
            };
            assert_eq!(value.to_bits(), expected.to_bits(), "{text}");
        }
        for text in ["NaN", "+nan", "-NAN"] {
            let DynamicValue::F32(value) = DynamicFieldKind::F32.parse_value(text).unwrap() else {
                panic!("expected float32");
            };
            assert!(value.is_nan());
        }
        for value in [f32::MAX, f32::MIN, f32::MIN_POSITIVE, f32::from_bits(1)] {
            assert_eq!(
                DynamicFieldKind::F32
                    .parse_value(&value.to_string())
                    .unwrap(),
                DynamicValue::F32(value)
            );
        }
    }

    #[test]
    fn parses_strings_quoted_and_bare() {
        assert_eq!(
            DynamicFieldKind::String.parse_value("hello").unwrap(),
            DynamicValue::String("hello".into())
        );
        assert_eq!(
            DynamicFieldKind::String.parse_value(r#""""#).unwrap(),
            DynamicValue::String("".into())
        );
        assert_eq!(
            DynamicFieldKind::String
                .parse_value(r#""a\"b\\c\nd\u{1b}""#)
                .unwrap(),
            DynamicValue::String("a\"b\\c\nd\u{1b}".into())
        );
    }

    #[test]
    fn parses_property_set_forms() {
        // Bits 0, 1 and 3 — bit 0 is unused by DOTS but representable,
        // and every notation below must agree on it.
        let expected = DynamicValue::PropertySet(PropertySet::from_bits(0b1011));
        for text in ["PropertySet{0,1,3}", "0b1011", "0xb", "11"] {
            assert_eq!(
                DynamicFieldKind::PropertySet.parse_value(text).unwrap(),
                expected,
                "{text}"
            );
        }
        assert_eq!(
            DynamicFieldKind::PropertySet
                .parse_value("PropertySet{}")
                .unwrap(),
            DynamicValue::PropertySet(PropertySet::EMPTY)
        );
    }

    #[test]
    fn parses_uuid_in_every_notation() {
        let expected = DynamicValue::Uuid([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef,
        ]);
        for text in [
            "0123456789abcdef0123456789abcdef",
            "01234567-89ab-cdef-0123-456789abcdef",
            "0x0123456789abcdef0123456789abcdef",
        ] {
            assert_eq!(
                DynamicFieldKind::Uuid.parse_value(text).unwrap(),
                expected,
                "{text}"
            );
        }
    }

    /// dots-cpp's `timePoint1`: "2020-03-11T21:07:57.500+00:00" is
    /// 1583960877.5 seconds since the epoch.
    #[test]
    fn parses_timepoint_forms() {
        for text in [
            "1583960877.5",
            "2020-03-11T21:07:57.500Z",
            "2020-03-11 21:07:57.5",
            "2020-03-11T22:07:57.5+01:00",
            "2020-03-11T20:07:57.5-0100",
        ] {
            assert_eq!(
                DynamicFieldKind::Timepoint.parse_value(text).unwrap(),
                DynamicValue::Timepoint(1583960877.5),
                "{text}"
            );
        }
        assert_eq!(
            DynamicFieldKind::Timepoint
                .parse_value("1970-01-01")
                .unwrap(),
            DynamicValue::Timepoint(0.0)
        );
        assert_eq!(
            DynamicFieldKind::Timepoint
                .parse_value("1969-12-31T23:59:59Z")
                .unwrap(),
            DynamicValue::Timepoint(-1.0)
        );
    }

    #[test]
    fn parses_duration_with_and_without_suffix() {
        assert_eq!(
            DynamicFieldKind::Duration.parse_value("123.456s").unwrap(),
            DynamicValue::Duration(123.456)
        );
        assert_eq!(
            DynamicFieldKind::Duration.parse_value("123.456").unwrap(),
            DynamicValue::Duration(123.456)
        );
    }

    #[test]
    fn parses_vectors_without_splitting_inside_strings() {
        let kind = DynamicFieldKind::Vec(Box::new(DynamicFieldKind::String));
        assert_eq!(
            kind.parse_value(r#"{ "a,b", "c" }"#).unwrap(),
            DynamicValue::Vec(vec![
                DynamicValue::String("a,b".into()),
                DynamicValue::String("c".into()),
            ])
        );

        let ints = DynamicFieldKind::Vec(Box::new(DynamicFieldKind::U8));
        assert_eq!(
            ints.parse_value("{ 1, 2, 3 }").unwrap(),
            DynamicValue::Vec(vec![
                DynamicValue::U8(1),
                DynamicValue::U8(2),
                DynamicValue::U8(3)
            ])
        );
        assert_eq!(ints.parse_value("{}").unwrap(), DynamicValue::Vec(vec![]));
        assert_eq!(
            ints.parse_value("1,2").unwrap(),
            DynamicValue::Vec(vec![DynamicValue::U8(1), DynamicValue::U8(2)])
        );
        assert_eq!(
            ints.parse_value("{ 1, 2, }").unwrap(),
            DynamicValue::Vec(vec![DynamicValue::U8(1), DynamicValue::U8(2)])
        );
    }

    #[test]
    fn parses_enum_by_name_and_tag() {
        assert_eq!(mood().parse_value("Grumpy").unwrap(), DynamicValue::Enum(2));
        assert_eq!(mood().parse_value("2").unwrap(), DynamicValue::Enum(2));
        // Tags this descriptor doesn't know still round-trip.
        assert_eq!(mood().parse_value("9").unwrap(), DynamicValue::Enum(9));
        assert_eq!(
            mood().parse_value("Sad").unwrap_err().reason,
            ParseErrorReason::UnknownEnumerator
        );
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(
            DynamicFieldKind::U8.parse_value("   ").unwrap_err().reason,
            ParseErrorReason::Empty
        );
        assert_eq!(
            DynamicFieldKind::U8.parse_value("abc").unwrap_err().reason,
            ParseErrorReason::Malformed("expected an integer")
        );
        assert_eq!(
            DynamicFieldKind::U8.parse_value("300").unwrap_err().reason,
            ParseErrorReason::OutOfRange
        );
        assert_eq!(
            DynamicFieldKind::U8.parse_value("-1").unwrap_err().reason,
            ParseErrorReason::OutOfRange
        );
        assert_eq!(
            DynamicFieldKind::I8.parse_value("128").unwrap_err().reason,
            ParseErrorReason::OutOfRange
        );
        assert_eq!(
            DynamicFieldKind::Bool
                .parse_value("maybe")
                .unwrap_err()
                .reason,
            ParseErrorReason::Malformed("expected true or false")
        );
        assert_eq!(
            DynamicFieldKind::Uuid
                .parse_value("beef")
                .unwrap_err()
                .reason,
            ParseErrorReason::Malformed("expected 32 hex digits")
        );
        for bad_date in [
            "2020-13-01T00:00:00Z",
            "2021-02-30",
            "2021-04-31T00:00:00Z",
            "2100-02-29", // not a leap year (century rule)
        ] {
            assert_eq!(
                DynamicFieldKind::Timepoint
                    .parse_value(bad_date)
                    .unwrap_err()
                    .reason,
                ParseErrorReason::Malformed("expected seconds or ISO-8601"),
                "{bad_date}"
            );
        }
        // Leap days that do exist.
        assert!(
            DynamicFieldKind::Timepoint
                .parse_value("2020-02-29")
                .is_ok()
        );
        assert!(
            DynamicFieldKind::Timepoint
                .parse_value("2000-02-29")
                .is_ok()
        );
        assert_eq!(
            DynamicFieldKind::F32
                .parse_value("1e39")
                .unwrap_err()
                .reason,
            ParseErrorReason::OutOfRange
        );
        assert_eq!(
            DynamicFieldKind::F32
                .parse_value("1e-50")
                .unwrap_err()
                .reason,
            ParseErrorReason::OutOfRange
        );
        assert_eq!(
            DynamicFieldKind::Any
                .parse_value("any<Ping>")
                .unwrap_err()
                .reason,
            ParseErrorReason::Unsupported
        );
        let struct_kind = DynamicFieldKind::Struct(Arc::new(DynamicStructDescriptor {
            name: "Ping".into(),
            flags: StructFlags::NONE,
            properties: vec![],
        }));
        let err = struct_kind.parse_value("Ping{ id: 1 }").unwrap_err();
        assert_eq!(err.reason, ParseErrorReason::Unsupported);
        assert_eq!(err.to_string(), "invalid Ping: not parsable from text");
    }

    #[test]
    fn element_errors_carry_index_and_context() {
        let kind = DynamicFieldKind::Vec(Box::new(DynamicFieldKind::U8));
        let err = kind.parse_value("{ 1, 2, 300 }").unwrap_err();
        assert_eq!(err.type_name, "vector<uint8>");
        assert_eq!(
            err.reason,
            ParseErrorReason::Element {
                index: 2,
                source: Box::new(ParseValueError::out_of_range("uint8")),
            }
        );
        assert_eq!(
            err.to_string(),
            "invalid vector<uint8>: element 2: invalid uint8: out of range"
        );
    }

    #[test]
    fn formats_timepoints_and_durations() {
        assert_eq!(format_timepoint(1583960877.5), "2020-03-11T21:07:57.5Z");
        assert_eq!(format_timepoint(0.0), "1970-01-01T00:00:00Z");
        assert_eq!(format_timepoint(-1.0), "1969-12-31T23:59:59Z");
        assert_eq!(format_duration(123.456), "123.456s");
        // Outside years 0000–9999 (and far beyond i64) the text form
        // falls back to raw seconds, which parses back unchanged.
        for out_of_calendar in [1e19, -1e19, 1e300, 253_402_300_800.0, -62_167_219_201.0] {
            let text = format_timepoint(out_of_calendar);
            assert_eq!(text, format!("{out_of_calendar}"));
            assert_eq!(parse_timepoint(&text).unwrap(), out_of_calendar);
        }
        assert_eq!(format_timepoint(253_402_300_799.0), "9999-12-31T23:59:59Z");
    }

    /// Negative instants split into a floored second and a positive
    /// fraction; reassembly must not round twice.
    #[test]
    fn negative_fractional_timepoints_round_trip() {
        for secs in [-0.3, -0.7, -1.1, -86_401.75, -1583960877.123_456_7] {
            let text = format_timepoint(secs);
            assert_eq!(parse_timepoint(&text).unwrap(), secs, "{text}");
        }
    }

    /// Every kind's `format_value` output must parse back to the same
    /// value, bit for bit (timepoints at epoch scale, where nanosecond
    /// text is finer than an `f64` ulp).
    #[test]
    fn round_trips_every_kind() {
        let cases: alloc::vec::Vec<(DynamicFieldKind, DynamicValue)> = vec![
            (DynamicFieldKind::Bool, DynamicValue::Bool(true)),
            (DynamicFieldKind::U8, DynamicValue::U8(200)),
            (DynamicFieldKind::U16, DynamicValue::U16(60_000)),
            (DynamicFieldKind::U32, DynamicValue::U32(u32::MAX)),
            (DynamicFieldKind::U64, DynamicValue::U64(u64::MAX)),
            (DynamicFieldKind::I8, DynamicValue::I8(i8::MIN)),
            (DynamicFieldKind::I16, DynamicValue::I16(-30_000)),
            (DynamicFieldKind::I32, DynamicValue::I32(i32::MIN)),
            (DynamicFieldKind::I64, DynamicValue::I64(i64::MIN)),
            (DynamicFieldKind::F32, DynamicValue::F32(-1.25e-7)),
            (
                DynamicFieldKind::F64,
                DynamicValue::F64(core::f64::consts::PI),
            ),
            (
                DynamicFieldKind::String,
                DynamicValue::String("a\"b\\c\nd, e\u{1b}".into()),
            ),
            (
                DynamicFieldKind::PropertySet,
                DynamicValue::PropertySet(PropertySet::EMPTY.with_tag(1).with_tag(31)),
            ),
            (DynamicFieldKind::Uuid, DynamicValue::Uuid([0xab; 16])),
            (
                DynamicFieldKind::Timepoint,
                DynamicValue::Timepoint(1583960877.5),
            ),
            (
                DynamicFieldKind::Timepoint,
                // Sub-microsecond fraction: the printed nanosecond
                // digits must recover the same f64 bit pattern.
                DynamicValue::Timepoint(1_583_960_877.123_456_7),
            ),
            (
                DynamicFieldKind::Timepoint,
                DynamicValue::Timepoint(-86_401.75),
            ),
            (DynamicFieldKind::Duration, DynamicValue::Duration(342.073)),
            (
                DynamicFieldKind::Vec(Box::new(DynamicFieldKind::String)),
                DynamicValue::Vec(vec![
                    DynamicValue::String("a,b".into()),
                    DynamicValue::String("}".into()),
                ]),
            ),
            (
                DynamicFieldKind::Vec(Box::new(DynamicFieldKind::U8)),
                DynamicValue::Vec(vec![]),
            ),
            (mood(), DynamicValue::Enum(2)),
            (mood(), DynamicValue::Enum(9)),
        ];

        for (kind, value) in cases {
            let text = kind.format_value(&value);
            let parsed = kind
                .parse_value(&text)
                .unwrap_or_else(|e| panic!("{text:?} ({}): {e}", kind.type_name()));
            assert_eq!(parsed, value, "round-trip via {text:?}");
        }
    }

    #[test]
    fn type_names_match_dots_source_spelling() {
        assert_eq!(DynamicFieldKind::U8.type_name(), "uint8");
        assert_eq!(DynamicFieldKind::F64.type_name(), "float64");
        assert_eq!(DynamicFieldKind::PropertySet.type_name(), "property_set");
        assert_eq!(
            DynamicFieldKind::Vec(Box::new(DynamicFieldKind::Vec(Box::new(
                DynamicFieldKind::String
            ))))
            .type_name(),
            "vector<vector<string>>"
        );
        assert_eq!(mood().type_name(), "Mood");
    }
}
