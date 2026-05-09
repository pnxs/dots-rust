//! Recursive-descent parser implementing the `dots.lark` grammar.
//!
//! Grammar (from dots.lark):
//!
//! ```text
//! ?start: (struct | enum | import | package)*
//! struct: [doc_comments] "struct" struct_name [options] struct_properties
//! enum:   [doc_comments] "enum"   enum_name   enum_items
//! import: "import" CNAME
//! package: "package" PACKAGE_NAME
//! struct_properties: "{" property+ "}"
//! property: [doc_comments] TAG ":" [options] type PROPERTY_NAME ";" [doc_comment]
//! options: "[" [option ("," option)*] "]"
//! option: CNAME ["=" option_value]
//! ?option_value: string | "true"i | "false"i
//! type: CNAME | vector_type
//! vector_type: "vector" "<" type ">"
//! enum_items: "{" enum_item+ "}"
//! enum_item: [doc_comment] TAG ":" CNAME ["=" INT] ","? [doc_comment]
//! ```

use core::fmt;

use crate::ast::{EnumDef, EnumItem, File, Item, Opt, OptValue, Property, PropertyType, StructDef};
use crate::lexer::{Span, Token, TokenKind};

#[derive(Debug)]
pub struct ParseError {
    pub span: Option<Span>,
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.span {
            Some(s) => write!(f, "line {} col {}: {}", s.line, s.col, self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for ParseError {}

pub fn parse(tokens: Vec<Token>) -> Result<File, ParseError> {
    let mut p = Parser::new(tokens);
    let mut items = Vec::new();
    while !p.is_eof() {
        // Capture leading doc comments — they may apply to the next
        // struct, enum, property, etc.
        let docs = p.take_doc_comments();
        if p.is_eof() {
            break;
        }
        let kind_keyword = p.expect_ident_keyword(&["struct", "enum", "import", "package"])?;
        match kind_keyword.as_str() {
            "struct" => items.push(Item::Struct(p.parse_struct(docs)?)),
            "enum" => items.push(Item::Enum(p.parse_enum(docs)?)),
            "import" => {
                let name = p.expect_ident()?;
                items.push(Item::Import { name });
            }
            "package" => {
                let name = p.parse_dotted_name()?;
                items.push(Item::Package { name });
            }
            _ => unreachable!(),
        }
    }
    Ok(File { items })
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.peek().map(|t| &t.kind)
    }

    fn bump(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let t = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(t)
        } else {
            None
        }
    }

    fn take_doc_comments(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(TokenKind::Doc(_)) = self.peek_kind() {
            if let Some(Token {
                kind: TokenKind::Doc(text),
                ..
            }) = self.bump()
            {
                out.push(text);
            }
        }
        out
    }

    fn expect_kind(&mut self, want: &TokenKind, label: &str) -> Result<Token, ParseError> {
        let span = self.peek().map(|t| t.span);
        if std::mem::discriminant(self.peek_kind().ok_or_else(|| ParseError {
            span,
            message: format!("unexpected end of file, expected {label}"),
        })?) == std::mem::discriminant(want)
        {
            Ok(self.bump().unwrap())
        } else {
            Err(ParseError {
                span,
                message: format!("expected {label}, got {:?}", self.peek_kind().unwrap()),
            })
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        let span = self.peek().map(|t| t.span);
        match self.peek_kind() {
            Some(TokenKind::Ident(_)) => match self.bump().unwrap().kind {
                TokenKind::Ident(s) => Ok(s),
                _ => unreachable!(),
            },
            other => Err(ParseError {
                span,
                message: format!("expected identifier, got {other:?}"),
            }),
        }
    }

    fn expect_ident_keyword(&mut self, allowed: &[&str]) -> Result<String, ParseError> {
        let span = self.peek().map(|t| t.span);
        let name = self.expect_ident()?;
        if allowed.iter().any(|k| *k == name) {
            Ok(name)
        } else {
            Err(ParseError {
                span,
                message: format!(
                    "expected one of {}, got `{name}`",
                    allowed
                        .iter()
                        .map(|k| format!("`{k}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            })
        }
    }

    fn expect_int(&mut self) -> Result<i64, ParseError> {
        let span = self.peek().map(|t| t.span);
        match self.peek_kind() {
            Some(TokenKind::Int(_)) => match self.bump().unwrap().kind {
                TokenKind::Int(n) => Ok(n),
                _ => unreachable!(),
            },
            other => Err(ParseError {
                span,
                message: format!("expected integer literal, got {other:?}"),
            }),
        }
    }

    fn parse_dotted_name(&mut self) -> Result<String, ParseError> {
        let mut out = self.expect_ident()?;
        while matches!(self.peek_kind(), Some(TokenKind::Dot)) {
            self.bump();
            out.push('.');
            out.push_str(&self.expect_ident()?);
        }
        Ok(out)
    }

    fn parse_struct(&mut self, doc: Vec<String>) -> Result<StructDef, ParseError> {
        let name = self.expect_ident()?;
        let options = if matches!(self.peek_kind(), Some(TokenKind::LBracket)) {
            self.parse_options()?
        } else {
            Vec::new()
        };
        self.expect_kind(&TokenKind::LBrace, "`{`")?;
        let mut properties = Vec::new();
        while !matches!(self.peek_kind(), Some(TokenKind::RBrace)) {
            properties.push(self.parse_property()?);
        }
        self.expect_kind(&TokenKind::RBrace, "`}`")?;
        Ok(StructDef {
            doc,
            name,
            options,
            properties,
        })
    }

    fn parse_property(&mut self) -> Result<Property, ParseError> {
        let doc = self.take_doc_comments();
        let tag_signed = self.expect_int()?;
        let tag: u32 = tag_signed.try_into().map_err(|_| ParseError {
            span: None,
            message: format!("property tag {tag_signed} must fit in u32"),
        })?;
        self.expect_kind(&TokenKind::Colon, "`:`")?;
        let options = if matches!(self.peek_kind(), Some(TokenKind::LBracket)) {
            self.parse_options()?
        } else {
            Vec::new()
        };
        let ty = self.parse_type()?;
        let name = self.expect_ident()?;
        self.expect_kind(&TokenKind::Semicolon, "`;`")?;
        // Optional trailing same-line doc comment.
        let trailing_doc = if matches!(self.peek_kind(), Some(TokenKind::Doc(_))) {
            self.take_doc_comments()
        } else {
            Vec::new()
        };
        Ok(Property {
            doc,
            trailing_doc,
            tag,
            options,
            ty,
            name,
        })
    }

    fn parse_type(&mut self) -> Result<PropertyType, ParseError> {
        let span = self.peek().map(|t| t.span);
        let name = self.expect_ident()?;
        if name == "vector" {
            self.expect_kind(&TokenKind::LAngle, "`<`")?;
            let inner = self.parse_type()?;
            self.expect_kind(&TokenKind::RAngle, "`>`")?;
            return Ok(PropertyType::Vector(Box::new(inner)));
        }
        if name.is_empty() {
            return Err(ParseError {
                span,
                message: "expected type name".into(),
            });
        }
        Ok(PropertyType::Named(name))
    }

    fn parse_options(&mut self) -> Result<Vec<Opt>, ParseError> {
        self.expect_kind(&TokenKind::LBracket, "`[`")?;
        let mut out = Vec::new();
        if !matches!(self.peek_kind(), Some(TokenKind::RBracket)) {
            out.push(self.parse_option()?);
            while matches!(self.peek_kind(), Some(TokenKind::Comma)) {
                self.bump();
                out.push(self.parse_option()?);
            }
        }
        self.expect_kind(&TokenKind::RBracket, "`]`")?;
        Ok(out)
    }

    fn parse_option(&mut self) -> Result<Opt, ParseError> {
        let name = self.expect_ident()?;
        if matches!(self.peek_kind(), Some(TokenKind::Eq)) {
            self.bump();
            let value = self.parse_option_value(&name)?;
            return Ok(Opt { name, value });
        }
        Ok(Opt {
            name,
            value: OptValue::Bool(true),
        })
    }

    fn parse_option_value(&mut self, opt_name: &str) -> Result<OptValue, ParseError> {
        let span = self.peek().map(|t| t.span);
        let kind_clone = self.peek_kind().cloned();
        match kind_clone {
            Some(TokenKind::Str(_)) => match self.bump().unwrap().kind {
                TokenKind::Str(s) => Ok(OptValue::Str(s)),
                _ => unreachable!(),
            },
            Some(TokenKind::Ident(s)) => {
                let bumped = self.bump().unwrap();
                match s.to_lowercase().as_str() {
                    "true" => Ok(OptValue::Bool(true)),
                    "false" => Ok(OptValue::Bool(false)),
                    _ => Err(ParseError {
                        span: Some(bumped.span),
                        message: format!(
                            "option `{opt_name}` value must be a string or true/false, got `{s}`"
                        ),
                    }),
                }
            }
            other => Err(ParseError {
                span,
                message: format!("expected option value for `{opt_name}`, got {other:?}"),
            }),
        }
    }

    fn parse_enum(&mut self, doc: Vec<String>) -> Result<EnumDef, ParseError> {
        let name = self.expect_ident()?;
        self.expect_kind(&TokenKind::LBrace, "`{`")?;
        let mut items = Vec::new();
        while !matches!(self.peek_kind(), Some(TokenKind::RBrace)) {
            items.push(self.parse_enum_item()?);
        }
        self.expect_kind(&TokenKind::RBrace, "`}`")?;
        Ok(EnumDef { doc, name, items })
    }

    fn parse_enum_item(&mut self) -> Result<EnumItem, ParseError> {
        let doc = self.take_doc_comments();
        let tag_signed = self.expect_int()?;
        let tag: u32 = tag_signed.try_into().map_err(|_| ParseError {
            span: None,
            message: format!("enum tag {tag_signed} must fit in u32"),
        })?;
        self.expect_kind(&TokenKind::Colon, "`:`")?;
        let name = self.expect_ident()?;
        let value = if matches!(self.peek_kind(), Some(TokenKind::Eq)) {
            self.bump();
            Some(self.expect_int()?)
        } else {
            None
        };
        // Optional trailing comma.
        if matches!(self.peek_kind(), Some(TokenKind::Comma)) {
            self.bump();
        }
        let trailing_doc = if matches!(self.peek_kind(), Some(TokenKind::Doc(_))) {
            self.take_doc_comments()
        } else {
            Vec::new()
        };
        Ok(EnumItem {
            doc,
            tag,
            name,
            value,
            trailing_doc,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::tokenize;

    fn parse_source(src: &str) -> File {
        parse(tokenize(src).unwrap()).unwrap()
    }

    #[test]
    fn empty_file() {
        let f = parse_source("");
        assert!(f.items.is_empty());
    }

    #[test]
    fn simple_struct() {
        let f = parse_source(
            r#"
            struct Foo {
                1: [key] uint32 id;
                2: string name;
            }
        "#,
        );
        assert_eq!(f.items.len(), 1);
        let Item::Struct(s) = &f.items[0] else {
            panic!("expected struct");
        };
        assert_eq!(s.name, "Foo");
        assert_eq!(s.properties.len(), 2);
        assert_eq!(s.properties[0].tag, 1);
        assert!(s.properties[0].is_key());
        assert!(matches!(&s.properties[0].ty, PropertyType::Named(t) if t == "uint32"));
        assert_eq!(s.properties[1].name, "name");
    }

    #[test]
    fn struct_options_and_flags() {
        let f = parse_source("struct H [internal,cached=false] { 1: string n; }");
        let Item::Struct(s) = &f.items[0] else {
            panic!()
        };
        assert!(s.is_internal());
        assert!(!s.is_cached());
    }

    #[test]
    fn struct_default_cached_true() {
        let f = parse_source("struct H [internal] { 1: string n; }");
        let Item::Struct(s) = &f.items[0] else {
            panic!()
        };
        assert!(s.is_internal());
        assert!(s.is_cached(), "default cached should be true");
    }

    #[test]
    fn vector_property() {
        let f = parse_source("struct C { 1: vector<string> names; }");
        let Item::Struct(s) = &f.items[0] else {
            panic!()
        };
        let PropertyType::Vector(inner) = &s.properties[0].ty else {
            panic!()
        };
        assert!(matches!(inner.as_ref(), PropertyType::Named(n) if n == "string"));
    }

    #[test]
    fn nested_vector() {
        let f = parse_source("struct C { 1: vector<vector<int32>> nested; }");
        let Item::Struct(s) = &f.items[0] else {
            panic!()
        };
        let PropertyType::Vector(outer) = &s.properties[0].ty else {
            panic!()
        };
        let PropertyType::Vector(inner) = outer.as_ref() else {
            panic!()
        };
        assert!(matches!(inner.as_ref(), PropertyType::Named(n) if n == "int32"));
    }

    #[test]
    fn enum_with_explicit_values() {
        let f = parse_source("enum State { 1: connecting, 2: connected = 7, 3: closed }");
        let Item::Enum(e) = &f.items[0] else {
            panic!()
        };
        assert_eq!(e.items.len(), 3);
        assert_eq!(e.items[0].tag, 1);
        assert_eq!(e.items[0].value, None);
        assert_eq!(e.items[1].value, Some(7));
        assert_eq!(e.items[2].name, "closed");
    }

    #[test]
    fn doc_comments_attach_to_items() {
        let f = parse_source(
            r#"
            /// Header docs
            /// continued
            struct DotsHeader {
                /// per-property doc
                1: [key] string name; /// trailing too
            }
        "#,
        );
        let Item::Struct(s) = &f.items[0] else {
            panic!()
        };
        assert_eq!(s.doc, vec!["Header docs".to_string(), "continued".into()]);
        assert_eq!(s.properties[0].doc, vec!["per-property doc".to_string()]);
        assert_eq!(s.properties[0].trailing_doc, vec!["trailing too".to_string()]);
    }

    #[test]
    fn import_and_package() {
        let f = parse_source("package com.example.foo\nimport Bar\nstruct S { 1: Bar b; }");
        assert_eq!(f.items.len(), 3);
        assert!(matches!(&f.items[0], Item::Package { name } if name == "com.example.foo"));
        assert!(matches!(&f.items[1], Item::Import { name } if name == "Bar"));
    }
}
