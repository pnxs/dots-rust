//! Hand-rolled lexer for `.dots` source.
//!
//! Implements the terminal rules of `dots.lark`:
//! - keywords: `struct`, `enum`, `import`, `package`, `vector`, `true`,
//!   `false`
//! - `CNAME` identifiers
//! - `INT` literals
//! - `ESCAPED_STRING` (only used inside option values)
//! - `PACKAGE_NAME` — a dotted CNAME-like, lexed as identifier with
//!   embedded dots; here we just lex `.` as punctuation and let the
//!   parser stitch them together
//! - `///` and `//<` doc comments — captured with their text content
//! - `//` line comments and `/* */` block comments — skipped
//! - punctuation: `{ } [ ] < > : ; , =`

use core::fmt;

/// Token + the source span it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

/// Half-open byte range into the original source string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
    pub line: u32,
    pub col: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// A bare identifier — keywords are returned as `Ident("struct")`
    /// etc. and disambiguated by the parser. Simpler than maintaining
    /// a keyword enum since the keyword set is small and contextual.
    Ident(String),
    /// Decimal integer literal.
    Int(i64),
    /// `"..."` string literal with escapes resolved.
    Str(String),
    /// `///` or `//<` doc-comment text, with the marker stripped.
    Doc(String),
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `<`
    LAngle,
    /// `>`
    RAngle,
    /// `:`
    Colon,
    /// `;`
    Semicolon,
    /// `,`
    Comma,
    /// `=`
    Eq,
    /// `.` — used inside `package` dotted names.
    Dot,
}

/// Errors produced during lexing.
#[derive(Debug)]
pub struct LexError {
    pub span: Span,
    pub kind: LexErrorKind,
}

#[derive(Debug)]
pub enum LexErrorKind {
    UnexpectedChar(char),
    UnterminatedString,
    UnterminatedBlockComment,
    InvalidEscape(char),
    BadInteger(String),
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {} col {}: ", self.span.line, self.span.col)?;
        match &self.kind {
            LexErrorKind::UnexpectedChar(c) => write!(f, "unexpected character {c:?}"),
            LexErrorKind::UnterminatedString => f.write_str("unterminated string literal"),
            LexErrorKind::UnterminatedBlockComment => f.write_str("unterminated /* */ block comment"),
            LexErrorKind::InvalidEscape(c) => write!(f, "invalid escape \\{c}"),
            LexErrorKind::BadInteger(s) => write!(f, "invalid integer literal {s:?}"),
        }
    }
}

impl std::error::Error for LexError {}

/// Tokenize a complete `.dots` source. Returns the full token stream
/// (no `Eof` sentinel; the parser handles end-of-input by checking
/// length).
pub fn tokenize(source: &str) -> Result<Vec<Token>, LexError> {
    let mut lex = Lexer::new(source);
    let mut tokens = Vec::new();
    while let Some(tok) = lex.next_token()? {
        tokens.push(tok);
    }
    Ok(tokens)
}

struct Lexer<'a> {
    source: &'a str,
    bytes: &'a [u8],
    pos: usize,
    line: u32,
    col: u32,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            bytes: source.as_bytes(),
            pos: 0,
            line: 1,
            col: 1,
        }
    }

    fn next_token(&mut self) -> Result<Option<Token>, LexError> {
        loop {
            self.skip_whitespace();
            // Comments.
            if self.starts_with("///") || self.starts_with("//<") {
                let tok = self.read_doc_comment();
                return Ok(Some(tok));
            }
            if self.starts_with("//") {
                self.skip_line_comment();
                continue;
            }
            if self.starts_with("/*") {
                self.skip_block_comment()?;
                continue;
            }
            break;
        }

        if self.pos >= self.bytes.len() {
            return Ok(None);
        }

        let start_pos = self.pos;
        let start_line = self.line;
        let start_col = self.col;
        let c = self.bytes[self.pos] as char;

        // Single-char punctuation.
        let punct = match c {
            '{' => Some(TokenKind::LBrace),
            '}' => Some(TokenKind::RBrace),
            '[' => Some(TokenKind::LBracket),
            ']' => Some(TokenKind::RBracket),
            '<' => Some(TokenKind::LAngle),
            '>' => Some(TokenKind::RAngle),
            ':' => Some(TokenKind::Colon),
            ';' => Some(TokenKind::Semicolon),
            ',' => Some(TokenKind::Comma),
            '=' => Some(TokenKind::Eq),
            '.' => Some(TokenKind::Dot),
            _ => None,
        };
        if let Some(kind) = punct {
            self.advance();
            return Ok(Some(Token {
                kind,
                span: Span {
                    start: start_pos,
                    end: self.pos,
                    line: start_line,
                    col: start_col,
                },
            }));
        }

        // String literal.
        if c == '"' {
            return Ok(Some(self.read_string()?));
        }

        // Identifier or keyword.
        if is_ident_start(c) {
            return Ok(Some(self.read_ident(start_pos, start_line, start_col)));
        }

        // Integer.
        if c.is_ascii_digit() || (c == '-' && self.peek(1).is_some_and(|b| b.is_ascii_digit())) {
            return Ok(Some(
                self.read_int(start_pos, start_line, start_col)?,
            ));
        }

        Err(LexError {
            span: Span {
                start: start_pos,
                end: start_pos + c.len_utf8(),
                line: start_line,
                col: start_col,
            },
            kind: LexErrorKind::UnexpectedChar(c),
        })
    }

    // ----- helpers -----

    fn starts_with(&self, prefix: &str) -> bool {
        self.bytes[self.pos..].starts_with(prefix.as_bytes())
    }

    fn peek(&self, offset: usize) -> Option<char> {
        self.bytes.get(self.pos + offset).map(|&b| b as char)
    }

    fn advance(&mut self) {
        if self.pos < self.bytes.len() {
            if self.bytes[self.pos] == b'\n' {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
            self.pos += 1;
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.bytes.len() {
            let c = self.bytes[self.pos] as char;
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn skip_line_comment(&mut self) {
        // Already at "//"; consume until newline.
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
            self.advance();
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), LexError> {
        let start_pos = self.pos;
        let start_line = self.line;
        let start_col = self.col;
        // Consume "/*"
        self.advance();
        self.advance();
        loop {
            if self.pos + 1 >= self.bytes.len() {
                return Err(LexError {
                    span: Span {
                        start: start_pos,
                        end: self.pos,
                        line: start_line,
                        col: start_col,
                    },
                    kind: LexErrorKind::UnterminatedBlockComment,
                });
            }
            if self.bytes[self.pos] == b'*' && self.bytes[self.pos + 1] == b'/' {
                self.advance();
                self.advance();
                return Ok(());
            }
            self.advance();
        }
    }

    fn read_doc_comment(&mut self) -> Token {
        let start_pos = self.pos;
        let start_line = self.line;
        let start_col = self.col;
        // Consume "///" or "//<"
        self.advance();
        self.advance();
        self.advance();
        // Optional single space after the marker — common style.
        if self.peek(0) == Some(' ') {
            self.advance();
        }
        let text_start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
            self.advance();
        }
        let text = self.source[text_start..self.pos].trim_end().to_string();
        Token {
            kind: TokenKind::Doc(text),
            span: Span {
                start: start_pos,
                end: self.pos,
                line: start_line,
                col: start_col,
            },
        }
    }

    fn read_string(&mut self) -> Result<Token, LexError> {
        let start_pos = self.pos;
        let start_line = self.line;
        let start_col = self.col;
        self.advance(); // opening "
        let mut out = String::new();
        loop {
            if self.pos >= self.bytes.len() {
                return Err(LexError {
                    span: Span {
                        start: start_pos,
                        end: self.pos,
                        line: start_line,
                        col: start_col,
                    },
                    kind: LexErrorKind::UnterminatedString,
                });
            }
            let c = self.bytes[self.pos] as char;
            if c == '"' {
                self.advance();
                return Ok(Token {
                    kind: TokenKind::Str(out),
                    span: Span {
                        start: start_pos,
                        end: self.pos,
                        line: start_line,
                        col: start_col,
                    },
                });
            }
            if c == '\\' {
                self.advance();
                let escape_line = self.line;
                let escape_col = self.col;
                let escape = self.bytes.get(self.pos).copied().unwrap_or(0) as char;
                let resolved = match escape {
                    '\\' => '\\',
                    '"' => '"',
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '0' => '\0',
                    other => {
                        return Err(LexError {
                            span: Span {
                                start: self.pos,
                                end: self.pos + 1,
                                line: escape_line,
                                col: escape_col,
                            },
                            kind: LexErrorKind::InvalidEscape(other),
                        });
                    }
                };
                out.push(resolved);
                self.advance();
            } else {
                out.push(c);
                self.advance();
            }
        }
    }

    fn read_ident(&mut self, start_pos: usize, start_line: u32, start_col: u32) -> Token {
        while self.pos < self.bytes.len() {
            let c = self.bytes[self.pos] as char;
            if is_ident_continue(c) {
                self.advance();
            } else {
                break;
            }
        }
        let text = self.source[start_pos..self.pos].to_string();
        Token {
            kind: TokenKind::Ident(text),
            span: Span {
                start: start_pos,
                end: self.pos,
                line: start_line,
                col: start_col,
            },
        }
    }

    fn read_int(
        &mut self,
        start_pos: usize,
        start_line: u32,
        start_col: u32,
    ) -> Result<Token, LexError> {
        if self.bytes[self.pos] == b'-' {
            self.advance();
        }
        while self.pos < self.bytes.len() && (self.bytes[self.pos] as char).is_ascii_digit() {
            self.advance();
        }
        let raw = &self.source[start_pos..self.pos];
        let value = raw.parse::<i64>().map_err(|_| LexError {
            span: Span {
                start: start_pos,
                end: self.pos,
                line: start_line,
                col: start_col,
            },
            kind: LexErrorKind::BadInteger(raw.into()),
        })?;
        Ok(Token {
            kind: TokenKind::Int(value),
            span: Span {
                start: start_pos,
                end: self.pos,
                line: start_line,
                col: start_col,
            },
        })
    }
}

fn is_ident_start(c: char) -> bool {
    c == '_' || c.is_ascii_alphabetic()
}

fn is_ident_continue(c: char) -> bool {
    c == '_' || c.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<TokenKind> {
        tokenize(src).unwrap().into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn punctuation_and_keywords() {
        let toks = kinds("struct Foo { 1: int32 x; }");
        assert_eq!(
            toks,
            vec![
                TokenKind::Ident("struct".into()),
                TokenKind::Ident("Foo".into()),
                TokenKind::LBrace,
                TokenKind::Int(1),
                TokenKind::Colon,
                TokenKind::Ident("int32".into()),
                TokenKind::Ident("x".into()),
                TokenKind::Semicolon,
                TokenKind::RBrace,
            ]
        );
    }

    #[test]
    fn doc_comments_kept_separately_from_regular() {
        let src = "/// docs only here\n// regular skipped\n//< also docs\nstruct X {}";
        let toks = kinds(src);
        assert_eq!(
            toks,
            vec![
                TokenKind::Doc("docs only here".into()),
                TokenKind::Doc("also docs".into()),
                TokenKind::Ident("struct".into()),
                TokenKind::Ident("X".into()),
                TokenKind::LBrace,
                TokenKind::RBrace,
            ]
        );
    }

    #[test]
    fn block_comments_are_skipped() {
        let toks = kinds("/* skip */ enum E /* mid */ {}");
        assert_eq!(
            toks,
            vec![
                TokenKind::Ident("enum".into()),
                TokenKind::Ident("E".into()),
                TokenKind::LBrace,
                TokenKind::RBrace,
            ]
        );
    }

    #[test]
    fn escaped_string() {
        let toks = kinds(r#"[name="hello \"world\"\n"]"#);
        // 0:[ 1:name 2:= 3:"hello ..." 4:]
        assert!(matches!(&toks[3], TokenKind::Str(s) if s == "hello \"world\"\n"));
    }

    #[test]
    fn dotted_package() {
        let toks = kinds("package com.example.foo");
        assert_eq!(
            toks,
            vec![
                TokenKind::Ident("package".into()),
                TokenKind::Ident("com".into()),
                TokenKind::Dot,
                TokenKind::Ident("example".into()),
                TokenKind::Dot,
                TokenKind::Ident("foo".into()),
            ]
        );
    }

    #[test]
    fn negative_integer() {
        let toks = kinds("3: enum_value = -1");
        // -1 is allowed in enum item values.
        assert!(matches!(toks[3], TokenKind::Eq));
        assert!(matches!(toks[4], TokenKind::Int(-1)));
    }

    #[test]
    fn unexpected_char_reports_position() {
        let err = tokenize("struct Foo { @ }").unwrap_err();
        assert!(matches!(err.kind, LexErrorKind::UnexpectedChar('@')));
    }
}
