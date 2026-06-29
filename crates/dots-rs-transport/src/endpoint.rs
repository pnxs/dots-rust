//! Endpoint URI parsing + binding helpers for the host and guest
//! transports.
//!
//! Two URI schemes are supported:
//!
//! - `tcp://<addr>:<port>` — bind to a TCP socket.
//! - `uds:///<path>` — bind to a Unix-domain socket. The path uses
//!   the absolute form (note the *three* slashes: empty authority +
//!   absolute path), e.g. `uds:///tmp/dotsd.sock`.
//!
//! Centralizing the parsing here keeps the `dotsd` binary thin and
//! lets any embedded broker or client accept the same URI strings
//! without re-implementing the parser.

use std::path::{Path, PathBuf};

/// Parsed transport endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// TCP — host string is `addr:port` (passed through to
    /// [`tokio::net::TcpListener::bind`] / [`tokio::net::TcpStream::connect`]).
    Tcp(String),
    /// Unix domain socket at the given filesystem path.
    Uds(PathBuf),
}

/// Default URI when neither the caller nor the `DOTS_ENDPOINT`
/// environment variable specifies one. Matches dots-cpp's
/// `Application` default (`tcp://127.0.0.1`); the port `11235` is
/// supplied explicitly because [`parse_endpoint`] does not infer a
/// default port.
pub const DEFAULT_ENDPOINT_URI: &str = "tcp://127.0.0.1:11235";

/// Environment variable consulted by [`Endpoint::from_env_or_default`].
/// Same name as dots-cpp, so a value set in a mixed-language deployment
/// is honoured by both clients.
pub const DOTS_ENDPOINT_ENV: &str = "DOTS_ENDPOINT";

impl Endpoint {
    /// `tcp://addr:port` form. Borrowed so callers can pass `&str`
    /// without allocating.
    pub fn tcp(addr: impl Into<String>) -> Self {
        Self::Tcp(addr.into())
    }

    /// `uds:///path/to/sock` form.
    pub fn uds(path: impl AsRef<Path>) -> Self {
        Self::Uds(path.as_ref().to_path_buf())
    }

    /// Resolve the default broker endpoint.
    ///
    /// If the `DOTS_ENDPOINT` environment variable is set, its value
    /// is parsed via [`parse_endpoint`] — a malformed value yields
    /// [`EndpointError::UnknownScheme`] rather than silently falling
    /// back, so the user sees the typo. Unset or empty → fall back to
    /// [`DEFAULT_ENDPOINT_URI`].
    pub fn from_env_or_default() -> Result<Self, EndpointError> {
        let from_env = std::env::var(DOTS_ENDPOINT_ENV).ok();
        let uri = from_env
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_ENDPOINT_URI);
        parse_endpoint(uri)
    }
}

/// Errors produced by [`parse_endpoint`].
#[derive(Debug)]
pub enum EndpointError {
    /// The string didn't start with a recognised scheme.
    UnknownScheme {
        input: String,
    },
}

impl core::fmt::Display for EndpointError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownScheme { input } => write!(
                f,
                "unrecognized endpoint URI {input:?}; expected `tcp://addr:port` or `uds:///path`"
            ),
        }
    }
}

impl std::error::Error for EndpointError {}

/// Parse a single endpoint URI.
pub fn parse_endpoint(s: &str) -> Result<Endpoint, EndpointError> {
    if let Some(rest) = s.strip_prefix("tcp://") {
        return Ok(Endpoint::Tcp(rest.to_string()));
    }
    if let Some(rest) = s.strip_prefix("uds://") {
        // After the scheme, the remainder of a `uds:///path` URI is
        // `/path` (absolute) — the empty authority before the third
        // slash is intentional. We pass it through verbatim.
        return Ok(Endpoint::Uds(PathBuf::from(rest)));
    }
    Err(EndpointError::UnknownScheme { input: s.into() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tcp() {
        let ep = parse_endpoint("tcp://127.0.0.1:11235").unwrap();
        assert_eq!(ep, Endpoint::Tcp("127.0.0.1:11235".into()));
    }

    #[test]
    fn parse_tcp_zero_addr() {
        let ep = parse_endpoint("tcp://0.0.0.0:11235").unwrap();
        assert_eq!(ep, Endpoint::Tcp("0.0.0.0:11235".into()));
    }

    #[test]
    fn parse_uds_absolute_path() {
        let ep = parse_endpoint("uds:///tmp/dotsd.sock").unwrap();
        assert_eq!(ep, Endpoint::Uds(PathBuf::from("/tmp/dotsd.sock")));
    }

    #[test]
    fn parse_unknown_scheme_errors() {
        let err = parse_endpoint("ws://localhost:8080").unwrap_err();
        let EndpointError::UnknownScheme { input } = err;
        assert_eq!(input, "ws://localhost:8080");
    }

    #[test]
    fn parse_no_scheme_errors() {
        assert!(parse_endpoint("127.0.0.1:11235").is_err());
    }

    /// The hard-coded default URI must itself be parseable; otherwise
    /// `Endpoint::from_env_or_default` would error in the
    /// no-env-var branch.
    #[test]
    fn default_uri_parses() {
        assert!(parse_endpoint(DEFAULT_ENDPOINT_URI).is_ok());
    }
}
