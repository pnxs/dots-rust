//! Endpoint URI parsing + binding helpers for the host and guest
//! transports.
//!
//! Two URI schemes are supported:
//!
//! - `tcp://<addr>[:<port>]` — bind to a TCP socket. The port is
//!   optional and defaults to [`DEFAULT_TCP_PORT`], so `tcp://1.2.3.4`
//!   and `tcp://1.2.3.4:11235` are equivalent (same as dots-cpp).
//!   IPv6 literals must be bracketed: `tcp://[::1]`.
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
/// `Application` default; the port is spelled out so that log and
/// error messages name the address that is actually dialled, even
/// though [`parse_endpoint`] would infer it.
pub const DEFAULT_ENDPOINT_URI: &str = "tcp://127.0.0.1:11235";

/// Port used for `tcp://` URIs that omit one. Mirrors dots-cpp's
/// `TcpChannel::DefaultPort` for the v2 transmission format — the
/// only format dots-rust speaks (v1 used `11234`).
pub const DEFAULT_TCP_PORT: u16 = 11235;

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
    /// is parsed via [`parse_endpoint`] — a malformed value yields an
    /// [`EndpointError`] rather than silently falling back, so the
    /// user sees the typo. The port may be omitted
    /// (`DOTS_ENDPOINT=tcp://1.2.3.4` → port [`DEFAULT_TCP_PORT`]).
    /// Unset or empty → fall back to [`DEFAULT_ENDPOINT_URI`].
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
    UnknownScheme { input: String },
    /// A `tcp://` URI carried no host, e.g. `tcp://` or `tcp://:11235`.
    MissingHost { input: String },
    /// A `tcp://` URI had a port that isn't a `u16`.
    InvalidPort { input: String, port: String },
    /// A `tcp://` authority couldn't be split into host and port —
    /// currently an unterminated or trailing-garbage IPv6 bracket.
    MalformedAuthority { input: String },
    /// A `tcp://` URI carried `user[:password]@` userinfo. dots-cpp
    /// reads credentials from there; dots-rust takes them through
    /// [`crate::App::connect_with_auth`] instead, so rather than
    /// dialling a bogus host we say so.
    UserInfoUnsupported { input: String },
}

impl core::fmt::Display for EndpointError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownScheme { input } => write!(
                f,
                "unrecognized endpoint URI {input:?}; expected `tcp://addr[:port]` or `uds:///path`"
            ),
            Self::MissingHost { input } => write!(
                f,
                "endpoint URI {input:?} has no host; expected `tcp://addr[:port]` \
                 (IPv6 literals must be bracketed, e.g. `tcp://[::1]`)"
            ),
            Self::InvalidPort { input, port } => write!(
                f,
                "endpoint URI {input:?} has a non-numeric port {port:?}; expected 0..=65535"
            ),
            Self::MalformedAuthority { input } => write!(
                f,
                "endpoint URI {input:?} has a malformed authority; \
                 expected `tcp://addr[:port]` or `tcp://[ipv6][:port]`"
            ),
            Self::UserInfoUnsupported { input } => write!(
                f,
                "endpoint URI {input:?} carries userinfo; pass credentials to \
                 `App::connect_with_auth` instead"
            ),
        }
    }
}

impl std::error::Error for EndpointError {}

/// Parse a single endpoint URI.
pub fn parse_endpoint(s: &str) -> Result<Endpoint, EndpointError> {
    if let Some(rest) = s.strip_prefix("tcp://") {
        return Ok(Endpoint::Tcp(tcp_authority(rest, s)?));
    }
    if let Some(rest) = s.strip_prefix("uds://") {
        // After the scheme, the remainder of a `uds:///path` URI is
        // `/path` (absolute) — the empty authority before the third
        // slash is intentional. We pass it through verbatim.
        return Ok(Endpoint::Uds(PathBuf::from(rest)));
    }
    Err(EndpointError::UnknownScheme { input: s.into() })
}

/// Normalize the part after `tcp://` into the `host:port` form that
/// [`tokio::net::TcpStream::connect`] / [`tokio::net::TcpListener::bind`]
/// resolve, supplying [`DEFAULT_TCP_PORT`] when the URI omits the port.
///
/// This mirrors dots-cpp, where the URI parser leaves the port empty
/// and `TcpChannel`/`TcpListener` substitute `DefaultPort`.
fn tcp_authority(rest: &str, input: &str) -> Result<String, EndpointError> {
    // The authority ends at the first `/`; a path carries no meaning
    // for TCP, so `tcp://1.2.3.4/` is just `1.2.3.4` (dots-cpp splits
    // authority from path the same way and ignores the latter).
    let authority = rest.split('/').next().unwrap_or_default();

    if authority.contains('@') {
        return Err(EndpointError::UserInfoUnsupported {
            input: input.into(),
        });
    }

    let (host, port) = if authority.starts_with('[') {
        // Bracketed IPv6: only a `:` *after* the closing bracket is a
        // port separator; the ones inside the brackets are the address.
        let close = authority
            .find(']')
            .ok_or_else(|| EndpointError::MalformedAuthority {
                input: input.into(),
            })?;
        let (host, tail) = authority.split_at(close + 1);
        let port = match tail {
            "" => None,
            _ => Some(
                tail.strip_prefix(':')
                    .ok_or_else(|| EndpointError::MalformedAuthority {
                        input: input.into(),
                    })?,
            ),
        };
        (host, port)
    } else if let Some((host, port)) = authority.split_once(':') {
        (host, Some(port))
    } else {
        (authority, None)
    };

    if host.is_empty() || host == "[]" {
        return Err(EndpointError::MissingHost {
            input: input.into(),
        });
    }

    // An empty port (`tcp://host:`) is treated as absent, matching
    // dots-cpp's `port().empty()` check.
    match port.filter(|p| !p.is_empty()) {
        Some(port) => {
            port.parse::<u16>()
                .map_err(|_| EndpointError::InvalidPort {
                    input: input.into(),
                    port: port.into(),
                })?;
            Ok(format!("{host}:{port}"))
        }
        None => Ok(format!("{host}:{DEFAULT_TCP_PORT}")),
    }
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

    /// Bracketed IPv6 authority: the `[::1]:12345` form is passed
    /// through verbatim, which is exactly the `addr:port` syntax that
    /// [`tokio::net::TcpListener::bind`] /
    /// [`tokio::net::TcpStream::connect`] resolve (it parses as a
    /// `std::net::SocketAddrV6`).
    #[test]
    fn parse_tcp_bracketed_ipv6() {
        let ep = parse_endpoint("tcp://[::1]:12345").unwrap();
        assert_eq!(ep, Endpoint::Tcp("[::1]:12345".into()));
        // Sanity check that the preserved form is resolvable as-is.
        let Endpoint::Tcp(addr) = &ep else {
            unreachable!()
        };
        assert!(addr.parse::<std::net::SocketAddr>().is_ok());
    }

    /// dots-cpp parity: an omitted port falls back to
    /// `TcpChannel::DefaultPort`, so `tcp://1.2.3.4` dials `:11235`.
    #[test]
    fn parse_tcp_without_port_uses_default() {
        let ep = parse_endpoint("tcp://1.2.3.4").unwrap();
        assert_eq!(ep, Endpoint::Tcp("1.2.3.4:11235".into()));
    }

    #[test]
    fn parse_tcp_hostname_without_port_uses_default() {
        let ep = parse_endpoint("tcp://broker.local").unwrap();
        assert_eq!(ep, Endpoint::Tcp("broker.local:11235".into()));
    }

    /// A trailing `:` is an empty port, which dots-cpp also treats as
    /// absent.
    #[test]
    fn parse_tcp_empty_port_uses_default() {
        let ep = parse_endpoint("tcp://1.2.3.4:").unwrap();
        assert_eq!(ep, Endpoint::Tcp("1.2.3.4:11235".into()));
    }

    /// The colons inside the brackets belong to the address, not to a
    /// port — the default is appended after the bracket.
    #[test]
    fn parse_tcp_bracketed_ipv6_without_port_uses_default() {
        let ep = parse_endpoint("tcp://[::1]").unwrap();
        assert_eq!(ep, Endpoint::Tcp("[::1]:11235".into()));
        let Endpoint::Tcp(addr) = &ep else {
            unreachable!()
        };
        assert!(addr.parse::<std::net::SocketAddr>().is_ok());
    }

    /// A path after the authority is meaningless for TCP; dots-cpp
    /// splits it off and ignores it, so we do too.
    #[test]
    fn parse_tcp_ignores_path() {
        let ep = parse_endpoint("tcp://1.2.3.4/").unwrap();
        assert_eq!(ep, Endpoint::Tcp("1.2.3.4:11235".into()));
        let ep = parse_endpoint("tcp://1.2.3.4:9/ignored").unwrap();
        assert_eq!(ep, Endpoint::Tcp("1.2.3.4:9".into()));
    }

    #[test]
    fn parse_tcp_without_host_errors() {
        assert!(matches!(
            parse_endpoint("tcp://"),
            Err(EndpointError::MissingHost { .. })
        ));
        assert!(matches!(
            parse_endpoint("tcp://:11235"),
            Err(EndpointError::MissingHost { .. })
        ));
        // Unbracketed IPv6 is not a valid authority — the first colon
        // reads as the port separator, leaving an empty host.
        assert!(matches!(
            parse_endpoint("tcp://::1"),
            Err(EndpointError::MissingHost { .. })
        ));
    }

    #[test]
    fn parse_tcp_non_numeric_port_errors() {
        let err = parse_endpoint("tcp://1.2.3.4:dots").unwrap_err();
        let EndpointError::InvalidPort { port, .. } = err else {
            panic!("expected InvalidPort");
        };
        assert_eq!(port, "dots");
    }

    #[test]
    fn parse_tcp_malformed_brackets_error() {
        assert!(matches!(
            parse_endpoint("tcp://[::1"),
            Err(EndpointError::MalformedAuthority { .. })
        ));
        assert!(matches!(
            parse_endpoint("tcp://[::1]junk"),
            Err(EndpointError::MalformedAuthority { .. })
        ));
    }

    #[test]
    fn parse_tcp_userinfo_errors() {
        assert!(matches!(
            parse_endpoint("tcp://user:secret@1.2.3.4"),
            Err(EndpointError::UserInfoUnsupported { .. })
        ));
    }

    #[test]
    fn parse_uds_absolute_path() {
        let ep = parse_endpoint("uds:///tmp/dotsd.sock").unwrap();
        assert_eq!(ep, Endpoint::Uds(PathBuf::from("/tmp/dotsd.sock")));
    }

    #[test]
    fn parse_unknown_scheme_errors() {
        let err = parse_endpoint("ws://localhost:8080").unwrap_err();
        let EndpointError::UnknownScheme { input } = err else {
            panic!("expected UnknownScheme");
        };
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
