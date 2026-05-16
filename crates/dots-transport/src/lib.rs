//! Async transport for dots-rust.
//!
//! Bridges the synchronous framing layer in `dots-model` to tokio's
//! async I/O via [`tokio_util::codec`]. The codec is generic over the
//! underlying byte stream — TCP, Unix domain sockets, or
//! [`tokio::io::duplex`] for in-process / testing all work the same
//! way.
//!
//! ```ignore
//! use std::sync::Arc;
//! use tokio_util::codec::Framed;
//! use dots_transport::TransmissionCodec;
//!
//! # async fn run(stream: tokio::net::TcpStream, registry: Arc<dots_model::Registry>) {
//! let codec = TransmissionCodec::new(registry);
//! let mut framed = Framed::new(stream, codec);
//! // framed: Stream<Item = Result<Transmission, TransportError>>
//! //         + Sink<Transmission, Error = TransportError>
//! # }
//! ```

mod app;
mod auth;
mod codec;
mod connection;
mod container;
mod endpoint;
mod error;
pub mod filter;
mod guest;
mod host;
mod view;
#[cfg(feature = "tracing-init")]
mod tracing_init;

pub use app::{App, AppError, Client, now_timepoint};
pub use codec::{RawTransmissionCodec, TransmissionCodec};
pub use connection::{Connection, ConnectionBuilder, ConnectionError, Event, Subscription};
pub use container::{CloneInfo, Container, ContainerEntry, Operation};
pub use endpoint::{
    DEFAULT_ENDPOINT_URI, DOTS_ENDPOINT_ENV, Endpoint, EndpointError, parse_endpoint,
};
pub use dots_core::PropertySet;
pub use error::TransportError;
pub use guest::{
    AllTypesSubscription, GuestDriver, GuestError, GuestTransceiver, SubscriptionHandle,
};
pub use host::{EndpointHandle, HOST_ID, HostTransceiver};
pub use view::{View, ViewError, ViewEvent, ViewOp, ViewSubscription};
#[cfg(feature = "tracing-init")]
pub use tracing_init::init_tracing;

// Re-export the framing layer's public types so callers don't need to
// pull `dots-model` directly when wiring up a transport.
pub use dots_model::{
    DotsHeader, FramingError, MAX_BODY_SIZE, Registry, SIZE_PREFIX_LEN, Transmission,
};
