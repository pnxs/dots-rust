//! # dots-rs
//!
//! DOTS (Distributed Objects in Time and Space) pub/sub for Rust.
//!
//! This is the **umbrella crate**: it re-exports the runtime types, the
//! `#[derive(DotsStruct)]` / `#[derive(DotsEnum)]` / `dots!` macros, and
//! the async transport from the underlying `dots-rs-*` crates. A normal
//! application only needs `dots-rs` as a dependency — and, when compiling
//! `.dots` schema files, `dots-rs-build` as a build-dependency.
//!
//! ```no_run
//! use dots_rs::{DotsStruct, dots, App};
//!
//! #[derive(DotsStruct, Default, Debug, Clone)]
//! #[dots(name = "Pinger", cached)]
//! struct Pinger {
//!     #[dots(tag = 1, key)] id: u32,
//!     #[dots(tag = 2)]      message: Option<String>,
//!     #[dots(tag = 3)]      sequence: Option<u64>,
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let app = App::connect_tcp("127.0.0.1:11235", "my-client").await?;
//! let _sub = app.subscribe::<Pinger>(|event| {
//!     println!("got {:?}", event.transmitted);
//! });
//! app.client().publish(&dots!(Pinger { id: 1_u32, sequence: 42_u64 }));
//! # Ok(()) }
//! ```

// Hidden runtime module that `#[derive(DotsStruct)]`-generated code
// targets by default (`::dots_rs::__rt::…`). It unifies everything the
// generated code needs — the core runtime plus the filter DSL's `Attr`
// — behind a single path so downstream crates depend on `dots-rs` alone.
// Not part of the public API; do not use directly.
#[doc(hidden)]
pub mod __rt {
    pub use dots_rs_core::*;
    pub use dots_rs_model::filter;
}

// ----- derive + constructor macros -----
pub use dots_rs_derive::{DotsEnum, DotsStruct};
// `dots!` also arrives via the `dots_rs_core::*` glob below, but is
// re-exported here too so it shows up in the crate's own docs.
pub use dots_rs_derive::dots;

// ----- core runtime -----
// Descriptors, the codec, dynamic structs, PropertySet, temporal types,
// registration hooks, and the re-exported `linkme` / `minicbor`.
pub use dots_rs_core::*;

// ----- model: DOTS system types + the filter DSL -----
// Connection/handshake messages, daemon stats types, wire descriptors,
// the `filter` predicate DSL, framing, and the type `Registry`.
pub use dots_rs_model::*;

// ----- async transport -----
// Listed explicitly (rather than glob-imported) so the transport's own
// `filter` module does not collide with the model's `filter` DSL above.
pub use dots_rs_transport::{
    AllTypesSubscription, App, AppError, Client, CloneInfo, Connection, ConnectionBuilder,
    ConnectionError, ConnectionTransition, Container, ContainerEntry, DEFAULT_ENDPOINT_URI,
    DOTS_ENDPOINT_ENV, Endpoint, EndpointError, EndpointHandle, Event, GuestDriver, GuestError,
    GuestStats, GuestTransceiver, HOST_ID, HostTransceiver, Operation, RawTransmissionCodec,
    Subscription, SubscriptionHandle, TransmissionCodec, TransportError, View, ViewError,
    ViewEvent, ViewOp, ViewSubscription, connect_over_stream, new_uuid, now_timepoint,
    parse_endpoint,
};

/// The global, process-wide DOTS API (`publish` / `subscribe` /
/// `container` / `view` against an installed guest).
pub use dots_rs_transport::global;

/// The transport's compiled-predicate types, exposed under a distinct
/// name so they don't shadow the model's [`filter`] predicate DSL.
pub use dots_rs_transport::filter as compiled_filter;

/// Install a `tracing_subscriber` with sensible defaults. Requires the
/// `tracing-init` feature.
#[cfg(feature = "tracing-init")]
pub use dots_rs_transport::init_tracing;

/// In-process Host + Guest test harness. Requires the `testing` feature.
#[cfg(feature = "testing")]
pub use dots_rs_testing as testing;

/// The most commonly used items, for `use dots_rs::prelude::*;`.
pub mod prelude {
    pub use dots_rs_core::{Publishable, StructValue, dots};
    pub use dots_rs_derive::{DotsEnum, DotsStruct};
    pub use dots_rs_transport::{App, Client, Container, View};
}
