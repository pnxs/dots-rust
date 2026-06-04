//! High-level `App` API — a TCP-flavored convenience wrapper around
//! [`GuestTransceiver`] + [`GuestDriver`]. The shape mirrors C++ DOTS's
//! `dots::Application` so apps porting from C++ find familiar idioms.
//!
//! ```ignore
//! let app = App::connect("127.0.0.1:11235", "my-name").await?;
//!
//! let _sub = app.subscribe::<Pinger>(|event| {
//!     println!("got Pinger from {:?}", event.header.sender);
//! });
//!
//! let client = app.client();
//! tokio::spawn(async move {
//!     loop {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//!         client.publish(&dots!(Pinger { id: 1_u32 })).ok();
//!     }
//! });
//!
//! app.run_until_signal().await?;
//! ```
//!
//! For non-TCP carriers (in-memory `tokio::io::duplex`, Unix sockets,
//! etc.), drop down to [`GuestTransceiver::from_connection`] directly.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use dots_core::{
    DynamicStruct, DynamicStructDescriptor, PUBLISHED_TYPES, PropertySet, Publishable,
    SUBSCRIBED_TYPES, StructValue,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::connection::{Connection, ConnectionBuilder, ConnectionError, Event};
use crate::container::Container;
use crate::error::TransportError;
use crate::guest::{GuestError, GuestTransceiver, SubscriptionHandle};

pub use crate::guest::now_timepoint;

/// Errors produced by the [`App`] lifecycle.
#[derive(Debug)]
pub enum AppError {
    Connection(ConnectionError),
    Transport(TransportError),
    Io(std::io::Error),
    /// The `DOTS_ENDPOINT` environment variable held a value that
    /// could not be parsed as an endpoint URI. Only produced by the
    /// default-endpoint resolver used by [`App::new`].
    Endpoint(crate::EndpointError),
}

impl core::fmt::Display for AppError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Connection(e) => write!(f, "{e}"),
            Self::Transport(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "{e}"),
            Self::Endpoint(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connection(e) => Some(e),
            Self::Transport(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::Endpoint(e) => Some(e),
        }
    }
}

impl From<ConnectionError> for AppError {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}
impl From<TransportError> for AppError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}
impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<crate::EndpointError> for AppError {
    fn from(e: crate::EndpointError) -> Self {
        Self::Endpoint(e)
    }
}
impl From<GuestError> for AppError {
    fn from(e: GuestError) -> Self {
        match e {
            GuestError::Connection(e) => Self::Connection(e),
            GuestError::Transport(e) => Self::Transport(e),
        }
    }
}

/// Cheap, cloneable handle for use inside callback handlers and
/// spawned tasks. Equivalent to `Arc<GuestTransceiver>`; method calls
/// (`publish`, `subscribe`, …) come from [`GuestTransceiver`] via
/// `Deref`.
pub type Client = Arc<GuestTransceiver>;

/// Type-erased future for the guest-side I/O loop. Used by [`App`] so
/// it can hold a driver without exposing its underlying stream type
/// (TCP, UDS, etc.) in the [`App`] struct.
type DriverFuture = Pin<Box<dyn Future<Output = Result<(), GuestError>> + Send>>;

/// High-level DOTS client.
///
/// Owns an [`Arc<GuestTransceiver>`](GuestTransceiver) (the shareable
/// API surface) and a type-erased driver future (consumed by
/// [`run`](Self::run)). Constructed via [`connect`](Self::connect_tcp)
/// for TCP or [`connect_unix`](Self::connect_unix) for Unix domain
/// sockets.
pub struct App {
    transceiver: Arc<GuestTransceiver>,
    driver: Option<DriverFuture>,
}

impl App {
    /// Connect to a DOTS broker using the default endpoint.
    ///
    /// Resolution order, matching dots-cpp's `Application(name)`:
    ///
    /// 1. `DOTS_ENDPOINT` environment variable (parsed as a URI such
    ///    as `tcp://host:port` or `uds:///path/to/sock`).
    /// 2. Fallback: [`crate::DEFAULT_ENDPOINT_URI`]
    ///    (`tcp://127.0.0.1:11235`).
    ///
    /// A malformed `DOTS_ENDPOINT` returns
    /// [`AppError::Endpoint`] rather than silently falling back, so a
    /// typo is surfaced rather than hidden behind a working default.
    ///
    /// Returns once the connection has completed the EarlySubscribe
    /// phase and reached `Connected` — descriptors for every link-time
    /// `PUBLISHED_TYPES` / `SUBSCRIBED_TYPES` entry have been shipped,
    /// `DotsMember(Join)` has been published for each subscribed type,
    /// the broker's cache replay has finished, and the per-type
    /// containers in the transceiver's pool have been populated with
    /// the replayed instances. A subsequent
    /// [`crate::global::container::<T>`] / [`Self::container::<T>`]
    /// call returns a typed view of that already-populated container.
    ///
    /// Use [`connect`](Self::connect),
    /// [`connect_tcp`](Self::connect_tcp), or
    /// [`connect_unix`](Self::connect_unix) when the caller wants to
    /// pin the endpoint programmatically.
    pub async fn new(client_name: &str) -> Result<App, AppError> {
        let endpoint = crate::Endpoint::from_env_or_default()?;
        Self::connect(endpoint, client_name).await
    }

    /// Connect to a DOTS broker over TCP and run the handshake (with
    /// `preload = true`). Returns an `App` ready for the user to add
    /// subscriptions, then `run()`.
    pub async fn connect_tcp(addr: &str, client_name: &str) -> Result<App, AppError> {
        Self::connect_tcp_inner(addr, client_name, None).await
    }

    /// Same as [`connect`](Self::connect_tcp) but supplies a shared secret
    /// for SHA-256 challenge-response authentication.
    pub async fn connect_tcp_with_auth(
        addr: &str,
        client_name: &str,
        secret: &str,
    ) -> Result<App, AppError> {
        Self::connect_tcp_inner(addr, client_name, Some(secret)).await
    }

    /// Connect to a DOTS broker over a Unix domain socket. `path` is
    /// the filesystem path of the broker's listening socket.
    #[cfg(unix)]
    pub async fn connect_unix(
        path: impl AsRef<Path>,
        client_name: &str,
    ) -> Result<App, AppError> {
        Self::connect_unix_inner(path.as_ref(), client_name, None).await
    }

    /// Same as [`connect_unix`](Self::connect_unix) but supplies a
    /// shared secret for challenge-response authentication.
    #[cfg(unix)]
    pub async fn connect_unix_with_auth(
        path: impl AsRef<Path>,
        client_name: &str,
        secret: &str,
    ) -> Result<App, AppError> {
        Self::connect_unix_inner(path.as_ref(), client_name, Some(secret)).await
    }

    /// Connect over a parsed [`crate::Endpoint`]. Dispatches to
    /// [`connect`](Self::connect_tcp) for `tcp://` URIs and
    /// [`connect_unix`](Self::connect_unix) for `uds://` URIs. Use
    /// [`crate::parse_endpoint`] to build the [`crate::Endpoint`].
    pub async fn connect(
        endpoint: crate::Endpoint,
        client_name: &str,
    ) -> Result<App, AppError> {
        match endpoint {
            crate::Endpoint::Tcp(addr) => Self::connect_tcp(&addr, client_name).await,
            #[cfg(unix)]
            crate::Endpoint::Uds(path) => Self::connect_unix(path, client_name).await,
            #[cfg(not(unix))]
            crate::Endpoint::Uds(_) => Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix domain sockets are unix-only",
            ))),
        }
    }

    /// Same as [`connect_endpoint`](Self::connect) but with
    /// authentication.
    pub async fn connect_with_auth(
        endpoint: crate::Endpoint,
        client_name: &str,
        secret: &str,
    ) -> Result<App, AppError> {
        match endpoint {
            crate::Endpoint::Tcp(addr) => {
                Self::connect_tcp_with_auth(&addr, client_name, secret).await
            }
            #[cfg(unix)]
            crate::Endpoint::Uds(path) => {
                Self::connect_unix_with_auth(path, client_name, secret).await
            }
            #[cfg(not(unix))]
            crate::Endpoint::Uds(_) => Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix domain sockets are unix-only",
            ))),
        }
    }

    async fn connect_tcp_inner(
        addr: &str,
        client_name: &str,
        secret: Option<&str>,
    ) -> Result<App, AppError> {
        tracing::debug!(
            addr,
            client_name,
            with_auth = secret.is_some(),
            transport = "tcp",
            "connecting to dotsd"
        );
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Self::build_app(stream, client_name, secret).await
    }

    #[cfg(unix)]
    async fn connect_unix_inner(
        path: &Path,
        client_name: &str,
        secret: Option<&str>,
    ) -> Result<App, AppError> {
        tracing::debug!(
            path = %path.display(),
            client_name,
            with_auth = secret.is_some(),
            transport = "uds",
            "connecting to dotsd"
        );
        let stream = tokio::net::UnixStream::connect(path).await?;
        Self::build_app(stream, client_name, secret).await
    }

    async fn build_app<S>(
        stream: S,
        client_name: &str,
        secret: Option<&str>,
    ) -> Result<App, AppError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let registry = Arc::new(dots_model::registry_with_internal_types());
        let mut builder =
            ConnectionBuilder::new(stream, client_name, registry.clone()).preload(true);
        if let Some(s) = secret {
            builder = builder.with_auth(s);
        }
        let conn: Connection<S> = builder.connect().await?;

        // Hand the link-time `PUBLISHED_TYPES` / `SUBSCRIBED_TYPES`
        // slices to the transceiver. The `#[derive(DotsStruct)]` macro
        // emits `linkme` entries inside `register_as_published` /
        // `register_as_subscribed`; each `publish::<T>` /
        // `subscribe::<T>` / `container::<T>` monomorphization pulls
        // those entries into the binary, so these slices reflect the
        // binary's actual link-time intent. The driver's Phase 1
        // walks them transitively (registering and shipping every
        // nested struct/enum descriptor in declaration order) and
        // Phase 1b emits `DotsMember(Join, T.name)` for each
        // subscribed type so the broker starts cache replay before
        // `preloadClientFinished`. Mirrors dots-cpp's `Application`
        // passing `io::global_subscribe_types()` into
        // `GuestTransceiver::open`.
        let published_types = PUBLISHED_TYPES
            .iter()
            .copied()
            .filter(|d| !d.flags.is_internal());
        let subscribed_types = SUBSCRIBED_TYPES
            .iter()
            .copied()
            .filter(|d| !d.flags.is_internal());

        let (transceiver, mut driver) = GuestTransceiver::from_connection(
            registry,
            conn,
            published_types,
            subscribed_types,
        );
        // Run the EarlySubscribe phase (descriptor exchange, Join for
        // every subscribed type, finish_preload) before returning, so
        // the connection is in `Connected` state by the time the caller
        // receives the `App`. Cache events that arrive during this
        // phase have no user-side subscribers yet — they're dropped.
        // Live events flow normally once `App::run` starts.
        driver.early_subscribe().await?;
        // Install the transceiver as the process-wide singleton so
        // `dots_transport::global::{publish, subscribe, container, …}`
        // resolve to it. Panics if another `App` is already active —
        // matches dots-cpp's single-`Application` constraint.
        crate::global::init(transceiver.clone());
        let driver_future: DriverFuture = Box::pin(driver.run());
        Ok(App {
            transceiver,
            driver: Some(driver_future),
        })
    }

    /// Cheap, cloneable handle to the underlying [`GuestTransceiver`].
    /// Use this from spawned tasks or callback handlers that outlive
    /// the [`App`] handle.
    pub fn client(&self) -> Client {
        self.transceiver.clone()
    }

    /// Direct reference to the shared [`GuestTransceiver`] — useful
    /// when you want to call its methods without cloning the `Arc`.
    pub fn transceiver(&self) -> &Arc<GuestTransceiver> {
        &self.transceiver
    }

    /// The shared type [`Registry`](dots_model::Registry) — the union
    /// of compile-time types and types learned over the wire. Useful
    /// for registry-aware rendering, e.g.
    /// [`Registry::display_struct`](dots_model::Registry::display_struct)
    /// to expand `any` fields in a trace.
    pub fn registry(&self) -> &Arc<dots_model::Registry> {
        self.transceiver.registry()
    }

    pub fn subscribe<T>(
        &self,
        handler: impl FnMut(&Event<T>) + Send + 'static,
    ) -> SubscriptionHandle
    where
        T: StructValue + Send + 'static + dots_core::GlobalRegistration,
    {
        self.transceiver.subscribe(handler)
    }

    pub fn subscribe_stream<T>(&self) -> crate::Subscription<T>
    where
        T: StructValue + Send + 'static + dots_core::GlobalRegistration,
    {
        self.transceiver.subscribe_stream::<T>()
    }

    /// Subscribe to a runtime-described type — see
    /// [`GuestTransceiver::subscribe_dynamic`].
    pub fn subscribe_dynamic(
        &self,
        descriptor: Arc<DynamicStructDescriptor>,
        handler: impl FnMut(&Event<DynamicStruct>) + Send + 'static,
    ) -> SubscriptionHandle {
        self.transceiver.subscribe_dynamic(descriptor, handler)
    }

    /// Subscribe to type-system events — see
    /// [`GuestTransceiver::subscribe_new_struct_type`].
    pub fn subscribe_new_struct_type<F>(&self, handler: F) -> SubscriptionHandle
    where
        F: FnMut(&Arc<DynamicStructDescriptor>) + Send + 'static,
    {
        self.transceiver.subscribe_new_struct_type(handler)
    }

    /// Subscribe to every DOTS type with a single handler — see
    /// [`GuestTransceiver::subscribe_all_types`].
    pub fn subscribe_all_types<F>(&self, handler: F) -> crate::AllTypesSubscription
    where
        F: FnMut(&Event<DynamicStruct>) + Send + 'static,
    {
        self.transceiver.subscribe_all_types(handler)
    }

    pub fn container<T>(&self) -> Container<T>
    where
        T: StructValue + Send + 'static + dots_core::GlobalRegistration,
    {
        self.transceiver.container::<T>()
    }

    /// Open a filtered subscription on `T` — see
    /// [`GuestTransceiver::view`]. Returns
    /// [`ViewError::Unsupported`](crate::ViewError::Unsupported) when
    /// the broker hasn't advertised the filtered-subscriptions
    /// capability.
    pub fn view<T>(
        &self,
        filter: dots_model::filter::DotsFilter,
    ) -> Result<crate::View<T>, crate::ViewError>
    where
        T: StructValue + Send + Clone + 'static + dots_core::GlobalRegistration,
    {
        self.transceiver.view::<T>(filter)
    }

    pub fn publish<P: Publishable>(&self, value: &P) {
        self.transceiver.publish(value)
    }

    /// Publish a partial update — see [`GuestTransceiver::publish_with_mask`].
    pub fn publish_with_mask<P: Publishable>(&self, value: &P, included: PropertySet) {
        self.transceiver.publish_with_mask(value, included)
    }

    /// Publish a removal — see [`GuestTransceiver::remove`].
    pub fn remove<P: Publishable>(&self, value: &P) {
        self.transceiver.remove(value)
    }

    pub fn exit(&self) {
        self.transceiver.exit()
    }

    /// Run the read/write event loop until [`exit`](Self::exit) is
    /// called or the connection closes.
    pub async fn run(mut self) -> Result<(), AppError> {
        let fut = self.driver.take().expect("App::run called twice");
        fut.await.map_err(Into::into)
    }

    /// Same as [`run`](Self::run) but also installs a Ctrl-C handler
    /// that calls [`exit`](Self::exit) on the first interrupt.
    pub async fn run_until_signal(self) -> Result<(), AppError> {
        let transceiver = self.transceiver.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                transceiver.exit();
            }
        });
        self.run().await
    }
}

impl Drop for App {
    /// Release the [`crate::global`] singleton slot so a subsequent
    /// `App::new` (e.g. in the next test, or in a re-connect path)
    /// can install a fresh transceiver.
    fn drop(&mut self) {
        crate::global::destroy();
    }
}
