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
//!         client.publish(&Pinger { id: Some(1), ..Default::default() }).ok();
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

use dots_core::{EnumDescriptor, StructValue};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::connection::{Connection, ConnectionBuilder, ConnectionError, Event};
use crate::container::Container;
use crate::error::TransportError;
use crate::guest::{GuestError, GuestTransceiver, SubscriptionHandle};

pub use crate::guest::{ClientClosed, now_timepoint};

/// Errors produced by the [`App`] lifecycle.
#[derive(Debug)]
pub enum AppError {
    Connection(ConnectionError),
    Transport(TransportError),
    Io(std::io::Error),
}

impl core::fmt::Display for AppError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Connection(e) => write!(f, "{e}"),
            Self::Transport(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connection(e) => Some(e),
            Self::Transport(e) => Some(e),
            Self::Io(e) => Some(e),
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
/// [`run`](Self::run)). Constructed via [`connect`](Self::connect)
/// for TCP or [`connect_unix`](Self::connect_unix) for Unix domain
/// sockets.
pub struct App {
    transceiver: Arc<GuestTransceiver>,
    driver: Option<DriverFuture>,
}

impl App {
    /// Connect to a DOTS broker over TCP and run the handshake (with
    /// `preload = true`). Returns an `App` ready for the user to add
    /// subscriptions, then `run()`.
    pub async fn connect(addr: &str, client_name: &str) -> Result<App, AppError> {
        Self::connect_tcp_inner(addr, client_name, None).await
    }

    /// Same as [`connect`](Self::connect) but supplies a shared secret
    /// for SHA-256 challenge-response authentication.
    pub async fn connect_with_auth(
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

    async fn connect_tcp_inner(
        addr: &str,
        client_name: &str,
        secret: Option<&str>,
    ) -> Result<App, AppError> {
        tracing::info!(
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
        tracing::info!(
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
        let (transceiver, driver) =
            GuestTransceiver::from_connection(client_name.to_string(), registry, conn);
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

    pub fn subscribe<T>(
        &self,
        handler: impl FnMut(&Event<T>) + Send + 'static,
    ) -> SubscriptionHandle
    where
        T: StructValue + Default + Send + 'static,
    {
        self.transceiver.subscribe(handler)
    }

    pub fn subscribe_stream<T>(&self) -> crate::Subscription<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        self.transceiver.subscribe_stream::<T>()
    }

    pub fn container<T>(&self) -> Container<T>
    where
        T: StructValue + Default + Send + 'static,
    {
        self.transceiver.container::<T>()
    }

    pub fn register_enum(&self, descriptor: &'static EnumDescriptor) {
        self.transceiver.register_enum(descriptor)
    }

    pub fn publish<T>(&self, value: &T) -> Result<(), ClientClosed>
    where
        T: StructValue,
    {
        self.transceiver.publish(value)
    }

    /// Publish a removal — see [`GuestTransceiver::remove`].
    pub fn remove<T>(&self, value: &T) -> Result<(), ClientClosed>
    where
        T: StructValue,
    {
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
