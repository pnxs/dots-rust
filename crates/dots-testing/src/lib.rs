//! In-process test harness for dots-rust.
//!
//! This crate is the Rust counterpart of dots-cpp's
//! `dots::testing::EventTestBase`. It lets you write unit tests that
//! exercise functions and structs which use the **global DOTS API**
//! (`dots_transport::global::{publish, subscribe, container, …}`) — or
//! a [`GuestTransceiver`] handle directly — without standing up a real
//! broker or opening a socket.
//!
//! Unlike dots-cpp, dots-rust needs no dedicated `LocalChannel` *type*:
//! [`Connection`](dots_transport::Connection) is already generic over
//! any `AsyncRead + AsyncWrite`, so the in-memory loopback is just
//! [`tokio::io::duplex`]. [`TestHarness`] wires that pipe between an
//! in-process [`HostTransceiver`] and a guest, runs the handshake +
//! EarlySubscribe phase, spawns the guest driver, and (for the primary
//! guest) installs it as the process-wide global.
//!
//! # Example
//!
//! ```no_run
//! use dots_core::dots;
//! use dots_derive::DotsStruct;
//! use dots_transport::global as dots;
//! use dots_testing::TestHarness;
//!
//! #[derive(DotsStruct, Default, Debug, Clone, PartialEq)]
//! #[dots(name = "Greeting", cached)]
//! struct Greeting {
//!     #[dots(tag = 1, key)]
//!     id: Option<u32>,
//!     #[dots(tag = 2)]
//!     text: Option<String>,
//! }
//!
//! #[tokio::test]
//! async fn greeting_roundtrips_through_the_broker() {
//!     let harness = TestHarness::new().await;
//!
//!     // The primary guest is the global, so the free functions work:
//!     let mut sub = dots::subscribe_stream::<Greeting>();
//!     dots::publish(&dots!(Greeting { id: 1_u32, text: "hi" }));
//!
//!     let event = harness.recv(&mut sub).await.expect("event");
//!     assert_eq!(event.value.id, Some(1));
//!     // `harness` drops here: guests exit, the global slot is cleared,
//!     // driver tasks are aborted, and the per-process test lock is
//!     // released for the next test.
//! }
//! ```
//!
//! # Determinism
//!
//! There is no synchronous `io_context.poll()` like dots-cpp's
//! `processEvents()`: the guest drivers run as spawned tokio tasks. The
//! deterministic way to assert on traffic is to **await** it —
//! [`TestHarness::recv`] (a timeout-wrapped
//! [`Subscription::recv`](dots_transport::Subscription::recv)) is the
//! idiom. [`TestHarness::settle`] is a *best-effort* scheduler barrier
//! for fire-and-forget cases and makes no ordering guarantee across
//! guests; prefer `recv` when you can.
//!
//! # Serialization of tests
//!
//! The global DOTS API is a process-wide singleton (see
//! [`dots_transport::global`]). Constructing a [`TestHarness`] acquires
//! a process-wide async lock held for the harness's lifetime, so tests
//! that build a harness run one-at-a-time within a test binary even
//! under a multi-threaded runtime. A panicking test releases the lock
//! cleanly (the lock does not poison), so later tests still run.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dots_core::{
    DynamicStruct, DynamicStructDescriptor, GlobalRegistration, Publishable, StructValue, to_any,
};
use dots_transport::{
    AppError, Client, Container, Event, GuestError, GuestTransceiver, HostTransceiver,
    Subscription, connect_over_stream, global,
};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use tokio::task::JoinHandle;

/// Default size (bytes) of each direction of the in-memory
/// [`tokio::io::duplex`] pipe connecting a guest to the host.
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Process-wide lock serializing harnesses that install the global
/// singleton. Held for the lifetime of each [`TestHarness`]. A
/// [`tokio::sync::Mutex`] (rather than `std`) so the guard can be held
/// across `.await` points and is `Send`; it also never poisons, so a
/// panicking test frees it for the next.
static GLOBAL_TEST_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

struct Inner {
    guests: Vec<Arc<GuestTransceiver>>,
    drivers: Vec<JoinHandle<Result<(), GuestError>>>,
    next_guest_id: u32,
}

/// A self-contained DOTS test environment: an in-process
/// [`HostTransceiver`] (broker) plus one or more guests connected to it
/// over in-memory pipes.
///
/// The *primary* guest created by [`new`](Self::new) /
/// [`TestHarnessBuilder::build`] is installed as the process-wide
/// global, so `dots_transport::global::*` (and the convenience methods
/// on this struct) operate on it. Additional ("spoof") guests created
/// with [`add_guest`](Self::add_guest) simulate other clients and are
/// *not* the global.
///
/// Dropping the harness exits every guest, clears the global slot,
/// aborts the driver tasks, and releases the process-wide test lock.
pub struct TestHarness {
    host: Arc<HostTransceiver>,
    primary: Arc<GuestTransceiver>,
    buffer_size: usize,
    inner: Mutex<Inner>,
    // Released on drop, after teardown below it in declaration order is
    // unwound. Held to serialize global-installing harnesses.
    _guard: AsyncMutexGuard<'static, ()>,
}

impl TestHarness {
    /// Spin up a host and a single primary (global) guest with default
    /// settings. Panics if the in-process handshake fails — which, over
    /// an in-memory pipe, indicates a bug rather than a transient error.
    pub async fn new() -> TestHarness {
        Self::builder()
            .build()
            .await
            .expect("in-process TestHarness setup should not fail")
    }

    /// Start configuring a harness (host/guest names, buffer size,
    /// preload). Call [`TestHarnessBuilder::build`] to finish.
    pub fn builder() -> TestHarnessBuilder {
        TestHarnessBuilder::default()
    }

    /// The in-process broker. Use e.g.
    /// [`HostTransceiver::group_size`] to observe subscription state, or
    /// [`HostTransceiver::publish`] to inject traffic from the broker
    /// itself.
    pub fn host(&self) -> &Arc<HostTransceiver> {
        &self.host
    }

    /// The primary guest — the one installed as the process-wide
    /// global. Equivalent to what `dots_transport::global::client()`
    /// returns while this harness is alive.
    pub fn guest(&self) -> &Client {
        &self.primary
    }

    /// Connect an additional guest to the host (the dots-cpp "spoof
    /// guest"). Useful for simulating a *different* client publishing or
    /// subscribing. The returned guest is **not** the global; call its
    /// methods directly.
    pub async fn add_guest(&self, name: &str) -> Result<Client, AppError> {
        let (host_io, guest_io) = tokio::io::duplex(self.buffer_size);
        self.host.accept(host_io);
        let (guest, driver) =
            connect_over_stream(guest_io, name, None, true, false).await?;
        let handle = tokio::spawn(driver);
        let mut inner = self.inner.lock().expect("harness mutex poisoned");
        inner.guests.push(guest.clone());
        inner.drivers.push(handle);
        Ok(guest)
    }

    /// Connect an additional guest with an auto-generated name
    /// (`spoof-1`, `spoof-2`, …).
    pub async fn add_spoof_guest(&self) -> Result<Client, AppError> {
        let id = {
            let mut inner = self.inner.lock().expect("harness mutex poisoned");
            inner.next_guest_id += 1;
            inner.next_guest_id
        };
        self.add_guest(&format!("spoof-{id}")).await
    }

    /// Receive the next event from `sub`, failing the test (via
    /// `panic`) if nothing arrives within two seconds. The deterministic
    /// way to assert on published traffic. See
    /// [`recv_timeout`](Self::recv_timeout) to choose the deadline.
    pub async fn recv<T>(&self, sub: &mut Subscription<T>) -> Option<Event<T>>
    where
        T: Send + 'static,
    {
        self.recv_timeout(sub, Duration::from_secs(2)).await
    }

    /// Like [`recv`](Self::recv) but with a caller-chosen timeout.
    /// Returns `None` if the timeout elapses or the subscription closes.
    pub async fn recv_timeout<T>(
        &self,
        sub: &mut Subscription<T>,
        timeout: Duration,
    ) -> Option<Event<T>>
    where
        T: Send + 'static,
    {
        tokio::time::timeout(timeout, sub.recv()).await.ok().flatten()
    }

    /// Block until at least `count` subscribers for type `T` are
    /// registered at the broker, or `timeout` elapses. Returns `true`
    /// if the count was reached. Useful before publishing from one
    /// guest to ensure another guest's subscription has propagated.
    pub async fn wait_for_subscribers<T>(&self, count: usize, timeout: Duration) -> bool
    where
        T: StructValue,
    {
        let name = T::type_descriptor().name;
        let step = Duration::from_millis(2);
        let mut waited = Duration::ZERO;
        loop {
            if self.host.group_size(name) >= count {
                return true;
            }
            if waited >= timeout {
                return false;
            }
            tokio::time::sleep(step).await;
            waited += step;
        }
    }

    /// Best-effort barrier that yields the scheduler so already-queued
    /// publish/dispatch work can drain. **Not** a substitute for
    /// awaiting an actual event: it makes no cross-guest ordering or
    /// delivery guarantee. Prefer [`recv`](Self::recv) /
    /// [`wait_for_subscribers`](Self::wait_for_subscribers) for
    /// assertions.
    pub async fn settle(&self) {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    // --- thin forwarders to the primary guest, for terse tests ---

    /// Publish from the primary guest. Same as
    /// `dots_transport::global::publish`.
    pub fn publish<P: Publishable>(&self, value: &P) {
        self.primary.publish(value);
    }

    /// Subscribe (stream form) on the primary guest. Same as
    /// `dots_transport::global::subscribe_stream`.
    pub fn subscribe_stream<T>(&self) -> Subscription<T>
    where
        T: StructValue + Send + Sync + Clone + 'static + GlobalRegistration,
    {
        self.primary.subscribe_stream::<T>()
    }

    /// A typed cache mirror on the primary guest. Same as
    /// `dots_transport::global::container`.
    pub fn container<T>(&self) -> Container<T>
    where
        T: StructValue + Send + Sync + 'static + GlobalRegistration,
    {
        self.primary.container::<T>()
    }

    // --- expectation helpers, the dots-cpp `EXPECT_DOTS_PUBLISH_*` analogs ---

    /// Publish `obj` from `publisher` (typically a spoof guest standing in
    /// for another client) and block until the *primary* guest observes it,
    /// so a following read on the primary sees the new value. Use this to
    /// seed desired state before driving the code under test — it is the
    /// readable counterpart of "publish, then wait for it to land".
    ///
    /// Subscribes the primary, publishes, and awaits the round-trip. Panics
    /// (failing the test) if the publish is not observed within the default
    /// [`recv`](Self::recv) timeout.
    pub async fn sync_publish<T>(&self, publisher: &Client, obj: &T)
    where
        T: StructValue + Publishable + Clone + Send + Sync + 'static + GlobalRegistration,
    {
        let mut sub = self.subscribe_stream::<T>();
        publisher.publish(obj);
        self.recv(&mut sub)
            .await
            .expect("primary observes published object");
    }

    /// Receive the next event on `sub` and assert it is a *publish* whose
    /// payload matches `expected` on every field `expected` sets (see
    /// [`assert_fields_match`]). Returns the event so a test can make extra
    /// assertions — e.g. on a field whose exact value it doesn't want to pin,
    /// or that must merely be present.
    ///
    /// The Rust analog of dots-cpp's
    /// `EXPECT_DOTS_PUBLISH_AT_SUBSCRIBER(sub, Obj{ … })`.
    pub async fn expect_publish<T>(
        &self,
        sub: &mut Subscription<T>,
        expected: &T,
    ) -> Event<T>
    where
        T: StructValue + Clone + Send + 'static,
    {
        let event = self.recv(sub).await.expect("expected a published event");
        assert_ne!(
            event.header.remove_obj,
            Some(true),
            "expected a publish, got a remove"
        );
        assert_fields_match(&event.transmitted, expected);
        event
    }

    /// Like [`expect_publish`](Self::expect_publish) but asserts the event is
    /// a *removal*. A remove transmits only the type's key fields, so
    /// `expected` need only set those.
    ///
    /// The Rust analog of dots-cpp's
    /// `EXPECT_DOTS_REMOVE_AT_SUBSCRIBER(sub, Obj{ … })`.
    pub async fn expect_remove<T>(
        &self,
        sub: &mut Subscription<T>,
        expected: &T,
    ) -> Event<T>
    where
        T: StructValue + Clone + Send + 'static,
    {
        let event = self.recv(sub).await.expect("expected a removal event");
        assert_eq!(
            event.header.remove_obj,
            Some(true),
            "expected a remove, got a publish"
        );
        assert_fields_match(&event.transmitted, expected);
        event
    }
}

/// Assert `actual` matches `expected` on every property `expected` actually
/// sets. DOTS serializes only set fields, so an `expected` built with
/// `dots!{ … }` that names a subset of fields compares just that subset —
/// fields left unset (`None`) are ignored, so a test states only what it
/// cares about. The comparison is over the dynamic encoding, so it works for
/// any DOTS type without per-type field lists.
///
/// Pairs with [`TestHarness::expect_publish`] /
/// [`TestHarness::expect_remove`], which call this on the received event, but
/// is also usable standalone on any two values of the same DOTS type.
pub fn assert_fields_match<T: StructValue>(actual: &T, expected: &T) {
    let desc = T::type_descriptor();
    let decode = |v: &T| {
        let d = Arc::new(DynamicStructDescriptor::from_static(desc));
        DynamicStruct::decode(d, to_any(v).payload()).expect("dynamic decode")
    };
    let exp = decode(expected);
    let act = decode(actual);
    for (tag, want) in &exp.properties {
        let got = act.properties.iter().find(|(t, _)| t == tag).map(|(_, v)| v);
        assert_eq!(
            got,
            Some(want),
            "{}: field with tag {tag} differs (expected {want:?}, got {got:?})",
            desc.name,
        );
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        let inner = self.inner.get_mut().expect("harness mutex poisoned");
        // Signal every guest to stop so its driver loop returns.
        for guest in &inner.guests {
            guest.exit();
        }
        // Release the global slot so the next harness can install one.
        global::destroy();
        // Drivers are spawned tasks; abort in case `exit` hasn't been
        // observed yet (Drop can't await their graceful completion).
        for handle in &inner.drivers {
            handle.abort();
        }
        // `_guard` drops after this, releasing GLOBAL_TEST_LOCK.
    }
}

/// Builder for [`TestHarness`]. Obtain via [`TestHarness::builder`].
pub struct TestHarnessBuilder {
    host_name: String,
    guest_name: String,
    buffer_size: usize,
    preload: bool,
}

impl Default for TestHarnessBuilder {
    fn default() -> Self {
        Self {
            host_name: "dots-test-host".to_string(),
            guest_name: "dots-test-guest".to_string(),
            buffer_size: DEFAULT_BUFFER_SIZE,
            preload: true,
        }
    }
}

impl TestHarnessBuilder {
    /// Name reported by the in-process broker (default
    /// `dots-test-host`).
    pub fn host_name(mut self, name: impl Into<String>) -> Self {
        self.host_name = name.into();
        self
    }

    /// Name of the primary guest (default `dots-test-guest`).
    pub fn guest_name(mut self, name: impl Into<String>) -> Self {
        self.guest_name = name.into();
        self
    }

    /// Size (bytes) of each direction of the in-memory pipe (default
    /// [`DEFAULT_BUFFER_SIZE`]).
    pub fn buffer_size(mut self, bytes: usize) -> Self {
        self.buffer_size = bytes;
        self
    }

    /// Whether the primary guest runs the cache-preload phase during
    /// EarlySubscribe (default `true`, matching a real `App`).
    pub fn preload(mut self, on: bool) -> Self {
        self.preload = on;
        self
    }

    /// Acquire the process-wide test lock, start the host, connect the
    /// primary guest, and install it as the global.
    pub async fn build(self) -> Result<TestHarness, AppError> {
        let guard = GLOBAL_TEST_LOCK.lock().await;

        let host = HostTransceiver::new(self.host_name);

        let (host_io, guest_io) = tokio::io::duplex(self.buffer_size);
        host.accept(host_io);
        let (primary, driver) =
            connect_over_stream(guest_io, &self.guest_name, None, self.preload, true).await?;
        let handle = tokio::spawn(driver);

        Ok(TestHarness {
            host,
            primary: primary.clone(),
            buffer_size: self.buffer_size,
            inner: Mutex::new(Inner {
                guests: vec![primary],
                drivers: vec![handle],
                next_guest_id: 0,
            }),
            _guard: guard,
        })
    }
}
